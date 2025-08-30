# app/ingest_handler.py
import os
import io
import re
import json
import base64
import logging
import urllib.parse
from typing import Tuple, Dict, Any, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from pypdf import PdfReader

from bedrock import embed_texts
from retrieval import upsert_doc_and_chunks

# --- logging ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.getLogger().setLevel(LOG_LEVEL)
log = logging.getLogger(__name__)

# --- AWS clients with sane defaults ---
AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
_BOTO_CFG = Config(
    region_name=AWS_REGION,
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=5,
    read_timeout=30,
    user_agent_extra="project-agentic-rag/ingest",
)
s3 = boto3.client("s3", config=_BOTO_CFG)

# --- defaults for chunking ---
DEFAULT_CHUNK_SIZE = 900
DEFAULT_CHUNK_OVERLAP = 150


# ---------------------------
# Content readers & chunking
# ---------------------------
def _read_object(bucket: str, key: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Returns (text, meta) or (None, None) if unsupported or unreadable.
    meta includes: etag, size, content_type
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        log.exception("Failed to get s3://%s/%s", bucket, key)
        return None, None

    body_stream = obj["Body"]
    try:
        body: bytes = body_stream.read()
    finally:
        try:
            body_stream.close()
        except Exception:
            pass

    etag = obj.get("ETag", "").strip('"')
    size = obj.get("ContentLength", 0)
    ctype = obj.get("ContentType", "application/octet-stream")
    k = key.lower()

    # txt / md
    if k.endswith((".txt", ".md")):
        try:
            return body.decode("utf-8", "ignore"), {"etag": etag, "size": size, "content_type": ctype}
        except Exception:
            return None, None

    # html
    if k.endswith((".html", ".htm")):
        try:
            soup = BeautifulSoup(body, "html.parser")
            for t in soup(["script", "style", "noscript"]):
                t.extract()
            text = "\n".join(
                el.get_text(" ", strip=True)
                for el in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "th", "td"])
            )
            return text, {"etag": etag, "size": size, "content_type": ctype}
        except Exception:
            log.exception("Failed to parse HTML for s3://%s/%s", bucket, key)
            return None, None

    # pdf
    if k.endswith(".pdf"):
        try:
            r = PdfReader(io.BytesIO(body))
            text = "\n".join((p.extract_text() or "") for p in r.pages)
            return text, {"etag": etag, "size": size, "content_type": ctype}
        except Exception:
            log.exception("Failed to read PDF for s3://%s/%s", bucket, key)
            return None, None

    # default: try utf-8 as last resort
    try:
        return body.decode("utf-8", "ignore"), {"etag": etag, "size": size, "content_type": ctype}
    except Exception:
        return None, None


def _chunk(text: str, size: int, overlap: int) -> List[str]:
    t = re.sub(r"\s+", " ", text or "").strip()
    if not t:
        return []
    out: List[str] = []
    i = 0
    step = max(1, size - overlap)
    while i < len(t):
        out.append(t[i : i + size])
        i += step
    return out


# ---------------------------
# Core ingest helpers
# ---------------------------
def process_one_s3_object(
    bucket: str,
    key: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    source: str = "corp",
) -> Dict[str, Any]:
    text, meta = _read_object(bucket, key)
    if not text:
        log.info("Skipping unsupported or empty file: s3://%s/%s", bucket, key)
        return {"skipped": True, "reason": "unsupported"}

    chunks = _chunk(text, chunk_size, chunk_overlap)
    if not chunks:
        return {"skipped": True, "reason": "empty"}

    embs = embed_texts(chunks)
    meta = {**(meta or {}), "source": source, "s3_key": key, "bucket": bucket}
    upsert_doc_and_chunks(f"s3://{bucket}/{key}", text[:2000], chunks, embs, meta)
    return {"ok": True, "chunks": len(chunks)}


def reconcile_and_ingest(
    bucket: str,
    prefix: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    source: str = "corp",
) -> int:
    """
    Walk S3 under prefix and (re)ingest everything.
    DB layer can dedupe by doc_uri/meta if needed.
    """
    count = 0
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith("/"):
                    continue
                try:
                    r = process_one_s3_object(bucket, key, chunk_size, chunk_overlap, source)
                    if r.get("ok"):
                        count += 1
                except Exception:
                    log.exception("Failed reconcile for s3://%s/%s", bucket, key)
    except ClientError:
        log.exception("Failed to list s3://%s/%s", bucket, prefix)
    return count


# ---------------------------
# Event parsing for admin path
# ---------------------------
def _parse_admin_body(event: dict) -> Tuple[Dict[str, Any], bool]:
    """
    Accepts:
      - API Gateway v2: event['body'] (optionally base64)
      - Direct invoke: event has fields directly
    Returns (body_dict, apigw_proxy_flag)
    """
    apigw_proxy = False
    body: Dict[str, Any] = {}

    try:
        if isinstance(event, dict) and "body" in event:
            apigw_proxy = True
            raw = event.get("body") or "{}"
            if event.get("isBase64Encoded"):
                raw = base64.b64decode(raw).decode()
            body = json.loads(raw)
        else:
            # assume direct invoke with dict payload already
            body = dict(event or {})
    except Exception:
        body = {}

    return body, apigw_proxy


# ---------------------------
# Lambda entrypoint
# ---------------------------
def handler(event, context):
    # 1) SQS batch from S3 notifications (auto-ingest)
    if isinstance(event, dict) and "Records" in event and event["Records"]:
        # Distinguish SQS vs S3 direct events; we expect SQS here.
        if event["Records"][0].get("eventSource") == "aws:sqs":
            failures = []
            processed = 0
            for rec in event["Records"]:
                msg_id = rec.get("messageId", "unknown")
                try:
                    payload = json.loads(rec["body"])
                    # payload is the original S3 event (might contain multiple Records)
                    for r in payload.get("Records", []):
                        if r.get("eventSource") != "aws:s3":
                            continue
                        b = r["s3"]["bucket"]["name"]
                        k = urllib.parse.unquote_plus(r["s3"]["object"]["key"])
                        # Only object created events
                        if not str(r.get("eventName", "")).startswith("ObjectCreated:"):
                            continue
                        if k.endswith("/"):
                            continue
                        process_one_s3_object(b, k)
                        processed += 1
                except Exception:
                    log.exception("Error processing SQS messageId=%s", msg_id)
                    failures.append({"itemIdentifier": msg_id})

            # Partial batch failure contract for SQS
            return {"batchItemFailures": failures, "ok": True, "processed": processed}

    # 2) EventBridge nightly backfill
    if isinstance(event, dict) and event.get("mode") == "scan_prefix":
        bucket = event.get("bucket") or os.environ.get("DOCS_BUCKET", "")
        prefix = event.get("prefix") or os.environ.get("DEFAULT_PREFIX", "")
        size = int(event.get("chunk_size", DEFAULT_CHUNK_SIZE))
        overlap = int(event.get("chunk_overlap", DEFAULT_CHUNK_OVERLAP))
        if not bucket:
            return {"ok": False, "error": "DOCS_BUCKET not set"}
        count = reconcile_and_ingest(bucket, prefix, size, overlap)
        return {"ok": True, "ingested_files": count}

    # 3) Admin API (/admin/ingest → invokes this Lambda)
    body, apigw_proxy = _parse_admin_body(event)
    bucket = body.get("s3_bucket") or os.environ.get("DOCS_BUCKET", "")
    prefix = body.get("s3_prefix") or os.environ.get("DEFAULT_PREFIX", "")
    size = int(body.get("chunk_size", DEFAULT_CHUNK_SIZE))
    overlap = int(body.get("chunk_overlap", DEFAULT_CHUNK_OVERLAP))

    if not bucket:
        resp = {"ok": False, "error": "s3_bucket required"}
        if apigw_proxy:
            return {"statusCode": 400, "headers": {"content-type": "application/json"}, "body": json.dumps(resp)}
        return resp

    count = reconcile_and_ingest(bucket, prefix, size, overlap)

    resp = {"ok": True, "ingested_files": count}
    if apigw_proxy:
        return {"statusCode": 200, "headers": {"content-type": "application/json"}, "body": json.dumps(resp)}
    return resp
