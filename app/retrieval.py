# app/retrieval.py
# -----------------------------------------------------------------------------
# Retrieval & storage helpers for pgvector-backed RAG.
# - Keeps your current docs schema (doc_uri, preview, meta).
# - Adds distance to retrieval results for confidence gating in /ask.
# -----------------------------------------------------------------------------

from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from typing import Any, Dict, List

import boto3
import psycopg
from botocore.config import Config
from pgvector.psycopg import register_vector  # enables binding Python lists as vectors

# --- logging ------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.getLogger().setLevel(LOG_LEVEL)
log = logging.getLogger(__name__)

# --- AWS clients / region -----------------------------------------------------
AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
_BOTO_CFG = Config(
    region_name=AWS_REGION,
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=5,
    read_timeout=20,
    user_agent_extra="project-agentic-rag/retrieval",
)
_SM = boto3.client("secretsmanager", config=_BOTO_CFG)

# --- secrets & connection -----------------------------------------------------


@lru_cache(maxsize=2)
def _load_secret(arn: str) -> Dict[str, str]:
    """Fetch and parse a Secrets Manager JSON secret. Caches per ARN in-container."""
    try:
        val = _SM.get_secret_value(SecretId=arn)
        return json.loads(val.get("SecretString") or "{}")
    except Exception:
        log.error("Failed to read or parse DB secret (arn redacted).")
        raise


def _conn() -> psycopg.Connection:
    """Open a PostgreSQL connection and register pgvector adapters."""
    cfg = _load_secret(os.environ["PG_SECRET_ARN"])
    try:
        conn = psycopg.connect(
            host=os.environ.get("PG_HOST"),
            port=int(os.environ.get("PG_PORT", "5432")),
            dbname=os.environ.get("PG_DB"),
            user=cfg["username"],
            password=cfg["password"],
            sslmode=os.environ.get("PG_SSLMODE", "require"),
            connect_timeout=10,
            application_name="retrieval",
        )
    except Exception:
        log.error("Failed to connect to Postgres (host/db redacted).")
        raise

    # Important for pgvector parameter binding (list[float] -> vector)
    register_vector(conn)
    return conn


# --- write path: used by ingest ----------------------------------------------


def upsert_doc_and_chunks(
    doc_uri: str,
    preview: str,
    chunks: List[str],
    embeddings: List[List[float]],
    meta: Dict[str, Any],
) -> int:
    """
    Idempotently upsert a document and replace its chunks.
    Returns number of chunks written.
    Assumes a unique index exists on docs.doc_uri.
    """
    if len(chunks) != len(embeddings):
        raise ValueError("chunks and embeddings length mismatch")

    with _conn() as conn, conn.cursor() as cur:
        # Upsert doc row
        cur.execute(
            """
            INSERT INTO docs (doc_uri, preview, meta)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (doc_uri)
              DO UPDATE SET preview = EXCLUDED.preview,
                            meta    = EXCLUDED.meta
            RETURNING id
            """,
            [doc_uri, preview, json.dumps(meta)],
        )
        (doc_id,) = cur.fetchone()

        # Replace existing chunks for this doc
        cur.execute("DELETE FROM doc_chunks WHERE doc_id = %s", [doc_id])

        # Insert chunks
        inserted = 0
        for ch, emb in zip(chunks, embeddings):
            cur.execute(
                """
                INSERT INTO doc_chunks (doc_id, chunk, embedding, meta)
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                [doc_id, ch, emb, json.dumps(meta)],
            )
            inserted += 1

        conn.commit()
        return inserted


# --- read path: used by /ask --------------------------------------------------


def retrieve_by_embedding(
    query_emb: List[float],
    k: int = 6,
    filters: Dict[str, Any] | None = None
) -> List[Dict[str, Any]]:
    """
    KNN over pgvector with optional metadata filters on c.meta.
    Returns: [{doc_uri, chunk, meta, dist}] where dist is cosine distance
    (smaller is closer).
    """
    filters = filters or {}
    where, params = [], []
    for key, val in filters.items():
        # JSONB -> text comparison on chunk metadata
        where.append("(c.meta->>%s) = %s")
        params.extend([str(key), str(val)])

    wsql = ("WHERE " + " AND ".join(where)) if where else ""

    # Put the query embedding in a CTE so it's always $1.
    sql = f"""
        WITH q AS (SELECT %s::vector AS emb)
        SELECT
            d.doc_uri,
            c.chunk,
            c.meta,
            (c.embedding <=> (SELECT emb FROM q)) AS dist  -- cosine distance
        FROM doc_chunks c
        JOIN docs d ON d.id = c.doc_id
        {wsql}
        ORDER BY dist
        LIMIT %s
    """

    # $1 = embedding, then any filter params, then LIMIT
    exec_params = [query_emb] + params + [k]

    out: List[Dict[str, Any]] = []
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, exec_params)
        for doc_uri, chunk, meta, dist in cur.fetchall():
            out.append({"doc_uri": doc_uri, "chunk": chunk, "meta": meta, "dist": float(dist)})
    return out


def retrieve_top_for_doc_uri(
    query_emb: List[float],
    doc_uri: str,
    m: int = 8,
) -> List[Dict[str, Any]]:
    """
    Re-rank chunks but *only* within a single doc (by doc_uri), using vector distance.
    Returns rows with 'doc_uri', 'chunk', 'meta', 'dist'.
    """
    sql = """
        SELECT d.doc_uri, c.chunk, c.meta, (c.embedding <=> %s::vector) AS dist
          FROM doc_chunks c
          JOIN docs d ON d.id = c.doc_id
         WHERE d.doc_uri = %s
         ORDER BY c.embedding <=> %s::vector
         LIMIT %s
    """
    out: List[Dict[str, Any]] = []
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, [query_emb, doc_uri, query_emb, m])
        for doc_uri, chunk, meta, dist in cur.fetchall():
            out.append({"doc_uri": doc_uri, "chunk": chunk, "meta": meta, "dist": float(dist)})
    return out
