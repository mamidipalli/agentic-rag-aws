# app/db_init_handler.py
import os
import json
import base64
import logging
import boto3
import psycopg
from functools import lru_cache
from botocore.config import Config
from typing import Dict, Any

# --- logging (no secrets, level via env) ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# --- AWS region & boto config ---
AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
_BOTO_CFG = Config(
    region_name=AWS_REGION,
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=5,
    read_timeout=20,
    user_agent_extra="project-agentic-rag/db-init",
)
_SM = boto3.client("secretsmanager", config=_BOTO_CFG)

@lru_cache(maxsize=2)
def _load_secret(arn: str) -> Dict[str, Any]:
    """Load and parse a JSON secret from Secrets Manager. Caches per ARN."""
    try:
        val = _SM.get_secret_value(SecretId=arn)
    except Exception:
        logger.error("Failed to read secret from Secrets Manager (arn redacted).")
        raise

    secret_str = val.get("SecretString")
    if secret_str is None and "SecretBinary" in val:
        try:
            secret_str = base64.b64decode(val["SecretBinary"]).decode("utf-8")
        except Exception:
            logger.error("Failed to decode binary secret (arn redacted).")
            raise

    try:
        return json.loads(secret_str or "{}")
    except json.JSONDecodeError:
        logger.error("Secret value is not valid JSON (arn redacted).")
        raise

def _conn():
    """Create a DB connection using env + secret. Behavior unchanged."""
    secret = _load_secret(os.environ["PG_SECRET_ARN"])
    host = os.environ["PG_HOST"]
    port = os.environ.get("PG_PORT", "5432")
    db   = os.environ.get("PG_DB", "ragdb")
    user = secret["username"]
    pwd  = secret["password"]
    ssl  = os.environ.get("PG_SSLMODE", "require")

    dsn = (
        f"host={host} port={port} dbname={db} "
        f"user={user} password={pwd} sslmode={ssl} "
        f"application_name=db-init-handler connect_timeout=10"
    )
    return psycopg.connect(dsn)

SCHEMA_SQL = """
-- vector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- docs table (keep content; add preview)
CREATE TABLE IF NOT EXISTS docs (
  id BIGSERIAL PRIMARY KEY,
  doc_uri TEXT,
  content TEXT,
  preview TEXT,
  meta JSONB
);

-- If table existed without preview, add it
ALTER TABLE docs ADD COLUMN IF NOT EXISTS preview TEXT;

-- Backfill preview from content if preview is NULL but content exists
UPDATE docs
   SET preview = LEFT(content, 2000)
 WHERE preview IS NULL
   AND content IS NOT NULL;

-- Make doc_uri unique (use a constraint; guard via catalog check)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conrelid = 'public.docs'::regclass
       AND conname = 'docs_doc_uri_key'
  ) THEN
    -- This will fail only if duplicate doc_uri rows already exist
    ALTER TABLE public.docs
      ADD CONSTRAINT docs_doc_uri_key UNIQUE (doc_uri);
  END IF;
END $$;

-- doc_chunks (embedding dim must match your embed model; Titan v2 = 1024)
CREATE TABLE IF NOT EXISTS doc_chunks (
  id BIGSERIAL PRIMARY KEY,
  doc_id BIGINT REFERENCES docs(id) ON DELETE CASCADE,
  chunk TEXT,
  embedding VECTOR(1024),
  meta JSONB
);

-- feedback (unchanged)
CREATE TABLE IF NOT EXISTS feedback (
  id BIGSERIAL PRIMARY KEY,
  session_id TEXT,
  query TEXT,
  answer TEXT,
  rating INT,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Helpful indexes
-- ANN index: cosine distance, lists=200 (tune per data size)
CREATE INDEX IF NOT EXISTS doc_chunks_embedding_idx
  ON doc_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 200);

-- Metadata filter index
CREATE INDEX IF NOT EXISTS doc_chunks_meta_gin
  ON doc_chunks USING gin (meta);

ANALYZE doc_chunks;
"""

def handler(event, context):
    logger.info("Applying database schema (idempotent).")
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            conn.commit()
        logger.info("Schema applied successfully.")
        return {"PhysicalResourceId": "DbInitOnce", "Data": {"status": "applied"}}
    except Exception:
        logger.error("Failed to apply schema.", exc_info=True)
        raise
