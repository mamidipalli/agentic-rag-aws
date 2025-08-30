# app/main.py
import os
import re
import logging
from collections import Counter
from typing import Dict, Any, List

from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from bedrock import embed_texts, generate_text
from retrieval import retrieve_by_embedding, _conn

# --- logging ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.getLogger().setLevel(LOG_LEVEL)
log = logging.getLogger(__name__)

app = FastAPI()

@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}

@app.post("/ask")
def ask(payload: Dict[str, Any] = Body(...)):
    try:
        q = str(payload.get("q", "")).strip()
        k = int(payload.get("k", 6))
        if not q:
            return JSONResponse({"error": "Missing 'q' in body"}, status_code=400)

        # knobs
        max_cosine_dist = float(os.getenv("MAX_COSINE_DIST", "0.65"))  # smaller = closer; use 0.6–0.7 to start
        min_hits        = int(os.getenv("MIN_CTX_HITS", "1"))
        coarse_k        = max(k, 12)       # widen the first pass a bit
        doc_ctx_chunks  = int(os.getenv("DOC_CTX_CHUNKS", "4"))

        # 1) embed query
        [emb] = embed_texts([q])

        # 2) coarse retrieval across all docs
        coarse_hits = retrieve_by_embedding(emb, k=coarse_k)

        if not coarse_hits:
            return JSONResponse({
                "answer": "I don't know that yet based on the current knowledge base.",
                "citations": [], "abstained": True,
                "debug": {"reason": "no_hits"}
            }, status_code=200)

        # pick doc URI with the most votes (ties → best distance wins)
        doc_counts = Counter(h["doc_uri"] for h in coarse_hits)
        doc_min: Dict[str, float] = {}
        for h in coarse_hits:
            d = h["doc_uri"]
            doc_min[d] = min(doc_min.get(d, 1.0), h.get("dist", 1.0))
        # break ties by min distance (then by most votes)
        best_doc = min(doc_min.keys(), key=lambda d: (doc_min[d], -doc_counts[d]))

        # 3) narrow: only chunks from selected doc
        doc_hits = [h for h in coarse_hits if h["doc_uri"] == best_doc]
        doc_hits = sorted(doc_hits, key=lambda h: h["dist"])[:max(2, doc_ctx_chunks)]

        best_dist = min((h.get("dist", 1.0) for h in doc_hits), default=1.0)
        if best_dist > max_cosine_dist or len(doc_hits) < min_hits:
            return JSONResponse({
                "answer": "I don't know that yet based on the current knowledge base.",
                "citations": [],
                "abstained": True,
                "debug": {
                    "best_dist": best_dist,
                    "hits": len(coarse_hits),
                    "unique_docs": len(doc_counts),
                    "selected_doc": best_doc
                }
            }, status_code=200)

        # 4) build a small, clean context from the chosen doc
        context = "\n\n".join(h["chunk"].strip() for h in doc_hits)

        prompt = f"""You are a careful assistant. Answer ONLY from the context below.
If the answer is not present, reply exactly: "I don't know."

Context:
{context}

Question: {q}
Answer concisely. Provide a short phrase or sentence.
"""

        answer = (generate_text(prompt) or "").strip()

        # 5) single, precise citation
        citations = [{"doc_uri": best_doc}]
        return {
            "answer": answer,
            "citations": citations,
            "debug": {
                "best_dist": best_dist,
                "hits": len(coarse_hits),
                "unique_docs": len(doc_counts),
                "selected_doc": best_doc,
                "chunks_used": len(doc_hits)
            }
        }

    except Exception as e:
        log.exception("ASK_ERROR")
        return JSONResponse({"detail": str(e)}, status_code=500)

class Feedback(BaseModel):
    session_id: str
    query: str
    answer: str
    rating: int  # -1,0,1
    notes: str | None = None

@app.post("/feedback")
def feedback(fb: Feedback):
    sql = """
      INSERT INTO feedback (session_id, query, answer, rating, notes)
      VALUES (%s, %s, %s, %s, %s)
      RETURNING id, created_at
    """
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (fb.session_id, fb.query, fb.answer, fb.rating, fb.notes))
        row = cur.fetchone()
    return {"ok": True, "id": row[0], "created_at": row[1].isoformat()}
