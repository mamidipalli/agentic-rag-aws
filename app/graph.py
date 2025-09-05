# app/graph.py
from __future__ import annotations
from typing import TypedDict, List, Dict, Any
from collections import Counter
from langgraph.graph import StateGraph, END

from bedrock import embed_texts, generate_text
from retrieval import retrieve_by_embedding

class QAState(TypedDict, total=False):
    q: str
    k: int
    # knobs
    max_cosine_dist: float
    min_hits: int
    doc_ctx_chunks: int
    coarse_k: int

    # working vars
    query_emb: List[float]
    coarse_hits: List[Dict[str, Any]]
    best_doc: str
    doc_hits: List[Dict[str, Any]]
    best_dist: float
    abstain: bool

    # output
    answer: str
    citations: List[Dict[str, str]]
    debug: Dict[str, Any]

def _embed_node(state: QAState) -> QAState:
    [emb] = embed_texts([state["q"]])
    state["query_emb"] = emb
    # widen first pass a bit
    state["coarse_k"] = max(state.get("k", 6), 12)
    return state

def _retrieve_node(state: QAState) -> QAState:
    state["coarse_hits"] = retrieve_by_embedding(state["query_emb"], k=state["coarse_k"])
    return state

def _select_doc_node(state: QAState) -> QAState:
    hits = state.get("coarse_hits") or []
    if not hits:
        state["abstain"] = True
        state["debug"] = {"reason": "no_hits"}
        return state

    doc_counts = Counter(h["doc_uri"] for h in hits)
    doc_min: Dict[str, float] = {}
    for h in hits:
        d = h["doc_uri"]
        doc_min[d] = min(doc_min.get(d, 1.0), float(h.get("dist", 1.0)))

    best_doc = min(doc_min.keys(), key=lambda d: (doc_min[d], -doc_counts[d]))
    state["best_doc"] = best_doc

    # narrow to chosen doc
    doc_hits = [h for h in hits if h["doc_uri"] == best_doc]
    doc_hits = sorted(doc_hits, key=lambda h: h["dist"])[: max(2, state.get("doc_ctx_chunks", 4))]
    state["doc_hits"] = doc_hits
    state["best_dist"] = min((float(h.get("dist", 1.0)) for h in doc_hits), default=1.0)
    return state

def _gate_node(state: QAState) -> QAState:
    if (
        state.get("best_dist", 1.0) > state.get("max_cosine_dist", 0.65)
        or len(state.get("doc_hits") or []) < state.get("min_hits", 1)
    ):
        state["abstain"] = True
    else:
        state["abstain"] = False
    return state

def _reason_node(state: QAState) -> QAState:
    context = "\n\n".join(h["chunk"].strip() for h in state["doc_hits"])
    prompt = f"""You are a careful assistant. Answer ONLY from the context below.
If the answer is not present, reply exactly: "I don't know."

Context:
{context}

Question: {state['q']}
Answer concisely. Provide a short phrase or sentence.
"""
    answer = (generate_text(prompt) or "").strip()
    state["answer"] = answer
    state["citations"] = [{"doc_uri": state["best_doc"]}]
    state["debug"] = {
        "best_dist": state.get("best_dist"),
        "hits": len(state.get("coarse_hits") or []),
        "unique_docs": len(set(h["doc_uri"] for h in state.get("coarse_hits") or [])),
        "selected_doc": state.get("best_doc"),
        "chunks_used": len(state.get("doc_hits") or []),
    }
    return state

def _abstain_out_node(state: QAState) -> QAState:
    state["answer"] = "I don't know that yet based on the current knowledge base."
    state["citations"] = []
    state.setdefault("debug", {})
    state["debug"].update({
        "best_dist": state.get("best_dist"),
        "hits": len(state.get("coarse_hits") or []),
        "selected_doc": state.get("best_doc"),
    })
    return state

# Build the tiny agent graph
_builder = StateGraph(QAState)
_builder.add_node("embed", _embed_node)
_builder.add_node("retrieve", _retrieve_node)
_builder.add_node("select_doc", _select_doc_node)
_builder.add_node("gate", _gate_node)
_builder.add_node("reason", _reason_node)
_builder.add_node("abstain_out", _abstain_out_node)

_builder.set_entry_point("embed")
_builder.add_edge("embed", "retrieve")
_builder.add_edge("retrieve", "select_doc")
_builder.add_edge("select_doc", "gate")
_builder.add_conditional_edges(
    "gate",
    lambda s: "reason" if not s.get("abstain") else "abstain_out",
    {"reason": "reason", "abstain_out": "abstain_out"},
)
_builder.add_edge("reason", END)
_builder.add_edge("abstain_out", END)

qa_graph = _builder.compile()

def run_qa(q: str, k: int, *, max_cosine_dist: float, min_hits: int, doc_ctx_chunks: int) -> Dict[str, Any]:
    """Convenience wrapper to keep /ask response shape unchanged."""
    state: QAState = {
        "q": q, "k": k,
        "max_cosine_dist": max_cosine_dist,
        "min_hits": min_hits,
        "doc_ctx_chunks": doc_ctx_chunks,
    }
    out = qa_graph.invoke(state)
    return {"answer": out["answer"], "citations": out["citations"], "debug": out.get("debug", {})}
