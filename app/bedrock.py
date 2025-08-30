# app/bedrock.py
import os
import json
import boto3
from botocore.config import Config
from typing import List, Sequence, Dict, Any

# Region & model IDs (CDK sets these as env vars)
AWS_REGION     = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-south-1"
EMBED_MODEL_ID = os.getenv("EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")
TEXT_MODEL_ID  = os.getenv("TEXT_MODEL_ID",  "anthropic.claude-3-5-sonnet-20240620-v1:0")

# Constants
EMBED_TEXT_MAX_CHARS = 8000

# Singleton Bedrock Runtime client (with sane timeouts/retries)
_config = Config(
    region_name=AWS_REGION,
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=5,
    read_timeout=30,
    user_agent_extra="project-agentic-rag/bedrock-wrapper",
)
_brt = boto3.client("bedrock-runtime", config=_config)
_bedrock = _brt  # back-compat alias if other code elsewhere imported _bedrock (do not use both here)

def _invoke(model_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Invoke a Bedrock model and return parsed JSON."""
    resp = _brt.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload).encode("utf-8"),
    )
    raw = resp["body"].read()
    return json.loads(raw)

def embed_texts(texts: Sequence[str]) -> List[List[float]]:
    """
    Amazon Titan Text Embeddings v2.
    Request:  {"inputText": "..."}
    Response: {"embedding": [float, ...]}
    """
    out: List[List[float]] = []
    for t in texts:
        payload = {"inputText": t[:EMBED_TEXT_MAX_CHARS]}
        data = _invoke(EMBED_MODEL_ID, payload)
        emb = data.get("embedding") or data.get("vector") or data.get("Embeddings")
        if emb is None:
            keys_preview = list(data.keys())[:5] if isinstance(data, dict) else type(data).__name__
            raise RuntimeError(f"Unexpected embedding response from {EMBED_MODEL_ID}; keys={keys_preview}")
        out.append(emb)
    return out

def generate_text(prompt: str, max_tokens: int = 512, temperature: float = 0.2) -> str:
    """
    Minimal wrapper for Bedrock text generation.

    Supports:
      - Anthropic Claude 3/3.5  (model_id startswith "anthropic.")
      - Amazon Titan Text       (model_id startswith "amazon.titan-text")
      - Meta Llama 3 / 3.1      (model_id startswith "meta.llama3")
    """
    model_id = TEXT_MODEL_ID

    if model_id.startswith("anthropic."):
        payload: Dict[str, Any] = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": prompt}]}
            ],
        }
        body = _invoke(model_id, payload)
        content = body.get("content") or []
        text_join = "".join(part.get("text", "") for part in content if isinstance(part, dict))
        return text_join or body.get("output_text", "") or ""

    elif model_id.startswith("amazon.titan-text"):
        payload = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": max_tokens,
                "temperature": temperature,
                "topP": 0.9,
            },
        }
        body = _invoke(model_id, payload)
        results = body.get("results") or []
        if results and isinstance(results, list) and isinstance(results[0], dict):
            return results[0].get("outputText", "") or ""
        raise RuntimeError(f"Unexpected Titan text response from {model_id}; keys={list(body.keys())[:5]}")

    elif model_id.startswith("meta.llama3"):
        # Bedrock Llama 3/3.1 Instruct expects a chat-formatted "prompt"
        # with special tokens. This is the simplest safe format.
        chat_prompt = (
            "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n"
            f"{prompt}\n"
            "<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n"
        )
        payload: Dict[str, Any] = {
            "prompt": chat_prompt,
            "max_gen_len": max_tokens,
            "temperature": temperature,
            "top_p": 0.9,
        }
        body = _invoke(model_id, payload)
        # Llama on Bedrock may return either `generation` or a `generations` list.
        if isinstance(body, dict):
            if "generation" in body:
                return body["generation"]
            gens = body.get("generations") or body.get("outputs")
            if gens and isinstance(gens, list):
                first = gens[0]
                # try common fields
                return first.get("text") or first.get("generation") or first.get("output_text", "")
        # last resort
        return str(body)

    else:
        raise ValueError(f"Unsupported TEXT_MODEL_ID for generate_text: {model_id}")
