"""Mock vLLM OpenAI-compatible server for end-to-end tests.

Implements just enough of the vLLM HTTP surface that the sidecar exercises:

  * GET  /health              -> 200 (used by the sidecar health gate)
  * GET  /metrics             -> Prometheus text (GPU KV usage for soft divert)
  * POST /v1/chat/completions -> OpenAI ChatCompletion (streaming + non-stream)

The response content is deterministic ("echo: <last user message>") so the
e2e tests can assert the full router -> sidecar -> vLLM -> router round-trip
without needing a real model or GPU.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, PlainTextResponse, JSONResponse

app = FastAPI(title="mock-vllm")

# Emulated model context window. A request whose (input + output) tokens exceed
# this returns vLLM's real 400 (OpenAI-compatible) so the e2e stack can
# reproduce the context-length overflow incident end to end.
MAX_MODEL_LEN = int(os.environ.get("MOCK_MAX_MODEL_LEN", "196608"))


def _estimate_input_tokens(body: Dict[str, Any], messages: List[Dict[str, Any]]) -> int:
    """Approximate prompt tokens. A test may pass an explicit ``mock_input_tokens``
    hint to reproduce exact incident numbers without shipping a huge prompt; real
    vLLM ignores unknown fields, and production never sends this."""
    hint = body.get("mock_input_tokens")
    if isinstance(hint, int) and hint > 0:
        return hint
    total_chars = 0
    for m in messages or []:
        c = m.get("content")
        if isinstance(c, str):
            total_chars += len(c)
        elif isinstance(c, list):
            total_chars += sum(len(p.get("text", "")) for p in c if isinstance(p, dict))
    return max(1, total_chars // 4)


def _last_user_text(messages: List[Dict[str, Any]]) -> str:
    for msg in reversed(messages or []):
        if msg.get("role") == "user":
            content = msg.get("content")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                # OpenAI "content parts" form.
                parts = [p.get("text", "") for p in content if isinstance(p, dict)]
                return "".join(parts)
    return ""


def _make_completion(model: str, text: str) -> Dict[str, Any]:
    prompt_tokens = 8
    completion_tokens = max(1, len(text.split()))
    return {
        "id": f"chatcmpl-mock-{int(time.time()*1000)}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    # Static dual-engine KV gauges so sidecar KV_USAGE_REPORT can scrape.
    body = (
        "# HELP vllm:kv_cache_usage_perc Fraction of GPU KV cache used\n"
        "# TYPE vllm:kv_cache_usage_perc gauge\n"
        'vllm:kv_cache_usage_perc{engine="0",model_name="served-model"} 0.42\n'
        'vllm:kv_cache_usage_perc{engine="1",model_name="served-model"} 0.58\n'
    )
    return PlainTextResponse(body, media_type="text/plain; version=0.0.4")


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    model = body.get("model", "served-model")
    messages = body.get("messages", [])
    text = f"echo: {_last_user_text(messages)}"
    stream = bool(body.get("stream", False))

    # Context-length overflow -> legitimate vLLM 400 (OpenAI-compatible body).
    requested_output = int(body.get("max_tokens") or 16)
    input_tokens = _estimate_input_tokens(body, messages)
    total_tokens = input_tokens + requested_output
    if total_tokens > MAX_MODEL_LEN:
        msg = (
            f"This model's maximum context length is {MAX_MODEL_LEN} tokens. "
            f"However, you requested {requested_output} output tokens and your "
            f"prompt contains at least {input_tokens} input tokens, for a total "
            f"of at least {total_tokens} tokens. Please reduce the length of the "
            f"input prompt or the number of requested output tokens."
        )
        return JSONResponse(
            status_code=400,
            content={
                "error": {
                    "message": msg,
                    "type": "BadRequestError",
                    "param": "input_tokens",
                    "code": 400,
                }
            },
        )

    if not stream:
        return _make_completion(model, text)

    def _sse() -> Any:
        created = int(time.time())
        cid = f"chatcmpl-mock-{created}"
        base = {"id": cid, "object": "chat.completion.chunk", "created": created, "model": model}

        first = dict(base)
        first["choices"] = [{"index": 0, "delta": {"role": "assistant", "content": text}, "finish_reason": None}]
        yield f"data: {json.dumps(first)}\n\n"

        final = dict(base)
        final["choices"] = [{"index": 0, "delta": {}, "finish_reason": "stop"}]
        final["usage"] = _make_completion(model, text)["usage"]
        yield f"data: {json.dumps(final)}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(_sse(), media_type="text/event-stream")
