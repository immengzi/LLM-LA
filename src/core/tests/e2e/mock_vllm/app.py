"""Mock vLLM OpenAI-compatible server for end-to-end tests.

Implements just enough of the vLLM HTTP surface that the sidecar exercises:

  * GET  /health              -> 200 (used by the sidecar health gate)
  * POST /v1/chat/completions -> OpenAI ChatCompletion (streaming + non-stream)

The response content is deterministic ("echo: <last user message>") so the
e2e tests can assert the full router -> sidecar -> vLLM -> router round-trip
without needing a real model or GPU.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI(title="mock-vllm")


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


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    model = body.get("model", "served-model")
    messages = body.get("messages", [])
    text = f"echo: {_last_user_text(messages)}"
    stream = bool(body.get("stream", False))

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
