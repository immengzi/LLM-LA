# -*- coding: utf-8 -*-
"""In-container KV-block hash service for the Go gateway (inline mode).

This is a thin HTTP wrapper around the router's `prefix_hash.py` so the Go
gateway produces byte-identical block hashes to the Python router. It is bound
to 127.0.0.1 inside the gateway container and is only started when
KV_HASH_SOURCE=inline (see entrypoint.sh).

`prefix_hash.py` is the single source of truth; it is staged into this directory
at image-build time from src/core/services/router_service/router/prefix_hash.py.
"""

import os
from typing import Any, Dict, List, Optional

# prefix_hash computes NONE_HASH from this at import time; set before importing.
os.environ.setdefault("PYTHONHASHSEED", "0")

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from router import prefix_hash as ph

app = FastAPI()

_BLOCK_SIZE = int(os.getenv("KV_BLOCK_SIZE", "128"))
_TOKENIZER_PATH = os.getenv("KV_TOKENIZER_PATH", "/model")
_SGLANG_CONTRACT_VERSION = "0.5.15"


def _validated_backend(value: Optional[str], *, fallback: Optional[str] = None) -> str:
    backend = str(value or "").strip().lower()
    if backend in ("vllm", "sglang"):
        return backend
    if fallback is not None:
        return fallback
    raise HTTPException(
        status_code=400, detail=f"unsupported KV hash backend: {value!r}"
    )


_BACKEND = _validated_backend(os.getenv("KV_HASH_BACKEND", "vllm"), fallback="vllm")
_CONTRACT_VERSION = os.getenv("SGLANG_CONTRACT_VERSION", "").strip()


@app.on_event("startup")
def _startup() -> None:
    ph.init_tokenizer(_TOKENIZER_PATH)


class HashRequest(BaseModel):
    prompt: Optional[str] = None
    messages: Optional[List[Dict[str, Any]]] = None
    tools: Optional[List[Any]] = None
    block_size: Optional[int] = None
    backend: Optional[str] = None


class HashResponse(BaseModel):
    block_hashes: List[int]


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "backend": _BACKEND,
        "sglang_contract_version": _CONTRACT_VERSION or "unset",
        "sglang_expected_contract_version": _SGLANG_CONTRACT_VERSION,
    }


@app.post("/compute_hashes", response_model=HashResponse)
def compute_hashes(req: HashRequest) -> HashResponse:
    backend = _BACKEND if req.backend is None else _validated_backend(req.backend)
    if backend == "sglang" and _CONTRACT_VERSION != _SGLANG_CONTRACT_VERSION:
        raise HTTPException(
            status_code=409,
            detail=(
                "sglang_contract_version_mismatch:"
                f"expected={_SGLANG_CONTRACT_VERSION},"
                f"actual={_CONTRACT_VERSION or 'unset'}"
            ),
        )
    block_size = int(_BLOCK_SIZE if req.block_size is None else req.block_size)
    messages = req.messages
    block_hashes = ph.compute_request_block_hashes_int(
        messages=messages,
        prompt=req.prompt if not messages else None,
        tools=req.tools,
        block_size=block_size,
        backend=backend,
    )
    return HashResponse(block_hashes=block_hashes)
