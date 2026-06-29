# -*- coding: utf-8 -*-
"""In-container KV-block hash service for the Go gateway (inline mode).

This is a thin HTTP wrapper around the router's `prefix_hash.py` so the Go
gateway produces byte-identical block hashes to the Python router. It is bound
to 127.0.0.1 inside the gateway container and is only started when
KV_HASH_SOURCE=inline (see entrypoint.sh).

`prefix_hash.py` is the single source of truth; it is staged into this directory
at image-build time from src/services/router_service/router/prefix_hash.py.
"""

import os
from typing import Any, Dict, List, Optional

# prefix_hash computes NONE_HASH from this at import time; set before importing.
os.environ.setdefault("PYTHONHASHSEED", "0")

from fastapi import FastAPI
from pydantic import BaseModel

import prefix_hash as ph

app = FastAPI()

_BLOCK_SIZE = int(os.getenv("KV_BLOCK_SIZE", "128"))
_TOKENIZER_PATH = os.getenv("KV_TOKENIZER_PATH", "/model")


@app.on_event("startup")
def _startup() -> None:
    ph.init_tokenizer(_TOKENIZER_PATH)


class HashRequest(BaseModel):
    prompt: Optional[str] = None
    messages: Optional[List[Dict[str, Any]]] = None
    tools: Optional[List[Any]] = None
    block_size: Optional[int] = None


class HashResponse(BaseModel):
    block_hashes: List[int]


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/compute_hashes", response_model=HashResponse)
def compute_hashes(req: HashRequest) -> HashResponse:
    block_size = int(req.block_size or _BLOCK_SIZE)
    messages = req.messages
    block_hashes = ph.compute_request_block_hashes_int(
        messages=messages,
        prompt=req.prompt if not messages else None,
        tools=req.tools,
        block_size=block_size,
    )
    return HashResponse(block_hashes=block_hashes)
