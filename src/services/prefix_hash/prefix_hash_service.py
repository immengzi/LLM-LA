#!/usr/bin/env python3
# prefix_hash_service.py
"""
Small HTTP service that computes vLLM KV block hashes for prompts.

- Uses BlockHashComputer from prefix_hash_estimation.py
- Intended to run in a CPU-only vLLM image
- Example startup:
    python prefix_hash_service.py \
        --model-path /model \
        --host 0.0.0.0 \
        --port 9095 \
        --block-size 128
"""

import os
import argparse
from typing import List, Optional

os.environ.setdefault("PYTHONHASHSEED", "0")

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from prefix_hash_estimation import BlockHashComputer


# ------------------------------
# CLI / config parsing
# ------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="vLLM KV block hash service (CPU)"
    )
    parser.add_argument(
        "--model-path",
        type=str,
        required=True,
        help="Path to model/tokenizer (e.g. /model)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind HTTP server on",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9095,
        help="Port to bind HTTP server on",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=128,
        help="KV block size (must match vLLM server)",
    )
    parser.add_argument(
        "--eos-token-id",
        type=int,
        default=151643,
        help="EOS token id (Qwen default shown; must match model config)",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=32,
        help="Max tokens for sampling params (does not affect hashing much)",
    )
    return parser.parse_args()


# We parse args once at startup so we can initialize the hash computer.
_ARGS = parse_args()

# Initialize global BlockHashComputer (tokenizer + block_hasher reused)
_HASH_COMPUTER = BlockHashComputer(
    model_path=_ARGS.model_path,
    block_size=_ARGS.block_size,
    eos_token_id=_ARGS.eos_token_id,
)

# ------------------------------
# FastAPI models
# ------------------------------

class HashRequest(BaseModel):
    prompt: str
    block_size: Optional[int] = None  # optional override; must match server
    max_tokens: Optional[int] = None


class HashResponse(BaseModel):
    block_hashes: List[int]
    token_ids: List[int]
    num_blocks: int
    num_tokens: int
    block_size: int
    model_path: str


# ------------------------------
# FastAPI app
# ------------------------------

app = FastAPI(title="vLLM KV Block Hash Service", version="0.1.0")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/compute_hashes", response_model=HashResponse)
async def compute_hashes(req: HashRequest) -> HashResponse:
    if not req.prompt:
        raise HTTPException(status_code=400, detail="Prompt must not be empty")

    # Optional per-request override for block_size / max_tokens
    block_size = req.block_size or _ARGS.block_size
    max_tokens = req.max_tokens or _ARGS.max_tokens

    # If block size override is different from the initialized one, that's
    # currently unsupported (we could extend to handle multiple instances).
    if block_size != _ARGS.block_size:
        raise HTTPException(
            status_code=400,
            detail=f"Requested block_size={block_size} "
                   f"but service is configured with block_size={_ARGS.block_size}",
        )

    # Compute hashes via BlockHashComputer
    block_hashes, token_ids = _HASH_COMPUTER.compute(
        prompt_text=req.prompt,
        max_tokens=max_tokens,
    )

    return HashResponse(
        block_hashes=block_hashes,
        token_ids=token_ids,
        num_blocks=len(block_hashes),
        num_tokens=len(token_ids),
        block_size=block_size,
        model_path=_ARGS.model_path,
    )


# ------------------------------
# Uvicorn entrypoint
# ------------------------------

def main() -> None:
    import uvicorn

    uvicorn.run(
        app,
        host=_ARGS.host,
        port=_ARGS.port,
        # You can tune workers here if needed; 1 is fine initially
        workers=1,
    )


if __name__ == "__main__":
    main()
