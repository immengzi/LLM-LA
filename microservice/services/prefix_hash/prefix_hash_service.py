#!/usr/bin/env python3
# prefix_hash_service.py
"""
Small HTTP service that computes vLLM KV block hashes for prompts or chat messages.

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
from typing import List, Optional, Dict, Any

os.environ.setdefault("PYTHONHASHSEED", "0")

import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from prefix_hash_estimation import BlockHashComputer, HASH_IMPL_NAME

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logger = logging.getLogger("prefix_hash_service")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


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

logger.info(
    "[hash-service] Starting with model_path=%s, block_size=%d, eos_token_id=%d, max_tokens=%d",
    _ARGS.model_path,
    _ARGS.block_size,
    _ARGS.eos_token_id,
    _ARGS.max_tokens,
)
logger.info("[hash-service] Hash implementation: %s", HASH_IMPL_NAME)

# Initialize global BlockHashComputer (tokenizer + block_hasher reused)
_HASH_COMPUTER = BlockHashComputer(
    model_path=_ARGS.model_path,
    block_size=_ARGS.block_size,
    eos_token_id=_ARGS.eos_token_id,
)

# Optional: quick self-test on startup (can be toggled via env)
if os.getenv("HASH_SERVICE_SELFTEST", "1") == "1":
    test_prompt = "KV cache self-test prompt."
    logger.info("[hash-service] Running startup self-test for prompt: %r", test_prompt)
    try:
        test_hashes, test_toks = _HASH_COMPUTER.compute(
            test_prompt,
            max_tokens=_ARGS.max_tokens,
        )
        logger.info(
            "[hash-service] Self-test: num_tokens=%d, num_blocks=%d, first_hashes=%s",
            len(test_toks),
            len(test_hashes),
            test_hashes[:5],
        )
    except Exception as e:
        logger.exception("[hash-service] Self-test FAILED: %s", e)


# ------------------------------
# FastAPI models
# ------------------------------

class HashRequest(BaseModel):
    # For backward compatibility:
    #   - either "prompt" OR "messages" must be provided
    prompt: Optional[str] = None
    messages: Optional[List[Dict[str, Any]]] = None
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


@app.get("/debug_config")
async def debug_config() -> dict:
    """
    Simple endpoint to confirm what the service is using.
    """
    return {
        "model_path": _ARGS.model_path,
        "block_size": _ARGS.block_size,
        "eos_token_id": _ARGS.eos_token_id,
        "max_tokens_default": _ARGS.max_tokens,
        "hash_impl": HASH_IMPL_NAME,
    }


@app.post("/compute_hashes", response_model=HashResponse)
async def compute_hashes(req: HashRequest) -> HashResponse:
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

    if not req.prompt and not req.messages:
        raise HTTPException(
            status_code=400,
            detail="Either 'prompt' or 'messages' must be provided",
        )

    logger.info(
        "[hash-service] /compute_hashes has_prompt=%s, has_messages=%s, "
        "block_size=%d, max_tokens=%d",
        req.prompt is not None,
        req.messages is not None,
        block_size,
        max_tokens,
    )

    # Main logic: prefer messages (OpenAI-style), fall back to prompt
    if req.messages is not None:
        block_hashes, token_ids = _HASH_COMPUTER.compute_from_messages(
            messages=req.messages,
            max_tokens=max_tokens,
        )
    else:
        block_hashes, token_ids = _HASH_COMPUTER.compute(
            prompt_text=req.prompt,
            max_tokens=max_tokens,
        )

    logger.info(
        "[hash-service] /compute_hashes result: num_tokens=%d, num_blocks=%d, first_hashes=%s",
        len(token_ids),
        len(block_hashes),
        block_hashes[:5],
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

    logger.info(
        "[hash-service] Uvicorn starting on %s:%d (model_path=%s, block_size=%d, hash_impl=%s)",
        _ARGS.host,
        _ARGS.port,
        _ARGS.model_path,
        _ARGS.block_size,
        HASH_IMPL_NAME,
    )

    uvicorn.run(
        app,
        host=_ARGS.host,
        port=_ARGS.port,
        workers=1,
    )


if __name__ == "__main__":
    main()
