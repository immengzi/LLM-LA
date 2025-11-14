#!/usr/bin/env python3
# prefix_hash_estimation.py
"""
Compute vLLM prefix cache block hashes by constructing a vLLM Request.
This uses the same code path as the vLLM server.

Provides:
- sha256_cbor(obj)               : hash helper
- BlockHashComputer              : reusable class (load tokenizer once)
- compute_vllm_block_hashes(...) : convenience function (one-off usage)
"""

import os
os.environ.setdefault("PYTHONHASHSEED", "0")  # deterministic hashing

import time
import hashlib
from typing import List, Tuple

import cbor2
from transformers import AutoTokenizer

# vLLM imports (must match your vLLM version)
from vllm.sampling_params import SamplingParams
from vllm.v1.request import Request
from vllm.v1.core.kv_cache_utils import (
    get_request_block_hasher,
    init_none_hash,
    maybe_convert_block_hash,
)


def sha256_cbor(obj) -> bytes:
    """
    Replacement for vllm.utils.hashing.sha256_cbor:
    Computes SHA256 hash of a CBOR-encoded object and returns raw bytes.
    """
    cbor_bytes = cbor2.dumps(obj)
    return hashlib.sha256(cbor_bytes).digest()


# Initialize NONE_HASH once using the same hash function the server uses.
# This should be done once at import time.
init_none_hash(sha256_cbor)


class BlockHashComputer:
    """
    Reusable helper that:
      - Loads tokenizer once
      - Prepares block_hasher once
      - Computes block hashes for prompts
    """

    def __init__(
        self,
        model_path: str,
        block_size: int = 128,
        eos_token_id: int = 151643,
    ) -> None:
        """
        Args:
            model_path: Path or HF identifier for the model/tokenizer.
            block_size: KV block size used by vLLM.
            eos_token_id: EOS token id for the model (Qwen default shown).
        """
        self.model_path = model_path
        self.block_size = block_size
        self.eos_token_id = eos_token_id

        # Load tokenizer once; we assume files are local in your setup
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_path,
            trust_remote_code=True,
            local_files_only=True,
        )

        # Create a block hasher compatible with the server
        self.block_hasher = get_request_block_hasher(block_size, sha256_cbor)

    def compute(
        self,
        prompt_text: str,
        max_tokens: int = 32,
    ) -> Tuple[List[int], List[int]]:
        """
        Compute KV block hashes and token IDs for a given prompt.

        Returns:
            block_hashes_as_int: List of block hashes as ints (KV event format)
            prompt_token_ids   : List of token IDs used for the prompt
        """
        # Tokenize prompt (no special tokens)
        prompt_token_ids = self.tokenizer.encode(
            prompt_text,
            add_special_tokens=False,
        )

        # Create sampling params (these don't affect block hashing)
        sampling_params = SamplingParams(
            ignore_eos=False,
            max_tokens=max_tokens,
            stop_token_ids=None,
            prompt_logprobs=None,
        )

        # Create Request object (this triggers block hashing inside vLLM)
        req = Request(
            request_id="compute-hash-demo",
            prompt_token_ids=prompt_token_ids,
            sampling_params=sampling_params,
            pooling_params=None,
            eos_token_id=self.eos_token_id,
            client_index=0,
            arrival_time=time.time(),
            prompt_embeds=None,
            mm_features=None,
            lora_request=None,
            cache_salt=None,
            priority=0,
            trace_headers=None,
            block_hasher=self.block_hasher,
        )

        # Convert block hashes to the format used in KV events (int)
        block_hashes_as_int: List[int] = []
        for block_hash in req.block_hashes:
            hash_int = maybe_convert_block_hash(block_hash)
            block_hashes_as_int.append(hash_int)

        return block_hashes_as_int, prompt_token_ids


def compute_vllm_block_hashes(
    prompt_text: str,
    model_path: str,
    block_size: int = 128,
    eos_token_id: int = 151643,
    max_tokens: int = 32,
) -> Tuple[List[int], List[int]]:
    """
    Convenience wrapper for one-off usage.

    NOTE: This creates a new BlockHashComputer each time; use the class
    directly in services for better performance.
    """
    computer = BlockHashComputer(
        model_path=model_path,
        block_size=block_size,
        eos_token_id=eos_token_id,
    )
    return computer.compute(prompt_text, max_tokens=max_tokens)


if __name__ == "__main__":
    # Simple CLI demo
    demo_prompt = "Hello world."
    demo_model_path = "/model/qwen-test"
    demo_block_size = 128

    hashes, token_ids = compute_vllm_block_hashes(
        prompt_text=demo_prompt,
        model_path=demo_model_path,
        block_size=demo_block_size,
    )

    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Prompt: {demo_prompt!r}")
    print(f"Total tokens: {len(token_ids)}")
    print(f"Block size: {demo_block_size}")
    print(f"Number of block hashes: {len(hashes)}")
    print(f"Computed hashes (ints):")
    print(hashes)
