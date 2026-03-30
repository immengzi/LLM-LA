#!/usr/bin/env python3
# prefix_hash_estimation.py
"""
Compute vLLM prefix cache block hashes by creating an actual Request object.
This uses (as closely as we can) the same code path as the vLLM OpenAI
chat server: we take a `messages` array, apply the chat template, tokenize,
then feed those tokens into vLLM's Request + block hasher.
"""

import os
os.environ["PYTHONHASHSEED"] = "0"

import time
import logging
from typing import List, Tuple, Dict, Any

from transformers import AutoTokenizer, PreTrainedTokenizerFast

# vLLM imports
from vllm.sampling_params import SamplingParams
from vllm.v1.request import Request
from vllm.v1.core.kv_cache_utils import (
    get_request_block_hasher,
    init_none_hash,
    maybe_convert_block_hash,
)

import hashlib
import cbor2

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logger = logging.getLogger("prefix_hash_estimation")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

HASH_IMPL_NAME = "local_sha256_cbor (cbor2 + sha256)"


def sha256_cbor(obj) -> bytes:
    """
    CBOR + SHA256, returning 32-byte digest.
    This is our stand-in for the server's sha256_cbor implementation.
    """
    cbor_bytes = cbor2.dumps(obj)
    return hashlib.sha256(cbor_bytes).digest()


# Make vLLM's "NONE_HASH" use the same hash function
init_none_hash(sha256_cbor)
logger.info("[prefix-hash] init_none_hash configured with %s", HASH_IMPL_NAME)


class BlockHashComputer:
    """
    Reusable helper that:
      - Loads tokenizer once
      - Prepares block_hasher once
      - Computes block hashes for prompts or chat messages
    """

    def __init__(
        self,
        model_path: str,
        block_size: int = 128,
        eos_token_id: int = 151643,
    ) -> None:
        self.model_path = model_path
        self.block_size = block_size
        self.eos_token_id = eos_token_id

        logger.info(
            "[prefix-hash] Initializing BlockHashComputer(model_path=%s, "
            "block_size=%d, eos_token_id=%d, hash_impl=%s)",
            model_path,
            block_size,
            eos_token_id,
            HASH_IMPL_NAME,
        )

        # Load tokenizer directly from tokenizer.json, bypassing AutoConfig
        # (needed for models like GLM-5 whose model_type is not recognized
        # by older transformers versions)
        self.tokenizer = PreTrainedTokenizerFast.from_pretrained(
            model_path,
            local_files_only=True,
            extra_special_tokens={},
        )
        logger.info(
            "[prefix-hash] Loaded tokenizer from %s (vocab_size=%s, has_chat_template=%s)",
            model_path,
            getattr(self.tokenizer, "vocab_size", "unknown"),
            hasattr(self.tokenizer, "chat_template"),
        )

        # Create block hasher using our sha256_cbor
        self.block_hasher = get_request_block_hasher(
            block_size,
            sha256_cbor,
        )
        logger.info(
            "[prefix-hash] Created block_hasher with block_size=%d using %s",
            block_size,
            HASH_IMPL_NAME,
        )

    # ------------------------------------------------------------------
    # Tokenization helpers
    # ------------------------------------------------------------------

    def _tokens_from_messages(self, messages: List[Dict[str, Any]]) -> List[int]:
        """
        Try to mimic vLLM OpenAI chat behaviour:
        - if tokenizer.apply_chat_template exists => use it
        - else: simple concatenation "role: content"
        """
        if hasattr(self.tokenizer, "apply_chat_template"):
            # Qwen3 and other chat models usually define this
            text = self.tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True,
            )
        else:
            # Fallback: not perfect, but at least deterministic
            parts = []
            for m in messages:
                role = m.get("role", "user")
                content = m.get("content", "")
                parts.append(f"{role}: {content}")
            text = "\n".join(parts)

        token_ids = self.tokenizer.encode(
            text,
            add_special_tokens=False,
        )

        logger.info(
            "[prefix-hash] Built chat tokens: len(text)=%d, num_tokens=%d",
            len(text),
            len(token_ids),
        )
        return token_ids

    # ------------------------------------------------------------------
    # Public compute APIs
    # ------------------------------------------------------------------

    def compute_from_messages(
        self,
        messages: List[Dict[str, Any]],
        max_tokens: int = 32,
    ) -> Tuple[List[int], List[int]]:
        """
        Compute KV block hashes for a chat `messages` array
        (OpenAI-style messages).
        """
        prompt_token_ids = self._tokens_from_messages(messages)

        logger.info(
            "[prefix-hash] Computing hashes from messages: num_tokens=%d, max_tokens=%d",
            len(prompt_token_ids),
            max_tokens,
        )

        sampling_params = SamplingParams(
            ignore_eos=False,
            max_tokens=max_tokens,
            stop_token_ids=None,
            prompt_logprobs=None,
        )

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

        block_hashes_as_int: List[int] = []
        for block_hash in req.block_hashes:
            hash_int = maybe_convert_block_hash(block_hash)
            block_hashes_as_int.append(hash_int)

        logger.info(
            "[prefix-hash] Result from messages: num_blocks=%d, first_hashes=%s",
            len(block_hashes_as_int),
            block_hashes_as_int[:5],
        )

        return block_hashes_as_int, prompt_token_ids

    def compute(
        self,
        prompt_text: str,
        max_tokens: int = 32,
    ) -> Tuple[List[int], List[int]]:
        """
        Legacy single-prompt API. Wraps into one user message.
        """
        messages = [{"role": "user", "content": prompt_text}]
        return self.compute_from_messages(messages, max_tokens=max_tokens)


def compute_vllm_block_hashes(
    prompt_text: str,
    model_path: str,
    block_size: int = 128,
    eos_token_id: int = 151643,
    max_tokens: int = 32,
) -> Tuple[List[int], List[int]]:
    """
    Convenience wrapper for one-off usage.
    """
    computer = BlockHashComputer(
        model_path=model_path,
        block_size=block_size,
        eos_token_id=eos_token_id,
    )
    return computer.compute(prompt_text, max_tokens=max_tokens)


if __name__ == "__main__":
    # Simple CLI demo
    prompt_text = "Hello world."
    model_path = "/model/qwen-test"
    block_size = 128

    hashes, token_ids = compute_vllm_block_hashes(
        prompt_text=prompt_text,
        model_path=model_path,
        block_size=block_size,
    )

    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Prompt: {prompt_text!r}")
    print(f"Total tokens: {len(token_ids)}")
    print(f"Block size: {block_size}")
    print(f"Number of block hashes: {len(hashes)}")
    print(f"Computed hashes (ints):")
    print(hashes)