# -*- coding: utf-8 -*-
"""
Model-neutral inline KV-block hash computation for router-side prefix routing.

The hash primitives mirror vLLM's chained block hashing without importing vLLM:

    NONE_HASH = sha256(cbor2.dumps(os.environ["PYTHONHASHSEED"]))
    block_hash[i] = sha256(cbor2.dumps((
        parent_block_hash_or_NONE_HASH,
        tuple(token_ids[i * B : (i + 1) * B]),
        extra_keys,
    )))

Only full blocks are emitted. The returned hash form is the low 64-bit integer
wire form observed in vLLM KV events.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from typing import Dict, List, Optional, Tuple, Union

import cbor2

logger = logging.getLogger("router.prefix_hash")

# A JSON/CBOR-serialisable value as handled by the router hash path.
JSONValue = Union[
    None,
    bool,
    int,
    float,
    str,
    List["JSONValue"],
    Tuple["JSONValue", ...],
    Dict[str, "JSONValue"],
]


def _canonicalise_value(obj: JSONValue) -> JSONValue:
    """Recursively sort dict keys to mirror older BooM serde_json Value output.

    This router-side compatibility hook must be revalidated if BooM's
    serialization behavior changes.
    """
    if isinstance(obj, dict):
        return {k: _canonicalise_value(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, (list, tuple)):
        return [_canonicalise_value(v) for v in obj]
    return obj


def canonicalise_tools_enabled() -> bool:
    """Gate Boom-compatible tools canonicalisation.

    Default OFF: the current BooM gateway serialises request tools with
    serde_json `preserve_order` (insertion order), so sorting tool
    function.parameters keys here would diverge from what vLLM actually
    tokenises. Set KV_CANONICALISE_TOOLS=1 only against a BooM build that
    alphabetically sorts tools (legacy BTreeMap behaviour).
    """
    return os.getenv("KV_CANONICALISE_TOOLS", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def canonicalise_tools_for_boom(tools: Optional[list]) -> Optional[list]:
    """Apply the router-side tool ordering compatibility transform.

    Only `function.parameters` is recursively sorted. The outer OpenAI tool
    shape is rebuilt in the stable order expected by the existing router hash
    path. This does not modify BooM Gateway.
    """
    if not tools:
        return tools

    out = []
    for t in tools:
        if not isinstance(t, dict):
            out.append(t)
            continue
        fn = t.get("function") or {}
        rebuilt_fn = {"name": fn.get("name")}
        if fn.get("description") is not None:
            rebuilt_fn["description"] = fn["description"]
        if "parameters" in fn:
            rebuilt_fn["parameters"] = _canonicalise_value(fn["parameters"])
        out.append({"type": t.get("type", "function"), "function": rebuilt_fn})
    return out


def sha256_cbor(obj: object) -> bytes:
    """sha256(cbor2.dumps(obj)) using the same primitive as vLLM block hashing."""
    return hashlib.sha256(cbor2.dumps(obj)).digest()


def _compute_none_hash() -> bytes:
    seed = os.getenv("PYTHONHASHSEED")
    if seed is None:
        raise RuntimeError(
            "PYTHONHASHSEED is required for KV-block hash alignment with vLLM. "
            "Set PYTHONHASHSEED=0 on the router pod to match the vLLM pod."
        )
    return sha256_cbor(seed)


NONE_HASH: bytes = _compute_none_hash()


def hash_block_tokens(
    parent_block_hash: Optional[bytes],
    curr_block_token_ids: tuple,
    extra_keys: Optional[tuple] = None,
) -> bytes:
    """Hash one block using the previous block digest as the chain parent."""
    if not parent_block_hash:
        parent_block_hash = NONE_HASH
    return sha256_cbor((parent_block_hash, curr_block_token_ids, extra_keys))


def to_external_int(digest: bytes) -> int:
    """Convert a 32-byte digest into the observed vLLM int block hash form."""
    return int.from_bytes(digest, byteorder="big") & ((1 << 64) - 1)


def compute_block_hashes_int(token_ids: List[int], block_size: int) -> List[int]:
    """Compute chained int block hashes for full blocks in a token sequence."""
    if block_size <= 0 or not token_ids:
        return []

    out: List[int] = []
    parent: Optional[bytes] = None
    start = 0
    n = len(token_ids)
    while start + block_size <= n:
        block = tuple(token_ids[start : start + block_size])
        digest = hash_block_tokens(parent, block, None)
        out.append(to_external_int(digest))
        parent = digest
        start += block_size
    return out


_TOKENIZER = None
_TOKENIZER_LOCK = threading.RLock()


def init_tokenizer(model_path: str) -> None:
    """Load tokenizer and chat template from the configured model directory."""
    global _TOKENIZER

    from transformers import PreTrainedTokenizerFast  # lazy import

    with _TOKENIZER_LOCK:
        if _TOKENIZER is not None:
            return

        logger.info("Loading tokenizer from %s", model_path)
        _TOKENIZER = PreTrainedTokenizerFast.from_pretrained(
            model_path,
            local_files_only=True,
            extra_special_tokens={},
        )
        logger.info(
            "Tokenizer ready (vocab_size=%s, chat_template_present=%s)",
            getattr(_TOKENIZER, "vocab_size", "?"),
            bool(getattr(_TOKENIZER, "chat_template", None)),
        )


def get_tokenizer():
    if _TOKENIZER is None:
        raise RuntimeError("init_tokenizer() must be called before get_tokenizer()")
    return _TOKENIZER


def _normalize_messages_for_chat_template(messages):
    """Apply model-neutral OpenAI message normalisation before chat templating.

    Kept generic on purpose:
    - tool message string content -> text-block list;
    - assistant tool call arguments JSON string -> dict.

    Model-specific reasoning field rewrites are intentionally not performed
    here; Track B should add them behind model-specific gates if needed.
    """
    if not messages:
        return messages

    out = []
    for m in messages:
        if not isinstance(m, dict):
            out.append(m)
            continue

        role = m.get("role")
        if role == "tool" and isinstance(m.get("content"), str):
            out.append({**m, "content": [{"type": "text", "text": m["content"]}]})
            continue

        if role != "assistant":
            out.append(m)
            continue

        tcs = m.get("tool_calls")
        if not tcs:
            out.append(m)
            continue

        new_tcs = []
        for tc in tcs:
            if not isinstance(tc, dict):
                new_tcs.append(tc)
                continue
            fn = tc.get("function")
            if not isinstance(fn, dict):
                new_tcs.append(tc)
                continue
            args = fn.get("arguments")
            if isinstance(args, str):
                try:
                    parsed = json.loads(args)
                except Exception:
                    new_tcs.append(tc)
                    continue
                new_tcs.append({**tc, "function": {**fn, "arguments": parsed}})
            else:
                new_tcs.append(tc)
        out.append({**m, "tool_calls": new_tcs})

    return out


def tokenize_messages(
    messages,
    *,
    tools: Optional[list] = None,
    add_generation_prompt: bool = True,
) -> List[int]:
    """Render messages with the configured tokenizer chat template and encode."""
    tok = get_tokenizer()

    if hasattr(tok, "apply_chat_template") and getattr(tok, "chat_template", None):
        text = tok.apply_chat_template(
            _normalize_messages_for_chat_template(messages),
            tools=tools if tools else None,
            tokenize=False,
            add_generation_prompt=add_generation_prompt,
        )
    else:
        parts = []
        for m in messages:
            role = m.get("role", "user") if isinstance(m, dict) else getattr(m, "role", "user")
            content = m.get("content", "") if isinstance(m, dict) else getattr(m, "content", "")
            if isinstance(content, list):
                content = "\n".join(
                    p.get("text", "") for p in content if isinstance(p, dict)
                )
            parts.append(f"{role}: {content}")
        text = "\n".join(parts)

    return tok.encode(text, add_special_tokens=False)


def compute_request_block_hashes_with_len(
    *,
    messages: Optional[list] = None,
    prompt: Optional[str] = None,
    tools: Optional[list] = None,
    block_size: int,
) -> Tuple[List[int], int]:
    """Tokenize messages/prompt once and return (full-block hashes, token count).

    The token count is the exact rendered input length (ISL), needed for
    token/KV-block-budgeted pull sizing. Returns ([], 0) when there is nothing
    to tokenize.
    """
    canon_tools = canonicalise_tools_for_boom(tools) if tools and canonicalise_tools_enabled() else tools

    if messages:
        token_ids = tokenize_messages(
            messages,
            tools=canon_tools,
            add_generation_prompt=True,
        )
    elif prompt is not None:
        token_ids = tokenize_messages(
            [{"role": "user", "content": prompt}],
            tools=canon_tools,
            add_generation_prompt=True,
        )
    else:
        return [], 0

    return compute_block_hashes_int(token_ids, block_size), len(token_ids)


def compute_request_block_hashes_int(
    *,
    messages: Optional[list] = None,
    prompt: Optional[str] = None,
    tools: Optional[list] = None,
    block_size: int,
) -> List[int]:
    """Tokenize messages/prompt and return vLLM-compatible full-block hashes."""
    hashes, _ = compute_request_block_hashes_with_len(
        messages=messages,
        prompt=prompt,
        tools=tools,
        block_size=block_size,
    )
    return hashes
