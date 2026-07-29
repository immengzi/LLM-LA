"""SGLang v0.5.15 radix-cache page hashing.

This module is deliberately version-pinned. SGLang's event hash is the signed
int64 represented by the first eight bytes of each chained SHA-256 digest.
"""

from __future__ import annotations

import hashlib
import struct
from typing import Any, Dict, List, Optional, Sequence

CONTRACT_VERSION = "0.5.15"
_DIGEST_SIZE = 32


def encode_page_tokens(token_ids: Sequence[int]) -> bytes:
    """Encode page tokens as unsigned 32-bit little-endian integers."""
    try:
        return b"".join(struct.pack("<I", token_id) for token_id in token_ids)
    except struct.error as exc:
        raise ValueError("SGLang token IDs must fit uint32") from exc


def hash_page(page_bytes: bytes, previous_digest: bytes | None = None) -> bytes:
    """Return one raw page digest using SGLang v0.5.15 chaining."""
    if previous_digest is None:
        payload = page_bytes
    else:
        if len(previous_digest) != _DIGEST_SIZE:
            raise ValueError("previous SGLang page digest must be 32 bytes")
        payload = previous_digest + page_bytes
    return hashlib.sha256(payload).digest()


def to_event_hash(digest: bytes) -> int:
    """Convert a digest to SGLang's signed int64 cache-event hash."""
    if len(digest) != _DIGEST_SIZE:
        raise ValueError("SGLang page digest must be 32 bytes")
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def compute_page_hashes_int(token_ids: Sequence[int], page_size: int) -> List[int]:
    """Hash all complete pages; intentionally exclude any trailing partial page."""
    if page_size <= 0 or not token_ids:
        return []

    hashes: List[int] = []
    previous_digest: bytes | None = None
    for start in range(0, len(token_ids) - page_size + 1, page_size):
        page_bytes = encode_page_tokens(token_ids[start : start + page_size])
        previous_digest = hash_page(page_bytes, previous_digest)
        hashes.append(to_event_hash(previous_digest))
    return hashes


def request_ineligibility(chat_request: Dict[str, Any]) -> Optional[str]:
    """Return why request tokenization cannot safely match the pinned engine."""
    messages = chat_request.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if not isinstance(content, list):
                continue
            for part in content:
                if isinstance(part, str):
                    continue
                if not isinstance(part, dict):
                    return "multimodal_message_part"
                part_type = str(part.get("type", "")).strip().lower()
                if part_type not in ("text", "input_text"):
                    return "multimodal_message_part"
                if any(
                    key in part
                    for key in (
                        "image",
                        "image_url",
                        "input_image",
                        "audio",
                        "audio_url",
                        "input_audio",
                        "video",
                        "video_url",
                        "input_video",
                    )
                ):
                    return "multimodal_message_part"

    request_field_reasons = {
        "cache_salt": "cache_salt",
        "extra_key": "extra_key",
        "extra_keys": "extra_keys",
        "chat_template": "chat_template_override",
        "chat_template_kwargs": "chat_template_override",
        "enable_thinking": "chat_template_override",
        "add_special_tokens": "special_tokenization_override",
        "continue_final_message": "special_tokenization_override",
    }
    alternate_inputs = {"prompt", "token_ids", "input_ids"}

    def _walk(value: Any) -> Optional[str]:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = str(key).strip().lower().replace("-", "_")
                if normalized in request_field_reasons:
                    return request_field_reasons[normalized]
                if normalized == "tokenizer" or normalized.startswith("tokenizer_"):
                    return "tokenizer_override"
                if "processor" in normalized:
                    return "custom_processor_override"
                if "lora" in normalized or normalized in (
                    "adapter",
                    "adapters",
                    "adapter_id",
                    "adapter_name",
                    "adapter_path",
                    "adapter_request",
                ):
                    return "lora_or_adapter"
                if (
                    "speculative" in normalized
                    or "bigram" in normalized
                    or normalized.startswith("draft_")
                ):
                    return "speculative_or_bigram"
                reason = _walk(child)
                if reason:
                    return reason
        elif isinstance(value, list):
            for child in value:
                reason = _walk(child)
                if reason:
                    return reason
        return None

    if isinstance(messages, list):
        for field in alternate_inputs:
            if field in chat_request:
                return "alternate_chat_input"

    # Tool schemas and tool-call arguments may legitimately contain property
    # names such as "prompt" or "processor". Tools are represented in router
    # hashing, so do not interpret their schema as request-level overrides.
    hash_unrepresented_fields = {
        key: value
        for key, value in chat_request.items()
        if str(key).strip().lower() not in ("messages", "tools")
    }
    return _walk(hash_unrepresented_fields)
