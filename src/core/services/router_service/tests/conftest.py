# tests/conftest.py
# -*- coding: utf-8 -*-
"""Shared pytest fixtures & test-time environment for the router service.

Key responsibilities:
  * Guarantee PYTHONHASHSEED is present BEFORE any router module is imported.
    router.prefix_hash computes its NONE_HASH from this env var at import time
    and raises RuntimeError if it is missing, so it must be set at collection
    time (this file is imported before test modules).
  * Provide helpers to reset the module-level singletons the router uses
    (RouterConfig, kv_aware global maps) so tests are isolated.
  * Provide a lightweight fake tokenizer so the inline hashing path can be
    exercised without downloading a real (multi-GB) transformers model.
"""
import os

# Must happen before importing router.prefix_hash (directly or transitively).
os.environ.setdefault("PYTHONHASHSEED", "0")

import pytest


@pytest.fixture
def reset_config(monkeypatch):
    """Reset the RouterConfig singleton so a test can re-read env vars.

    Usage:
        def test_x(reset_config, monkeypatch):
            monkeypatch.setenv("ROUTER_MODE", "push-rr")
            cfg = reset_config()
            assert cfg.ROUTER_MODE == "push-rr"

    Returns a callable that rebuilds and returns a fresh config. The original
    singleton is restored on teardown so modules that cached ``get_config()``
    at import time are not left pointing at a mutated global.
    """
    from router import config as cfg_mod

    saved_cfg = cfg_mod._CONFIG
    saved_registry = cfg_mod._MODEL_REGISTRY

    def _rebuild():
        cfg_mod._CONFIG = None
        cfg_mod._MODEL_REGISTRY = None
        return cfg_mod.get_config()

    yield _rebuild

    cfg_mod._CONFIG = saved_cfg
    cfg_mod._MODEL_REGISTRY = saved_registry


@pytest.fixture
def clean_kv_state():
    """Clear the process-global kv_aware maps before and after a test."""
    from router import kv_aware

    def _clear():
        with kv_aware._LOCK:
            kv_aware._REQ_BLOCKS.clear()
            kv_aware._REQ_OWNERS.clear()
            kv_aware._BLOCK_OWNERS.clear()
            kv_aware._REQ_ROUTING.clear()

    _clear()
    yield kv_aware
    _clear()


class FakeTokenizer:
    """Minimal stand-in for a transformers tokenizer.

    * ``chat_template`` truthy so prefix_hash.tokenize_messages takes the
      apply_chat_template branch.
    * ``apply_chat_template`` renders messages to a deterministic string.
    * ``encode`` maps each character to a stable integer token id, which is all
      the block-hashing path needs (it only cares about the token id sequence).
    """

    chat_template = "{{fake}}"
    vocab_size = 256

    def apply_chat_template(self, messages, tools=None, tokenize=False,
                            add_generation_prompt=True):
        parts = []
        if tools:
            parts.append(f"<tools:{len(tools)}>")
        for m in messages:
            role = m.get("role", "user") if isinstance(m, dict) else "user"
            content = m.get("content", "") if isinstance(m, dict) else ""
            if isinstance(content, list):
                content = "".join(
                    b.get("text", "") for b in content if isinstance(b, dict)
                )
            parts.append(f"{role}:{content}")
        if add_generation_prompt:
            parts.append("assistant:")
        return "\n".join(parts)

    def encode(self, text, add_special_tokens=False):
        return [ord(c) % 251 for c in text]


@pytest.fixture
def fake_tokenizer(monkeypatch):
    """Install a FakeTokenizer into router.prefix_hash for the duration of a test."""
    from router import prefix_hash

    tok = FakeTokenizer()
    monkeypatch.setattr(prefix_hash, "_TOKENIZER", tok, raising=False)
    return tok
