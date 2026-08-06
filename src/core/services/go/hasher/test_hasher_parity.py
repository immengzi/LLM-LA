# -*- coding: utf-8 -*-
"""Parity guard for the in-container hasher (inline mode).

The in-container hasher (`hasher_app.py`) must produce the exact block hashes
the Python router computes in-process, because both import the SAME
`router/prefix_hash.py`. These tests assert the wrapper feeds inputs through
unchanged (messages vs prompt selection, tool canonicalisation, block size) and
that the underlying chained block-hash algorithm is deterministic.

A fake tokenizer is injected so the test runs offline (no model download).
"""

import os

os.environ.setdefault("PYTHONHASHSEED", "0")

import json
import sys
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[3]  # .../src
_ROUTER_DIR = _SRC / "services" / "router_service" / "router"
_HASHER_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROUTER_DIR.parent))
sys.path.insert(0, str(_HASHER_DIR))

from router import prefix_hash as ph  # noqa: E402
import hasher_app  # noqa: E402


class _FakeTokenizer:
    """Deterministic stand-in for an HF fast tokenizer + chat template."""

    chat_template = "fake-template"

    def apply_chat_template(
        self, messages, tools=None, tokenize=False, add_generation_prompt=True
    ):
        payload = {"messages": messages, "tools": tools, "gen": add_generation_prompt}
        return json.dumps(payload, sort_keys=True)

    def encode(self, text, add_special_tokens=False):
        return [(ord(c) % 5000) + 1 for c in text]


def setup_function(_func):
    ph._TOKENIZER = _FakeTokenizer()
    hasher_app._CONTRACT_VERSION = ""


def test_wrapper_matches_inline_for_messages_and_tools():
    messages = [
        {"role": "system", "content": "You are helpful."},
        {"role": "user", "content": "Hello world. " * 200},
    ]
    tools = [
        {"type": "function", "function": {"name": "f", "parameters": {"b": 1, "a": 2}}}
    ]
    block_size = 16

    expected = ph.compute_request_block_hashes_int(
        messages=messages, prompt=None, tools=tools, block_size=block_size
    )
    resp = hasher_app.compute_hashes(
        hasher_app.HashRequest(messages=messages, tools=tools, block_size=block_size)
    )

    assert len(expected) > 0
    assert resp.block_hashes == expected


def test_wrapper_matches_inline_for_flat_prompt():
    prompt = "x" * 1000
    block_size = 16

    expected = ph.compute_request_block_hashes_int(
        messages=None, prompt=prompt, tools=None, block_size=block_size
    )
    resp = hasher_app.compute_hashes(
        hasher_app.HashRequest(prompt=prompt, block_size=block_size)
    )

    assert len(expected) > 0
    assert resp.block_hashes == expected


def test_block_hash_chain_is_deterministic():
    # Pure token-level hashing needs no tokenizer; guards the algorithm itself.
    token_ids = list(range(40))
    out = ph.compute_block_hashes_int(token_ids, block_size=16)
    assert len(out) == 2  # 40 tokens -> two full 16-blocks (trailing 8 dropped)
    assert all(isinstance(x, int) for x in out)
    assert ph.compute_block_hashes_int(token_ids, 16) == out


def test_sglang_golden_vectors_through_hasher_boundary():
    class _GoldenTokenizer:
        chat_template = None

        def encode(self, _text, add_special_tokens=False):
            return [1, 2, 3, 4]

    ph._TOKENIZER = _GoldenTokenizer()
    hasher_app._CONTRACT_VERSION = "0.5.15"
    response = hasher_app.compute_hashes(
        hasher_app.HashRequest(
            prompt="ignored",
            block_size=2,
            backend="sglang",
        )
    )
    assert response.block_hashes == [
        3817746824117602890,
        -4216701448867210342,
    ]


def test_vllm_golden_vectors_remain_default():
    class _GoldenTokenizer:
        chat_template = None

        def encode(self, _text, add_special_tokens=False):
            return [1, 2, 3, 4]

    ph._TOKENIZER = _GoldenTokenizer()
    response = hasher_app.compute_hashes(
        hasher_app.HashRequest(prompt="ignored", block_size=2)
    )
    assert response.block_hashes == [
        12009346384364793183,
        11890034342157281616,
    ]


def test_sglang_contract_mismatch_fails_closed():
    with pytest.raises(Exception) as exc:
        hasher_app.compute_hashes(
            hasher_app.HashRequest(prompt="x", block_size=2, backend="sglang")
        )
    assert getattr(exc.value, "status_code", None) == 409
    assert "actual=unset" in str(getattr(exc.value, "detail", ""))


def test_request_backend_is_validated():
    with pytest.raises(Exception) as exc:
        hasher_app.compute_hashes(
            hasher_app.HashRequest(prompt="x", block_size=2, backend="unknown")
        )
    assert getattr(exc.value, "status_code", None) == 400


def test_health_exposes_backend_and_contract(monkeypatch):
    monkeypatch.setattr(hasher_app, "_BACKEND", "sglang")
    monkeypatch.setattr(hasher_app, "_CONTRACT_VERSION", "0.5.15")
    assert hasher_app.health() == {
        "status": "ok",
        "backend": "sglang",
        "sglang_contract_version": "0.5.15",
        "sglang_expected_contract_version": "0.5.15",
    }
