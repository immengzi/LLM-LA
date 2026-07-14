# tests/test_prefix_hash.py
# -*- coding: utf-8 -*-
"""Unit tests for inline vLLM-compatible block hashing (router.prefix_hash).

The heavy `transformers` tokenizer is replaced by the FakeTokenizer fixture so
the full compute_request_block_hashes_int path is exercised without a model
download. The hash *primitives* are tested directly (no tokenizer needed).
"""
import cbor2

from router import prefix_hash as ph


def test_to_external_int_is_64bit():
    digest = b"\xff" * 32
    val = ph.to_external_int(digest)
    assert 0 <= val < (1 << 64)
    assert val == (1 << 64) - 1


def test_sha256_cbor_matches_reference():
    obj = ("a", (1, 2, 3), None)
    import hashlib
    assert ph.sha256_cbor(obj) == hashlib.sha256(cbor2.dumps(obj)).digest()


def test_hash_block_tokens_chains_on_parent():
    b1 = ph.hash_block_tokens(None, (1, 2, 3), None)
    b2_a = ph.hash_block_tokens(b1, (4, 5, 6), None)
    b2_b = ph.hash_block_tokens(b1, (4, 5, 6), None)
    # Deterministic given the same parent + tokens.
    assert b2_a == b2_b
    # Different parent -> different digest for identical tokens.
    other = ph.hash_block_tokens(b"\x00" * 32, (4, 5, 6), None)
    assert other != b2_a


def test_none_parent_uses_none_hash():
    explicit = ph.hash_block_tokens(ph.NONE_HASH, (1, 2), None)
    implicit = ph.hash_block_tokens(None, (1, 2), None)
    assert explicit == implicit


def test_compute_block_hashes_only_full_blocks():
    tokens = list(range(10))
    # block_size 4 -> two full blocks (8 tokens), last 2 dropped.
    hashes = ph.compute_block_hashes_int(tokens, block_size=4)
    assert len(hashes) == 2
    # Empty / degenerate inputs.
    assert ph.compute_block_hashes_int([], 4) == []
    assert ph.compute_block_hashes_int(tokens, 0) == []
    assert ph.compute_block_hashes_int([1, 2, 3], 4) == []


def test_compute_block_hashes_chain_is_prefix_stable():
    """A longer sequence shares the leading block hashes with a shorter one -
    this is the property that makes prefix routing work."""
    short = ph.compute_block_hashes_int(list(range(8)), block_size=4)
    longer = ph.compute_block_hashes_int(list(range(12)), block_size=4)
    assert longer[: len(short)] == short


def test_canonicalise_tools_sorts_parameter_keys():
    tools = [
        {
            "type": "function",
            "function": {
                "name": "f",
                "description": "d",
                "parameters": {"b": 1, "a": {"z": 1, "y": 2}},
            },
        }
    ]
    out = ph.canonicalise_tools_for_boom(tools)
    params = out[0]["function"]["parameters"]
    assert list(params.keys()) == ["a", "b"]
    assert list(params["a"].keys()) == ["y", "z"]


def test_canonicalise_tools_enabled_env_gate(monkeypatch):
    monkeypatch.delenv("KV_CANONICALISE_TOOLS", raising=False)
    assert ph.canonicalise_tools_enabled() is False
    monkeypatch.setenv("KV_CANONICALISE_TOOLS", "1")
    assert ph.canonicalise_tools_enabled() is True
    monkeypatch.setenv("KV_CANONICALISE_TOOLS", "no")
    assert ph.canonicalise_tools_enabled() is False


def test_normalize_messages_tool_string_to_block():
    msgs = [{"role": "tool", "content": "result text"}]
    out = ph._normalize_messages_for_chat_template(msgs)
    assert out[0]["content"] == [{"type": "text", "text": "result text"}]


def test_normalize_messages_parses_tool_call_arguments():
    msgs = [
        {
            "role": "assistant",
            "tool_calls": [
                {"function": {"name": "f", "arguments": '{"x": 1}'}}
            ],
        }
    ]
    out = ph._normalize_messages_for_chat_template(msgs)
    assert out[0]["tool_calls"][0]["function"]["arguments"] == {"x": 1}


def test_compute_request_block_hashes_with_fake_tokenizer(fake_tokenizer):
    hashes = ph.compute_request_block_hashes_int(
        messages=[{"role": "user", "content": "hello world"}],
        block_size=4,
    )
    assert isinstance(hashes, list)
    assert all(isinstance(h, int) for h in hashes)


def test_compute_request_block_hashes_prompt_matches_user_message(fake_tokenizer):
    from_prompt = ph.compute_request_block_hashes_int(prompt="hi there", block_size=4)
    from_msgs = ph.compute_request_block_hashes_int(
        messages=[{"role": "user", "content": "hi there"}], block_size=4
    )
    assert from_prompt == from_msgs


def test_compute_request_block_hashes_empty_input(fake_tokenizer):
    assert ph.compute_request_block_hashes_int(block_size=4) == []
