"""SGLang v0.5.15 router hash contract tests.

Golden values are derived from the pinned upstream implementation at
sglang/srt/mem_cache/utils.py (commit f63458b5beaceabbd9d749b9fc956370e1b649e6).
No live model is required; live publisher conformance remains a cluster test.
"""

import asyncio
import os

import pytest

os.environ.setdefault("PYTHONHASHSEED", "0")

from router import api, config, prefix_hash
from router.hash_backends import sglang_v0_5_15 as sglang


def test_upstream_golden_page_bytes_and_first_hash():
    page_bytes = sglang.encode_page_tokens([1, 2])
    assert page_bytes.hex() == "0100000002000000"
    assert sglang.hash_page(page_bytes).hex() == (
        "34fb5c825de7ca4aea6e712f19d439c1"
        "da0c92c37b423936c5f618545ca4fa1f"
    )


def test_upstream_golden_multi_page_chain():
    assert sglang.compute_page_hashes_int([1, 2, 3, 4], 2) == [
        3817746824117602890,
        -4216701448867210342,
    ]


def test_signed_event_hash_conversion_boundary():
    assert sglang.to_event_hash(bytes.fromhex("7fffffffffffffff") + bytes(24)) == (
        (1 << 63) - 1
    )
    assert sglang.to_event_hash(bytes.fromhex("8000000000000000") + bytes(24)) == (
        -(1 << 63)
    )


def test_partial_page_is_excluded():
    assert sglang.compute_page_hashes_int([1, 2, 3], 2) == [
        3817746824117602890
    ]
    assert sglang.compute_page_hashes_int([1], 2) == []
    assert sglang.compute_page_hashes_int([1, 2], 0) == []


def test_backend_dispatch_selects_sglang():
    assert prefix_hash.compute_block_hashes_int_for_backend(
        [1, 2, 3, 4],
        2,
        backend="sglang",
    ) == [3817746824117602890, -4216701448867210342]


def test_default_dispatch_preserves_vllm_hashing():
    expected = [12009346384364793183, 11890034342157281616]
    assert prefix_hash.compute_block_hashes_int([1, 2, 3, 4], 2) == expected
    assert prefix_hash.compute_block_hashes_int_for_backend(
        [1, 2, 3, 4],
        2,
    ) == expected


@pytest.mark.parametrize(
    "chat_body, expected",
    [
        ({"messages": [], "cache_salt": "x"}, "cache_salt"),
        ({"messages": [], "extra_key": "x"}, "extra_key"),
        ({"messages": [], "lora_path": "/adapter"}, "lora_or_adapter"),
        ({"messages": [], "adapter": "tenant-a"}, "lora_or_adapter"),
        (
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [{"type": "image_url", "image_url": {"url": "x"}}],
                    }
                ]
            },
            "multimodal_message_part",
        ),
        ({"messages": [], "speculative_num_steps": 2}, "speculative_or_bigram"),
        ({"messages": [], "bigram_index": 4}, "speculative_or_bigram"),
        ({"messages": [], "chat_template": "custom"}, "chat_template_override"),
        (
            {"messages": [], "chat_template_kwargs": {"enable_thinking": True}},
            "chat_template_override",
        ),
        ({"enable_thinking": False}, "chat_template_override"),
        ({"messages": [], "tokenizer": "custom"}, "tokenizer_override"),
        ({"messages": [], "tokenizer_path": "/tmp/tok"}, "tokenizer_override"),
        ({"messages": [], "tokenizer_kwargs": {"legacy": True}}, "tokenizer_override"),
        ({"messages": [], "tokenizer_mode": "slow"}, "tokenizer_override"),
        (
            {"messages": [], "add_special_tokens": False},
            "special_tokenization_override",
        ),
        (
            {"messages": [], "continue_final_message": True},
            "special_tokenization_override",
        ),
        ({"messages": [], "prompt": "alternate"}, "alternate_chat_input"),
        ({"messages": [], "token_ids": [1, 2]}, "alternate_chat_input"),
        ({"messages": [], "input_ids": [1, 2]}, "alternate_chat_input"),
        (
            {"messages": [], "custom_logit_processor": "pkg.Processor"},
            "custom_processor_override",
        ),
        (
            {"messages": [], "sampling_params": {"logits_processors": ["custom"]}},
            "custom_processor_override",
        ),
    ],
)
def test_unsupported_chat_request_eligibility(chat_body, expected):
    assert sglang.request_ineligibility(chat_body) == expected


def test_plain_text_chat_request_is_eligible():
    request = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": "hello"}],
            }
        ],
        "temperature": 0.2,
    }
    assert sglang.request_ineligibility(request) is None


@pytest.mark.parametrize(
    "override",
    [
        {"enable_thinking": False},
        {"chat_template_kwargs": {"enable_thinking": True}},
        {"tokenizer_mode": "slow"},
    ],
)
def test_explicit_tokenizer_overrides_skip_router_sglang_hash(
    monkeypatch, override
):
    monkeypatch.setattr(api._cfg, "KV_AWARE", True)
    monkeypatch.setattr(api._cfg, "ROUTER_MEASURE_PREFIX", False)
    monkeypatch.setattr(api._cfg, "ROUTER_LOG_BLOCK_HASHES", False)
    monkeypatch.setattr(api._cfg, "INFERENCE_ENGINE", "sglang")
    monkeypatch.setattr(api._cfg, "KV_HASH_BACKEND", "sglang")
    monkeypatch.setattr(api._cfg, "KV_HASH_SOURCE", "inline")
    monkeypatch.setattr(api._cfg, "KV_BLOCK_SIZE", 16)
    monkeypatch.setattr(api._cfg, "SGLANG_CONTRACT_VERSION", "0.5.15")

    chat_request = {
        "messages": [{"role": "user", "content": "hello"}],
        **override,
    }
    meta = asyncio.run(api._maybe_register_kv_blocks(
        "req-override",
        "hello",
        meta={"__chat_request__": chat_request},
        is_pull_mode=False,
        messages=chat_request["messages"],
    ))

    assert meta["kv_hash_skip_reason"].startswith("sglang_request_unsupported:")


def test_tool_schema_override_like_names_remain_supported():
    request = {
        "messages": [{"role": "user", "content": "use the tool"}],
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "run",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "prompt": {"type": "string"},
                            "processor": {"type": "string"},
                            "tokenizer_mode": {"type": "string"},
                        },
                    },
                },
            }
        ],
    }
    assert sglang.request_ineligibility(request) is None


def test_config_parses_valid_engine_and_backend(monkeypatch):
    monkeypatch.setenv("INFERENCE_ENGINE", "SGLANG")
    monkeypatch.setenv("KV_HASH_BACKEND", "SGLANG")
    monkeypatch.setenv("SGLANG_CONTRACT_VERSION", "0.5.15")
    monkeypatch.setenv("KV_BLOCK_SIZE", "16")
    config._CONFIG = None
    cfg = config.get_config()
    assert cfg.INFERENCE_ENGINE == "sglang"
    assert cfg.KV_HASH_BACKEND == "sglang"
    assert config.sglang_hash_contract_error(cfg) is None
    config._CONFIG = None


def test_config_invalid_values_fall_back_to_vllm(monkeypatch):
    monkeypatch.setenv("INFERENCE_ENGINE", "unknown")
    monkeypatch.setenv("KV_HASH_BACKEND", "unknown")
    config._CONFIG = None
    cfg = config.get_config()
    assert cfg.INFERENCE_ENGINE == "vllm"
    assert cfg.KV_HASH_BACKEND == "vllm"
    config._CONFIG = None


@pytest.mark.parametrize(
    "overrides, expected",
    [
        (
            {"INFERENCE_ENGINE": "vllm"},
            "inference_engine_must_be_sglang",
        ),
        ({"KV_HASH_BACKEND": "vllm"}, "kv_hash_backend_must_be_sglang"),
        ({"KV_HASH_SOURCE": "external"}, "kv_hash_source_must_be_inline"),
        ({"KV_BLOCK_SIZE": 0}, "sglang_page_size_must_be_positive"),
        (
            {"SGLANG_CONTRACT_VERSION": "0.5.16"},
            "sglang_contract_version_mismatch",
        ),
    ],
)
def test_sglang_runtime_contract_fails_closed(overrides, expected):
    cfg = config.RouterConfig(
        INFERENCE_ENGINE="sglang",
        KV_HASH_BACKEND="sglang",
        KV_HASH_SOURCE="inline",
        KV_BLOCK_SIZE=16,
        SGLANG_CONTRACT_VERSION="0.5.15",
    )
    for key, value in overrides.items():
        setattr(cfg, key, value)
    assert expected in config.sglang_hash_contract_error(cfg)
