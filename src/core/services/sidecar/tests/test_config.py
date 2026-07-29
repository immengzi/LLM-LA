# tests/test_config.py
# -*- coding: utf-8 -*-
"""Unit tests for SidecarConfig env parsing (sidecar.config)."""
from unittest.mock import patch

from sidecar.config import get_config


def test_defaults():
    cfg = get_config()
    assert cfg.SIDECAR_MODE == "pull"
    assert cfg.BATCH_SIZE == 8
    assert cfg.PREFETCH == 0
    assert cfg.RESULT_TRANSPORT_MODE == "sync"
    assert cfg.KV_EVENT_REDIS_PING_INTERVAL_S == 5.0
    assert cfg.KV_EVENT_REDIS_PING_TIMEOUT_S == 1.0


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "16")
    monkeypatch.setenv("PREFETCH", "4")
    monkeypatch.setenv("SIDECAR_MODE", "push")
    monkeypatch.setenv("ROUTER_URL", "http://router:9999")
    monkeypatch.setenv("FORCE_IGNORE_EOS", "true")
    monkeypatch.setenv("STREAMING_MODE", "TRUE")
    cfg = get_config()
    assert cfg.BATCH_SIZE == 16
    assert cfg.PREFETCH == 4
    assert cfg.SIDECAR_MODE == "push"
    assert cfg.ROUTER_URL == "http://router:9999"
    assert cfg.FORCE_IGNORE_EOS is True
    assert cfg.STREAMING_MODE is True


def test_result_transport_mode_lowercased(monkeypatch):
    monkeypatch.setenv("RESULT_TRANSPORT_MODE", "SUBMIT_ACK")
    assert get_config().RESULT_TRANSPORT_MODE == "submit_ack"


def test_dp_sizes(monkeypatch):
    monkeypatch.setenv("DP_SIZE", "4")
    monkeypatch.setenv("DP_SIZE_LOCAL", "2")
    cfg = get_config()
    assert cfg.DP_SIZE == 4
    assert cfg.DP_SIZE_LOCAL == 2


def test_redis_health_check_overrides(monkeypatch):
    monkeypatch.setenv("KV_EVENT_REDIS_PING_INTERVAL_S", "2.5")
    monkeypatch.setenv("KV_EVENT_REDIS_PING_TIMEOUT_S", "0.75")
    cfg = get_config()
    assert cfg.KV_EVENT_REDIS_PING_INTERVAL_S == 2.5
    assert cfg.KV_EVENT_REDIS_PING_TIMEOUT_S == 0.75


def test_legacy_vllm_environment_remains_supported():
    env = {
        "VLLM_URL": "http://legacy:8000",
        "VLLM_HOST": "legacy",
        "VLLM_SUB_PORT": "6001",
        "VLLM_TIMEOUT_S": "45",
        "VLLM_REPLAY_PORT": "6002",
        "VLLM_EVENT_TOPIC": "legacy-kv",
        "VLLM_HEALTH_PATH": "/health_generate",
    }
    with patch.dict("os.environ", env, clear=True):
        cfg = get_config()

    assert cfg.INFERENCE_URL == cfg.VLLM_URL == "http://legacy:8000"
    assert cfg.INFERENCE_HOST == cfg.VLLM_HOST == "legacy"
    assert cfg.KV_EVENT_PORT == cfg.VLLM_SUB_PORT == 6001
    assert cfg.INFERENCE_TIMEOUT_S == cfg.VLLM_TIMEOUT_S == 45.0
    assert cfg.KV_EVENT_REPLAY_PORT == 6002
    assert cfg.KV_EVENT_TOPIC == "legacy-kv"
    assert cfg.INFERENCE_HEALTH_PATH == "/health_generate"


def test_generic_environment_takes_priority_over_vllm_aliases():
    env = {
        "INFERENCE_ENGINE": "sglang",
        "INFERENCE_URL": "http://generic:30000",
        "VLLM_URL": "http://legacy:8000",
        "INFERENCE_HOST": "generic",
        "VLLM_HOST": "legacy",
        "KV_EVENT_PORT": "7001",
        "VLLM_SUB_PORT": "6001",
        "KV_EVENT_REPLAY_PORT": "7002",
        "VLLM_REPLAY_PORT": "6002",
        "KV_EVENT_TOPIC": "sglang-kv",
        "KV_EVENT_DISCOVERY_ENABLED": "false",
        "KV_EVENT_DISCOVERY_TIMEOUT_S": "3.5",
        "KV_EVENT_EXPECTED_PAGE_SIZE": "32",
        "VLLM_EVENT_TOPIC": "legacy-kv",
        "INFERENCE_TIMEOUT_S": "90",
        "VLLM_TIMEOUT_S": "45",
        "INFERENCE_HEALTH_PATH": "/health",
        "VLLM_HEALTH_PATH": "/health_generate",
    }
    with patch.dict("os.environ", env, clear=True):
        cfg = get_config()

    assert cfg.INFERENCE_ENGINE == "sglang"
    assert cfg.INFERENCE_URL == cfg.VLLM_URL == "http://generic:30000"
    assert cfg.INFERENCE_HOST == cfg.VLLM_HOST == "generic"
    assert cfg.KV_EVENT_PORT == cfg.VLLM_SUB_PORT == 7001
    assert cfg.KV_EVENT_REPLAY_PORT == 7002
    assert cfg.KV_EVENT_TOPIC == "sglang-kv"
    assert cfg.KV_EVENT_DISCOVERY_ENABLED is False
    assert cfg.KV_EVENT_DISCOVERY_TIMEOUT_S == 3.5
    assert cfg.KV_EVENT_EXPECTED_PAGE_SIZE == 32
    assert cfg.INFERENCE_TIMEOUT_S == cfg.VLLM_TIMEOUT_S == 90.0
    assert cfg.INFERENCE_HEALTH_PATH == "/health"
