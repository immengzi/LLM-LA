# tests/test_config.py
# -*- coding: utf-8 -*-
"""Unit tests for RouterConfig env parsing, normalization and the model registry."""
import textwrap

import pytest

from router.config import RouterConfig, load_model_registry


def test_defaults_are_sane():
    cfg = RouterConfig()
    assert cfg.PORT == 8080
    assert cfg.ROUTER_MODE == "pull"
    assert cfg.KV_AWARE is True
    assert cfg.AFFINITY_ENABLED is False
    assert cfg.KV_HASH_SOURCE == "inline"
    assert cfg.KV_OWNER_SOURCE == "lookup"


def test_router_strategy_overrides_flags(reset_config, monkeypatch):
    cases = {
        "none": (False, False),
        "prefix": (True, False),
        "affinity": (False, True),
        "both": (True, True),
    }
    for strategy, (want_kv, want_aff) in cases.items():
        monkeypatch.setenv("ROUTER_STRATEGY", strategy)
        # Set the individual flags to the opposite to prove the override wins.
        monkeypatch.setenv("KV_AWARE", str(not want_kv))
        monkeypatch.setenv("AFFINITY_ENABLED", str(not want_aff))
        cfg = reset_config()
        assert cfg.KV_AWARE is want_kv, strategy
        assert cfg.AFFINITY_ENABLED is want_aff, strategy


def test_unknown_strategy_leaves_flags_untouched(reset_config, monkeypatch):
    monkeypatch.setenv("ROUTER_STRATEGY", "bogus")
    monkeypatch.setenv("KV_AWARE", "false")
    monkeypatch.setenv("AFFINITY_ENABLED", "true")
    cfg = reset_config()
    assert cfg.KV_AWARE is False
    assert cfg.AFFINITY_ENABLED is True


def test_router_mode_aliases_and_fallback(reset_config, monkeypatch):
    monkeypatch.setenv("ROUTER_MODE", "push_least_queue")
    assert reset_config().ROUTER_MODE == "push-leastq"

    monkeypatch.setenv("ROUTER_MODE", "central_push")
    assert reset_config().ROUTER_MODE == "central-push"

    monkeypatch.setenv("ROUTER_MODE", "nonsense")
    assert reset_config().ROUTER_MODE == "pull"


def test_sidecar_enabled_default_true(reset_config, monkeypatch):
    monkeypatch.delenv("ROUTER_SIDECAR_ENABLED", raising=False)
    cfg = reset_config()
    assert cfg.ROUTER_SIDECAR_ENABLED is True
    assert cfg.VLLM_KV_EVENTS_PORT == 5557
    assert cfg.VLLM_KV_EVENTS_TOPIC == "kv@"


def test_sidecar_disabled_takes_effect_for_push_and_central_push(reset_config, monkeypatch):
    # push-* and central-push honor the disable flag ...
    for mode in (
        "central-push",
        "push-rr",
        "push-random",
        "push-leastq",
        "push-throughput",
        "push-p2c",
        "push-kv-cost",
        "push-least-kv",
        "push-least-latency",
        "push-least-busy",
    ):
        monkeypatch.setenv("ROUTER_MODE", mode)
        monkeypatch.setenv("ROUTER_SIDECAR_ENABLED", "false")
        cfg = reset_config()
        assert cfg.ROUTER_MODE == mode
        assert cfg.ROUTER_SIDECAR_ENABLED is False, mode

    # ... but for pull / external-push the false value is ignored (kept on).
    for mode in ("pull", "external-push"):
        monkeypatch.setenv("ROUTER_MODE", mode)
        monkeypatch.setenv("ROUTER_SIDECAR_ENABLED", "false")
        cfg = reset_config()
        assert cfg.ROUTER_SIDECAR_ENABLED is True, mode


def test_vllm_kv_events_overrides(reset_config, monkeypatch):
    monkeypatch.setenv("VLLM_KV_EVENTS_PORT", "6000")
    monkeypatch.setenv("VLLM_KV_EVENTS_TOPIC", "  ")  # blank -> default
    cfg = reset_config()
    assert cfg.VLLM_KV_EVENTS_PORT == 6000
    assert cfg.VLLM_KV_EVENTS_TOPIC == "kv@"


def test_numeric_clamps(reset_config, monkeypatch):
    monkeypatch.setenv("POOL_FACTOR", "0")
    monkeypatch.setenv("KV_BLOCK_SIZE", "-5")
    monkeypatch.setenv("ROUTER_CENTRAL_PUSH_CAP", "0")
    cfg = reset_config()
    assert cfg.POOL_FACTOR == 1
    assert cfg.KV_BLOCK_SIZE == 1
    assert cfg.CENTRAL_PUSH_CAP == 1


def test_fair_margin_floor_and_boolean(reset_config, monkeypatch):
    monkeypatch.setenv("ROUTER_FAIR_PULL", "true")
    monkeypatch.setenv("ROUTER_FAIR_MARGIN", "0.5")  # below 1.0 -> clamped to 1.0
    cfg = reset_config()
    assert cfg.FAIR_PULL is True
    assert cfg.FAIR_MARGIN == 1.0


def test_kv_soft_divert_env(reset_config, monkeypatch):
    monkeypatch.setenv("ROUTER_KV_SOFT_DIVERT", "true")
    monkeypatch.setenv("ROUTER_KV_PRESSURE_HIGH", "0.9")
    monkeypatch.setenv("ROUTER_KV_SOFT_MIN_HITS", "2")
    cfg = reset_config()
    assert cfg.KV_SOFT_DIVERT is True
    assert cfg.KV_PRESSURE_HIGH == pytest.approx(0.9)
    assert cfg.KV_SOFT_MIN_HITS == 2


def test_result_submit_path_gets_leading_slash(reset_config, monkeypatch):
    monkeypatch.setenv("RESULT_SUBMIT_PATH", "custom_result")
    cfg = reset_config()
    assert cfg.RESULT_SUBMIT_PATH == "/custom_result"


def test_invalid_owner_source_falls_back(reset_config, monkeypatch):
    monkeypatch.setenv("KV_OWNER_SOURCE", "banana")
    assert reset_config().KV_OWNER_SOURCE == "lookup"


def test_transport_mode_validation(reset_config, monkeypatch):
    monkeypatch.setenv("TRANSPORT_MODE", "async_pubsub")
    assert reset_config().TRANSPORT_MODE == "async_pubsub"
    monkeypatch.setenv("TRANSPORT_MODE", "weird")
    assert reset_config().TRANSPORT_MODE == "sync"


def test_load_model_registry(tmp_path):
    yaml_path = tmp_path / "models.yaml"
    yaml_path.write_text(
        textwrap.dedent(
            """
            model_list:
              - model_name: qwen
                router_params:
                  label_selector: app=vllm-qwen
                  batch_size: 16
              - model_name: llama
                router_params:
                  label_selector: app=vllm-llama
              - model_name: ""      # skipped: empty name
                router_params: {}
            """
        )
    )
    reg = load_model_registry(str(yaml_path))
    assert set(reg.keys()) == {"qwen", "llama"}
    assert reg["qwen"].batch_size == 16
    assert reg["qwen"].label_selector == "app=vllm-qwen"
    assert reg["llama"].batch_size == 0
