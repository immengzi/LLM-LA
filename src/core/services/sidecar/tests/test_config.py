# tests/test_config.py
# -*- coding: utf-8 -*-
"""Unit tests for SidecarConfig env parsing (sidecar.config)."""
from sidecar.config import get_config


def test_defaults():
    cfg = get_config()
    assert cfg.SIDECAR_MODE == "pull"
    assert cfg.BATCH_SIZE == 8
    assert cfg.PREFETCH == 0
    assert cfg.RESULT_TRANSPORT_MODE == "sync"


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
