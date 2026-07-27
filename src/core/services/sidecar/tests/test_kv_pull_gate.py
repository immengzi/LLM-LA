# tests/test_kv_pull_gate.py
"""KV-memory pull gate (P1) unit tests.

Covers the pure decision logic of RouterPullWorker._apply_kv_pull_gate:
disabled -> passthrough, no sample -> fail open, and the
stop / taper / passthrough behavior across the [LOW, HIGH] window.
"""
from __future__ import annotations

import pytest

from sidecar import kv_usage as kv_usage_mod
from sidecar.local_queue import LocalQueue
from sidecar.router_client import RouterPullWorker, _cfg


@pytest.fixture
def worker():
    return RouterPullWorker(local_q=LocalQueue("ep-test"), endpoint_id="ep-test")


@pytest.fixture(autouse=True)
def gate_defaults():
    """Snapshot/restore the gate config so tests don't leak into each other."""
    saved = (
        _cfg.KV_PULL_GATE_ENABLED,
        _cfg.KV_PULL_GATE_HIGH,
        _cfg.KV_PULL_GATE_LOW,
    )
    _cfg.KV_PULL_GATE_ENABLED = True
    _cfg.KV_PULL_GATE_HIGH = 0.90
    _cfg.KV_PULL_GATE_LOW = 0.70
    yield
    (
        _cfg.KV_PULL_GATE_ENABLED,
        _cfg.KV_PULL_GATE_HIGH,
        _cfg.KV_PULL_GATE_LOW,
    ) = saved


def _set_kv(monkeypatch, value):
    monkeypatch.setattr(kv_usage_mod, "get_cached_kv_usage", lambda: value)


def test_disabled_is_passthrough(worker, monkeypatch):
    _cfg.KV_PULL_GATE_ENABLED = False
    _set_kv(monkeypatch, 0.99)  # would block if enabled
    assert worker._apply_kv_pull_gate(8) == 8


def test_no_sample_fails_open(worker, monkeypatch):
    _set_kv(monkeypatch, None)
    assert worker._apply_kv_pull_gate(8) == 8


def test_below_low_is_passthrough(worker, monkeypatch):
    _set_kv(monkeypatch, 0.50)
    assert worker._apply_kv_pull_gate(8) == 8


def test_at_or_above_high_blocks(worker, monkeypatch):
    _set_kv(monkeypatch, 0.90)
    assert worker._apply_kv_pull_gate(8) == 0
    _set_kv(monkeypatch, 0.97)
    assert worker._apply_kv_pull_gate(8) == 0


def test_midpoint_tapers(worker, monkeypatch):
    # kv=0.80 -> scale ~= (0.90-0.80)/(0.90-0.70) = 0.5 -> ~half of 8.
    # int() truncation + float noise can land on 3 or 4; both are valid taper.
    _set_kv(monkeypatch, 0.80)
    assert worker._apply_kv_pull_gate(8) in (3, 4)


def test_taper_is_monotonic_non_increasing(worker, monkeypatch):
    last = 100
    for kv in [0.70, 0.75, 0.80, 0.85, 0.89, 0.90]:
        _set_kv(monkeypatch, kv)
        got = worker._apply_kv_pull_gate(16)
        assert got <= last
        last = got
    assert last == 0


def test_gate_never_raises_want(worker, monkeypatch):
    _set_kv(monkeypatch, 0.85)
    assert worker._apply_kv_pull_gate(4) <= 4
