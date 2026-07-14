# tests/test_router_state_pull.py
# -*- coding: utf-8 -*-
"""Component tests for the core scheduler: RouterState.pull_for_endpoint.

These construct a fresh RouterState() and drive the queue/KV/affinity/fairness
paths directly, monkeypatching the shared module-level config object
(router.router_state._cfg) to flip routing knobs. They isolate scheduling logic
from HTTP, Redis, and tokenization.
"""
import pytest

import router.router_state as rs_mod
from router.router_state import RouterState
from router.affinity import AffinityMap


@pytest.fixture
def cfg(monkeypatch):
    """Return the shared config with a clean, deterministic baseline.

    Every knob relevant to pull_for_endpoint is reset so each test starts from
    a known state regardless of process env. monkeypatch restores afterwards.
    """
    c = rs_mod._cfg
    monkeypatch.setattr(c, "KV_AWARE", False)
    monkeypatch.setattr(c, "LEN_AWARE", False)
    monkeypatch.setattr(c, "LEN_POLICY", "short_first")
    monkeypatch.setattr(c, "POOL_FACTOR", 4)
    monkeypatch.setattr(c, "POOL_BIDIRECTIONAL", False)
    monkeypatch.setattr(c, "AFFINITY_MODE", "soft")
    monkeypatch.setattr(c, "SLO_AWARE", False)
    monkeypatch.setattr(c, "FIXED_BATCH_SIZE", 0)
    monkeypatch.setattr(c, "FAIR_PULL", False)
    monkeypatch.setattr(c, "TRACE_ENABLED", False)
    monkeypatch.setattr(c, "ROUTER_LOG_BLOCK_HASHES", False)
    return c


def _inject(rs, items, model=None):
    """Append (rid, prompt, ts, meta) tuples directly into a model queue."""
    model = model or rs_mod._DEFAULT_MODEL
    q = rs._get_queue(model)
    for rid, prompt, meta in items:
        q.append((rid, prompt, 0.0, meta))


def test_want_zero_returns_empty(cfg):
    rs = RouterState()
    _inject(rs, [("r1", "p", {})])
    assert rs.pull_for_endpoint("epA", 0) == []


def test_basic_fifo_pull(cfg):
    rs = RouterState()
    _inject(rs, [("r1", "a", {}), ("r2", "b", {}), ("r3", "c", {})])
    items = rs.pull_for_endpoint("epA", 2)
    assert [i.req_id for i in items] == ["r1", "r2"]
    # The un-pulled item remains queued.
    assert rs.size() == 1


def test_kv_aware_orders_by_prefix_hits(cfg, clean_kv_state):
    monkeypatch_kv = clean_kv_state
    rs_mod._cfg.KV_AWARE = True
    rs = RouterState()
    # rA has 3 contiguous hits on epA, rB has 1, rC has 0.
    monkeypatch_kv.register_request_blocks("rA", [1, 2, 3])
    monkeypatch_kv.set_request_owners("rA", {1: {"epA"}, 2: {"epA"}, 3: {"epA"}})
    monkeypatch_kv.register_request_blocks("rB", [1, 2])
    monkeypatch_kv.set_request_owners("rB", {1: {"epA"}})
    monkeypatch_kv.register_request_blocks("rC", [1])
    monkeypatch_kv.set_request_owners("rC", {})
    _inject(rs, [("rC", "c", {}), ("rA", "a", {}), ("rB", "b", {})])

    items = rs.pull_for_endpoint("epA", 3)
    assert [i.req_id for i in items] == ["rA", "rB", "rC"]


def test_length_aware_within_equal_kv_tier(cfg):
    rs_mod._cfg.KV_AWARE = False
    rs_mod._cfg.LEN_AWARE = True
    rs_mod._cfg.LEN_POLICY = "short_first"
    rs = RouterState()
    # No KV hits -> single tier -> length-aware ordering by prompt length.
    _inject(rs, [
        ("long", "x" * 100, {}),
        ("short", "xx", {}),
        ("mid", "x" * 40, {}),
    ])
    items = rs.pull_for_endpoint("epA", 3)
    assert [i.req_id for i in items] == ["short", "mid", "long"]


def test_fixed_batch_size_caps_grant(cfg):
    rs_mod._cfg.FIXED_BATCH_SIZE = 2
    rs = RouterState()
    _inject(rs, [("r1", "a", {}), ("r2", "b", {}), ("r3", "c", {}), ("r4", "d", {})])
    items = rs.pull_for_endpoint("epA", 10)
    assert len(items) == 2
    assert rs.size() == 2


def test_pool_factor_limits_scan(cfg):
    # pool_factor=1, want=1 -> only the head item is even considered, so KV
    # ordering can't pull a better item from deeper in the queue.
    rs_mod._cfg.POOL_FACTOR = 1
    rs = RouterState()
    _inject(rs, [("r1", "a", {}), ("r2", "b", {})])
    items = rs.pull_for_endpoint("epA", 1)
    assert [i.req_id for i in items] == ["r1"]


def test_inflight_incremented_on_dispatch(cfg):
    rs = RouterState()
    _inject(rs, [("r1", "a", {}), ("r2", "b", {})])
    rs.pull_for_endpoint("epA", 2)
    assert rs.get_endpoint_inflight("epA") == 2


def test_release_inflight_is_idempotent(cfg):
    rs = RouterState()
    _inject(rs, [("r1", "a", {})])
    rs.pull_for_endpoint("epA", 1)
    assert rs.get_endpoint_inflight("epA") == 1
    rs.release_inflight("r1")
    assert rs.get_endpoint_inflight("epA") == 0
    # Second release is a no-op (cannot go negative / double-decrement).
    rs.release_inflight("r1")
    assert rs.get_endpoint_inflight("epA") == 0


def test_requeue_front_preserves_order(cfg):
    rs = RouterState()
    _inject(rs, [("r3", "c", {})])
    rs.requeue_front(rs_mod._DEFAULT_MODEL, [("r1", "a", 0.0, {}), ("r2", "b", 0.0, {})])
    items = rs.pull_for_endpoint("epA", 3)
    assert [i.req_id for i in items] == ["r1", "r2", "r3"]


def test_fair_pull_trims_overloaded_pod(cfg):
    rs_mod._cfg.FAIR_PULL = True
    rs_mod._cfg.FAIR_MARGIN = 1.0
    rs_mod._cfg.FAIR_FLOOR = 1
    rs = RouterState()
    # Two known endpoints; epA is heavily loaded, epB idle. avg = 4, ceiling = 4.
    rs._inflight_by_endpoint = {"epA": 8, "epB": 0}
    rs._last_pull_ts = {"epA": 0.0, "epB": 0.0}
    _inject(rs, [(f"r{i}", "p", {}) for i in range(5)])
    # epA is above the ceiling -> only the floor (1) movable item granted.
    items = rs.pull_for_endpoint("epA", 5)
    assert len(items) == 1


def test_fair_pull_no_op_for_underloaded_pod(cfg):
    rs_mod._cfg.FAIR_PULL = True
    rs_mod._cfg.FAIR_MARGIN = 1.0
    rs_mod._cfg.FAIR_FLOOR = 1
    rs = RouterState()
    rs._inflight_by_endpoint = {"epA": 8, "epB": 0}
    rs._last_pull_ts = {"epA": 0.0, "epB": 0.0}
    _inject(rs, [(f"r{i}", "p", {}) for i in range(5)])
    # epB is well below the ceiling -> gets its full want.
    items = rs.pull_for_endpoint("epB", 3)
    assert len(items) == 3


def test_soft_affinity_prefers_matched_endpoint(cfg):
    rs = RouterState()
    rs._affinity = AffinityMap(300.0)
    rs._affinity.claim("conv-x", "epA")
    _inject(rs, [
        ("r1", "a", {}),
        ("r2", "b", {"__affinity_key__": "conv-x"}),
        ("r3", "c", {}),
    ])
    # r2 is pinned (soft) to epA -> it should sort to the front of its tier.
    items = rs.pull_for_endpoint("epA", 3)
    assert items[0].req_id == "r2"


def test_size_per_model(cfg):
    rs = RouterState()
    _inject(rs, [("r1", "a", {})], model="m1")
    _inject(rs, [("r2", "b", {}), ("r3", "c", {})], model="m2")
    assert rs.size("m1") == 1
    assert rs.size("m2") == 2
    assert rs.size() == 3
