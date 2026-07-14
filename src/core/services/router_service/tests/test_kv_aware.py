# tests/test_kv_aware.py
# -*- coding: utf-8 -*-
"""Unit tests for the KV-prefix ownership/scoring state (router.kv_aware)."""


def test_prefix_len_uses_fresh_per_request_owners(clean_kv_state):
    kv = clean_kv_state
    kv.register_request_blocks("r1", [10, 20, 30, 40])
    # epA owns a contiguous leading run of 3; block 40 owned by nobody.
    kv.set_request_owners(
        "r1",
        {10: {"epA", "epB"}, 20: {"epA"}, 30: {"epA"}},
    )
    assert kv.prefix_len("epA", "r1") == 3
    assert kv.prefix_len("epB", "r1") == 1
    assert kv.prefix_len("epC", "r1") == 0


def test_prefix_len_is_contiguous_from_head(clean_kv_state):
    kv = clean_kv_state
    kv.register_request_blocks("r1", [1, 2, 3])
    # epA owns block 1 and 3 but NOT 2 -> contiguous prefix stops at 1.
    kv.set_request_owners("r1", {1: {"epA"}, 3: {"epA"}})
    assert kv.prefix_len("epA", "r1") == 1


def test_prefix_len_zero_for_unknown_request(clean_kv_state):
    assert clean_kv_state.prefix_len("epA", "missing") == 0


def test_prefix_len_falls_back_to_watcher_map(clean_kv_state):
    kv = clean_kv_state
    kv.register_request_blocks("r1", [1, 2, 3])
    # No per-request owners recorded -> fall back to legacy global map.
    kv.register_block_owners(1, ["epA"])
    kv.register_block_owners(2, ["epA"])
    assert kv.prefix_len("epA", "r1") == 2


def test_per_request_owners_take_priority_over_watcher(clean_kv_state):
    kv = clean_kv_state
    kv.register_request_blocks("r1", [1, 2, 3])
    kv.register_block_owners(1, ["epA"])
    kv.register_block_owners(2, ["epA"])
    kv.register_block_owners(3, ["epA"])
    # Fresh lookup says epA only owns the first block (eviction-aware).
    kv.set_request_owners("r1", {1: {"epA"}})
    assert kv.prefix_len("epA", "r1") == 1


def test_drop_request_clears_state(clean_kv_state):
    kv = clean_kv_state
    kv.register_request_blocks("r1", [1, 2])
    kv.set_request_owners("r1", {1: {"epA"}})
    kv.drop_request("r1")
    assert kv.get_request_blocks("r1") == []
    assert kv.prefix_len("epA", "r1") == 0


def test_record_and_pop_routing(clean_kv_state):
    kv = clean_kv_state
    kv.record_routing("r1", endpoint="epA", kv_hits_len=2, total_blocks=5,
                      affinity_key="abc")
    info = kv.pop_routing("r1")
    assert info["endpoint"] == "epA"
    assert info["kv_hits_len"] == 2
    assert info["total_blocks"] == 5
    assert info["affinity_key"] == "abc"
    # Pop is one-shot.
    assert kv.pop_routing("r1") is None


def test_routing_eviction_bound(clean_kv_state, monkeypatch):
    kv = clean_kv_state
    monkeypatch.setattr(kv, "_ROUTING_MAX", 5, raising=False)
    for i in range(20):
        kv.record_routing(f"r{i}", endpoint="ep", kv_hits_len=0, total_blocks=0)
    # The oldest entries were evicted; only the last few survive.
    assert kv.pop_routing("r0") is None
    assert kv.pop_routing("r19") is not None
