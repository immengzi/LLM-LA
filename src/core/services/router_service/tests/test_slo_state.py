# tests/test_slo_state.py
# -*- coding: utf-8 -*-
"""Unit tests for the per-request SLO registry (router.slo_state)."""
import time

from router.slo_state import SLORegistry


def test_register_ttft_sets_absolute_deadline():
    reg = SLORegistry()
    now = time.time()
    e = reg.register("r1", slo_type="ttft", slo_ttft_ms=500, arrival_ts=now)
    assert e.slo_type == "ttft"
    assert abs(e.deadline_ttft - (now + 0.5)) < 1e-6
    assert reg.has_slo("r1") is True


def test_register_tpot_is_budget_not_timestamp():
    reg = SLORegistry()
    e = reg.register("r1", slo_type="tpot", slo_tpot_ms=40)
    assert e.deadline_tpot_s == 0.04
    assert e.deadline_ttft is None


def test_register_combined_and_e2e():
    reg = SLORegistry()
    now = time.time()
    e = reg.register("r1", slo_type="ttft+tpot", slo_ttft_ms=200, slo_tpot_ms=30,
                     arrival_ts=now)
    assert abs(e.deadline_ttft - (now + 0.2)) < 1e-6
    assert e.deadline_tpot_s == 0.03

    e2 = reg.register("r2", slo_type="e2e", slo_e2e_ms=1000, arrival_ts=now)
    assert abs(e2.deadline_e2e - (now + 1.0)) < 1e-6


def test_no_slo_type_has_slo_false():
    reg = SLORegistry()
    reg.register("r1")
    assert reg.has_slo("r1") is False
    assert reg.get("r1") is not None


def test_update_dispatch_and_ingest_result():
    reg = SLORegistry()
    reg.register("r1", slo_type="ttft", slo_ttft_ms=500)
    reg.update_dispatch("r1", endpoint="epA", slack=0.1, binding_constraint="ttft")
    e = reg.get("r1")
    assert e.assigned_endpoint == "epA"
    assert e.slack == 0.1
    assert e.dispatch_ts is not None

    out = reg.ingest_result("r1", actual_ttft=0.42, actual_output_len=17)
    assert out.actual_ttft == 0.42
    assert out.actual_output_len == 17


def test_ingest_result_unknown_returns_none():
    reg = SLORegistry()
    assert reg.ingest_result("ghost", actual_ttft=1.0) is None


def test_cleanup_expired():
    reg = SLORegistry(ttl_s=1.0)
    reg.register("old", arrival_ts=time.time() - 10.0)
    reg.register("new", arrival_ts=time.time())
    removed = reg.cleanup_expired()
    assert removed == 1
    assert reg.get("old") is None
    assert reg.get("new") is not None


def test_debug_summary_counts_by_type():
    reg = SLORegistry()
    reg.register("a", slo_type="ttft", slo_ttft_ms=100)
    reg.register("b", slo_type="ttft", slo_ttft_ms=100)
    reg.register("c", slo_type="e2e", slo_e2e_ms=100)
    reg.register("d")  # no SLO
    summary = reg.debug_summary()
    assert summary["total"] == 4
    assert summary["with_slo"] == 3
    assert summary["by_type"]["ttft"] == 2
    assert summary["by_type"]["e2e"] == 1
    assert summary["by_type"]["none"] == 1
