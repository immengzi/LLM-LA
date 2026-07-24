# tests/test_push_router_leastq.py
# -*- coding: utf-8 -*-
"""Unit tests for push-least-queue (local) in-flight accounting.

These tests pin down the invariant broken by issue #12: in ``local`` mode the
router keeps a per-pod logical in-flight counter that must be

  * incremented (+1) when a request is dispatched (``route_and_push``), and
  * decremented (-1) when the request completes (``notify_result``).

If ``notify_result`` never runs, the counters only grow and least-queue
selection degenerates into round-robin. The tests below exercise the counter
in isolation, the selector, the dispatch increment, and a full
dispatch/complete cycle that reproduces both the healthy and the degenerate
(round-robin) behaviours.
"""
import pytest

from router.push_router import PushRouter


def _make_leastq_router(endpoints):
    """Build a push-leastq/local router with a fixed, discovery-free fleet."""
    r = PushRouter(mode="push-leastq")
    r._leastq_mode = "local"
    # Freeze the endpoint snapshot so no Kubernetes discovery runs.
    r._eps = list(endpoints)
    r._urls = {ep: f"http://{ep}:8000" for ep in endpoints}
    r._ensure_endpoints = lambda: None            # type: ignore[method-assign]
    r._refresh_endpoints_locked = lambda **_: None  # type: ignore[method-assign]
    return r


# ---------------------------------------------------------------------------
# notify_result: the decrement half of the accounting
# ---------------------------------------------------------------------------

def test_notify_result_decrements_inflight():
    r = _make_leastq_router(["A", "B"])
    r._logical_inflight["A"] = 3
    r.notify_result("A")
    assert r._logical_inflight["A"] == 2


def test_notify_result_floors_at_zero():
    """A stray/duplicate completion must never drive the counter negative."""
    r = _make_leastq_router(["A"])
    r._logical_inflight["A"] = 0
    r.notify_result("A")
    assert r._logical_inflight["A"] == 0


def test_notify_result_ignores_unknown_endpoint():
    """Results for a pod no longer in the fleet must not touch the counters."""
    r = _make_leastq_router(["A", "B"])
    r._logical_inflight["A"] = 1
    r.notify_result("ghost-pod")
    assert dict(r._logical_inflight) == {"A": 1}


@pytest.mark.parametrize("bad", [None, ""])
def test_notify_result_ignores_empty_endpoint(bad):
    r = _make_leastq_router(["A"])
    r._logical_inflight["A"] = 2
    r.notify_result(bad)
    assert r._logical_inflight["A"] == 2


# ---------------------------------------------------------------------------
# selection: pick the least-loaded pod
# ---------------------------------------------------------------------------

def test_leastq_local_picks_least_loaded():
    r = _make_leastq_router(["A", "B", "C"])
    r._logical_inflight.update({"A": 5, "B": 1, "C": 9})
    assert r._pick_endpoint_leastq_local() == "B"


def test_leastq_local_empty_fleet_returns_none():
    r = _make_leastq_router([])
    assert r._pick_endpoint_leastq_local() is None


# ---------------------------------------------------------------------------
# dispatch: the increment half of the accounting
# ---------------------------------------------------------------------------

class _FakeResp:
    status_code = 200
    text = ""


@pytest.mark.asyncio
async def test_route_and_push_increments_inflight(monkeypatch):
    r = _make_leastq_router(["A", "B"])
    r._logical_inflight.update({"A": 0, "B": 4})  # A is the least-loaded target

    async def _fake_post(url, json=None):
        return _FakeResp()

    monkeypatch.setattr(r._push_client, "post", _fake_post)

    await r.route_and_push("req-1", "hello", {})

    # The picked (least-loaded) endpoint A must have been incremented.
    assert r._logical_inflight["A"] == 1
    assert r._logical_inflight["B"] == 4


@pytest.mark.asyncio
async def test_route_and_push_rolls_back_increment_on_push_failure(monkeypatch):
    """A failed push must not leave a phantom in-flight slot behind."""
    r = _make_leastq_router(["A"])

    async def _boom(url, json=None):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(r._push_client, "post", _boom)

    with pytest.raises(Exception):
        await r.route_and_push("req-1", "hello", {})

    assert r._logical_inflight["A"] == 0


# ---------------------------------------------------------------------------
# end-to-end behaviour: healthy least-queue vs. the round-robin regression
# ---------------------------------------------------------------------------

def _dispatch_pick(r, endpoint=None):
    """Simulate the counter side-effect of a dispatch to ``endpoint``.

    When ``endpoint`` is None the router selects the least-loaded pod itself,
    mirroring push-leastq/local selection.
    """
    ep = endpoint or r._pick_endpoint_leastq_local()
    with r._lock:
        r._logical_inflight[ep] += 1
    return ep


def test_least_queue_steers_toward_the_fast_worker():
    """With notify_result wired up, a fast pod that keeps completing work
    keeps looking least-loaded and therefore keeps being picked — the whole
    point of least-queue routing on a heterogeneous fleet."""
    r = _make_leastq_router(["fast", "slow1", "slow2"])
    counts = {"fast": 0, "slow1": 0, "slow2": 0}

    for _ in range(300):
        ep = _dispatch_pick(r)
        counts[ep] += 1
        # "fast" finishes immediately; the slow pods stay busy (no completion).
        if ep == "fast":
            r.notify_result("fast")

    # The fast worker should absorb the lion's share of traffic.
    assert counts["fast"] > counts["slow1"] + counts["slow2"]
    # And its logical backlog stays at zero because every dispatch completes.
    assert r._logical_inflight["fast"] == 0


def test_without_notify_result_degenerates_to_round_robin():
    """Regression guard for issue #12: if completions are never signalled the
    counters only grow and selection spreads work uniformly (round-robin),
    regardless of how fast each pod actually is."""
    r = _make_leastq_router(["fast", "slow1", "slow2"])
    counts = {"fast": 0, "slow1": 0, "slow2": 0}

    for _ in range(300):
        ep = _dispatch_pick(r)  # never call notify_result -> the bug
        counts[ep] += 1

    # Every pod received the same 1/N share: the "least-queue" signal is gone.
    assert counts["fast"] == counts["slow1"] == counts["slow2"] == 100
