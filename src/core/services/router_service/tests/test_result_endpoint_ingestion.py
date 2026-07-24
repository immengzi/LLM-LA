# tests/test_result_endpoint_ingestion.py
# -*- coding: utf-8 -*-
"""Contract tests for result-payload endpoint resolution in the router.

This is the router side of the issue #12 fix. ``_ingest_result_payload`` is the
single ingestion path for both ``/result`` and the submit-ack transport. It must
recover the completing pod's identity from the result payload so it can:

  * call ``PushRouter.notify_result`` (push-leastq/local decrement), and
  * call ``RouterState.release_inflight`` (pull-fairness / central-push decrement).

The sidecar fix sends a top-level ``endpoint`` field on every payload (success
*and* error). These tests verify that field is honoured, that the historical
``result.endpoint_id`` still works as a fallback, and that a payload carrying
neither (the pre-fix error path) signals no completion — the exact regression
that made least-queue behave like round-robin.
"""
import pytest

import router.api as api_mod


class _RecordingPushRouter:
    """Stand-in for the module-global PushRouter that records completions."""

    def __init__(self):
        self.notified = []

    def notify_result(self, endpoint):
        self.notified.append(endpoint)


@pytest.fixture
def ingest(monkeypatch):
    """Wire fakes into the api module and return (call, push_router, released).

    ``call`` invokes ``_ingest_result_payload``; ``push_router.notified`` and
    ``released`` capture the two decrement side-effects.
    """
    fake_pr = _RecordingPushRouter()
    monkeypatch.setattr(api_mod, "_push_router", fake_pr)

    released = []
    monkeypatch.setattr(
        api_mod.router_state,
        "release_inflight",
        lambda rid, endpoint=None: released.append((rid, endpoint)),
    )
    # Keep the test focused on endpoint resolution: neutralise result storage
    # and SLO side-effects so nothing else touches process-global state.
    monkeypatch.setattr(api_mod.router_state, "store_result", lambda *a, **k: None)
    monkeypatch.setattr(api_mod, "_ingest_slo_actuals", lambda *a, **k: None)

    return api_mod._ingest_result_payload, fake_pr, released


# ---------------------------------------------------------------------------
# The fix: top-level "endpoint"
# ---------------------------------------------------------------------------

def test_top_level_endpoint_triggers_completion(ingest):
    call, pr, released = ingest
    call({"req_id": "r1", "endpoint": "pod-a", "result": {"output": "ok"}})
    assert pr.notified == ["pod-a"]
    assert released == [("r1", "pod-a")]


def test_error_payload_with_endpoint_triggers_completion(ingest):
    """The sidecar error path now carries endpoint too, so a failed request
    still releases its in-flight slot instead of leaking it."""
    call, pr, released = ingest
    call({
        "req_id": "r-err",
        "endpoint": "pod-a",
        "result": {"output": "[sidecar error: boom]", "finish_reason": "error"},
    })
    assert pr.notified == ["pod-a"]
    assert released == [("r-err", "pod-a")]


# ---------------------------------------------------------------------------
# Backward-compatible fallback: result.endpoint_id
# ---------------------------------------------------------------------------

def test_endpoint_id_fallback_when_top_level_missing(ingest):
    call, pr, released = ingest
    call({"req_id": "r2", "result": {"output": "ok", "endpoint_id": "pod-b"}})
    assert pr.notified == ["pod-b"]
    assert released == [("r2", "pod-b")]


def test_top_level_endpoint_takes_precedence_over_endpoint_id(ingest):
    call, pr, released = ingest
    call({
        "req_id": "r3",
        "endpoint": "pod-top",
        "result": {"output": "ok", "endpoint_id": "pod-nested"},
    })
    assert pr.notified == ["pod-top"]
    assert released == [("r3", "pod-top")]


# ---------------------------------------------------------------------------
# The regression: no endpoint identity => no completion signal
# ---------------------------------------------------------------------------

def test_missing_endpoint_signals_no_completion(ingest):
    """Reproduces the pre-fix error path: neither top-level endpoint nor
    endpoint_id, so notify_result is never called and least-queue would leak."""
    call, pr, released = ingest
    call({"req_id": "r4", "result": {"output": "[sidecar error: boom]"}})
    assert pr.notified == []
    # release_inflight is still invoked (idempotent), but with no endpoint hint.
    assert released == [("r4", None)]


# ---------------------------------------------------------------------------
# Guard rails: malformed payloads are ignored, not crashed on
# ---------------------------------------------------------------------------

def test_missing_req_id_is_ignored(ingest):
    call, pr, released = ingest
    call({"endpoint": "pod-a", "result": {"output": "ok"}})
    assert pr.notified == []
    assert released == []


def test_missing_result_is_ignored(ingest):
    call, pr, released = ingest
    call({"req_id": "r5", "endpoint": "pod-a"})
    assert pr.notified == []
    assert released == []
