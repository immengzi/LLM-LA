# tests/test_api_endpoints.py
# -*- coding: utf-8 -*-
"""Component tests for the router FastAPI surface via Starlette's TestClient.

TestClient is constructed WITHOUT the context-manager form so the app's startup
hook (which would spawn the KVWatcher thread and can fatally init the tokenizer)
does not run. Handlers that only touch in-process state (health, metrics, pull,
result) are exercised directly; the full enqueue->result roundtrip is covered by
the docker-compose e2e suite instead.
"""
import pytest
from fastapi.testclient import TestClient

import router.api as api_mod


@pytest.fixture
def client():
    return TestClient(api_mod.app)


@pytest.fixture(autouse=True)
def clean_router_state():
    """Reset the module-singleton RouterState between tests."""
    rs = api_mod.router_state
    with rs._lock:
        rs._queues.clear()
        rs._result_values.clear()
        rs._result_store_ts.clear()
        rs._result_futs.clear()
        rs._inflight_by_endpoint.clear()
        rs._req_endpoint.clear()
    yield
    with rs._lock:
        rs._queues.clear()


def test_health_router_ok(client):
    r = client.get("/health/router")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert isinstance(body["queue_len"], int)


def test_metrics_endpoint(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    assert "router_central_queue_length" in r.text


def test_health_backends_503_when_no_pods(client, monkeypatch):
    monkeypatch.setattr(api_mod, "_discover_vllm_leaders", lambda cfg: {})
    r = client.get("/health/backends")
    assert r.status_code == 503
    assert r.json()["status"] == "unhealthy"


def test_health_backends_503_on_discovery_failure(client, monkeypatch):
    monkeypatch.setattr(api_mod, "_discover_vllm_leaders", lambda cfg: None)
    r = client.get("/health/backends")
    assert r.status_code == 503


def test_pull_returns_enqueued_items(client):
    rid = api_mod.router_state.enqueue("hello", None, {}, model="")
    r = client.post("/pull", json={"endpoint": "pod-a", "want": 4})
    assert r.status_code == 200
    items = r.json()["items"]
    assert any(it["req_id"] == rid for it in items)


def test_pull_empty_queue_returns_no_items(client):
    r = client.post("/pull", json={"endpoint": "pod-a", "want": 4})
    assert r.status_code == 200
    assert r.json()["items"] == []


def test_result_callback_ok_and_resolves_waiter(client):
    rid = api_mod.router_state.enqueue("hello", None, {}, model="")
    # Simulate the sidecar posting a completion back to the router.
    r = client.post("/result", json={
        "req_id": rid,
        "result": {"output": "world", "endpoint_id": "pod-a"},
    })
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    # The result is now retrievable from router state.
    assert api_mod.router_state._result_values.get(rid)["output"] == "world"


def test_submit_returns_202(client, monkeypatch):
    async def _noop_register(rid, prompt, *, meta, is_pull_mode):
        return meta
    monkeypatch.setattr(api_mod, "_maybe_register_kv_blocks", _noop_register)
    r = client.post("/submit", json={"prompt": "hi"})
    assert r.status_code == 202
    assert "req_id" in r.json()


def test_enqueue_times_out_with_504(client, monkeypatch):
    async def _noop_register(rid, prompt, *, meta, is_pull_mode):
        return meta
    monkeypatch.setattr(api_mod, "_maybe_register_kv_blocks", _noop_register)
    monkeypatch.setattr(api_mod._cfg, "RESULT_TIMEOUT_S", 0.2)
    # No sidecar posts a result -> the router should give up with 504.
    r = client.post("/enqueue", json={"prompt": "hi"})
    assert r.status_code == 504


def test_debug_slo_summary(client):
    r = client.get("/debug/slo")
    assert r.status_code == 200
    body = r.json()
    assert "total" in body and "by_type" in body


def test_latency_log_endpoint(client):
    r = client.get("/latency_log?last=10")
    assert r.status_code == 200
    assert isinstance(r.json(), list)
