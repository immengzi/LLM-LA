# tests/test_api.py
# -*- coding: utf-8 -*-
"""Component tests for the sidecar FastAPI surface (sidecar.api)."""
import pytest
from fastapi.testclient import TestClient

import sidecar.api as api_mod
from sidecar.local_queue import LocalQueue


@pytest.fixture
def client():
    return TestClient(api_mod.app)


@pytest.fixture(autouse=True)
def reset_globals(monkeypatch):
    """Start each test with a fresh bound queue, no pull worker, healthy vLLM."""
    api_mod.bind_local_queue(LocalQueue("pod-test"))
    monkeypatch.setattr(api_mod, "_pull_worker", None)

    async def _healthy():
        return True
    monkeypatch.setattr(api_mod, "_vllm_healthy_cached", _healthy)
    yield


def test_metrics(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    assert "sidecar_queue_length" in r.text


def test_health_ok_without_pull_worker(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["vllm_healthy"] is True
    assert body["logical"] == 0


def test_health_reports_queue_state(client):
    api_mod._local_q.put("r1", "p", {})
    r = client.get("/health")
    body = r.json()
    assert body["queue_len"] == 1
    assert body["logical"] == 1


def test_health_503_when_vllm_unhealthy(client, monkeypatch):
    class _UnhealthyWorker:
        vllm_healthy = False
    monkeypatch.setattr(api_mod, "_pull_worker", _UnhealthyWorker())
    r = client.get("/health")
    assert r.status_code == 503
    assert r.json()["status"] == "vllm_unhealthy"


def test_push_accepts_and_enqueues(client):
    r = client.post("/push", json={"req_id": "r1", "prompt": "hi", "meta": {}})
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    assert api_mod._local_q.state()[0] == 1  # pending increased


def test_push_503_when_vllm_unhealthy(client, monkeypatch):
    async def _unhealthy():
        return False
    monkeypatch.setattr(api_mod, "_vllm_healthy_cached", _unhealthy)
    r = client.post("/push", json={"req_id": "r1", "prompt": "hi"})
    assert r.status_code == 503
    assert r.json()["reason"] == "vllm_unhealthy"


def test_push_503_when_queue_full(client, monkeypatch):
    monkeypatch.setattr(api_mod._cfg, "BATCH_SIZE", 1)
    monkeypatch.setattr(api_mod._cfg, "PREFETCH", 0)
    # Pre-fill to capacity so the next push is rejected as busy.
    api_mod._local_q.put("existing", "p", {})
    r = client.post("/push", json={"req_id": "r2", "prompt": "hi"})
    assert r.status_code == 503
    assert r.json()["reason"] == "queue_full"
