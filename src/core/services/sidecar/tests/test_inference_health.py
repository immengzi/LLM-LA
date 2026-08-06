import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import requests

SIDECAR_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SIDECAR_ROOT))

from sidecar import api, router_client  # noqa: E402


class EmptyQueue:
    def state(self):
        return (2, 1)


def test_router_probe_uses_generic_url_path_and_aliases(monkeypatch):
    requested = []

    def fake_get(url, timeout):
        requested.append((url, timeout))
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(router_client._cfg, "INFERENCE_URL", "http://engine:30000/")
    monkeypatch.setattr(
        router_client._cfg, "INFERENCE_HEALTH_PATH", "/health"
    )
    monkeypatch.setattr(
        router_client._cfg, "INFERENCE_READINESS_PATH", "/health_generate"
    )
    monkeypatch.setattr(router_client.requests, "get", fake_get)
    worker = router_client.RouterPullWorker(EmptyQueue(), "pod-1")

    assert worker.check_engine_health() is True
    assert requested == [("http://engine:30000/health_generate", 2.0)]
    assert worker.engine_healthy is True
    assert worker.vllm_healthy is True
    assert worker.check_vllm_health() is True
    assert len(requested) == 1


def test_api_probe_uses_generic_url_and_health_path(monkeypatch):
    requested = []

    def fake_get(url, timeout):
        requested.append((url, timeout))
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(api._cfg, "INFERENCE_URL", "http://engine:30000/")
    monkeypatch.setattr(api._cfg, "INFERENCE_HEALTH_PATH", "health")
    monkeypatch.setattr(api.requests, "get", fake_get)

    assert api._probe_engine_sync() is True
    assert api._probe_vllm_sync() is True
    assert requested == [
        ("http://engine:30000/health", 2.0),
        ("http://engine:30000/health", 2.0),
    ]


def test_health_generate_is_readiness_only(monkeypatch):
    requested = []

    def fake_get(url, timeout):
        requested.append(url)
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(api._cfg, "INFERENCE_ENGINE", "sglang")
    monkeypatch.setattr(api._cfg, "INFERENCE_URL", "http://engine:30000")
    monkeypatch.setattr(api._cfg, "INFERENCE_HEALTH_PATH", "/health_generate")
    monkeypatch.setattr(api._cfg, "INFERENCE_READINESS_PATH", "/health_generate")
    monkeypatch.setattr(api.requests, "get", fake_get)

    assert api._probe_engine_sync() is True
    assert api._probe_engine_sync(readiness=True) is True
    assert requested == [
        "http://engine:30000/health",
        "http://engine:30000/health_generate",
    ]


def test_engine_probe_timeout_and_http_failure(monkeypatch):
    monkeypatch.setattr(
        api.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(requests.Timeout("slow")),
    )
    assert api._probe_engine_sync() is False

    monkeypatch.setattr(
        api.requests, "get", lambda *args, **kwargs: SimpleNamespace(status_code=503)
    )
    assert api._probe_engine_sync() is False


def test_health_response_has_generic_and_compatibility_fields(monkeypatch):
    monkeypatch.setattr(api, "_local_q", EmptyQueue())
    monkeypatch.setattr(api._cfg, "INFERENCE_ENGINE", "sglang")

    async def unhealthy(*, readiness=True):
        return False

    monkeypatch.setattr(api, "_engine_healthy_cached", unhealthy)

    response = asyncio.run(api.health())
    assert response.status_code == 503
    body = json.loads(response.body)
    assert body["status"] == "engine_unhealthy"
    assert body["engine"] == "sglang"
    assert body["engine_ready"] is False
    assert body["engine_healthy"] is False
    assert body["vllm_healthy"] is False
    assert body["logical"] == 3


def test_ready_response_reports_engine_success(monkeypatch):
    monkeypatch.setattr(api, "_local_q", EmptyQueue())
    monkeypatch.setattr(api._cfg, "INFERENCE_ENGINE", "sglang")
    monkeypatch.setattr(
        api,
        "_kv_subscriber",
        SimpleNamespace(
            status=SimpleNamespace(
                ready=True,
                healthy=True,
                fail_closed=False,
                phase="ready",
                detail="",
                cache_visibility="full",
            )
        ),
    )

    async def healthy(*, readiness=True):
        assert readiness is True
        return True

    monkeypatch.setattr(api, "_engine_healthy_cached", healthy)

    body = asyncio.run(api.ready())
    assert body["status"] == "ok"
    assert body["engine"] == "sglang"
    assert body["engine_ready"] is True
    assert body["probe"] == "readiness"
    assert body["kv_ready"] is True


def test_sglang_ready_is_gated_by_kv_but_liveness_is_not(monkeypatch):
    monkeypatch.setattr(api, "_local_q", EmptyQueue())
    monkeypatch.setattr(api._cfg, "INFERENCE_ENGINE", "sglang")
    monkeypatch.setattr(
        api,
        "_kv_subscriber",
        SimpleNamespace(
            status=SimpleNamespace(
                ready=False,
                healthy=False,
                fail_closed=True,
                phase="discovering",
                detail="engine loading",
                cache_visibility="none",
            )
        ),
    )

    async def healthy(*, readiness=True):
        return True

    monkeypatch.setattr(api, "_engine_healthy_cached", healthy)

    readiness = asyncio.run(api.ready())
    assert readiness.status_code == 503
    readiness_body = json.loads(readiness.body)
    assert readiness_body["status"] == "kv_unready"
    assert readiness_body["engine_ready"] is True
    assert readiness_body["kv_status"]["fail_closed"] is True

    liveness = asyncio.run(api.health())
    assert liveness["status"] == "ok"
    assert liveness["probe"] == "liveness"


def test_vllm_readiness_does_not_require_kv_status(monkeypatch):
    monkeypatch.setattr(api, "_local_q", EmptyQueue())
    monkeypatch.setattr(api._cfg, "INFERENCE_ENGINE", "vllm")
    monkeypatch.setattr(api, "_kv_subscriber", None)

    async def healthy(*, readiness=True):
        return True

    monkeypatch.setattr(api, "_engine_healthy_cached", healthy)

    body = asyncio.run(api.ready())
    assert body["status"] == "ok"
    assert body["engine"] == "vllm"
