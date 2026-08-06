# tests/test_api_endpoints.py
# -*- coding: utf-8 -*-
"""Component tests for the router FastAPI surface via Starlette's TestClient.

TestClient is constructed WITHOUT the context-manager form so the app's startup
hook (which would spawn the KVWatcher thread and can fatally init the tokenizer)
does not run. Handlers that only touch in-process state (health, metrics, pull,
result) are exercised directly; the full enqueue->result roundtrip is covered by
the docker-compose e2e suite instead.
"""
import asyncio
import json

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
        rs._kv_usage_by_endpoint.clear()
        rs._kv_pressure_active.clear()
    yield
    with rs._lock:
        rs._queues.clear()
        rs._kv_usage_by_endpoint.clear()
        rs._kv_pressure_active.clear()


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


def test_pull_accepts_kv_usage(client):
    r = client.post("/pull", json={"endpoint": "pod-a", "want": 1, "kv_usage": 0.77})
    assert r.status_code == 200
    rec = api_mod.router_state._kv_usage_by_endpoint.get("pod-a")
    assert rec is not None
    assert rec[0] == pytest.approx(0.77)


def test_pull_omitted_kv_usage_ok(client):
    r = client.post("/pull", json={"endpoint": "pod-b", "want": 1})
    assert r.status_code == 200
    assert "pod-b" not in api_mod.router_state._kv_usage_by_endpoint


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


def test_mode_helpers_sidecar_central_push(monkeypatch):
    """Default central-push (sidecar on) delivers via the sidecar PushRouter."""
    monkeypatch.setattr(api_mod._cfg, "ROUTER_MODE", "central-push")
    monkeypatch.setattr(api_mod._cfg, "ROUTER_SIDECAR_ENABLED", True)
    assert api_mod._is_central_push() is True
    assert api_mod._is_central_push_direct() is False
    assert api_mod._uses_push_delivery() is True
    assert api_mod._uses_direct_delivery() is False
    assert api_mod._uses_central_queue() is True


def test_mode_helpers_sidecarless_central_push(monkeypatch):
    """Sidecar-less central-push takes the direct-delivery path, not PushRouter."""
    monkeypatch.setattr(api_mod._cfg, "ROUTER_MODE", "central-push")
    monkeypatch.setattr(api_mod._cfg, "ROUTER_SIDECAR_ENABLED", False)
    assert api_mod._is_central_push_direct() is True
    assert api_mod._is_push_direct() is False
    assert api_mod._uses_push_delivery() is False
    assert api_mod._uses_direct_delivery() is True
    # Still admits through the central queue (same scheduling as pull/central-push).
    assert api_mod._uses_central_queue() is True


def test_mode_helpers_sidecarless_push_rr(monkeypatch):
    """Sidecar-less push-* keeps PushRouter (queue-less) and delivers direct-to-vLLM."""
    monkeypatch.setattr(api_mod._cfg, "ROUTER_MODE", "push-rr")
    monkeypatch.setattr(api_mod._cfg, "ROUTER_SIDECAR_ENABLED", False)
    assert api_mod._is_push_mode() is True
    assert api_mod._is_push_direct() is True
    assert api_mod._is_central_push_direct() is False
    assert api_mod._uses_push_delivery() is True
    assert api_mod._uses_direct_delivery() is False
    assert api_mod._uses_central_queue() is False


def test_mode_helpers_external_push_is_direct(monkeypatch):
    monkeypatch.setattr(api_mod._cfg, "ROUTER_MODE", "external-push")
    monkeypatch.setattr(api_mod._cfg, "ROUTER_SIDECAR_ENABLED", True)
    assert api_mod._is_central_push_direct() is False
    assert api_mod._is_push_direct() is False
    assert api_mod._uses_direct_delivery() is True
    assert api_mod._uses_push_delivery() is False


def test_mode_helpers_pull_unaffected_by_flag(monkeypatch):
    monkeypatch.setattr(api_mod._cfg, "ROUTER_MODE", "pull")
    monkeypatch.setattr(api_mod._cfg, "ROUTER_SIDECAR_ENABLED", False)
    assert api_mod._is_central_push_direct() is False
    assert api_mod._is_push_direct() is False
    assert api_mod._uses_direct_delivery() is False
    assert api_mod._uses_push_delivery() is False


def test_debug_slo_summary(client):
    r = client.get("/debug/slo")
    assert r.status_code == 200
    body = r.json()
    assert "total" in body and "by_type" in body


def test_latency_log_endpoint(client):
    r = client.get("/latency_log?last=10")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def _stream_events(client, monkeypatch, chunks):
    queue = asyncio.Queue()
    for chunk in chunks:
        queue.put_nowait(chunk)

    monkeypatch.setattr(api_mod.router_state, "enqueue", lambda *args, **kwargs: "stream-1")
    monkeypatch.setattr(
        api_mod.router_state,
        "register_chunk_queue",
        lambda _rid: queue,
    )
    monkeypatch.setattr(api_mod.router_state, "remove_chunk_queue", lambda _rid: None)
    monkeypatch.setattr(api_mod.router_state, "register_waiter", lambda _rid: None)

    async def _no_fallback(_rid, _timeout):
        return None

    async def _no_hashes(
        rid,
        prompt,
        *,
        meta,
        is_pull_mode,
        messages=None,
        tools=None,
        model=None,
    ):
        return meta

    monkeypatch.setattr(
        api_mod.router_state, "wait_for_result_async", _no_fallback
    )
    monkeypatch.setattr(api_mod, "_maybe_register_kv_blocks", _no_hashes)

    response = client.post(
        "/v1/chat/completions",
        json={
            "model": "served-model",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": True,
        },
    )
    assert response.status_code == 200
    events = []
    for block in response.text.strip().split("\n\n"):
        assert block.startswith("data: ")
        payload = block[6:]
        events.append(payload if payload == "[DONE]" else json.loads(payload))
    return events


def _choice_events(events):
    return [event for event in events if isinstance(event, dict) and event.get("choices")]


def test_stream_recovers_when_all_result_chunks_are_lost(client, monkeypatch):
    events = _stream_events(client, monkeypatch, [{
        "__full_result__": {
            "output": "hello world",
            "finish_reason": "stop",
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
        }
    }])

    choices = _choice_events(events)
    assert "".join(
        event["choices"][0]["delta"].get("content", "") for event in choices
    ) == "hello world"
    assert choices[-1]["choices"][0]["finish_reason"] == "stop"
    assert choices[-1]["usage"]["total_tokens"] == 3
    assert events[-1] == "[DONE]"


def test_stream_recovers_only_missing_text_delta(client, monkeypatch):
    events = _stream_events(client, monkeypatch, [
        {"delta": "hello", "is_final": False},
        {
            "__full_result__": {
                "output": "hello world",
                "finish_reason": "stop",
                "usage": {"total_tokens": 2},
            }
        },
    ])

    contents = [
        event["choices"][0]["delta"]["content"]
        for event in _choice_events(events)
        if "content" in event["choices"][0]["delta"]
        and event["choices"][0]["delta"]["content"]
    ]
    assert contents == ["hello", " world"]
    assert events[-1] == "[DONE]"


def test_stream_recovers_only_missing_tool_call_delta(client, monkeypatch):
    events = _stream_events(client, monkeypatch, [
        {
            "delta": "",
            "tool_calls": [{
                "index": 0,
                "id": "call-1",
                "type": "function",
                "function": {"name": "lookup", "arguments": '{"q":'},
            }],
            "is_final": False,
        },
        {
            "__full_result__": {
                "output": "",
                "finish_reason": "tool_calls",
                "usage": {"total_tokens": 3},
                "tool_calls": [{
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "lookup", "arguments": '{"q":"x"}'},
                }],
            }
        },
    ])

    tool_deltas = [
        tool_call
        for event in _choice_events(events)
        for tool_call in event["choices"][0]["delta"].get("tool_calls", [])
    ]
    assert tool_deltas == [
        {
            "index": 0,
            "id": "call-1",
            "type": "function",
            "function": {"name": "lookup", "arguments": '{"q":'},
        },
        {"index": 0, "function": {"arguments": '"x"}'}},
    ]
    assert _choice_events(events)[-1]["choices"][0]["finish_reason"] == "tool_calls"
    assert events[-1] == "[DONE]"


def test_stream_complete_chunks_do_not_use_recovery(client, monkeypatch):
    events = _stream_events(client, monkeypatch, [
        {"delta": "complete", "is_final": False},
        {
            "delta": "",
            "is_final": True,
            "finish_reason": "stop",
            "usage": {"total_tokens": 1},
        },
    ])

    choices = _choice_events(events)
    assert [
        event["choices"][0]["delta"].get("content")
        for event in choices
        if event["choices"][0]["delta"].get("content")
    ] == ["complete"]
    assert choices[-1]["choices"][0]["finish_reason"] == "stop"
    assert events.count("[DONE]") == 1


@pytest.mark.parametrize(
    "chunks, expected_code",
    [
        ([{"__full_result__": {"error": "engine disconnected"}}], "inference_error"),
        (
            [
                {"delta": "already sent", "is_final": False},
                {"__full_result__": {"output": "different result"}},
            ],
            "stream_recovery_failed",
        ),
    ],
)
def test_stream_failure_results_are_explicit_errors(
    client, monkeypatch, chunks, expected_code
):
    events = _stream_events(client, monkeypatch, chunks)

    errors = [event["error"] for event in events if isinstance(event, dict) and "error" in event]
    assert len(errors) == 1
    assert errors[0]["code"] == expected_code
    assert events[-1] == "[DONE]"
