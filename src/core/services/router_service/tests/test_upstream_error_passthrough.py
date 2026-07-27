# tests/test_upstream_error_passthrough.py
# -*- coding: utf-8 -*-
"""Router egress tests for vLLM upstream-error passthrough.

Verifies that when the sidecar flags a non-2xx upstream (vLLM) response via
result["upstream_error"] (4xx only), the router's /v1/chat/completions egress
relays vLLM's original status code + body + Content-Type verbatim instead of
wrapping the generic "[vLLM error 400]" string in a 200 chat.completion.

These exercise exactly the layer BooM Gateway receives (the router egress),
without depending on a live vLLM/sidecar.
"""
import json

import pytest
from fastapi.responses import Response
from fastapi.testclient import TestClient

import router.api as api_mod


# A realistic OpenAI-compatible vLLM 400 body (context-length overflow).
VLLM_400_BODY = json.dumps(
    {
        "error": {
            "message": (
                "This model's maximum context length is 196608 tokens. However, "
                "you requested 32000 output tokens and your prompt contains at "
                "least 164609 input tokens, for a total of at least 196609 tokens."
            ),
            "type": "BadRequestError",
            "param": "input_tokens",
            "code": 400,
        }
    }
)


@pytest.fixture
def client():
    return TestClient(api_mod.app)


@pytest.fixture(autouse=True)
def clean_router_state():
    rs = api_mod.router_state
    with rs._lock:
        rs._queues.clear()
        rs._result_values.clear()
        rs._result_store_ts.clear()
        rs._result_futs.clear()
        rs._inflight_by_endpoint.clear()
        rs._req_endpoint.clear()
    yield


# ----------------------------------------------------------------------------
# _upstream_error_response unit coverage
# ----------------------------------------------------------------------------

def test_upstream_error_response_relays_status_body_and_content_type():
    resp = api_mod._upstream_error_response(
        {"upstream_error": {"status": 400, "body": VLLM_400_BODY, "content_type": "application/json"}}
    )
    assert isinstance(resp, Response)
    assert resp.status_code == 400
    assert resp.media_type == "application/json"
    assert resp.body.decode() == VLLM_400_BODY


def test_upstream_error_response_none_for_normal_result():
    assert api_mod._upstream_error_response({"output": "hello", "usage": {}}) is None
    assert api_mod._upstream_error_response({}) is None
    assert api_mod._upstream_error_response("not-a-dict") is None


def test_upstream_error_response_defaults_content_type_json():
    resp = api_mod._upstream_error_response({"upstream_error": {"status": 413, "body": "{}"}})
    assert resp.status_code == 413
    assert resp.media_type == "application/json"


# ----------------------------------------------------------------------------
# Non-streaming egress
# ----------------------------------------------------------------------------

def test_chat_completions_nonstream_passthrough_4xx(client, monkeypatch):
    async def _fake_enqueue_and_wait(prompt, model, **kw):
        return "rid-err", 0.0, {
            "upstream_error": {
                "status": 400,
                "body": VLLM_400_BODY,
                "content_type": "application/json",
            }
        }

    monkeypatch.setattr(api_mod, "_enqueue_and_wait", _fake_enqueue_and_wait)

    r = client.post(
        "/v1/chat/completions",
        json={"model": "served-model", "messages": [{"role": "user", "content": "hi"}]},
    )

    assert r.status_code == 400
    assert r.headers["content-type"].startswith("application/json")
    body = r.json()
    # The full vLLM detail is preserved, not "[vLLM error 400]".
    assert "maximum context length" in body["error"]["message"]
    assert "total of at least 196609 tokens" in body["error"]["message"]
    assert "vLLM error" not in r.text


def test_chat_completions_nonstream_success_unaffected(client, monkeypatch):
    async def _fake_enqueue_and_wait(prompt, model, **kw):
        return "rid-ok", 0.0, {
            "output": "hello world",
            "finish_reason": "stop",
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            "endpoint_id": "pod-a",
        }

    monkeypatch.setattr(api_mod, "_enqueue_and_wait", _fake_enqueue_and_wait)

    r = client.post(
        "/v1/chat/completions",
        json={"model": "served-model", "messages": [{"role": "user", "content": "hi"}]},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["object"] == "chat.completion"
    assert body["choices"][0]["message"]["content"] == "hello world"


# ----------------------------------------------------------------------------
# Streaming egress: a pre-generation 4xx must return a real JSON error with the
# correct status, NOT a 200 text/event-stream.
# ----------------------------------------------------------------------------

def test_chat_completions_stream_passthrough_4xx(client, monkeypatch):
    async def _noop_register(rid, prompt, *, meta, is_pull_mode, messages=None, tools=None, model=None):
        return meta

    async def _fake_wait_for_result(rid, timeout):
        return {
            "upstream_error": {
                "status": 400,
                "body": VLLM_400_BODY,
                "content_type": "application/json",
            }
        }

    monkeypatch.setattr(api_mod, "_maybe_register_kv_blocks", _noop_register)
    monkeypatch.setattr(api_mod.router_state, "wait_for_result_async", _fake_wait_for_result)
    monkeypatch.setattr(api_mod._cfg, "RESULT_TIMEOUT_S", 2.0)

    r = client.post(
        "/v1/chat/completions",
        json={
            "model": "served-model",
            "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )

    assert r.status_code == 400
    assert r.headers["content-type"].startswith("application/json")
    assert "text/event-stream" not in r.headers.get("content-type", "")
    body = r.json()
    assert "maximum context length" in body["error"]["message"]
    assert "vLLM error" not in r.text
