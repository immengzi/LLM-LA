"""End-to-end tests for the pull-mode request path.

Exercises the full round trip:
    client -> router /v1/chat/completions
           -> central queue -> sidecar /pull
           -> mock vLLM /v1/chat/completions
           -> sidecar /result -> router -> client

The mock vLLM echoes the last user message ("echo: <text>"), so the assertions
prove the request body actually reached vLLM and the response flowed back.
"""
import json

import requests


def test_router_health(router_url):
    r = requests.get(f"{router_url}/health/router", timeout=5)
    assert r.status_code == 200


def test_sidecar_health(sidecar_url):
    r = requests.get(f"{sidecar_url}/health", timeout=5)
    assert r.status_code == 200


def test_sidecar_reports_kv_usage_from_mock_metrics(sidecar_url):
    """Accessibility: mock vLLM /metrics is reachable; sidecar caches max engine KV."""
    import time

    kv = None
    for _ in range(20):
        r = requests.get(f"{sidecar_url}/health", timeout=5)
        assert r.status_code == 200
        kv = r.json().get("kv_usage")
        if kv is not None:
            break
        time.sleep(0.5)
    assert kv is not None, "sidecar never reported kv_usage (scrape failed?)"
    # mock exposes 0.42 and 0.58 -> max = 0.58
    assert 0.50 <= float(kv) <= 0.65


def test_chat_completion_non_streaming(router_url):
    payload = {
        "model": "served-model",
        "messages": [{"role": "user", "content": "hello e2e"}],
        "max_tokens": 32,
        "temperature": 0.0,
    }
    r = requests.post(f"{router_url}/v1/chat/completions", json=payload, timeout=30)
    assert r.status_code == 200, r.text
    data = r.json()

    assert data["object"] == "chat.completion"
    choice = data["choices"][0]
    assert choice["message"]["role"] == "assistant"
    assert choice["message"]["content"] == "echo: hello e2e"
    assert choice["finish_reason"] == "stop"
    assert data["usage"]["total_tokens"] > 0


def test_chat_completion_streaming(router_url):
    payload = {
        "model": "served-model",
        "messages": [{"role": "user", "content": "stream please"}],
        "max_tokens": 32,
        "stream": True,
    }
    with requests.post(
        f"{router_url}/v1/chat/completions", json=payload, timeout=30, stream=True
    ) as r:
        assert r.status_code == 200, r.text
        collected = []
        saw_done = False
        for raw in r.iter_lines(decode_unicode=True):
            if not raw or not raw.startswith("data: "):
                continue
            body = raw[len("data: "):]
            if body.strip() == "[DONE]":
                saw_done = True
                break
            chunk = json.loads(body)
            delta = chunk["choices"][0].get("delta", {})
            if delta.get("content"):
                collected.append(delta["content"])

    assert saw_done
    assert "".join(collected) == "echo: stream please"


def test_multiple_requests_round_robin(router_url):
    # Fire several requests to prove the queue drains repeatedly through the
    # sidecar's pull loop (not just a single lucky request).
    for i in range(5):
        payload = {
            "model": "served-model",
            "messages": [{"role": "user", "content": f"req-{i}"}],
            "max_tokens": 16,
        }
        r = requests.post(f"{router_url}/v1/chat/completions", json=payload, timeout=30)
        assert r.status_code == 200, r.text
        assert r.json()["choices"][0]["message"]["content"] == f"echo: req-{i}"
