# tests/test_upstream_error_capture.py
# -*- coding: utf-8 -*-
"""Sidecar-side tests for inference-engine non-2xx capture (decisive first hop).

The sidecar is where the original engine status + body would otherwise be dropped
and replaced with the generic "[inference error N]" string. These tests drive one
VLLMWorker iteration against a fake engine response and assert what the worker
submits back to the router:

  * upstream 4xx  -> result["upstream_error"] carries status + body + content_type
  * upstream 5xx  -> legacy "[inference error N]" marker preserved (out of scope)
"""
import time


import sidecar.vllm_client as vc
from sidecar.local_queue import LocalQueue


class _FakeElapsed:
    def total_seconds(self):
        return 0.01


class _FakeResp:
    def __init__(self, status_code, text, content_type="application/json"):
        self.status_code = status_code
        self.text = text
        self.headers = {"content-type": content_type}
        self.elapsed = _FakeElapsed()

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        import json
        return json.loads(self.text)

    def close(self):
        pass


class _FakeSession:
    """Stands in for requests.Session(); .post returns a preset response."""

    def __init__(self, resp):
        self._resp = resp

    def post(self, url, json=None, timeout=None, stream=False):
        return self._resp

    def close(self):
        pass


class _CapturingPoster:
    def __init__(self):
        self.payloads = []

    def submit(self, payload):
        self.payloads.append(payload)


def _run_one(monkeypatch, resp):
    """Run a single VLLMWorker iteration against `resp`; return the submitted result."""
    monkeypatch.setattr(vc._cfg, "STREAMING_MODE", False)
    monkeypatch.setattr(vc._cfg, "TRACE_ENABLED", False)
    monkeypatch.setattr(vc.requests, "Session", lambda: _FakeSession(resp))

    q = LocalQueue("pod-test")
    q.put("req-1", "hello", {"max_tokens": 8})
    poster = _CapturingPoster()

    worker = vc.VLLMWorker(q, pull_worker=None, result_poster=poster)
    worker.start()
    try:
        deadline = time.time() + 3.0
        while not poster.payloads and time.time() < deadline:
            time.sleep(0.02)
    finally:
        worker.stop()

    assert poster.payloads, "worker did not submit any result"
    return poster.payloads[0]


VLLM_400_BODY = (
    '{"error":{"message":"This model\'s maximum context length is 196608 '
    'tokens. However, you requested 32000 output tokens and your prompt '
    'contains at least 164609 input tokens, for a total of at least 196609 '
    'tokens.","type":"BadRequestError","param":"input_tokens","code":400}}'
)


def test_sidecar_captures_upstream_4xx(monkeypatch):
    payload = _run_one(monkeypatch, _FakeResp(400, VLLM_400_BODY))
    result = payload["result"]

    ue = result.get("upstream_error")
    assert ue is not None, "expected upstream_error marker on 4xx"
    assert ue["status"] == 400
    assert ue["content_type"] == "application/json"
    assert "maximum context length" in ue["body"]
    assert "196609" in ue["body"]
    # The generic marker must NOT leak into the output text.
    assert result.get("output") == ""
    assert "inference error" not in str(result.get("output"))
    assert "vLLM error" not in str(result.get("output"))


def test_sidecar_5xx_keeps_legacy_marker(monkeypatch):
    payload = _run_one(monkeypatch, _FakeResp(503, "upstream unavailable", content_type="text/plain"))
    result = payload["result"]

    # 5xx is intentionally out of scope: no passthrough marker, legacy string kept.
    assert "upstream_error" not in result
    assert result.get("output") == "[inference error 503]"
