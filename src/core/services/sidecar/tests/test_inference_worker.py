import json
import sys
from pathlib import Path
from types import SimpleNamespace


SIDECAR_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SIDECAR_ROOT))

from sidecar import vllm_client


class FakeQueue:
    def __init__(self, item):
        self.item = item
        self.task_done_calls = 0

    def get_nowait(self):
        item, self.item = self.item, None
        return item

    def state(self):
        return (0, 1 if self.item is None else 0)

    def task_done(self):
        self.task_done_calls += 1


class FakeResponse:
    def __init__(self, data=None, lines=None):
        self.ok = True
        self.status_code = 200
        self.text = ""
        self.elapsed = SimpleNamespace(total_seconds=lambda: 0.25)
        self._data = data
        self._lines = lines or []
        self.closed = False

    def json(self):
        return self._data

    def iter_lines(self, decode_unicode=False):
        assert decode_unicode is True
        return iter(self._lines)

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.posts = []
        self.closed = False

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.response

    def close(self):
        self.closed = True


class CapturingPoster:
    def __init__(self):
        self.worker = None
        self.payloads = []

    def submit(self, payload):
        self.payloads.append(payload)
        self.worker._stop_evt.set()


def _make_worker(monkeypatch, engine, meta, response, streaming=False):
    monkeypatch.setattr(vllm_client._cfg, "INFERENCE_ENGINE", engine)
    monkeypatch.setattr(vllm_client._cfg, "INFERENCE_URL", "http://engine:30000")
    monkeypatch.setattr(vllm_client._cfg, "INFERENCE_TIMEOUT_S", 17.0)
    monkeypatch.setattr(vllm_client._cfg, "STREAMING_MODE", streaming)
    monkeypatch.setattr(vllm_client._cfg, "TRACE_ENABLED", False)
    monkeypatch.setattr(vllm_client._cfg, "LOG_LEVEL", "info")
    monkeypatch.setattr(vllm_client._cfg, "FORCE_IGNORE_EOS", False)
    monkeypatch.setattr(vllm_client._cfg, "MODEL_NAME", "served")
    monkeypatch.setattr(vllm_client._cfg, "CONTAINER_NAME", "pod-1")

    queue = FakeQueue(("req-1", "hello", meta))
    poster = CapturingPoster()
    session = FakeSession(response)
    monkeypatch.setattr(vllm_client.requests, "Session", lambda: session)
    worker = vllm_client.InferenceWorker(queue, result_poster=poster)
    poster.worker = worker
    return worker, queue, poster, session


def test_vllm_payload_behavior_is_unchanged(monkeypatch):
    response = FakeResponse({"choices": [{"message": {"content": "ok"}}]})
    worker, _, _, _ = _make_worker(
        monkeypatch,
        "vllm",
        {"max_tokens": 9, "temperature": 0.4, "min_tokens": 3, "ignore_eos": True},
        response,
    )

    assert worker._build_payload("hello", {
        "max_tokens": 9,
        "temperature": 0.4,
        "min_tokens": 3,
        "ignore_eos": True,
    }) == {
        "model": "served",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 9,
        "temperature": 0.4,
        "chat_template_kwargs": {"enable_thinking": False},
        "min_tokens": 3,
        "ignore_eos": True,
    }


def test_sglang_legacy_payload_uses_tokenizer_thinking_default(monkeypatch):
    worker, _, _, _ = _make_worker(
        monkeypatch,
        "sglang",
        {},
        FakeResponse({"choices": [{"message": {"content": "ok"}}]}),
    )

    assert worker._build_payload("hello", {}) == {
        "model": "served",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 128,
        "temperature": 0.0,
    }


def test_sglang_legacy_payload_forwards_explicit_thinking_override(monkeypatch):
    worker, _, _, _ = _make_worker(
        monkeypatch,
        "sglang",
        {"enable_thinking": False},
        FakeResponse({"choices": [{"message": {"content": "ok"}}]}),
    )

    assert worker._build_payload("hello", {"enable_thinking": False}) == {
        "model": "served",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 128,
        "temperature": 0.0,
        "chat_template_kwargs": {"enable_thinking": False},
    }


def test_sglang_preserves_supported_sampling_extensions(monkeypatch):
    chat_request = {
        "messages": [{"role": "user", "content": "hello"}],
        "min_tokens": 4,
        "max_tokens": 20,
        "temperature": 0.2,
        "tools": [{"type": "function", "function": {"name": "lookup"}}],
        "tool_choice": "auto",
        "response_format": {"type": "json_object"},
        "chat_template_kwargs": {"enable_thinking": True},
        "ignore_eos": True,
        "stream": True,
    }
    response = FakeResponse({"choices": [{"message": {"content": "ok"}}]})
    worker, _, _, _ = _make_worker(
        monkeypatch,
        "sglang",
        {"__chat_request__": chat_request},
        response,
    )

    payload = worker._build_payload("ignored", {"__chat_request__": chat_request})
    assert payload == chat_request | {"model": "served", "stream": False}


def test_non_streaming_result_uses_shared_worker(monkeypatch):
    response_data = {
        "id": "chatcmpl-1",
        "choices": [{
            "message": {"content": "answer"},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3},
    }
    worker, queue, poster, session = _make_worker(
        monkeypatch, "sglang", {}, FakeResponse(response_data)
    )

    worker._loop()

    assert session.posts[0][0] == "http://engine:30000/v1/chat/completions"
    assert session.posts[0][1]["timeout"] == 17.0
    assert poster.payloads[0]["result"]["output"] == "answer"
    assert poster.payloads[0]["result"]["finish_reason"] == "stop"
    assert poster.payloads[0]["result"]["usage"]["total_tokens"] == 3
    assert poster.payloads[0]["result"]["raw"] == response_data
    assert queue.task_done_calls == 1
    assert session.closed


def test_streaming_accumulates_usage_and_tool_calls(monkeypatch):
    chunks = [
        {
            "id": "chatcmpl-stream",
            "created": 123,
            "model": "served",
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call-1",
                        "type": "function",
                        "function": {"name": "lookup", "arguments": '{"q":'},
                    }]
                },
                "finish_reason": None,
            }],
        },
        {
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "function": {"arguments": '"x"}'},
                    }]
                },
                "finish_reason": "tool_calls",
            }],
        },
        {
            "choices": [],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
        },
    ]
    lines = [f"data: {json.dumps(chunk)}" for chunk in chunks] + ["data: [DONE]"]
    response = FakeResponse(lines=lines)
    worker, _, poster, session = _make_worker(
        monkeypatch, "sglang", {}, response, streaming=True
    )

    worker._loop()

    request = session.posts[0][1]
    assert request["stream"] is True
    assert request["json"]["stream"] is True
    assert request["json"]["stream_options"] == {"include_usage": True}
    result = poster.payloads[0]["result"]
    assert result["finish_reason"] == "tool_calls"
    assert result["usage"]["total_tokens"] == 8
    assert result["tool_calls"] == [{
        "id": "call-1",
        "type": "function",
        "function": {"name": "lookup", "arguments": '{"q":"x"}'},
    }]
    assert result["raw"]["usage"] == result["usage"]


def test_vllm_worker_is_compatibility_alias():
    assert vllm_client.VLLMWorker is vllm_client.InferenceWorker
