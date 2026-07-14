# tests/test_fingerprint_middleware.py
# -*- coding: utf-8 -*-
"""Unit tests for the vLLM hostname-fingerprint ASGI middleware.

Run from src/core/services with PYTHONPATH=. so `fingerprint_middleware` (a flat
module, not a package) is importable.
"""
import json

import pytest

import fingerprint_middleware as fp


@pytest.fixture(autouse=True)
def fixed_hostname(monkeypatch):
    monkeypatch.setattr(fp, "_HOSTNAME", "pod-xyz")


def test_patch_json_adds_fingerprint():
    body = json.dumps({"id": "x", "choices": []}).encode()
    out = json.loads(fp._patch_json(body))
    assert out["system_fingerprint"] == "pod-xyz"
    assert out["id"] == "x"


def test_patch_json_invalid_is_passthrough():
    body = b"not json at all"
    assert fp._patch_json(body) == body


def test_patch_sse_patches_each_data_chunk():
    sse = (
        'data: {"id": "1", "choices": []}\n'
        'data: {"id": "2", "choices": []}\n'
        "data: [DONE]\n"
    ).encode()
    out = fp._patch_sse(sse).decode()
    lines = [l for l in out.split("\n") if l.startswith("data: ") and "[DONE]" not in l]
    for l in lines:
        chunk = json.loads(l[6:])
        assert chunk["system_fingerprint"] == "pod-xyz"
    # [DONE] sentinel is preserved untouched.
    assert "data: [DONE]" in out


def test_patch_sse_ignores_non_json_data():
    sse = b"data: garbage\n\n"
    # Should not raise and should leave the line intact.
    assert b"garbage" in fp._patch_sse(sse)


async def _collect(app_call):
    sent = []

    async def send(msg):
        sent.append(msg)

    async def receive():
        return {"type": "http.request"}

    await app_call(receive, send)
    return sent


@pytest.mark.asyncio
async def test_middleware_patches_non_streaming_json():
    async def app(scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        })
        # Body split across two chunks to exercise buffering.
        await send({"type": "http.response.body",
                    "body": b'{"id":"a",', "more_body": True})
        await send({"type": "http.response.body",
                    "body": b'"choices":[]}', "more_body": False})

    mw = fp.HostnameFingerprint(app)
    scope = {"type": "http"}
    sent = await _collect(lambda receive, send: mw(scope, receive, send))

    body_msgs = [m for m in sent if m["type"] == "http.response.body"]
    assert len(body_msgs) == 1  # buffered into a single patched body
    out = json.loads(body_msgs[0]["body"])
    assert out["system_fingerprint"] == "pod-xyz"
    assert out["id"] == "a"


@pytest.mark.asyncio
async def test_middleware_patches_streaming_sse():
    async def app(scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/event-stream")],
        })
        await send({"type": "http.response.body",
                    "body": b'data: {"id":"1","choices":[]}\n',
                    "more_body": True})
        await send({"type": "http.response.body", "body": b"data: [DONE]\n",
                    "more_body": False})

    mw = fp.HostnameFingerprint(app)
    scope = {"type": "http"}
    sent = await _collect(lambda receive, send: mw(scope, receive, send))

    bodies = b"".join(m["body"] for m in sent if m["type"] == "http.response.body")
    assert b'"system_fingerprint": "pod-xyz"' in bodies
    assert b"data: [DONE]" in bodies


@pytest.mark.asyncio
async def test_middleware_passthrough_for_non_http_scope():
    called = {"n": 0}

    async def app(scope, receive, send):
        called["n"] += 1

    mw = fp.HostnameFingerprint(app)
    scope = {"type": "lifespan"}

    async def send(msg):
        pass

    async def receive():
        return {}

    await mw(scope, receive, send)
    assert called["n"] == 1
