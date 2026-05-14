"""
vLLM ASGI middleware that injects HOSTNAME into system_fingerprint.

Deploy alongside vLLM and pass: --middleware fingerprint_middleware.HostnameFingerprint

The middleware intercepts JSON and SSE responses, setting
system_fingerprint to the pod hostname so clients can identify
which vLLM instance served each request.
"""
import os
import json

_HOSTNAME = os.environ.get("HOSTNAME", os.environ.get("POD_NAME", "unknown"))


class HostnameFingerprint:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        chunks = []
        response_started = False
        content_type = ""

        async def patched_send(message):
            nonlocal response_started, content_type

            if message["type"] == "http.response.start":
                response_started = True
                for h_name, h_val in message.get("headers", []):
                    if h_name.lower() == b"content-type":
                        content_type = h_val.decode("utf-8", errors="replace").lower()
                await send(message)
                return

            if message["type"] == "http.response.body":
                body = message.get("body", b"")
                more = message.get("more_body", False)

                if "text/event-stream" in content_type:
                    body = _patch_sse(body)
                    await send({"type": "http.response.body", "body": body, "more_body": more})
                elif not more:
                    chunks.append(body)
                    full = b"".join(chunks)
                    full = _patch_json(full)
                    await send({"type": "http.response.body", "body": full, "more_body": False})
                else:
                    chunks.append(body)
                return

            await send(message)

        await self.app(scope, receive, patched_send)


def _patch_json(body: bytes) -> bytes:
    try:
        data = json.loads(body)
        data["system_fingerprint"] = _HOSTNAME
        return json.dumps(data).encode()
    except Exception:
        return body


def _patch_sse(body: bytes) -> bytes:
    text = body.decode("utf-8", errors="replace")
    lines = text.split("\n")
    out = []
    for line in lines:
        if line.startswith("data: ") and line.strip() != "data: [DONE]":
            data_str = line[6:]
            try:
                chunk = json.loads(data_str)
                chunk["system_fingerprint"] = _HOSTNAME
                line = f"data: {json.dumps(chunk)}"
            except Exception:
                pass
        out.append(line)
    return "\n".join(out).encode()
