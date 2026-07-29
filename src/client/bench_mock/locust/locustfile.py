"""Locust load for LiteLLM-style gateway overhead benchmarks.

Mirrors https://docs.litellm.ai/docs/benchmarks :
  * Large unique prompts (no cache hits)
  * Custom overhead metric from response headers
  * Recommended: 1000 users, 500 ramp-up

Overhead sources (first match wins):
  1. x-litellm-overhead-duration-ms  (LiteLLM)
  2. x-gateway-overhead-duration-ms  (optional custom gateways)
  3. e2e_ms − x-mock-backend-duration-ms  (derived against fake OpenAI)
"""
from __future__ import annotations

import os
import uuid

from locust import HttpUser, between, events, task

API_KEY = os.getenv("API_KEY", "sk-1234")
MODEL = os.getenv("BENCH_MODEL", "fake-openai-endpoint")
# LLM-LA / BooM often serve "served-model"; override per path via env.
CHAT_PATH = os.getenv("CHAT_PATH", "/v1/chat/completions")
PROMPT_REPEAT = int(os.getenv("PROMPT_REPEAT", "150"))


@events.request.add_listener
def on_request(request_type, name, response_time, response_length, **kwargs):
    # Nested Custom fires omit response/url; ignore them (and skip re-entry).
    if request_type == "Custom" or name == "Gateway Overhead Duration (ms)":
        return
    exception = kwargs.get("exception")
    response = kwargs.get("response")
    if exception or response is None:
        return
    headers = getattr(response, "headers", None) or {}
    overhead_ms = None

    for key in (
        "x-litellm-overhead-duration-ms",
        "x-litellm-overhead-duration",
        "x-gateway-overhead-duration-ms",
    ):
        raw = headers.get(key)
        if raw is not None:
            try:
                overhead_ms = float(raw)
            except (TypeError, ValueError):
                overhead_ms = None
            break

    if overhead_ms is None:
        backend_raw = headers.get("x-mock-backend-duration-ms")
        if backend_raw is not None:
            try:
                backend_ms = float(backend_raw)
                overhead_ms = max(0.0, float(response_time) - backend_ms)
            except (TypeError, ValueError):
                overhead_ms = None

    # Gateways that do not forward the mock header (LLM-LA / BooM): with a
    # near-zero fake backend, e2e ≈ gateway overhead (LiteLLM-style).
    if overhead_ms is None and os.getenv("ASSUME_E2E_IS_OVERHEAD", "1") == "1":
        try:
            overhead_ms = float(response_time)
        except (TypeError, ValueError):
            overhead_ms = None

    if overhead_ms is None:
        return

    events.request.fire(
        request_type="Custom",
        name="Gateway Overhead Duration (ms)",
        response_time=overhead_ms,
        response_length=0,
        exception=None,
        context={},
    )


class GatewayUser(HttpUser):
    # Match router RESULT_TIMEOUT_S (default 120s) so Locust doesn't
    # abandon in-flight pull requests as status=0 while the gateway still waits.
    network_timeout = float(os.getenv("LOCUST_NETWORK_TIMEOUT", "130"))
    connection_timeout = float(os.getenv("LOCUST_CONNECTION_TIMEOUT", "10"))
    wait_time = between(
        float(os.getenv("WAIT_MIN_S", "0.5")),
        float(os.getenv("WAIT_MAX_S", "1.0")),
    )


    def on_start(self):
        self.client.headers.update({"Authorization": f"Bearer {API_KEY}"})

    @task
    def chat_completions(self):
        payload = {
            "model": MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": (
                        f"{uuid.uuid4()} This is a test there will be no cache "
                        f"hits and we'll fill up the context" * PROMPT_REPEAT
                    ),
                }
            ],
            "user": "bench-mock-end-user",
            "max_tokens": int(os.getenv("MAX_TOKENS", "16")),
            "temperature": 0,
        }
        with self.client.post(
            CHAT_PATH,
            json=payload,
            name="/chat/completions",
            catch_response=True,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"status={resp.status_code} body={resp.text[:300]}")
            else:
                resp.success()
