# http_client.py
# Thin wrapper around POST /enqueue, returning both req_id and result.

from __future__ import annotations

from typing import Dict, Any, Tuple, Optional
import time

import requests
from requests.exceptions import RequestException


def send_one(
    session: requests.Session,
    router_url: str,
    prompt: str,
    meta: Dict[str, Any] | None = None,
) -> Tuple[int, Optional[Dict[str, Any]]]:
    """
    Send one synchronous /enqueue request.

    The router blocks until the sidecar posts /result or timeout.

    Response shape (happy path):

        {
          "req_id": "abc123",
          "result": {
             "output": "...",
             "finish_reason": "stop",
             "latency_s": 0.342,

             # --- NEW (if server-side trace is enabled) ---
             "trace": {
                 "endpoint": "vllm-pod-xyz",
                 "t_enq_router": 1700000000.123456,
                 "t_dispatch_router": 1700000000.234567,
                 "t_arrive_sidecar_push": 1700000000.345678,
                 "t_dequeue_sidecar": 1700000000.456789,
                 "t_vllm_send": 1700000000.567891,
                 "t_vllm_recv": 1700000001.012345,
                 "t_post_result_sidecar": 1700000001.123456,
                 "t_enqueue_response": 1700000001.234567
             },

             "raw": { ... full vLLM response ... }
          }
        }

    IMPORTANT:
    - The client does NOT generate or modify trace data.
    - The client simply returns whatever the server gives.
    - load_runner.py is responsible for printing/consuming trace fields.

    Returns:
        (req_id, result_dict_or_None)
    """
    t_enq = time.time()
    payload: Dict[str, Any] = {
        "prompt": prompt,
        "t_enq_client": t_enq,
        "meta": meta or {},
    }

    url = f"{router_url}/enqueue"
    try:
        resp = session.post(url, json=payload, timeout=1000000.0)
    except RequestException as e:
        print(f"[client] ✗ HTTP error talking to router: {e}")
        raise

    if not resp.ok:
        print(f"[client] ✗ /enqueue failed: {resp.status_code} {resp.text}")
        raise RuntimeError(f"/enqueue failed: {resp.status_code} {resp.text}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"/enqueue returned non-JSON body: {resp.text!r}")

    if "req_id" not in data:
        raise RuntimeError(f"/enqueue response missing 'req_id': {data!r}")

    rid = data["req_id"]
    result = data.get("result")

    # Sanity: result should be a dict, but don't crash if it's not.
    if result is not None and not isinstance(result, dict):
        print(f"[client] WARNING: unexpected 'result' type for req_id={rid}: {type(result)}")
        result = None

    return rid, result
