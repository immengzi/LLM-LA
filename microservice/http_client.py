# http_client.py
# Thin wrapper around POST /enqueue (sync) and POST /submit (async_pubsub).
#
# NOTE:
# - Reconciliation via GET /result/{req_id} has been removed (no longer used).
# - async_pubsub termination is handled in load_runner.py via:
#     - Preferred: Prometheus fleet-idle detection (requests_running==0 for idle_zero_running_s)
#     - Backstop: idle-timeout-after-last-recv (idle_timeout_s)

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
) -> Tuple[str, Optional[Dict[str, Any]]]:
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

             # ---  (if server-side trace is enabled) ---
             "trace": { ... },

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

    rid = str(data["req_id"])
    result = data.get("result")

    # Sanity: result should be a dict, but don't crash if it's not.
    if result is not None and not isinstance(result, dict):
        print(f"[client] WARNING: unexpected 'result' type for req_id={rid}: {type(result)}")
        result = None

    return rid, result


def submit_one(
    session: requests.Session,
    router_url: str,
    submit_path: str,
    prompt: str,
    meta: Dict[str, Any] | None = None,
    # run_id is optional; when provided we stamp meta["__run_id"] for router pubsub isolation.
    run_id: Optional[str] = None,
) -> str:
    """
    Send one async submit request (submit+ack).

    Expected router behavior:
      - Accept request, allocate req_id, enqueue/dispatch
      - Immediately return 202 Accepted with {"req_id": "<id>"}.

    Returns:
        req_id (string)
    """
    t_enq = time.time()

    # IMPORTANT: copy meta so we never mutate caller dict (caller may reuse it).
    m: Dict[str, Any] = dict(meta or {})

    # Attach run_id so router can publish on results.<run_id>
    # (router may look for meta["__run_id"]).
    if run_id is not None and str(run_id).strip():
        # If caller already provided __run_id, keep it (don't overwrite).
        m.setdefault("__run_id", str(run_id).strip())

    payload: Dict[str, Any] = {
        "prompt": prompt,
        "t_enq_client": t_enq,
        "meta": m,
    }

    # Normalize submit_path
    sp = submit_path or "/submit"
    if not sp.startswith("/"):
        sp = "/" + sp

    url = f"{router_url}{sp}"
    try:
        # Keep timeout short: this is submit+ack only.
        resp = session.post(url, json=payload, timeout=10.0)
    except RequestException as e:
        print(f"[client] ✗ HTTP error talking to router submit endpoint: {e}")
        raise

    # Accept either 202 (preferred) or 200 (tolerate)
    if resp.status_code not in (200, 202):
        print(f"[client] ✗ {sp} failed: {resp.status_code} {resp.text}")
        raise RuntimeError(f"{sp} failed: {resp.status_code} {resp.text}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"{sp} returned non-JSON body: {resp.text!r}")

    if "req_id" not in data:
        raise RuntimeError(f"{sp} response missing 'req_id': {data!r}")

    rid = str(data["req_id"]).strip()
    if not rid:
        raise RuntimeError(f"{sp} returned empty 'req_id': {data!r}")

    return rid
