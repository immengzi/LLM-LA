# -*- coding: utf-8 -*-
import asyncio
import time
from typing import Dict, Any

import requests
from fastapi import FastAPI
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from .local_queue import LocalQueue
from .config import get_config
from .metrics import inc_received

_cfg = get_config()

app = FastAPI(title="vLLM Sidecar", version="0.1.0")

_local_q: LocalQueue | None = None
_pull_worker = None  # RouterPullWorker | None — set via bind_pull_worker

# Cached vLLM health probe for push / central-push modes, where there is no
# RouterPullWorker to maintain the health signal. Probed at most once per
# interval so /push stays cheap under load.
_VLLM_PROBE_INTERVAL_S = 1.0
_vllm_last_probe = 0.0
_vllm_healthy_cache = True


def _probe_vllm_sync() -> bool:
    try:
        r = requests.get(f"{_cfg.VLLM_URL}/health", timeout=2.0)
        return r.status_code == 200
    except Exception:
        return False


async def _vllm_healthy_cached() -> bool:
    """Best-effort vLLM readiness. Uses the pull worker's signal when present
    (pull mode); otherwise a cached off-thread probe (push / central-push)."""
    global _vllm_last_probe, _vllm_healthy_cache
    if _pull_worker is not None:
        return bool(_pull_worker.vllm_healthy)
    now = time.monotonic()
    if now - _vllm_last_probe < _VLLM_PROBE_INTERVAL_S:
        return _vllm_healthy_cache
    _vllm_last_probe = now
    _vllm_healthy_cache = await asyncio.to_thread(_probe_vllm_sync)
    return _vllm_healthy_cache


def bind_local_queue(q: LocalQueue):
    global _local_q
    _local_q = q


def bind_pull_worker(pw):
    global _pull_worker
    _pull_worker = pw


class PushItem(BaseModel):
    req_id: str
    prompt: str
    meta: Dict[str, Any] = {}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health() -> dict:
    if _local_q is None:
        return {"status": "error", "queue_len": 0, "inflight": 0, "logical": 0}

    pending, inflight = _local_q.state()
    vllm_ok = _pull_worker.vllm_healthy if _pull_worker is not None else True
    status = "ok" if vllm_ok else "vllm_unhealthy"

    resp = {
        "status": status,
        "vllm_healthy": vllm_ok,
        "queue_len": pending,
        "inflight": inflight,
        "logical": pending + inflight,
    }

    from .kv_usage import get_cached_kv_usage
    kv = get_cached_kv_usage()
    if kv is not None:
        resp["kv_usage"] = kv

    from starlette.responses import JSONResponse
    if not vllm_ok:
        return JSONResponse(content=resp, status_code=503)
    return resp


@app.post("/push")
async def push(item: PushItem) -> dict:
    """
    Push-mode delivery from router → sidecar.

    If TRACE_ENABLED=true, annotate:
      - t_arrive_sidecar_push
      - sidecar_queue_len_before / after
      - sidecar_inflight_before
      - sidecar_logical_before / after

    Also stamps a receipt marker so this can be surfaced in experiment logs:
      - rcpt_push_recv_wall
    """
    if _local_q is None:
        return {"status": "error", "msg": "local queue not bound"}

    # Snapshot queue state *before* enqueue (also used by the backpressure gate).
    pending_before, inflight_before = _local_q.state()
    logical_before = pending_before + inflight_before

    # ------------------------------------------------------------------
    # Readiness + backpressure gate (central-push / push).
    #
    # Pull mode self-regulates: a sidecar only pulls when vLLM is healthy and it
    # has spare capacity. Central-push is router-driven, so gate here to give the
    # router a signal to requeue instead of overrunning a warming/full pod:
    #   - 503 when vLLM is not healthy (still loading / crashed)
    #   - 503 when the local queue is already at capacity (BATCH_SIZE + PREFETCH)
    # ------------------------------------------------------------------
    if not await _vllm_healthy_cached():
        return JSONResponse(
            {"status": "unavailable", "reason": "vllm_unhealthy"},
            status_code=503,
        )
    cap = int(getattr(_cfg, "BATCH_SIZE", 0)) + int(getattr(_cfg, "PREFETCH", 0))
    if cap > 0 and logical_before >= cap:
        return JSONResponse(
            {"status": "busy", "reason": "queue_full", "logical": logical_before},
            status_code=503,
        )

    # Prom: received (router -> sidecar)
    inc_received(_cfg.CONTAINER_NAME)

    meta = dict(item.meta or {})

    if getattr(_cfg, "TRACE_ENABLED", False):
        now_push = time.time()
        tr = dict(meta.get("__trace__") or {})

        # Existing trace
        tr["t_arrive_sidecar_push"] = now_push

        # receipt marker (lets you audit "did sidecar receive it?" from experiment logs)
        tr["rcpt_push_recv_wall"] = now_push

        # Sidecar-local queue lengths
        tr["sidecar_queue_len_before"] = pending_before
        tr["sidecar_inflight_before"] = inflight_before
        tr["sidecar_logical_before"] = logical_before

        # After enqueue, pending increases by 1
        tr["sidecar_queue_len_after"] = pending_before + 1
        tr["sidecar_logical_after"] = logical_before + 1

        meta["__trace__"] = tr

    _local_q.put(item.req_id, item.prompt, meta)
    return {"status": "ok"}
