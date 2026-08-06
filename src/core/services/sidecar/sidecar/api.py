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
from .engine_profile import get_engine_health_profile
from .metrics import inc_received

_cfg = get_config()

app = FastAPI(title="Inference Sidecar", version="0.1.0")

_local_q: LocalQueue | None = None
_pull_worker = None  # RouterPullWorker | None — set via bind_pull_worker
_kv_subscriber = None  # KVSubscriber | None — set via bind_kv_subscriber

# Cached inference health probe for push / central-push modes, where there is no
# RouterPullWorker to maintain the health signal. Probed at most once per
# interval so /push stays cheap under load.
_INFERENCE_PROBE_INTERVAL_S = 1.0
_engine_last_probe = 0.0
_engine_healthy_cache = True
_engine_live_last_probe = 0.0
_engine_live_cache = True


def _engine_profile():
    return get_engine_health_profile(
        _cfg.INFERENCE_ENGINE,
        health_path=_cfg.INFERENCE_HEALTH_PATH,
        readiness_path=getattr(_cfg, "INFERENCE_READINESS_PATH", ""),
    )


def _inference_health_url(*, readiness: bool = False) -> str:
    profile = _engine_profile()
    path = profile.readiness_path if readiness else profile.health_path
    return (
        f"{_cfg.INFERENCE_URL.rstrip('/')}/"
        f"{path.lstrip('/')}"
    )


def _probe_engine_sync(*, readiness: bool = False) -> bool:
    try:
        r = requests.get(
            _inference_health_url(readiness=readiness),
            timeout=float(getattr(_cfg, "INFERENCE_HEALTH_TIMEOUT_S", 2.0)),
        )
        return _engine_profile().accepts(r)
    except Exception:
        return False


async def _engine_healthy_cached(*, readiness: bool = True) -> bool:
    """Best-effort engine readiness. Uses the pull worker's signal when present
    (pull mode); otherwise a cached off-thread probe (push / central-push)."""
    global _engine_last_probe, _engine_healthy_cache
    global _engine_live_last_probe, _engine_live_cache
    if _pull_worker is not None:
        return bool(
            getattr(
                _pull_worker,
                "engine_healthy",
                getattr(_pull_worker, "vllm_healthy", False),
            )
        )
    if (
        _engine_profile().engine_type == "vllm"
        and str(getattr(_cfg, "SIDECAR_MODE", "pull")).lower() == "pull"
    ):
        # Preserve the legacy pre-bind health behavior used during vLLM startup.
        return True
    now = time.monotonic()
    if readiness:
        if now - _engine_last_probe < _INFERENCE_PROBE_INTERVAL_S:
            return _engine_healthy_cache
        _engine_last_probe = now
        _engine_healthy_cache = await asyncio.to_thread(
            _probe_engine_sync, readiness=True
        )
        return _engine_healthy_cache
    if now - _engine_live_last_probe < _INFERENCE_PROBE_INTERVAL_S:
        return _engine_live_cache
    _engine_live_last_probe = now
    _engine_live_cache = await asyncio.to_thread(
        _probe_engine_sync, readiness=False
    )
    return _engine_live_cache


# Internal compatibility aliases retained for older tests/imports.
_VLLM_PROBE_INTERVAL_S = _INFERENCE_PROBE_INTERVAL_S
def _probe_vllm_sync() -> bool:
    return _probe_engine_sync()


async def _vllm_healthy_cached() -> bool:
    return await _engine_healthy_cached()


def bind_local_queue(q: LocalQueue):
    global _local_q
    _local_q = q


def bind_pull_worker(pw):
    global _pull_worker
    _pull_worker = pw


def bind_kv_subscriber(subscriber):
    global _kv_subscriber
    _kv_subscriber = subscriber


class PushItem(BaseModel):
    req_id: str
    prompt: str
    meta: Dict[str, Any] = {}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health() -> dict:
    return await _health_response(readiness=False)


@app.get("/ready")
async def ready() -> dict:
    return await _health_response(readiness=True)


async def _health_response(*, readiness: bool) -> dict:
    profile = _engine_profile()
    if _local_q is None:
        resp = {
            "status": "error",
            "engine": profile.engine_type,
            "engine_ready": False,
            "engine_healthy": False,
            "vllm_healthy": False,
            "queue_len": 0,
            "inflight": 0,
            "logical": 0,
        }
        return JSONResponse(content=resp, status_code=503)

    pending, inflight = _local_q.state()
    engine_ok = await _engine_healthy_cached(readiness=readiness)
    kv_status = _kv_subscriber.status if _kv_subscriber is not None else None
    kv_required = readiness and profile.engine_type == "sglang"
    kv_ready = bool(kv_status and kv_status.ready)
    overall_ok = engine_ok and (not kv_required or kv_ready)
    if not engine_ok:
        status = (
            "vllm_unhealthy"
            if profile.engine_type == "vllm"
            else "engine_unhealthy"
        )
    elif kv_required and not kv_ready:
        status = "kv_unready"
    else:
        status = "ok"

    resp = {
        "status": status,
        "engine": profile.engine_type,
        "engine_ready": engine_ok,
        "probe": "readiness" if readiness else "liveness",
        "engine_healthy": engine_ok,
        "vllm_healthy": engine_ok,
        "kv_ready": kv_ready,
        "kv_status": (
            {
                "healthy": kv_status.healthy,
                "fail_closed": kv_status.fail_closed,
                "phase": kv_status.phase,
                "detail": kv_status.detail,
                "cache_visibility": kv_status.cache_visibility,
            }
            if kv_status is not None
            else None
        ),
        "queue_len": pending,
        "inflight": inflight,
        "logical": pending + inflight,
    }

    from .kv_usage import get_cached_kv_usage
    kv = get_cached_kv_usage()
    if kv is not None:
        resp["kv_usage"] = kv

    if not overall_ok:
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
        reason = (
            "vllm_unhealthy"
            if _engine_profile().engine_type == "vllm"
            else "engine_unhealthy"
        )
        return JSONResponse(
            {"status": "unavailable", "reason": reason},
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
