# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Dict, Any
import time

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from .local_queue import LocalQueue
from .config import get_config
from .metrics import inc_received

_cfg = get_config()

app = FastAPI(title="vLLM Sidecar", version="0.1.0")

_local_q: LocalQueue | None = None
_pull_worker = None  # RouterPullWorker | None — set via bind_pull_worker


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

    # Prom: received (router -> sidecar)
    inc_received(_cfg.CONTAINER_NAME)

    meta = dict(item.meta or {})

    # Snapshot queue state *before* enqueue
    pending_before = 0
    inflight_before = 0
    if _local_q is not None:
        pending_before, inflight_before = _local_q.state()
    logical_before = pending_before + inflight_before

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
