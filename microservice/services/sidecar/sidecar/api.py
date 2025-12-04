# -*- coding: utf-8 -*-
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any

from .local_queue import LocalQueue

app = FastAPI(title="vLLM Sidecar", version="0.1.0")

_local_q: LocalQueue | None = None


def bind_local_queue(q: LocalQueue):
    global _local_q
    _local_q = q


class PushItem(BaseModel):
    req_id: str
    prompt: str
    meta: Dict[str, Any] = {}


@app.get("/health")
async def health() -> dict:
    if _local_q is None:
        return {"status": "error", "queue_len": 0, "inflight": 0, "logical": 0}
    pending, inflight = _local_q.state()
    return {
        "status": "ok",
        "queue_len": pending,
        "inflight": inflight,
        "logical": pending + inflight,
    }


@app.post("/push")
async def push(item: PushItem) -> dict:
    if _local_q is None:
        return {"status": "error", "msg": "local queue not bound"}
    _local_q.put(item.req_id, item.prompt, item.meta)
    return {"status": "ok"}
