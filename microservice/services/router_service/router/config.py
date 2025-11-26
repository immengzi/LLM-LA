# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import get_config
from .models import EnqueueRequest, EnqueueResponse, PullRequest, PullResponse
from .router_state import router_state
from .kv_watcher import KVWatcher

_cfg = get_config()
app = FastAPI(title="KV-aware Router Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_kv_watcher: KVWatcher | None = None


@app.on_event("startup")
async def _startup():
    global _kv_watcher
    _kv_watcher = KVWatcher()
    _kv_watcher.start()
    print("[router] KVWatcher started.")


@app.on_event("shutdown")
async def _shutdown():
    global _kv_watcher
    if _kv_watcher:
        _kv_watcher.stop()
        print("[router] KVWatcher stopped.")


@app.get("/health")
async def health():
    return {"status": "ok", "queue_len": router_state.size()}


@app.post("/enqueue", response_model=EnqueueResponse)
async def enqueue(req: EnqueueRequest):
    rid = router_state.enqueue(
        prompt=req.prompt,
        t_enq_client=req.t_enq_client,
        meta=req.meta or {},
    )
    return EnqueueResponse(req_id=rid)


@app.post("/pull", response_model=PullResponse)
async def pull(req: PullRequest):
    items = router_state.pull_for_endpoint(
        endpoint=req.endpoint,
        want=req.want,
    )
    return PullResponse(items=items)
