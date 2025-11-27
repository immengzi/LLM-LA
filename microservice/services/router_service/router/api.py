# -*- coding: utf-8 -*-
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import httpx

from .config import get_config
from .models import EnqueueRequest, EnqueueResponse, PullRequest, PullResponse
from .router_state import router_state
from .kv_watcher import KVWatcher
from .kv_aware import register_request_blocks
from .push_router import PushRouter  # NEW

_cfg = get_config()
app = FastAPI(title="KV-aware Router Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_kv_watcher: KVWatcher | None = None
_push_router: PushRouter | None = None


@app.on_event("startup")
async def _startup():
    global _kv_watcher, _push_router
    _kv_watcher = KVWatcher()
    _kv_watcher.start()
    print("[router] KVWatcher started.")

    if _cfg.ROUTER_MODE.startswith("push-"):
        _push_router = PushRouter(mode=_cfg.ROUTER_MODE)
        print(f"[router] PushRouter started in mode={_cfg.ROUTER_MODE}")
    else:
        print("[router] running in PULL mode.")


@app.on_event("shutdown")
async def _shutdown():
    global _kv_watcher, _push_router
    if _kv_watcher:
        _kv_watcher.stop()
        print("[router] KVWatcher stopped.")

    # nothing persistent to close in PushRouter right now,
    # but we keep the variable for future extensions
    _push_router = None
    print("[router] PushRouter cleared.")


@app.get("/health")
async def health():
    return {"status": "ok", "queue_len": router_state.size()}


@app.post("/enqueue", response_model=EnqueueResponse)
async def enqueue(req: EnqueueRequest):
    """
    In pull mode:
      - enqueue into central router queue (like old router_core)
    In push-* modes:
      - only allocate req_id, do NOT enqueue (sidecars own the work)
    """
    # 1) Generate req_id and optionally enqueue
    if _cfg.ROUTER_MODE.startswith("push-"):
        # Push-mode: central queue is not used, we just allocate an ID
        rid = router_state.next_req_id()
    else:
        # Pull-mode: behave like the old router_core, queue holds pending jobs
        rid = router_state.enqueue(
            prompt=req.prompt,
            t_enq_client=req.t_enq_client,
            meta=req.meta or {},
        )

    # 2) Optionally compute KV block hashes & register them
    if _cfg.KV_AWARE:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{_cfg.HASH_SERVICE_URL}/compute_hashes",
                    json={"prompt": req.prompt},
                    timeout=2.0,
                )
                resp.raise_for_status()
                data = resp.json()
                block_hashes = data.get("block_hashes") or []
                if block_hashes:
                    register_request_blocks(rid, block_hashes)
        except Exception as e:
            # Best-effort: we still accept the request; just lose KV-awareness for this req
            print(f"[router] WARNING: failed to compute/register block hashes for req_id={rid}: {e}")

    # 3) If in push mode, immediately route + push to a sidecar
    if _cfg.ROUTER_MODE.startswith("push-"):
        if _push_router is None:
            raise HTTPException(status_code=500, detail="PushRouter not initialized")
        try:
            await _push_router.route_and_push(rid, req.prompt, req.meta or {})
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"push failed: {e}")

    return EnqueueResponse(req_id=rid)


@app.post("/pull", response_model=PullResponse)
async def pull(req: PullRequest):
    items = router_state.pull_for_endpoint(
        endpoint=req.endpoint,
        want=req.want,
    )
    return PullResponse(items=items)
