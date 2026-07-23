# router/external_push.py
# -*- coding: utf-8 -*-
"""External-push dispatcher.

Like central-push, external-push admits requests into the central queue (so
KV-affinity, fairness and SLO scheduling via ``pull_for_endpoint`` all apply),
and the router -- not a sidecar -- decides when/how much to dispatch. The one
difference: endpoints are STATIC external vLLM servers (no sidecar), so delivery
goes DIRECTLY to the external vLLM OpenAI endpoint and the response is ingested
inline via the router's ``_ingest_result_payload`` callback.

Concurrency model:
  * Each dispatch pass computes ``CAP - in-flight`` per endpoint and asks
    ``pull_for_endpoint`` for that many items (which increments in-flight).
  * Delivery to an external vLLM is the FULL inference and can be slow, so each
    item is delivered in its own background asyncio task; the pass returns as
    soon as tasks are spawned. In-flight is held from selection until the task
    ingests a result (success OR error), which releases it -- so the per-pod CAP
    is respected without blocking the dispatch loop.
  * Unhealthy endpoints (failed /health probe) are skipped for the pass.
"""
import asyncio
from typing import Any, Callable, Optional, Set

from .config import get_config
from .metrics import (
    inc_central_push_pass,
    inc_central_push_pushed,
    inc_central_push_failed,
    set_central_push_want,
)

_cfg = get_config()


def _log(msg: str, *, level: str = "summary") -> None:
    mode = str(getattr(_cfg, "REQ_LOG_MODE", "off")).lower()
    if mode == "off":
        return
    if level == "summary" or (level == "full" and mode == "full"):
        print(f"[ExternalPush] {msg}")


class ExternalPushDispatcher:
    def __init__(
        self,
        router_state: Any,
        registry: Any,
        vllm_client: Any,
        ingest_result: Callable[[dict], None],
        *,
        cap: int,
        interval_s: float,
    ) -> None:
        self._rs = router_state
        self._registry = registry
        self._client = vllm_client
        self._ingest = ingest_result
        self._cap = max(1, int(cap))
        self._interval_s = max(0.001, float(interval_s))

        self._event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._stop = False
        # Track live delivery tasks so shutdown can drain them.
        self._inflight_tasks: Set[asyncio.Task] = set()

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run())
        _log(f"started (cap={self._cap} interval_s={self._interval_s})")

    async def stop(self) -> None:
        self._stop = True
        self._event.set()
        if self._task is not None:
            try:
                await self._task
            except Exception:
                pass
            self._task = None
        # Let outstanding deliveries finish ingesting (best-effort, bounded).
        if self._inflight_tasks:
            try:
                await asyncio.wait(self._inflight_tasks, timeout=5.0)
            except Exception:
                pass
        _log("stopped")

    def kick(self) -> None:
        """Request a dispatch pass now (coalesced)."""
        try:
            self._event.set()
        except Exception:
            pass

    async def _run(self) -> None:
        while not self._stop:
            try:
                await asyncio.wait_for(self._event.wait(), timeout=self._interval_s)
            except asyncio.TimeoutError:
                pass
            self._event.clear()
            if self._stop:
                break
            try:
                await self._dispatch_pass()
            except Exception as e:
                _log(f"dispatch pass error: {e!r}", level="full")

    def _spawn_delivery(self, ep: str, item: Any) -> None:
        task = asyncio.create_task(self._deliver_one(ep, item))
        self._inflight_tasks.add(task)
        task.add_done_callback(self._inflight_tasks.discard)

    async def _deliver_one(self, ep: str, item: Any) -> None:
        """Deliver one request directly to the external vLLM, then ingest the
        result (releasing in-flight). Never raises."""
        try:
            result_obj = await self._client.deliver(ep, item.req_id, item.prompt, item.meta)
        except Exception as e:  # defensive: client.deliver already catches
            result_obj = {
                "output": f"[external error: {e}]",
                "finish_reason": "error",
                "error": str(e),
                "endpoint_id": ep,
            }
        payload = {"req_id": item.req_id, "result": result_obj, "endpoint": ep}
        try:
            self._ingest(payload)
        except Exception as e:
            _log(f"ingest failed ep={ep} req_id={item.req_id}: {e!r}", level="full")
            # Ensure in-flight is released even if ingest blew up.
            try:
                self._rs.release_inflight(item.req_id, ep)
            except Exception:
                pass
        if isinstance(result_obj, dict) and result_obj.get("finish_reason") == "error":
            inc_central_push_failed(ep)
        else:
            inc_central_push_pushed(ep)

    async def _dispatch_pass(self) -> None:
        async with self._lock:
            # Refresh KV-usage samples from external /health is not applicable
            # (no sidecar kv_usage); soft divert simply no-ops for external.
            await self._registry.refresh_health()

            endpoints = self._registry.healthy_ids()
            if not endpoints:
                return

            inc_central_push_pass()

            models = self._rs.active_models()
            inflight = self._rs.endpoint_inflight_snapshot()

            for model in models:
                for ep in endpoints:
                    cur = int(inflight.get(ep, 0))
                    want = self._cap - cur
                    set_central_push_want(ep, max(0, want))
                    if want <= 0:
                        continue

                    items = self._rs.pull_for_endpoint(ep, want, model)
                    if not items:
                        continue

                    inflight[ep] = cur + len(items)
                    for it in items:
                        self._spawn_delivery(ep, it)
