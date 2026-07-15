# router/central_push.py
# -*- coding: utf-8 -*-
"""Central-push dispatcher.

Central-push admits requests into the central queue exactly like pull (so
KV-affinity, fairness and SLO scheduling all apply), but the router -- not the
sidecar -- decides when and how much to dispatch. On each pass the dispatcher
computes a per-endpoint capacity of ``CAP - in-flight`` and asks the existing
scheduler (``pull_for_endpoint``) for that many items, then delivers them to the
sidecar via ``POST /push``. The sidecar never pulls.

Design notes:
  * Single-flight: a pass runs under an asyncio.Lock so the periodic tick and
    event-driven kicks never overlap.
  * Coalesced kicks: kick() just sets an Event, so a burst of enqueues triggers
    at most one extra pass.
  * Capacity is composed with existing throttles: ``pull_for_endpoint`` still
    applies FIXED_BATCH_SIZE, admission throttle and fair-throttle on top of the
    want we pass in. Self-pinned KV/affinity items are never overridden.
  * On delivery failure the item is requeued to the front and in-flight is
    decremented (pull_for_endpoint incremented it on selection); the endpoint is
    skipped for the rest of the pass to avoid hammering a full/unready pod.
"""
import asyncio
import time
from typing import Any, Optional, Set

from .config import get_config
from .metrics import (
    inc_central_push_pass,
    inc_central_push_pushed,
    inc_central_push_requeued,
    inc_central_push_failed,
    set_central_push_want,
)

_cfg = get_config()


def _log(msg: str, *, level: str = "summary") -> None:
    mode = str(getattr(_cfg, "REQ_LOG_MODE", "off")).lower()
    if mode == "off":
        return
    if level == "summary" or (level == "full" and mode == "full"):
        print(f"[CentralPush] {msg}")


class CentralPushDispatcher:
    def __init__(
        self,
        router_state: Any,
        push_router: Any,
        *,
        cap: int,
        interval_s: float,
    ) -> None:
        self._rs = router_state
        self._pr = push_router
        self._cap = max(1, int(cap))
        self._interval_s = max(0.001, float(interval_s))

        self._event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._stop = False
        self._last_kv_poll = 0.0

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
        _log("stopped")

    def kick(self) -> None:
        """Request a dispatch pass now (coalesced)."""
        try:
            self._event.set()
        except Exception:
            pass

    async def _run(self) -> None:
        while not self._stop:
            # Wake on kick or on the periodic tick, whichever comes first.
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

    async def _dispatch_pass(self) -> None:
        async with self._lock:
            # Soft KV divert needs fresh samples; sidecars never /pull in this mode.
            poll_s = float(getattr(_cfg, "KV_HEALTH_POLL_INTERVAL_S", 5.0) or 0.0)
            if bool(getattr(_cfg, "KV_SOFT_DIVERT", False)) and poll_s > 0:
                now = time.time()
                if (now - self._last_kv_poll) >= poll_s:
                    try:
                        await self._pr.refresh_kv_usage_from_health(self._rs)
                    except Exception as e:
                        _log(f"kv health poll error: {e!r}", level="full")
                    self._last_kv_poll = now

            endpoints = self._pr.endpoints_snapshot()
            if not endpoints:
                return

            inc_central_push_pass()

            models = self._rs.active_models()
            # Local view of in-flight, kept current within the pass so grants
            # across models/endpoints respect the per-pod cap cumulatively.
            inflight = self._rs.endpoint_inflight_snapshot()
            failed: Set[str] = set()

            for model in models:
                for ep in endpoints:
                    if ep in failed:
                        continue
                    cur = int(inflight.get(ep, 0))
                    want = self._cap - cur
                    set_central_push_want(ep, max(0, want))
                    if want <= 0:
                        continue

                    items = self._rs.pull_for_endpoint(ep, want, model)
                    if not items:
                        continue

                    inflight[ep] = cur + len(items)

                    for idx, it in enumerate(items):
                        try:
                            await self._pr.push_to_endpoint(
                                ep, it.req_id, it.prompt, it.meta,
                            )
                            inc_central_push_pushed(ep)
                        except Exception as e:
                            # Delivery failed. Every item from here to the end of
                            # this batch was already dequeued and counted in-flight
                            # by pull_for_endpoint but not delivered, so roll all of
                            # them back: release in-flight + re-admit to the front
                            # for a later pass, and skip this endpoint for the rest
                            # of this pass (likely full or not ready).
                            remaining = items[idx:]
                            for rem in remaining:
                                self._rs.release_inflight(rem.req_id, ep)
                            self._rs.requeue_front(
                                model,
                                [
                                    (r.req_id, r.prompt, r.t_enq_client, r.meta)
                                    for r in remaining
                                ],
                            )
                            inflight[ep] = max(
                                0, int(inflight.get(ep, 0)) - len(remaining)
                            )
                            inc_central_push_failed(ep)
                            inc_central_push_requeued(ep, len(remaining))
                            failed.add(ep)
                            _log(
                                f"delivery failed ep={ep} req_id={it.req_id}: {e!r}; "
                                f"requeued {len(remaining)} item(s)",
                                level="full",
                            )
                            break
