# router/router_state.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import deque
from threading import RLock
from typing import Deque, Dict, Tuple, List, Any, Optional
import sys
import uuid
import asyncio

from .config import get_config
from .kv_aware import prefix_len
from .predictors import get_length_predictor
from .len_select import select_len_aware
from .models import JobItem, now_s
from .metrics import set_central_queue_length, inc_dispatch

_cfg = get_config()
_pred = get_length_predictor()


def _log_req(msg: str, *, level: str = "summary") -> None:
    """
    Centralized logging for pull-routing decisions.
    Honors _cfg.REQ_LOG_MODE: off | summary | full
    """
    mode = str(_cfg.REQ_LOG_MODE).lower()

    if mode == "off":
        return

    if level == "summary":
        print(f"[PullRouter] {msg}")
        sys.stdout.flush()
    elif level == "full" and mode == "full":
        print(f"[PullRouter] {msg}")
        sys.stdout.flush()


class RouterState:
    """
    Central queue of pending jobs + KV + length-aware selection.

      - Result waiters are asyncio Futures
      - This avoids the router enqueue handler blocking in asyncio.to_thread(...)
        and eliminates massive router_wakeup_s artifacts under load.
    """

    def __init__(self):
        self._lock = RLock()
        # queue entries: (req_id, prompt, t_enq_client_or_router, meta)
        self._queue: Deque[Tuple[str, str, float, dict]] = deque()

        # Result tracking (async-native)
        # req_id -> asyncio.Future that will hold the result
        self._result_futs: Dict[str, asyncio.Future] = {}
        # req_id -> stored result (for early-arriving /result before waiter exists)
        self._result_values: Dict[str, Any] = {}

        # initialize gauge
        set_central_queue_length(0)

    # -------------------------------------------------------
    # ID allocation
    # -------------------------------------------------------

    def next_req_id(self) -> str:
        with self._lock:
            return uuid.uuid4().hex

    # -------------------------------------------------------
    # Enqueue
    # -------------------------------------------------------

    def enqueue(self, prompt: str, t_enq_client: float | None, meta: dict) -> str:
        """
        Enqueue a new request in pull mode.

        t_enq_client: client-side enqueue timestamp (if provided),
        otherwise we stamp with router now().
        """
        with self._lock:
            rid = self.next_req_id()
            ts = float(t_enq_client) if t_enq_client else now_s()
            self._queue.append((rid, prompt, ts, meta or {}))
            set_central_queue_length(len(self._queue))
            return rid

    def update_meta(self, req_id: str, meta: dict) -> None:
        """
        In-place update of meta for a queued request.

        Used by the API layer to inject trace info after enqueue without
        changing queue order or timestamps.
        """
        with self._lock:
            if not self._queue:
                return

            new_q: Deque[Tuple[str, str, float, dict]] = deque()
            updated = False
            while self._queue:
                rid, prompt, ts, old_meta = self._queue.popleft()
                if not updated and rid == req_id:
                    new_q.append((rid, prompt, ts, meta or {}))
                    updated = True
                else:
                    new_q.append((rid, prompt, ts, old_meta))
            self._queue = new_q

            # length unchanged, but keep gauge consistent anyway
            set_central_queue_length(len(self._queue))

    # -------------------------------------------------------
    # Pull (KV-aware + length-aware)
    # -------------------------------------------------------

    def pull_for_endpoint(self, endpoint: str, want: int) -> List[JobItem]:
        if want <= 0:
            return []

        with self._lock:
            if not self._queue:
                set_central_queue_length(0)
                return []

            pool_factor = max(1, int(_cfg.POOL_FACTOR))
            max_scan = min(len(self._queue), want * pool_factor)

            # 1) Build pool
            pool: List[Tuple[str, str, float, dict]] = []
            for _ in range(max_scan):
                rid, prompt, ts, meta = self._queue.popleft()
                pool.append((rid, prompt, ts, meta))

            # queue length changed after draining pool
            set_central_queue_length(len(self._queue))

            _log_req(
                f"endpoint={endpoint} want={want} pool_size={len(pool)} "
                f"queue_remaining={len(self._queue)}",
                level="full",
            )

            kv_enabled = bool(_cfg.KV_AWARE)
            len_enabled = bool(_cfg.LEN_AWARE)
            len_policy = str(_cfg.LEN_POLICY or "")

            # ---------------------------------------------------
            # KV-first ordering, then length refinement ONLY
            # within equal KV-hit tiers (no KV overwrite).
            # ---------------------------------------------------

            # 2) Compute KV hits for the pool (or 0s if KV disabled)
            kv_pairs: List[Tuple[str, int]] = []
            pool_with_kv: List[Tuple[str, str, float, dict, int]] = []

            if kv_enabled:
                for rid, prompt, ts, meta in pool:
                    kv_hits = prefix_len(endpoint, rid)
                    kv_pairs.append((rid, kv_hits))
                    pool_with_kv.append((rid, prompt, ts, meta, kv_hits))

                _log_req(
                    f"KV raw endpoint={endpoint}: {kv_pairs}",
                    level="full",
                )
            else:
                for rid, prompt, ts, meta in pool:
                    pool_with_kv.append((rid, prompt, ts, meta, 0))

            kv_hits_map: Dict[str, int] = {}
            if kv_enabled:
                for rid, _prompt, _ts, _meta, kv_hits in pool_with_kv:
                    kv_hits_map[rid] = int(kv_hits)

            # 3) Group by kv_hits, descending (KV is always primary)
            kv_to_items: Dict[int, List[Tuple[str, str, float, dict]]] = {}
            for rid, prompt, ts, meta, kv_hits in pool_with_kv:
                kv_to_items.setdefault(int(kv_hits), []).append((rid, prompt, ts, meta))

            kv_levels = sorted(kv_to_items.keys(), reverse=True)

            # Log KV tiering deterministically
            if kv_enabled:
                _log_req(
                    f"KV tiers endpoint={endpoint}: "
                    f"{[(k, [r for (r, _p, _t, _m) in kv_to_items[k]]) for k in kv_levels]}",
                    level="full",
                )

            # 4) Build final ordered list: concatenate KV tiers, and (optionally)
            #    apply length-aware ordering ONLY within each tier.
            ordered: List[Tuple[str, str, float, dict]] = []
            for kv_hits in kv_levels:
                tier = kv_to_items[kv_hits]

                # Stable deterministic baseline inside tier: req_id
                tier.sort(key=lambda x: x[0])

                if len_enabled and len_policy:
                    tier_refined = select_len_aware(tier, _pred, len_policy)
                    tier = tier_refined

                    _log_req(
                        f"Len refine within KV={kv_hits} policy='{len_policy}': "
                        f"{[r for (r, _p, _t, _m) in tier]}",
                        level="full",
                    )

                ordered.extend(tier)

            if kv_enabled:
                # For debugging: show final (rid, kv_hits) order
                _log_req(
                    f"KV-first final order endpoint={endpoint}: "
                    f"{[(r, prefix_len(endpoint, r)) for (r, _p, _t, _m) in ordered]}",
                    level="full",
                )

            # 5) Choose
            chosen_raw = ordered[:want]
            chosen_ids = [rid for (rid, _p, _t, _m) in chosen_raw]
            chosen_kv_hits = [(rid, prefix_len(endpoint, rid)) for rid in chosen_ids] if kv_enabled else []

            _log_req(
                f"chosen endpoint={endpoint}: {chosen_ids} kv_hits={chosen_kv_hits}",
                level="summary",
            )

            # 5a) Attach trace info (if enabled)
            chosen: List[Tuple[str, str, float, dict]] = []
            dispatch_ts = now_s()
            if getattr(_cfg, "TRACE_ENABLED", False):
                qlen_at_dispatch = len(self._queue)

                for rid, prompt, ts, meta in chosen_raw:
                    m = dict(meta or {})
                    tr = dict(m.get("__trace__") or {})

                    tr.setdefault("t_enq_router_queue", ts)
                    tr.setdefault("endpoint", endpoint)

                    tr["t_dispatch_router"] = dispatch_ts
                    tr["router_queue_len_at_dispatch"] = qlen_at_dispatch

                    if kv_enabled:
                        tr["kv_hits_len"] = int(kv_hits_map.get(rid, 0))

                    m["__trace__"] = tr
                    chosen.append((rid, prompt, ts, m))
            else:
                chosen = chosen_raw

            # Prom: outgoing dispatch (router -> sidecar) for each assigned item
            for rid, _prompt, _ts, _meta in chosen:
                inc_dispatch(endpoint)

            # 6) Requeue leftovers
            leftovers = ordered[want:]
            for rid, prompt, ts, meta in leftovers:
                self._queue.appendleft((rid, prompt, ts, meta))

            # queue length changed after requeue
            set_central_queue_length(len(self._queue))

            if leftovers:
                _log_req(
                    f"leftovers requeued: "
                    f"{[r for (r, _p, _t, _m) in leftovers]}",
                    level="full",
                )

            # 7) Build output
            items = [
                JobItem(req_id=rid, prompt=prompt, t_enq_client=ts, meta=meta)
                for (rid, prompt, ts, meta) in chosen
            ]
            return items

    # -------------------------------------------------------
    # Result wait/notify (ASYNC)
    # -------------------------------------------------------

    def register_waiter(self, req_id: str) -> None:
        """
        Ensure an asyncio Future exists for this req_id.
        If a result already arrived (early /result), resolve immediately.
        """
        with self._lock:
            if req_id in self._result_futs:
                return

            loop = None
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop is None:
                self._result_futs[req_id] = None  # type: ignore[assignment]
                return

            fut: asyncio.Future = loop.create_future()
            self._result_futs[req_id] = fut

            if req_id in self._result_values and not fut.done():
                fut.set_result(self._result_values[req_id])

    def store_result(self, req_id: str, result: Any) -> None:
        """
        Store result and resolve any waiting Future.
        """
        fut: Optional[asyncio.Future] = None
        loop = None
        with self._lock:
            self._result_values[req_id] = result
            fut = self._result_futs.get(req_id)

        if fut is None:
            return
        if not isinstance(fut, asyncio.Future):
            return
        if fut.done():
            return

        try:
            loop = fut.get_loop()
        except Exception:
            loop = None

        if loop is None:
            try:
                fut.set_result(result)
            except Exception:
                pass
            return

        def _set():
            if not fut.done():
                fut.set_result(result)

        try:
            loop.call_soon_threadsafe(_set)
        except Exception:
            try:
                _set()
            except Exception:
                pass

    async def wait_for_result_async(self, req_id: str, timeout_s: float) -> Optional[Any]:
        """
        Await the result for req_id up to timeout_s.
        """
        loop = asyncio.get_running_loop()

        with self._lock:
            fut = self._result_futs.get(req_id)
            if not isinstance(fut, asyncio.Future) or fut.get_loop() is not loop:
                fut = loop.create_future()
                self._result_futs[req_id] = fut
                if req_id in self._result_values and not fut.done():
                    fut.set_result(self._result_values[req_id])

        try:
            result = await asyncio.wait_for(fut, timeout=float(timeout_s))
        except asyncio.TimeoutError:
            return None
        finally:
            with self._lock:
                self._result_futs.pop(req_id, None)
                self._result_values.pop(req_id, None)

        return result

    # -------------------------------------------------------
    # Metrics
    # -------------------------------------------------------

    def size(self) -> int:
        with self._lock:
            return len(self._queue)


# global instance
router_state = RouterState()
