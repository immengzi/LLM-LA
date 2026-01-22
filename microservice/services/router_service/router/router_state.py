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
            len_policy = _cfg.LEN_POLICY

            # 2) KV scoring
            if kv_enabled:
                kv_pairs = []
                scored = []
                for rid, prompt, ts, meta in pool:
                    kv_hits = prefix_len(endpoint, rid)
                    kv_pairs.append((rid, kv_hits))
                    scored.append((rid, prompt, ts, meta, kv_hits))

                _log_req(
                    f"KV raw endpoint={endpoint}: {kv_pairs}",
                    level="full",
                )

                scored.sort(key=lambda x: (-x[4], x[0]))
                ordered: List[Tuple[str, str, float, dict]] = [
                    (r, p, t, m) for (r, p, t, m, _) in scored
                ]

                _log_req(
                    f"KV sorted endpoint={endpoint}: "
                    f"{[(r, kv) for (r, _p, _t, _m, kv) in scored]}",
                    level="full",
                )
            else:
                ordered = list(pool)

            # 3) Length-aware refinement
            if len_enabled and len_policy:
                refined = select_len_aware(ordered, _pred, len_policy)
                ordered = refined
                _log_req(
                    f"Len policy='{len_policy}' ordering: "
                    f"{[r for (r, _p, _t, _m) in refined]}",
                    level="full",
                )

            # 4) Choose
            chosen_raw = ordered[:want]
            chosen_ids = [rid for (rid, _p, _t, _m) in chosen_raw]
            chosen_kv_hits = [(rid, prefix_len(endpoint, rid)) for rid in chosen_ids]

            _log_req(
                f"chosen endpoint={endpoint}: {chosen_ids} kv_hits={chosen_kv_hits}",
                level="summary",
            )

            # 4a) Attach trace info (if enabled)
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

                    m["__trace__"] = tr
                    chosen.append((rid, prompt, ts, m))
            else:
                chosen = chosen_raw

            # Prom: outgoing dispatch (router -> sidecar) for each assigned item
            for rid, _prompt, _ts, _meta in chosen:
                inc_dispatch(endpoint)

            # 5) Requeue leftovers
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

            # 6) Build output
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

            # If called outside an event loop (shouldn't happen from API),
            # we still create a Future-like placeholder by deferring creation.
            # The async waiter will create it again if needed.
            if loop is None:
                # Store a sentinel None; async path will fix it up.
                self._result_futs[req_id] = None  # type: ignore[assignment]
                return

            fut: asyncio.Future = loop.create_future()
            self._result_futs[req_id] = fut

            # If result already stored, resolve right away
            if req_id in self._result_values and not fut.done():
                fut.set_result(self._result_values[req_id])

    def store_result(self, req_id: str, result: Any) -> None:
        """
        Store result and resolve any waiting Future.
        Safe to call from either sync or async context (FastAPI endpoint is async).
        """
        fut: Optional[asyncio.Future] = None
        loop = None
        with self._lock:
            self._result_values[req_id] = result
            fut = self._result_futs.get(req_id)

        # If there's no future yet (enqueue hasn't registered), that's fine.
        if fut is None:
            return

        # If we stored a sentinel because register_waiter ran outside loop, ignore here.
        if not isinstance(fut, asyncio.Future):
            return

        if fut.done():
            return

        # Resolve on the loop thread safely
        try:
            loop = fut.get_loop()
        except Exception:
            loop = None

        if loop is None:
            # Best-effort direct resolve
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
            # Best-effort fallback
            try:
                _set()
            except Exception:
                pass

    async def wait_for_result_async(self, req_id: str, timeout_s: float) -> Optional[Any]:
        """
        Await the result for req_id up to timeout_s.
        Cleans up waiter + stored result after completion (or timeout).
        """
        # Ensure a real Future exists on THIS running loop
        loop = asyncio.get_running_loop()

        with self._lock:
            fut = self._result_futs.get(req_id)

            if not isinstance(fut, asyncio.Future) or fut.get_loop() is not loop:
                # Create loop-local future and replace
                fut = loop.create_future()
                self._result_futs[req_id] = fut

                # If result already stored, resolve immediately
                if req_id in self._result_values and not fut.done():
                    fut.set_result(self._result_values[req_id])

        try:
            result = await asyncio.wait_for(fut, timeout=float(timeout_s))
        except asyncio.TimeoutError:
            return None
        finally:
            # Cleanup (avoid unbounded growth)
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
