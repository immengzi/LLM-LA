# -*- coding: utf-8 -*-
from collections import deque
from threading import RLock, Event
from typing import Deque, Dict, Tuple, List, Any, Optional
import sys

from .config import get_config
from .kv_aware import prefix_len
from .predictors import get_length_predictor
from .len_select import select_len_aware
from .models import JobItem, now_s

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
    """

    def __init__(self):
        self._lock = RLock()
        self._next_req_id = 0
        self._queue: Deque[Tuple[int, str, float, dict]] = deque()

        # Result tracking
        self._result_events: Dict[int, Event] = {}
        self._result_values: Dict[int, Any] = {}

    # -------------------------------------------------------
    # ID allocation
    # -------------------------------------------------------

    def next_req_id(self) -> int:
        with self._lock:
            rid = self._next_req_id
            self._next_req_id += 1
            return rid

    # -------------------------------------------------------
    # Enqueue
    # -------------------------------------------------------

    def enqueue(self, prompt: str, t_enq_client: float | None, meta: dict) -> int:
        with self._lock:
            rid = self._next_req_id
            self._next_req_id += 1
            ts = float(t_enq_client) if t_enq_client else now_s()
            self._queue.append((rid, prompt, ts, meta or {}))
            return rid

    # -------------------------------------------------------
    # Pull (KV-aware + length-aware)
    # -------------------------------------------------------

    def pull_for_endpoint(self, endpoint: str, want: int) -> List[JobItem]:
        if want <= 0:
            return []

        with self._lock:
            if not self._queue:
                return []

            pool_factor = max(1, int(_cfg.POOL_FACTOR))
            max_scan = min(len(self._queue), want * pool_factor)

            # 1) Build pool
            pool: List[Tuple[int, str, float, dict]] = []
            for _ in range(max_scan):
                rid, prompt, ts, meta = self._queue.popleft()
                pool.append((rid, prompt, ts, meta))

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
                ordered = [(r, p, t, m) for (r, p, t, m, _) in scored]

                _log_req(
                    f"KV sorted endpoint={endpoint}: {[(r, kv) for (r, _p, _t, _m, kv) in scored]}",
                    level="full",
                )
            else:
                kv_pairs = []
                ordered = list(pool)

            # 3) Length-aware refinement
            if len_enabled and len_policy:
                refined = select_len_aware(ordered, _pred, len_policy)
                ordered = refined
                _log_req(
                    f"Len policy='{len_policy}' ordering: {[r for (r, _p, _t, _m) in refined]}",
                    level="full",
                )

            # 4) Choose
            chosen = ordered[:want]
            chosen_ids = [rid for (rid, _p, _t, _m) in chosen]

            # KV stats for chosen items — available in summary mode
            chosen_kv_hits = [(rid, prefix_len(endpoint, rid)) for rid in chosen_ids]

            _log_req(
                f"chosen endpoint={endpoint}: {chosen_ids} kv_hits={chosen_kv_hits}",
                level="summary",
            )

            # 5) Requeue leftovers
            leftovers = ordered[want:]
            for rid, prompt, ts, meta in leftovers:
                self._queue.appendleft((rid, prompt, ts, meta))

            if leftovers:
                _log_req(
                    f"leftovers requeued: {[r for (r, _p, _t, _m) in leftovers]}",
                    level="full",
                )

            # 6) Build output
            items = [
                JobItem(req_id=rid, prompt=prompt, t_enq_client=ts, meta=meta)
                for (rid, prompt, ts, meta) in chosen
            ]

            return items

    # -------------------------------------------------------
    # Result wait/notify
    # -------------------------------------------------------

    def register_waiter(self, req_id: int) -> None:
        with self._lock:
            if req_id not in self._result_events:
                self._result_events[req_id] = Event()

    def store_result(self, req_id: int, result: Any) -> None:
        evt: Optional[Event] = None
        with self._lock:
            self._result_values[req_id] = result
            evt = self._result_events.get(req_id)
        if evt is not None:
            evt.set()

    def wait_for_result(self, req_id: int, timeout_s: float) -> Optional[Any]:
        with self._lock:
            evt = self._result_events.get(req_id)
            if evt is None:
                evt = Event()
                self._result_events[req_id] = evt

        ok = evt.wait(timeout_s)
        if not ok:
            return None

        with self._lock:
            result = self._result_values.get(req_id)
            self._result_events.pop(req_id, None)
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
