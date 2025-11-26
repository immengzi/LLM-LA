# -*- coding: utf-8 -*-
from collections import deque
from threading import RLock
from typing import Deque, Dict, Tuple, List

from .config import get_config
from .kv_aware import prefix_len
from .predictors import get_length_predictor
from .len_select import select_len_aware
from .models import JobItem, now_s

_cfg = get_config()
_pred = get_length_predictor()


class RouterState:
    """
    Central queue of pending jobs + KV + length-aware selection.

    Queue entries: (req_id, prompt, t_enq_client, meta)
    """

    def __init__(self):
        self._lock = RLock()
        self._next_req_id = 0
        self._queue: Deque[Tuple[int, str, float, dict]] = deque()

    # ------------- enqueue -------------

    def enqueue(self, prompt: str, t_enq_client: float | None, meta: dict) -> int:
        with self._lock:
            rid = self._next_req_id
            self._next_req_id += 1
            ts = float(t_enq_client) if t_enq_client else now_s()
            self._queue.append((rid, prompt, ts, meta or {}))
            return rid

    # ------------- pull for endpoint -------------

    def pull_for_endpoint(self, endpoint: str, want: int) -> List[JobItem]:
        if want <= 0:
            return []

        with self._lock:
            if not self._queue:
                return []

            pool_factor = max(1, int(_cfg.POOL_FACTOR))
            max_scan = min(len(self._queue), want * pool_factor)

            # 1) Build a pool (copy, keep original queue intact for the moment)
            pool: List[Tuple[int, str, float, dict]] = []
            for _ in range(max_scan):
                rid, prompt, ts, meta = self._queue.popleft()
                pool.append((rid, prompt, ts, meta))

            # 2) Score by KV first (if enabled)
            kv_enabled = bool(_cfg.KV_AWARE)
            len_enabled = bool(_cfg.LEN_AWARE)
            len_policy = _cfg.LEN_POLICY

            if kv_enabled:
                scored: List[Tuple[int, str, float, dict, int]] = []
                for rid, prompt, ts, meta in pool:
                    hits = prefix_len(endpoint, rid)
                    scored.append((rid, prompt, ts, meta, hits))

                # KV-first: sort by kv_hits desc, then FIFO
                scored.sort(key=lambda x: (-x[4], x[0]))
                ordered = [(r, p, t, m) for (r, p, t, m, _) in scored]
            else:
                # KV off -> FIFO order
                ordered = list(pool)

            # 3) Length-aware refinement inside the KV-ordered pool
            if len_enabled and len_policy:
                ordered = select_len_aware(ordered, _pred, len_policy)

            # 4) Take the first `want` items as chosen
            chosen = ordered[:want]
            chosen_ids = {rid for (rid, _p, _t, _m) in chosen}

            # 5) Everything else goes back into the queue (preserving order)
            leftovers: List[Tuple[int, str, float, dict]] = []
            for item in ordered[want:]:
                leftovers.append(item)

            # Reconstruct main queue: leftovers + everything that wasn’t scanned
            for rid, prompt, ts, meta in leftovers:
                self._queue.appendleft((rid, prompt, ts, meta))  # prepend leftovers

            # move remaining not-scanned tail after that
            # NOTE: we've already consumed max_scan from the left; the rest of
            # the original queue is still there, so we just leave it.

            # 6) Build JobItem list to return
            items: List[JobItem] = []
            for rid, prompt, ts, meta in chosen:
                items.append(JobItem(req_id=rid, prompt=prompt, t_enq_client=ts, meta=meta))

            return items

    # ------------- metrics / debug -------------

    def size(self) -> int:
        with self._lock:
            return len(self._queue)


# single global state for the FastAPI app
router_state = RouterState()
