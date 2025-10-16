# -*- coding: utf-8 -*-
"""
len_select: shared, length-aware queue selector for both pull & push routers.
This version has **no guardrail logic** (LONG_BATCH_GUARD_N removed).

Behavior:
- Build a temporary pool from the queue.
- Predict lengths for each item.
- Pick by predicted length order according to policy:
    * "short_first" (default): shortest → longer
    * "long_first" or "longest_first": longest → shorter
    * "even_short_long": alternate between short and long ends
- Push leftover items back to the queue safely.
"""

from typing import List, Tuple
from queue import Empty, Queue
from collections import deque


def _predict_len(predictor, prompt, default_max, req_id=None) -> int:
    try:
        v = predictor.predict_out_tokens(prompt, req_id=req_id)
        return int(v) if v is not None else int(default_max)
    except Exception:
        return int(default_max)


def select_batch(
    q: "Queue[Tuple[str, float, int]]",
    want: int,
    predictor,
    q_lock,
    *,
    policy: str = "short_first",
    pool_factor: int = 3,
    default_max_tokens: int = 256,
) -> List[Tuple[str, float, int, int]]:
    """
    Select a batch of requests from the queue according to predicted output lengths.
    Args:
        q: shared queue containing (prompt, t_enq_client, req_id)
        want: number of items to pull
        predictor: object implementing predict_out_tokens(prompt, req_id)
        q_lock: shared RLock for queue safety
        policy: "short_first", "long_first", or "even_short_long"
        pool_factor: how many items to peek (pool_size = want * pool_factor)
        default_max_tokens: fallback token length if prediction fails
    Returns:
        List of (prompt, t_enq_client, predicted_out_tokens, req_id)
    """
    if want <= 0:
        return []

    # Build pool by temporarily removing up to pool_size items
    pool_size = min(max(1, int(want)) * int(pool_factor), q.qsize())
    if pool_size <= 0:
        return []

    # Include a stable index to avoid collisions when there are duplicates
    pool: List[Tuple[str, float, int, int, int]] = []  # (p, t, pred, rid, idx)
    with q_lock:
        for idx in range(pool_size):
            try:
                p, t, rid = q.get_nowait()
            except Empty:
                break
            pred = _predict_len(predictor, p, default_max_tokens, req_id=rid)
            pool.append((p, t, pred, rid, idx))

    if not pool:
        return []

    # Sort by predicted length ascending
    pool_sorted = sorted(pool, key=lambda x: x[2])
    dq = deque(pool_sorted)

    picked: List[Tuple[str, float, int, int, int]] = []

    def pop_short():
        return dq.popleft() if dq else None

    def pop_long():
        return dq.pop() if dq else None

    pol = (policy or "short_first").lower().strip()

    # === Selection policies ===
    if pol in ("long_first", "longest_first"):
        while dq and len(picked) < want:
            itm = pop_long()
            if itm is None:
                break
            picked.append(itm)

    elif pol == "even_short_long":
        take_long_next = False  # start from short
        while dq and len(picked) < want:
            itm = pop_long() if take_long_next else pop_short()
            if itm is None:
                break
            picked.append(itm)
            take_long_next = not take_long_next

    else:  # short_first (default)
        while dq and len(picked) < want:
            itm = pop_short()
            if itm is None:
                break
            picked.append(itm)

    # Compute leftovers using stable index
    picked_idx = {idx for *_, idx in picked}
    leftovers = [(p, t, rid) for (p, t, pred, rid, idx) in pool if idx not in picked_idx]

    # Push leftovers back & ack only those
    if leftovers:
        with q_lock:
            for p, t, rid in leftovers:
                q.put((p, t, rid))
            for _ in range(len(leftovers)):
                q.task_done()

    # Strip internal index before returning
    return [(p, t, pred, rid) for (p, t, pred, rid, _idx) in picked]
