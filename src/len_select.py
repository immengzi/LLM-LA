# -*- coding: utf-8 -*-
"""
len_select: shared, length-aware queue selector for both pull & push routers.

API:
    picked = select_batch(
        q, want, predictor, q_lock,
        policy="short_first",  # or: "long_first", "longest_first", "even_short_long"
        threshold=512,
        pool_factor=3,
        ep=None,               # optional, for per-EP fairness state
        state=None,            # optional mutable dict for fairness/quota across refills
        guard_long_every_n=0,  # if >0: force a long batch at least every N refills (when longs exist)
        default_max_tokens=256,
    )

Queue shape:
    q holds tuples (prompt: str, t_enq: float, req_id: int)

Returns:
    List of tuples (prompt, t_enq, pred_tokens, req_id)

Leftovers:
    Pushed back to q as (prompt, t_enq, req_id) and task_done() is called for them.

Picked items:
    NOT acked here (caller will ack when the work completes).
"""

from typing import List, Tuple, Optional, Dict
from queue import Empty, Queue


def _predict_len(predictor, prompt, default_max, req_id=None):
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
    threshold: int = 512,
    pool_factor: int = 3,
    ep: Optional[str] = None,
    state: Optional[Dict] = None,
    guard_long_every_n: int = 0,
    default_max_tokens: int = 256,
) -> List[Tuple[str, float, int, int]]:
    if want <= 0:
        return []

    # 1) Build pool by temporarily removing up to pool_size items
    pool_size = min(max(1, want) * int(pool_factor), q.qsize())
    if pool_size <= 0:
        return []

    pool: List[Tuple[str, float, int, int]] = []
    with q_lock:
        for _ in range(pool_size):
            try:
                p, t, rid = q.get_nowait()
            except Empty:
                break
            pred = _predict_len(predictor, p, default_max_tokens, req_id=rid)
            pool.append((p, t, pred, rid))
    if not pool:
        return []

    # 2) Policy logic
    pol = (policy or "short_first").lower().strip()

    prefer_long_by_guard = False
    if guard_long_every_n and state is not None and ep is not None:
        ctr = int(state.setdefault("batches_since_long", {}).get(ep, 0))
        prefer_long_by_guard = (ctr >= int(guard_long_every_n))

    def _classify(pool_items, thr):
        short, long = [], []
        for item in pool_items:
            (p, t, pred, rid) = item
            (short if pred <= thr else long).append(item)
        return short, long

    short, long = _classify(pool, threshold)

    picked: List[Tuple[str, float, int, int]] = []

    if pol == "longest_first":
        pool.sort(key=lambda x: x[2], reverse=True)
        picked = pool[:want]

    elif pol == "even_short_long":
        s_sorted = sorted(short, key=lambda x: x[2])               # shortest-first
        l_sorted = sorted(long,  key=lambda x: x[2], reverse=True) # longest-first
        take_long_next = prefer_long_by_guard
        while len(picked) < want and (s_sorted or l_sorted):
            if take_long_next and l_sorted:
                picked.append(l_sorted.pop(0))
            elif s_sorted:
                picked.append(s_sorted.pop(0))
            elif l_sorted:
                picked.append(l_sorted.pop(0))
            take_long_next = not take_long_next

    else:
        # "short_first" (default) or "long_first"
        choose_long = (pol == "long_first") or prefer_long_by_guard
        bucket = long if choose_long else short
        other  = short if choose_long else long

        bucket.sort(key=lambda x: x[2])  # shortest-first within bucket
        other.sort(key=lambda x: x[2])

        if len(bucket) >= want:
            picked = bucket[:want]
        else:
            picked = (bucket + other)[:want]

    # 3) Compute leftovers from the ORIGINAL pool objects
    picked_set = set(picked)
    leftovers = [(p, t, rid) for (p, t, pred, rid) in pool if (p, t, pred, rid) not in picked_set]

    # 4) Push leftovers back and ACK them (we removed them above)
    if leftovers:
        with q_lock:
            for p, t, rid in leftovers:
                q.put((p, t, rid))
            # Mark the leftovers as done for the original get()s
            for _ in range(len(leftovers)):
                q.task_done()

    # 5) Update guardrail state
    if state is not None and ep is not None and guard_long_every_n:
        any_long = any(pred > threshold for (_, _, pred, _) in picked)
        d = state.setdefault("batches_since_long", {})
        d[ep] = 0 if any_long else int(d.get(ep, 0)) + 1

    return picked
