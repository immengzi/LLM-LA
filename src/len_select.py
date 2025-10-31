# -*- coding: utf-8 -*-
"""
len_select: shared, length-aware queue selector for both pull & push routers.
This version has **no guardrail logic** (LONG_BATCH_GUARD_N removed).

Behavior:
- Build a temporary pool from the queue.
- Compute **real input tokens** (via a provided tokenizer function) and **predicted output tokens**.
- Pick by chosen length basis and policy:
    * length_basis: "input" | "output" (default) | "total"
    * policy:
        - "short_first" (default): shortest → longer
        - "long_first" or "longest_first": longest → shorter
        - "even_short_long": alternate between short and long ends
- Push leftover items back to the queue safely.
"""

from typing import List, Tuple, Callable, Optional
from queue import Empty, Queue
from collections import deque


def _predict_len(predictor, prompt, default_max, req_id=None) -> int:
    """Predict completion tokens; fall back to default_max on failure."""
    try:
        v = predictor.predict_out_tokens(prompt, req_id=req_id)
        return int(v) if v is not None else int(default_max)
    except Exception:
        return int(default_max)


def _fallback_in_tokens(prompt: str) -> int:
    """Fallback heuristic if shared tokenizer unavailable."""
    return max(1, len(prompt.split()))


def select_batch(
    q: "Queue[Tuple[str, float, int]]",
    want: int,
    predictor,
    q_lock,
    *,
    policy: str = "short_first",
    pool_factor: int = 3,
    default_max_tokens: int = 256,
    length_basis: str = "output",                    # "input" | "output" | "total"
    input_len_fn: Optional[Callable[[str], int]] = None,  # shared tokenizer function
) -> List[Tuple[str, float, int, int]]:
    """
    Select a batch of requests from the queue according to chosen length basis.

    Args:
        q: shared queue containing (prompt, t_enq_client, req_id)
        want: number of items to pull
        predictor: object implementing predict_out_tokens(prompt, req_id)
        q_lock: shared RLock for queue safety
        policy: "short_first", "long_first", or "even_short_long"
        pool_factor: how many items to peek (pool_size = want * pool_factor)
        default_max_tokens: fallback token length if prediction fails
        length_basis: basis for sorting — "input", "output" (default), or "total"
        input_len_fn: callable returning number of input tokens using the SAME tokenizer
                      as length_backend / oracle predictor.

    Returns:
        List of (prompt, t_enq_client, key_len, req_id) where key_len is the
        value actually used for sorting (pred_out for "output", etc.).
    """
    if want <= 0:
        return []

    pool_size = min(max(1, int(want)) * int(pool_factor), q.qsize())
    if pool_size <= 0:
        return []

    input_len_fn = input_len_fn or _fallback_in_tokens
    basis = (length_basis or "output").lower().strip()

    # Include a stable index to avoid collisions when there are duplicates
    # pool entries: (p, t, pred_out, in_len, total, rid, idx)
    pool: List[Tuple[str, float, int, int, int, int, int]] = []
    with q_lock:
        for idx in range(pool_size):
            try:
                p, t, rid = q.get_nowait()
            except Empty:
                break
            pred_out = _predict_len(predictor, p, default_max_tokens, req_id=rid)
            in_len = int(input_len_fn(p))
            total = in_len + pred_out
            pool.append((p, t, pred_out, in_len, total, rid, idx))

    if not pool:
        return []

    # Choose sort key by basis
    if basis == "input":
        key_idx = 3
    elif basis == "total":
        key_idx = 4
    else:
        key_idx = 2  # default "output"

    pool_sorted = sorted(pool, key=lambda x: x[key_idx])
    dq = deque(pool_sorted)

    picked: List[Tuple[str, float, int, int, int, int, int]] = []

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
    leftovers = [(p, t, rid) for (p, t, _po, _inl, _tot, rid, idx) in pool if idx not in picked_idx]

    # Push leftovers back & ack only those
    if leftovers:
        with q_lock:
            for p, t, rid in leftovers:
                q.put((p, t, rid))
            for _ in range(len(leftovers)):
                q.task_done()

    # Strip internal index before returning; third field is the key used for sorting
    out: List[Tuple[str, float, int, int]] = []
    for (p, t, pred_out, in_len, total, rid, _idx) in picked:
        if basis == "input":
            key_len = in_len              # REAL input tokens
        elif basis == "total":
            key_len = total               # REAL input + PREDICTED output
        else:
            key_len = pred_out            # PREDICTED output (default)
        out.append((p, t, key_len, rid))
    return out
