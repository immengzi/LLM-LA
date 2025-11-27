# -*- coding: utf-8 -*-
# selector.py
"""
Unified selection per endpoint:
- KV stage: off | hash | shuffle
- Length stage: on/off using existing len_select.select_batch
"""

from typing import List, Tuple, Optional
from queue import Queue
from threading import RLock
import heapq

from config import get_config
from len_select import select_batch
from length_backend import count_input_tokens
from kv_aware import rank_key, reorder_for_endpoint

_cfg = get_config()

Item = Tuple[str, float, int]                    # (prompt, t_enq_client, req_id)
ItemSel = Tuple[str, float, Optional[int], int]  # (prompt, t_enq_client, key_len|None, req_id)

def _kv_stage(endpoint: str, pool: List[Item], want: int, mode: str) -> List[Item]:
    if want <= 0 or not pool:
        return []
    m = (mode or "off").lower()
    if m == "off":
        return pool[:min(want, len(pool))]
    if m == "hash":
        key = lambda it: rank_key(endpoint, it[2])
        return heapq.nsmallest(min(want, len(pool)), pool, key=key)
    if m == "shuffle":
        return reorder_for_endpoint(endpoint, pool)[:min(want, len(pool))]
    # future: "score" (real overlap), keep default fallback
    return pool[:min(want, len(pool))]

def _len_stage(
    selected_items: List[Item],
    want: int,
    *,
    use_len: bool,
    len_policy: str,
    predictor,
) -> List[ItemSel]:
    if not use_len or len(selected_items) <= 1:
        return [(p, t, None, rid) for (p, t, rid) in selected_items]

    subq: Queue[Item] = Queue()
    for p, t, rid in selected_items:
        subq.put((p, t, rid))

    refined = select_batch(
        subq,
        min(want, len(selected_items)),
        predictor,
        RLock(),
        policy=len_policy,
        pool_factor=1,
        default_max_tokens=int(getattr(_cfg, "MAX_TOKENS", 256)),
        length_basis=str(getattr(_cfg, "LEN_BASIS", "output")),
        input_len_fn=count_input_tokens,
    )
    # refined is already (p, t, key_len, rid)
    return refined

def select_for_endpoint(
    *,
    endpoint: str,
    central_take: List[Item],   # pool popped from central queue
    want: int,
    kv_mode: str,               # "off" | "hash" | "shuffle"
    use_len: bool,
    len_policy: str,
    predictor,
) -> Tuple[List[ItemSel], List[Item]]:
    if want <= 0 or not central_take:
        return [], central_take

    kv_selected = _kv_stage(endpoint, central_take, want, kv_mode)
    pick_ids = {rid for (_p, _t, rid) in kv_selected}
    leftovers = [(p, t, rid) for (p, t, rid) in central_take if rid not in pick_ids]
    selected = _len_stage(kv_selected, want, use_len=use_len, len_policy=len_policy, predictor=predictor)
    return selected, leftovers
