# -*- coding: utf-8 -*-
# kv_aware.py
"""
Event-driven epochs for deterministic KV-aware ordering.

- arrival_epoch bumps on *any* enqueue (you can batch this later if needed)
- kv_epoch[ep] bumps when that endpoint's KV state changes (hook later to real events)
- reorder_for_endpoint: epoch-seeded shuffle (optional mode)
- rank_key: deterministic 64-bit key (endpoint+epochs+req_id) for "hash" mode

NEW (logging helpers):
- rank_items_for_endpoint(...) returns both a reordered list of items and
  a parallel list of small dicts suitable for log_queue() "kv_order" payloads.
"""

from typing import Dict, Tuple, List, Any
import hashlib, random, time

_arrival_epoch: int = 0
_kv_epoch_by_ep: Dict[str, int] = {}
# Optional: a per-process seed to make the shuffle deterministic across processes
_proc_seed: int = int(time.time_ns() & 0xFFFFFFFF)


# -------------------------
# Epoch maintenance
# -------------------------

def notify_arrival(n: int = 1) -> None:
    """
    Called when new requests are enqueued into the global router queue.

    n: number of arrivals (batched increments are fine).
    """
    global _arrival_epoch
    _arrival_epoch += int(max(1, n))


def notify_kv_update(endpoint: str) -> None:
    """
    Called when the KV cache *structure* for a given endpoint changes in a
    way that should affect ranking (e.g., cache evictions, new blocks, etc.).
    """
    _kv_epoch_by_ep[endpoint] = _kv_epoch_by_ep.get(endpoint, 0) + 1


def get_epochs(endpoint: str) -> Tuple[int, int]:
    """
    Returns (kv_epoch, arrival_epoch) for the given endpoint.
    kv_epoch   : how many KV-relevant events we've seen for this endpoint
    arrival_epoch : global arrival epoch counter (all endpoints share this)
    """
    return int(_kv_epoch_by_ep.get(endpoint, 0)), int(_arrival_epoch)


# -------------------------
# Deterministic hash key
# -------------------------

def rank_key(endpoint: str, req_id: int) -> int:
    """
    Deterministic 64-bit key capturing (endpoint, kv_epoch, arrival_epoch, req_id).

    Can be used as a "KV-aware" pseudo-random order when combined with sorting:
        key = rank_key(ep, req_id)
        items_sorted = sorted(items, key=lambda x: rank_key(ep, x_req_id))
    """
    kv_e, arr_e = get_epochs(endpoint)
    s = f"{endpoint}|kv:{kv_e}|arr:{arr_e}|rid:{req_id}"
    return int(hashlib.blake2b(s.encode(), digest_size=8).hexdigest(), 16)


# -------------------------
# Epoch-seeded shuffle (legacy helper)
# -------------------------

def reorder_for_endpoint(endpoint: str, items: List[Tuple[str, float, int]]) -> List[Tuple[str, float, int]]:
    """Epoch-seeded shuffle; stable within (endpoint, kv, arrival) state."""
    kv_e, arr_e = get_epochs(endpoint)
    seed = hash((endpoint, kv_e, arr_e, _proc_seed)) & 0xFFFFFFFF
    rnd = random.Random(seed)
    out = list(items)
    rnd.shuffle(out)
    return out


# -------------------------
# Logging-friendly ranking helper (NEW)
# -------------------------

def rank_items_for_endpoint(
    endpoint: str,
    items: List[Tuple[str, float, int]],
    *,
    mode: str = "hash",
    include_scores: bool = True,
) -> Tuple[List[Tuple[str, float, int]], List[Dict[str, Any]]]:
    """
    Rank a list of (prompt, t_enq_client, req_id) items for a specific endpoint
    and return:

        (ordered_items, meta_list)

    ordered_items : reordered list of the same tuples
    meta_list     : parallel list of dicts with per-request KV ranking metadata:

        {
            "req_id": <int>,
            "kv_epoch": <int>,
            "arrival_epoch": <int>,
            "rank_key": <int or None>,
            "rank_index": <int>,        # position in ordered_items
        }

    mode:
        - "hash"   : stable sort by rank_key(endpoint, req_id)
        - "shuffle": epoch-seeded shuffle (same behavior as reorder_for_endpoint)
        - "fifo"   : no reordering (identity)
        - "none"   : alias of "fifo"

    include_scores:
        - If False, meta_list will be an empty list (no overhead for logging).
    """
    if not items:
        return [], []

    kv_e, arr_e = get_epochs(endpoint)
    mode = (mode or "hash").lower()

    # Normalize aliases
    if mode == "none":
        mode = "fifo"

    if mode == "fifo":
        ordered = list(items)
        rank_keys = [None for _ in ordered]
    elif mode == "shuffle":
        ordered = reorder_for_endpoint(endpoint, items)
        rank_keys = [None for _ in ordered]
    else:
        # default / "hash" mode: stable order by 64-bit rank_key
        tmp: List[Tuple[int, Tuple[str, float, int]]] = []
        for it in items:
            _, _, rid = it
            k = rank_key(endpoint, int(rid))
            tmp.append((k, it))
        tmp.sort(key=lambda kv: kv[0])
        ordered = [it for (k, it) in tmp]
        rank_keys = [k for (k, _it) in tmp]

    if not include_scores:
        return ordered, []

    meta: List[Dict[str, Any]] = []
    for idx, (it, rk) in enumerate(zip(ordered, rank_keys)):
        _prompt, _t_enq, rid = it
        meta.append(
            {
                "req_id": int(rid),
                "kv_epoch": int(kv_e),
                "arrival_epoch": int(arr_e),
                "rank_key": int(rk) if rk is not None else None,
                "rank_index": int(idx),
            }
        )

    return ordered, meta
