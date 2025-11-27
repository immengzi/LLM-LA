# -*- coding: utf-8 -*-
# kv_aware.py

"""
KV-prefix–aware deterministic routing.

This module provides:

  • register_block_owners(block_hash, owners)
      - Called by KVWatcher when Redis says "this KV block belongs to pod(s)".

  • register_request_blocks(req_id, block_hashes)
      - Stores the KV block prefix for a given request id.

  • maybe_register_request_blocks_from_prompt(req_id, prompt)
      - Uses a CPU hash service (bound via set_hash_service) to compute the
        KV blocks for a prompt and register them.

  • prefix_len(endpoint, req_id)
      - Returns how many *prefix* blocks of the request are owned by the
        given endpoint (server-level KV signal).

  • prefix_scores_for_request(endpoints, req_id)
      - Returns a sorted list [(endpoint, prefix_len), ...] for that request.

  • best_endpoint_for_request(endpoints, req_id, min_prefix=1)
      - Returns (best_endpoint, best_prefix_len) or (None, 0) if no endpoint
        has at least min_prefix prefix hits.

  • rank_items_for_endpoint(endpoint, items, mode, include_scores)
      - For a given endpoint, ranks items (prompt, t_enq_client, req_id)
        by (-prefix_len, rank_key) or FIFO/shuffle depending on mode.
        This is used for *within-endpoint* ordering when desired.

Notes:
  - Epoch counters (_arrival_epoch, _kv_epoch_by_ep) are used only for the
    deterministic hash tie-breaker rank_key() and shuffle seeding.
"""

import time
import hashlib
import random
from typing import Dict, List, Tuple, Any, Iterable, Optional

# -------------------------------------------------------------------
# Global state
# -------------------------------------------------------------------

# Epoch counters
_arrival_epoch: int = 0
_kv_epoch_by_ep: Dict[str, int] = {}

# KV data structures
_block_owners: Dict[int, List[str]] = {}       # block_hash → [endpoints]
_req_block_hashes: Dict[int, List[int]] = {}   # req_id → block_hashes

# Per-process salt for hash randomness
_proc_seed = int(time.time_ns() & 0xFFFFFFFF)

# Optional external hash service (populated by router_modes)
_hash_service = None
_hash_timeout = 10.0


# -------------------------------------------------------------------
# Bind CPU hash service
# -------------------------------------------------------------------

def set_hash_service(func, timeout_s: float = 10.0) -> None:
    """
    Bind the CPU-side hash service used to compute block hashes from prompts.

    func(prompt: str, timeout: float) -> (block_hashes: List[int], token_count: int)
    """
    global _hash_service, _hash_timeout
    _hash_service = func
    _hash_timeout = timeout_s


# -------------------------------------------------------------------
# Register Redis KV mappings
# -------------------------------------------------------------------

def register_block_owners(block_hash: int, owners: List[str]) -> None:
    """Called by KVWatcher when Redis says block belongs to pod(s)."""
    _block_owners[block_hash] = owners


# -------------------------------------------------------------------
# Request KV hash storage
# -------------------------------------------------------------------

def register_request_blocks(req_id: int, block_hashes: List[int]) -> None:
    """Store block prefix list for this req_id."""
    _req_block_hashes[req_id] = list(block_hashes or [])


# -------------------------------------------------------------------
# Compute block hashes from prompt using CPU hashing service
# -------------------------------------------------------------------

def maybe_register_request_blocks_from_prompt(req_id: int, prompt: str) -> None:
    """
    If hash service available, compute KV blocks for the request and register them.

    Safe no-op if:
      - no hash service is bound, or
      - the hash service raises an exception.
    """
    if _hash_service is None:
        return
    try:
        bh, _tok = _hash_service(prompt, timeout=_hash_timeout)
        register_request_blocks(req_id, bh)
    except Exception:
        # Non-fatal: routing just sees prefix_len == 0 for this request.
        pass


# -------------------------------------------------------------------
# Prefix similarity scoring (server-level signal)
# -------------------------------------------------------------------

def prefix_len(endpoint: str, req_id: int) -> int:
    """
    Return how many blocks from the request prefix this endpoint owns.

    We walk the stored block prefix for req_id and count contiguous hits until
    we hit a block that is *not* owned by this endpoint. That gives an
    estimate of how long a KV prefix could be reused.
    """
    blocks = _req_block_hashes.get(req_id)
    if not blocks:
        return 0

    n = 0
    for bh in blocks:
        owners = _block_owners.get(bh)
        if not owners or endpoint not in owners:
            break
        n += 1
    return n


def prefix_scores_for_request(
    endpoints: Iterable[str],
    req_id: int,
) -> List[Tuple[str, int]]:
    """
    For a given request, compute prefix_len for each endpoint and return a list
    of (endpoint, prefix_len) sorted by:
        - descending prefix_len
        - then lexicographically by endpoint (for determinism).
    """
    scores: List[Tuple[str, int]] = []
    for ep in endpoints:
        try:
            p = int(prefix_len(ep, req_id))
        except Exception:
            p = 0
        if p > 0:
            scores.append((ep, p))

    scores.sort(key=lambda t: (-t[1], t[0]))
    return scores


def best_endpoint_for_request(
    endpoints: Iterable[str],
    req_id: int,
    *,
    min_prefix: int = 1,
) -> Tuple[Optional[str], int]:
    """
    Choose the single best endpoint for this request based on prefix_len.

    Returns:
        (best_endpoint, best_prefix_len)
      or
        (None, 0) if no endpoint reaches min_prefix hits.
    """
    scores = prefix_scores_for_request(endpoints, req_id)
    if not scores:
        return None, 0

    best_ep, best_p = scores[0]
    if best_p < int(min_prefix):
        return None, 0

    return best_ep, best_p


# -------------------------------------------------------------------
# Epoch utilities
# -------------------------------------------------------------------

def notify_arrival(n: int = 1) -> None:
    """
    Bump the global "arrival epoch".

    The router should call this each time a logical "new arrival" happens
    into the central queue (including requeues on error). This is only used
    to make rank_key() / shuffle deterministic but evolving.
    """
    global _arrival_epoch
    _arrival_epoch += int(max(1, n))


def notify_kv_update(endpoint: str) -> None:
    """
    Bump the per-endpoint KV epoch.

    The KVWatcher calls this whenever it sees Redis updates for blocks that
    belong to pods behind a given endpoint. This lets rank_key() include
    a notion of "KV freshness" without reading any actual block IDs.
    """
    _kv_epoch_by_ep[endpoint] = _kv_epoch_by_ep.get(endpoint, 0) + 1


def get_epochs(endpoint: str) -> Tuple[int, int]:
    """
    Return (kv_epoch_for_endpoint, global_arrival_epoch).
    """
    return _kv_epoch_by_ep.get(endpoint, 0), _arrival_epoch


# -------------------------------------------------------------------
# Deterministic tie-breaker hash
# -------------------------------------------------------------------

def rank_key(endpoint: str, req_id: int) -> int:
    """
    Deterministic hash used as a tie-breaker when multiple requests have
    the same prefix_len for a given endpoint.

    Incorporates:
      - endpoint identifier
      - KV epoch for that endpoint
      - global arrival epoch
      - request id
    """
    kv_e, arr_e = get_epochs(endpoint)
    s = f"{endpoint}|kv:{kv_e}|arr:{arr_e}|rid:{req_id}"
    return int(hashlib.blake2b(s.encode(), digest_size=8).hexdigest(), 16)


# -------------------------------------------------------------------
# Optional shuffle mode
# -------------------------------------------------------------------

def reorder_for_endpoint(endpoint: str, items):
    """
    Deterministically shuffle items for an endpoint, using epochs + a
    per-process seed. Useful for "shuffle" mode in rank_items_for_endpoint.
    """
    kv_e, arr_e = get_epochs(endpoint)
    seed = hash((endpoint, kv_e, arr_e, _proc_seed)) & 0xFFFFFFFF
    rnd = random.Random(seed)
    out = list(items)
    rnd.shuffle(out)
    return out


# -------------------------------------------------------------------
# Main ranking function (USED BY ROUTER for within-endpoint ordering)
# -------------------------------------------------------------------

def rank_items_for_endpoint(
    endpoint: str,
    items: List[Tuple[str, float, int]],
    *,
    mode: str = "hash",
    include_scores: bool = True,
):
    """
    Rank items *for a single endpoint*.

    items: list of (prompt, t_enq_client, req_id)

    Modes:
      - "fifo"    : leave order unchanged (oldest first)
      - "shuffle" : deterministic shuffle via reorder_for_endpoint
      - "hash"    : KV-aware ordering: sort by (-prefix_len, rank_key)
      - "none"    : alias for "fifo"

    Returns:
        ordered_items, meta_info

    where meta_info is a list of dicts:
      {
        "req_id":        int,
        "kv_epoch":      int,
        "arrival_epoch": int,
        "prefix_len":    int,
        "rank_key":      Optional[int],
        "rank_index":    int,   # position after ranking
      }
    """

    if not items:
        return [], []

    kv_e, arr_e = get_epochs(endpoint)
    mode = (mode or "hash").lower()
    if mode == "none":
        mode = "fifo"

    # FIFO
    if mode == "fifo":
        ordered = list(items)
        metas = _empty_meta(ordered, kv_e, arr_e)
        return ordered, metas if include_scores else []

    # SHUFFLE
    if mode == "shuffle":
        ordered = reorder_for_endpoint(endpoint, items)
        metas = _empty_meta(ordered, kv_e, arr_e)
        return ordered, metas if include_scores else []

    # HASH / DEFAULT = KV PREFIX ROUTING WITH HASH TIE-BREAK
    tmp = []
    for it in items:
        _, _, rid = it
        p = prefix_len(endpoint, rid)          # MAIN KV SIGNAL
        rk = rank_key(endpoint, rid)
        tmp.append((-p, rk, it, p, rk))       # negative prefix → larger prefix first

    tmp.sort(key=lambda x: (x[0], x[1]))      # sort by (-prefix, hash)

    ordered = [t[2] for t in tmp]             # keep original item tuples
    if not include_scores:
        return ordered, []

    meta = []
    for idx, (_, _, it, p, rk) in enumerate(tmp):
        _pr, _t, rid = it
        meta.append(
            {
                "req_id": int(rid),
                "kv_epoch": int(kv_e),
                "arrival_epoch": int(arr_e),
                "prefix_len": int(p),
                "rank_key": int(rk),
                "rank_index": idx,
            }
        )

    return ordered, meta


def _empty_meta(items, kv_e, arr_e):
    metas = []
    for idx, it in enumerate(items):
        _p, _t, rid = it
        metas.append(
            {
                "req_id": int(rid),
                "kv_epoch": int(kv_e),
                "arrival_epoch": int(arr_e),
                "prefix_len": 0,
                "rank_key": None,
                "rank_index": idx,
            }
        )
    return metas
