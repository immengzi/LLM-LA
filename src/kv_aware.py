# -*- coding: utf-8 -*-
# kv_aware.py

"""
KV-prefix–aware deterministic routing.

This module provides:
  • register_block_owners(block_hash, owners)
  • register_request_blocks(req_id, block_hashes)
  • prefix_len(endpoint, req_id)
  • maybe_register_request_blocks_from_prompt(req_id, prompt)
  • rank_items_for_endpoint(...): now sorts using prefix_len first, then rank_key.
"""

import time
import hashlib
import random
from typing import Dict, List, Tuple, Any

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
    """Store block list for this req_id."""
    _req_block_hashes[req_id] = block_hashes


# -------------------------------------------------------------------
# Compute block hashes from prompt using CPU hashing service
# -------------------------------------------------------------------

def maybe_register_request_blocks_from_prompt(req_id: int, prompt: str) -> None:
    """If hash service available, compute KV blocks for the request."""
    if _hash_service is None:
        return
    try:
        bh, _tok = _hash_service(prompt, timeout=_hash_timeout)
        register_request_blocks(req_id, bh)
    except Exception:
        pass


# -------------------------------------------------------------------
# Prefix similarity scoring
# -------------------------------------------------------------------

def prefix_len(endpoint: str, req_id: int) -> int:
    """Return how many blocks from the request prefix this endpoint owns."""
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


# -------------------------------------------------------------------
# Epoch utilities
# -------------------------------------------------------------------

def notify_arrival(n: int = 1) -> None:
    global _arrival_epoch
    _arrival_epoch += int(max(1, n))


def notify_kv_update(endpoint: str) -> None:
    _kv_epoch_by_ep[endpoint] = _kv_epoch_by_ep.get(endpoint, 0) + 1


def get_epochs(endpoint: str) -> Tuple[int, int]:
    return _kv_epoch_by_ep.get(endpoint, 0), _arrival_epoch


# -------------------------------------------------------------------
# Deterministic tie-breaker hash
# -------------------------------------------------------------------

def rank_key(endpoint: str, req_id: int) -> int:
    kv_e, arr_e = get_epochs(endpoint)
    s = f"{endpoint}|kv:{kv_e}|arr:{arr_e}|rid:{req_id}"
    return int(hashlib.blake2b(s.encode(), digest_size=8).hexdigest(), 16)


# -------------------------------------------------------------------
# Optional shuffle mode
# -------------------------------------------------------------------

def reorder_for_endpoint(endpoint: str, items):
    kv_e, arr_e = get_epochs(endpoint)
    seed = hash((endpoint, kv_e, arr_e, _proc_seed)) & 0xFFFFFFFF
    rnd = random.Random(seed)
    out = list(items)
    rnd.shuffle(out)
    return out


# -------------------------------------------------------------------
# Main ranking function (USED BY ROUTER)
# -------------------------------------------------------------------

def rank_items_for_endpoint(
    endpoint: str,
    items: List[Tuple[str, float, int]],
    *,
    mode: str = "hash",
    include_scores: bool = True,
):
    """
    TRUE KV-AWARE PRIORITY:
        sort by (-prefix_len, rank_key)

    Returns:
        ordered_items, meta_info
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

    # HASH / DEFAULT = REAL KV PREFIX ROUTING
    tmp = []
    for it in items:
        _, _, rid = it
        p = prefix_len(endpoint, rid)          # MAIN KV SIGNAL
        rk = rank_key(endpoint, rid)
        tmp.append(( -p, rk, it, p, rk ))      # negative prefix → larger prefix first

    tmp.sort(key=lambda x: (x[0], x[1]))       # sort by (-prefix, hash)

    ordered = [t[2] for t in tmp]              # keep original items order
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
