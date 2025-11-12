# -*- coding: utf-8 -*-
# kv_aware.py
"""
Event-driven epochs for deterministic KV-aware ordering.

- arrival_epoch bumps on *any* enqueue (you can batch this later if needed)
- kv_epoch[ep] bumps when that endpoint's KV state changes (hook later to real events)
- reorder_for_endpoint: epoch-seeded shuffle (optional mode)
- rank_key: deterministic 64-bit key (endpoint+epochs+req_id) for "hash" mode
"""

from typing import Dict, Tuple, List
import hashlib, random, time

_arrival_epoch: int = 0
_kv_epoch_by_ep: Dict[str, int] = {}
# Optional: a per-process seed to make the shuffle deterministic across processes
_proc_seed: int = int(time.time_ns() & 0xFFFFFFFF)

def notify_arrival(n: int = 1) -> None:
    global _arrival_epoch
    _arrival_epoch += int(max(1, n))

def notify_kv_update(endpoint: str) -> None:
    _kv_epoch_by_ep[endpoint] = _kv_epoch_by_ep.get(endpoint, 0) + 1

def get_epochs(endpoint: str) -> Tuple[int, int]:
    return int(_kv_epoch_by_ep.get(endpoint, 0)), int(_arrival_epoch)

def rank_key(endpoint: str, req_id: int) -> int:
    kv_e, arr_e = get_epochs(endpoint)
    s = f"{endpoint}|kv:{kv_e}|arr:{arr_e}|rid:{req_id}"
    return int(hashlib.blake2b(s.encode(), digest_size=8).hexdigest(), 16)

def reorder_for_endpoint(endpoint: str, items: List[Tuple[str, float, int]]) -> List[Tuple[str, float, int]]:
    """Epoch-seeded shuffle; stable within (endpoint, kv, arrival) state."""
    kv_e, arr_e = get_epochs(endpoint)
    seed = hash((endpoint, kv_e, arr_e, _proc_seed)) & 0xFFFFFFFF
    rnd = random.Random(seed)
    out = list(items)
    rnd.shuffle(out)
    return out
