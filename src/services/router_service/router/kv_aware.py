# -*- coding: utf-8 -*-
"""
Minimal KV-aware state for router:

- register_request_blocks(req_id, [block_hashes])
- register_block_owners(block_hash, [endpoint_urls])
- prefix_len(endpoint_url, req_id) -> int

Debug helpers:
- get_request_blocks(req_id) -> List[int]
"""

from collections import OrderedDict
from typing import Dict, List, Iterable, Optional
from threading import RLock

# req_id -> [block_hashes...]
_REQ_BLOCKS: Dict[str, List[int]] = {}
# block_hash -> { endpoint_url: True }
_BLOCK_OWNERS: Dict[int, Dict[str, bool]] = {}

# req_id -> routing decision captured at dispatch time (independent of the
# TRACE system). Read once at completion to enrich the /latency_log ring.
# Bounded so timed-out / never-completed requests cannot leak memory.
_REQ_ROUTING: "OrderedDict[str, Dict]" = OrderedDict()
_ROUTING_MAX = 8192

_LOCK = RLock()


def record_routing(
    req_id: str,
    *,
    endpoint: str,
    kv_hits_len: int,
    total_blocks: int,
    affinity_key: Optional[str] = None,
    block_hashes: Optional[List[int]] = None,
) -> None:
    """Capture the router's per-request decision at dispatch time."""
    with _LOCK:
        info: Dict = {
            "endpoint": endpoint,
            "kv_hits_len": int(kv_hits_len),
            "total_blocks": int(total_blocks),
        }
        if affinity_key is not None:
            info["affinity_key"] = affinity_key
        if block_hashes is not None:
            info["block_hashes"] = list(block_hashes)
        _REQ_ROUTING[req_id] = info
        _REQ_ROUTING.move_to_end(req_id)
        while len(_REQ_ROUTING) > _ROUTING_MAX:
            _REQ_ROUTING.popitem(last=False)


def pop_routing(req_id: str) -> Optional[Dict]:
    """Return and remove the routing decision recorded for *req_id*."""
    with _LOCK:
        info = _REQ_ROUTING.pop(req_id, None)
        return dict(info) if info else None


def register_request_blocks(req_id: str, block_hashes: Iterable[int]) -> None:
    with _LOCK:
        _REQ_BLOCKS[req_id] = list(block_hashes)


def get_request_blocks(req_id: str) -> List[int]:
    """
    Debug helper: return a copy of the request's block_hash list.
    """
    with _LOCK:
        blocks = _REQ_BLOCKS.get(req_id) or []
        return list(blocks)


def register_block_owners(block_hash: int, owners: Iterable[str]) -> None:
    with _LOCK:
        entry = _BLOCK_OWNERS.get(block_hash, {})
        for ep in owners:
            entry[ep] = True
        _BLOCK_OWNERS[block_hash] = entry


def prefix_len(endpoint: str, req_id: str) -> int:
    """
    How many prefix blocks of this request are owned by this endpoint?
    """
    with _LOCK:
        blocks = _REQ_BLOCKS.get(req_id)
        if not blocks:
            return 0
        owners = _BLOCK_OWNERS
        count = 0
        for h in blocks:
            ep_map = owners.get(h)
            if not ep_map or endpoint not in ep_map:
                break
            count += 1
        return count
