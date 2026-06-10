# -*- coding: utf-8 -*-
"""
Minimal KV-aware state for router:

- register_request_blocks(req_id, [block_hashes])
- register_block_owners(block_hash, [endpoint_urls])
- prefix_len(endpoint_url, req_id) -> int

Debug helpers:
- get_request_blocks(req_id) -> List[int]
"""

from typing import Dict, List, Iterable
from threading import RLock

# req_id -> [block_hashes...]
_REQ_BLOCKS: Dict[str, List[int]] = {}
# block_hash -> { endpoint_url: True }
_BLOCK_OWNERS: Dict[int, Dict[str, bool]] = {}

_LOCK = RLock()


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
