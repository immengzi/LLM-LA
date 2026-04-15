# sim/models.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class SimRequest:
    idx: int
    req_id: str
    prompt: str
    in_tokens: int
    out_tokens: int

    # SSOT times (virtual)
    t_arrival_router: Optional[float] = None
    t_dispatch_router: Optional[float] = None
    t_response_router: Optional[float] = None

    endpoint: Optional[str] = None

    # breakdown
    t_prefill_s: Optional[float] = None
    t_decode_s: Optional[float] = None


@dataclass
class Endpoint:
    endpoint_id: str
    max_inflight: int
    prefill_tps: float
    decode_tps: float

    inflight: int = 0
    ok: int = 0
    err: int = 0


@dataclass
class SimResult:
    # per-request record compatible with your logs.json style
    record: Dict[str, Any]
