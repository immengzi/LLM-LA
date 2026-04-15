# sim/stats.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional


def _pct(sorted_vals: List[float], p: float) -> Optional[float]:
    if not sorted_vals:
        return None
    p = max(0.0, min(100.0, float(p)))
    k = (p / 100.0) * (len(sorted_vals) - 1)
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    if hi == lo:
        return float(sorted_vals[lo])
    w = k - lo
    return float(sorted_vals[lo] * (1.0 - w) + sorted_vals[hi] * w)


@dataclass
class Summary:
    n: int
    sim_end_time_s: float
    throughput_rps: float
    latency_ms: Dict[str, Optional[float]]
    queue_wait_ms: Dict[str, Optional[float]]
    server_ms: Dict[str, Optional[float]]


def summarize(lat_end_to_end_s: List[float], queue_wait_s: List[float], server_s: List[float], sim_end_time_s: float) -> Summary:
    lat = sorted(lat_end_to_end_s)
    q = sorted(queue_wait_s)
    sv = sorted(server_s)

    n = len(lat)
    t = float(sim_end_time_s) if sim_end_time_s > 0 else 1e-9
    thr = n / t

    def ms(x: Optional[float]) -> Optional[float]:
        return None if x is None else x * 1000.0

    return Summary(
        n=n,
        sim_end_time_s=float(sim_end_time_s),
        throughput_rps=float(thr),
        latency_ms={
            "p50": ms(_pct(lat, 50)),
            "p90": ms(_pct(lat, 90)),
            "p95": ms(_pct(lat, 95)),
            "p99": ms(_pct(lat, 99)),
            "max": ms(lat[-1]) if lat else None,
            "mean": ms(sum(lat) / len(lat)) if lat else None,
        },
        queue_wait_ms={
            "p50": ms(_pct(q, 50)),
            "p95": ms(_pct(q, 95)),
            "p99": ms(_pct(q, 99)),
            "max": ms(q[-1]) if q else None,
            "mean": ms(sum(q) / len(q)) if q else None,
        },
        server_ms={
            "p50": ms(_pct(sv, 50)),
            "p95": ms(_pct(sv, 95)),
            "p99": ms(_pct(sv, 99)),
            "max": ms(sv[-1]) if sv else None,
            "mean": ms(sum(sv) / len(sv)) if sv else None,
        },
    )
