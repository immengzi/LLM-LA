# sim/config.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class SimConfig:
    enabled: bool = True

    # compare all 4
    methods: List[str] = field(
        default_factory=lambda: ["pull", "push_rr", "push_random", "push_lq"]
    )

    seed: int = 12345

    # cluster shape
    n_endpoints: int = 8
    max_inflight_per_ep: int = 8

    # per-endpoint TPS (roughly aligned with your real experiments)
    prefill_tps: float = 62.0
    decode_tps: float = 440.0

    # saturation + noise
    batch_n_sat: int = 12
    noise_sigma: float = 0.05

    # logging
    output_log_mode: str = "preview"


DEFAULT_SIM_CONFIG = SimConfig()
