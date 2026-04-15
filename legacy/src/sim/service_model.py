# sim/service_model.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple
import math
import random


@dataclass
class ServiceModel:
    batch_n_sat: int = 8
    noise_sigma: float = 0.0  # multiplicative, applied to both prefill+decode proportionally

    def batch_eff(self, n_decode: int) -> float:
        n = max(1, int(n_decode))
        sat = max(1, int(self.batch_n_sat))
        return min(n, sat) / float(sat)

    def times(self, *, in_tokens: int, out_tokens: int, prefill_tps: float, decode_tps: float, n_decode: int, rng: random.Random) -> Tuple[float, float]:
        t_prefill = float(in_tokens) / max(1e-9, float(prefill_tps))
        eff = self.batch_eff(n_decode)
        eff_decode_tps = max(1e-9, float(decode_tps) * eff)
        t_decode = float(out_tokens) / eff_decode_tps

        if self.noise_sigma > 0:
            noise = math.exp(rng.gauss(0.0, float(self.noise_sigma)))
            t_prefill *= noise
            t_decode *= noise

        return t_prefill, t_decode
