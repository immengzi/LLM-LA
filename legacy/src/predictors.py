# -*- coding: utf-8 -*-
"""
predictors.py
Length predictors (pluggable).
- BaseLengthPredictor: interface
- OracleLengthPredictor: "perfect knowledge" using SIM's shared length policy,
  implemented via a side-effect-free preview function in length_backend.

NOTE:
- Propagate `req_id` for strict determinism across runs
  and retries.
- CLAMPS the preview to the same effective max tokens as generation
  (computed with compute_length_plan), so predicted == generated.
"""

from typing import Optional, Dict
from config import get_config
from length_backend import compute_length_plan


class BaseLengthPredictor:
    name: str = "base"

    def predict_out_tokens(self, prompt: str, req_id: Optional[int] = None) -> Optional[int]:
        """Return predicted completion tokens for this prompt, or None if unavailable."""
        raise NotImplementedError


class NoopLengthPredictor(BaseLengthPredictor):
    name = "none"
    def predict_out_tokens(self, prompt: str, req_id: Optional[int] = None) -> Optional[int]:
        return None


class OracleLengthPredictor(BaseLengthPredictor):
    """
    Oracle predictor that mirrors the REAL path:
    - Returns the effective cap (eff_max) computed by compute_length_plan
      with the SAME req_id the real request will use.
    - In strict-hist mode, eff_max already reflects the histogram draw
      for this req_id; in other modes it reflects targets/base caps.
    """
    name = "oracle"

    def predict_out_tokens(self, prompt: str, req_id: Optional[int] = None) -> Optional[int]:
        try:
            cfg = get_config()
            rid = 0 if req_id is None else int(req_id)

            base_cap = int(getattr(cfg, "MAX_TOKENS", 0) or 0)
            plan = compute_length_plan(
                plain_prompt=prompt,
                base_cap=base_cap,
                target_output_tokens=getattr(cfg, "TARGET_OUTPUT_TOKENS", None),
                target_total_tokens=getattr(cfg, "TARGET_TOTAL_TOKENS", None),
                ignore_eos=getattr(cfg, "IGNORE_EOS", False),
                length_mode=getattr(cfg, "LENGTH_MODE", "legacy"),
                req_id=rid,
            )
            return int(plan.get("eff_max", base_cap))
        except Exception:
            return None


_REGISTRY: Dict[str, BaseLengthPredictor] = {
    "none": NoopLengthPredictor(),
    "oracle": OracleLengthPredictor(),
}

_cached_predictor: Optional[BaseLengthPredictor] = None


def get_length_predictor() -> BaseLengthPredictor:
    """Return a singleton predictor instance based on config.PREDICTOR_NAME."""
    global _cached_predictor
    if _cached_predictor is not None:
        return _cached_predictor

    cfg = get_config()
    name = str(getattr(cfg, "PREDICTOR_NAME", "none") or "none").lower().strip()
    pred = _REGISTRY.get(name)
    if pred is None:
        pred = _REGISTRY["none"]
    _cached_predictor = pred
    return pred
