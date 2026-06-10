# -*- coding: utf-8 -*-
"""
Length-aware selection over a *local pool* of candidates.

Input is a list of (req_id, prompt, t_enq_client, meta)
plus a predictor and policy.
"""

from typing import List, Tuple, Dict
from .predictors import SimpleLengthPredictor
from .config import get_config

_cfg = get_config()


def select_len_aware(
    candidates: List[Tuple[int, str, float, Dict]],
    predictor: SimpleLengthPredictor,
    policy: str,
) -> List[Tuple[int, str, float, Dict]]:
    """
    Return candidates sorted according to a length policy.

    policy: "short_first" or "long_first"
    """
    scored = []
    for req_id, prompt, t_enq, meta in candidates:
        pred_out = predictor.predict_out_tokens(prompt, req_id=req_id)
        score = pred_out or _cfg.DEFAULT_MAX_TOKENS
        scored.append((req_id, prompt, t_enq, meta, score))

    reverse = (policy == "long_first")
    scored.sort(key=lambda x: x[4], reverse=reverse)
    return [(r, p, t, m) for (r, p, t, m, _) in scored]
