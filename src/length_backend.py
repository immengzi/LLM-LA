# -*- coding: utf-8 -*-
"""
length_backend.py

Shared length-backend helpers used by BOTH:
- simulator paths (output length sampling)
- utils.send_chat_request (real vLLM path that may mirror/simulate)
- oracle predictor preview (side-effect-free)

Key updates for determinism with prompts limiting:
- Prebuilt STRICT histogram plan sized to the number of enqueues (set_hist_plan).
- Index selection by stable `req_id` rather than a global advancing pointer
  (get_hist_index_for_req).
- Predictor preview and simulator sampling both accept `req_id` and use the same
  indexer so predicted and actual lengths match per request.
"""

import math
import random
import hashlib
import threading
from typing import Optional, Dict, Any, List, Callable

from config import get_config

# ---------------------------------------------------------------------
# Balanced multinomial sequencer (legacy prefix-fair, pointer-based)
# ---------------------------------------------------------------------
_hist_state: Dict[tuple, Dict[str, Any]] = {}
_hist_lock = threading.Lock()


def _next_hist_index(label: str, probs: List[float]) -> int:
    """
    Balanced multinomial sequencer (deterministic, prefix-fair).
    For each request, add fractional quota (probs) and pick the bin with max debt.
    NOTE: This ADVANCES a global state and is kept for backward-compat only.
    """
    m = len(probs)
    key = (label, m)
    with _hist_lock:
        st = _hist_state.get(key)
        if st is None:
            st = {"t": 0, "debt": [0.0] * m}
        debt = st["debt"]
        for i in range(m):
            debt[i] += float(probs[i])
        j = max(range(m), key=lambda i: (debt[i], -i))
        debt[j] -= 1.0
        st["t"] = int(st["t"]) + 1
        _hist_state[key] = st
        return j


def reset_hist_sequence(label: Optional[str] = None):
    """Optional: reset sequencer state (e.g., between experiments)."""
    with _hist_lock:
        if label is None:
            _hist_state.clear()
        else:
            for k in list(_hist_state.keys()):
                if isinstance(k, tuple) and k[0] == label:
                    _hist_state.pop(k, None)


# ---------------------------------------------------------------------
# Deterministic per-run STRICT plan (no pointer; size = number of enqueues)
# ---------------------------------------------------------------------
_plan_state: Dict[str, Dict[str, Any]] = {}
_plan_lock = threading.Lock()


def set_hist_plan(label: str, probs: List[float], n: int, seed_base: int):
    """
    Build a deterministic plan of length-bin indices of size n:
    counts ~ probs (rounded to sum exactly n), then shuffle with fixed seed.

    Called ONCE per run (after prompts are limited), so the plan exactly matches
    the number of enqueues and keeps 80/20 (or any) for that subset.
    """
    with _plan_lock:
        counts = [int(round(float(p) * n)) for p in probs]
        delta = n - sum(counts)
        order = sorted(range(len(probs)), key=lambda i: -probs[i])
        i = 0
        while delta != 0 and order:
            j = order[i % len(order)]
            counts[j] += 1 if delta > 0 else -1
            delta += -1 if delta > 0 else 1
            i += 1

        seq = [i for i, c in enumerate(counts) for _ in range(max(0, c))]
        if len(seq) < n:
            j = max(range(len(probs)), key=lambda k: probs[k])
            seq.extend([j] * (n - len(seq)))
        elif len(seq) > n:
            seq = seq[:n]

        rng = random.Random(seed_for_name(seed_base, f"hist_plan::{label}"))
        rng.shuffle(seq)
        _plan_state[label] = {"seq": seq, "ptr": 0}


def _next_hist_index_from_plan(label: str, default_fn: Callable[[], int]) -> int:
    """
    Legacy helper that ADVANCES an internal pointer. Kept for backwards compat.
    Prefer using get_hist_index_for_req with req_id indexing.
    """
    with _plan_lock:
        st = _plan_state.get(label)
        if not st or not st.get("seq"):
            return default_fn()
        ptr = st["ptr"]
        seq: List[int] = st["seq"]
        idx = seq[ptr % len(seq)]
        st["ptr"] = ptr + 1
        return idx


def _peek_hist_index_from_plan(label: str, default_fn: Callable[[], int]) -> int:
    """
    Non-advancing peek of the current pointer. Kept for backwards compat.
    """
    with _plan_lock:
        st = _plan_state.get(label)
        if not st or not st.get("seq"):
            return default_fn()
        ptr = st["ptr"]
        seq: List[int] = st["seq"]
        idx = seq[ptr % len(seq)]
        return idx


def get_hist_index_for_req(label: str, req_id: int, default_fn: Callable[[], int]) -> int:
    """
    Deterministically pick the histogram bin for a request ID using the prebuilt plan.
    Falls back to default_fn() if no plan is available.
    """
    with _plan_lock:
        st = _plan_state.get(label)
        if not st or not st.get("seq"):
            return default_fn()
        seq: List[int] = st["seq"]
        if not seq:
            return default_fn()
        return seq[req_id % len(seq)]


# ---------------------------------------------------------------------
# Deterministic PRNG fallback (non-STRICT)
# ---------------------------------------------------------------------
_prng_state: Dict[str, Dict[str, Any]] = {}
_prng_lock = threading.Lock()


def _next_hist_index_prng(label: str, probs: List[float], seed_base: int) -> int:
    key = f"prng:{label}"
    with _prng_lock:
        st = _prng_state.get(key)
        if st is None:
            s = seed_for_name(seed_base, f"hist_prng::{label}")
            st = {"rng": random.Random(s)}
            _prng_state[key] = st
        rng: random.Random = st["rng"]
        total = sum(float(p) for p in probs) or 1.0
        u, cum = rng.random(), 0.0
        for i, p in enumerate(probs):
            cum += float(p) / total
            if u <= cum:
                return i
        return len(probs) - 1


# ---------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------
def seed_for_name(seed_base: int, name: str) -> int:
    """Deterministic seed from a base seed and a stable string (endpoint/prompt/etc.)."""
    h = hashlib.sha256(f"{seed_base}:{name}".encode("utf-8")).hexdigest()
    return int(h[:16], 16)


def rng_for_prompt(seed_base: int, prompt: Optional[str], by_prompt: bool = True) -> random.Random:
    """Build an RNG either from base seed only, or mixed with the prompt content."""
    if by_prompt and prompt:
        s = seed_for_name(seed_base, prompt)
    else:
        s = int(seed_base)
    return random.Random(s)


def estimate_in_tokens_from_chars(prompt: str) -> int:
    cfg = get_config()
    if not cfg.SIM_VARY_IN_TOKENS:
        return int(cfg.SIM_IN_TOKENS)
    denom = max(float(cfg.SIM_CHARS_PER_TOKEN), 1e-9)
    est = int(math.ceil(len(prompt) / denom))
    lo = int(cfg.SIM_IN_MIN)
    hi = int(cfg.SIM_IN_MAX)
    return max(lo, min(hi, est))


# ---------------------------------------------------------------------
# Output token sampling (SIM) — accepts req_id for STRICT hist
# ---------------------------------------------------------------------
def sample_out_tokens_from_cfg(rng: random.Random, req_id: Optional[int] = None) -> int:
    cfg = get_config()
    if not cfg.SIM_VARY_OUT_TOKENS:
        return int(cfg.SIM_OUT_TOKENS)

    d = dict(cfg.SIM_OUT_DIST or {})
    kind = str(d.get("kind", "lognormal")).lower()
    lo = int(d.get("min", 1))
    hi = int(d.get("max", cfg.MAX_TOKENS or 4096))

    x = None
    if kind == "lognormal":
        mu = float(d.get("mu", 4.8))
        sigma = float(d.get("sigma", 0.8))
        x = rng.lognormvariate(mu, sigma)

    elif kind == "gamma":
        k = float(d.get("k", 2.0))
        theta = float(d.get("theta", 64.0 / max(k, 1e-9)))
        x = rng.gammavariate(k, theta)

    elif kind == "pareto":
        alpha = float(d.get("alpha", 1.5))
        xm = float(d.get("xm", 16.0))
        x = xm * rng.paretovariate(alpha)

    elif kind == "hist":
        values = list(map(int, d.get("values", [])))
        probs = d.get("probs") or [1.0 / max(1, len(values))] * max(1, len(values))
        if not values:
            return int(cfg.SIM_OUT_TOKENS)

        if bool(getattr(cfg, "LENGTH_DIST_STRICT_HIST", False)) and req_id is not None:
            label = str(getattr(cfg, "LENGTH_HIST_SERIES_LABEL", "default"))
            seed_base = int(
                getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0
            )
            idx = get_hist_index_for_req(
                label,
                int(req_id),
                lambda: _next_hist_index_prng(label, [float(p) for p in probs], seed_base),
            )
            idx = max(0, min(idx, len(values) - 1))
            x = int(values[idx])
        else:
            p = float(rng.random())
            cum = 0.0
            for v, w in zip(values, probs):
                cum += float(w)
                if p <= cum:
                    x = int(v)
                    break
            if x is None:
                x = int(values[-1])

    else:
        return int(cfg.SIM_OUT_TOKENS)

    return max(lo, min(hi, int(round(x if x is not None else cfg.SIM_OUT_TOKENS))))


# ---------------------------------------------------------------------
# Compute per-request cap/targets (unchanged, but kept here for completeness)
# ---------------------------------------------------------------------
def compute_length_plan(
    *,
    plain_prompt: str,
    base_cap: int,
    target_output_tokens: Optional[int],
    target_total_tokens: Optional[int],
    ignore_eos: Optional[bool],
    length_mode: Optional[str],
    req_id: Optional[int] = None,
) -> Dict[str, Any]:
    cfg = get_config()
    eff_mode = (
        (length_mode or getattr(cfg, "LENGTH_MODE", "legacy") or "legacy").lower().strip()
    )
    eff_ignore_eos = bool(
        ignore_eos if ignore_eos is not None else getattr(cfg, "IGNORE_EOS", False)
    )

    cfg_cap = int(getattr(cfg, "MAX_TOKENS", 0) or 0)
    if base_cap is None or base_cap < 0:
        base_cap = 0
    if base_cap == 0 and cfg_cap > 0:
        base_cap = cfg_cap

    if target_output_tokens is not None and target_total_tokens is not None:
        raise ValueError("Provide only one of target_output_tokens or target_total_tokens.")

    if target_output_tokens is not None or target_total_tokens is not None:
        forced_out = forced_tot = None
        if target_output_tokens is not None:
            forced_out = max(0, int(target_output_tokens))
        if target_total_tokens is not None:
            forced_tot = max(0, int(target_total_tokens))
        if forced_out is not None:
            eff_max = forced_out if base_cap == 0 else min(forced_out, base_cap)
        elif forced_tot is not None:
            eff_max = forced_tot if base_cap == 0 else min(forced_tot, base_cap)
        else:
            eff_max = base_cap
        return {
            "eff_mode": eff_mode,
            "eff_max": int(max(0, eff_max)),
            "forced_out": forced_out,
            "forced_tot": forced_tot,
            "eff_ignore_eos": eff_ignore_eos,
            "meta": {"_length_mode": eff_mode},
        }

    meta: Dict[str, Any] = {"_length_mode": eff_mode}

    if eff_mode == "target-output":
        tgt = getattr(cfg, "TARGET_OUTPUT_TOKENS", None)
        if tgt is None:
            return {
                "eff_mode": "legacy",
                "eff_max": int(max(0, base_cap)),
                "forced_out": None,
                "forced_tot": None,
                "eff_ignore_eos": eff_ignore_eos,
                "meta": meta,
            }
        tgt = int(tgt)
        eff_max = tgt if base_cap == 0 else min(tgt, base_cap)
        meta["_target_completion_tokens"] = int(tgt)
        return {
            "eff_mode": eff_mode,
            "eff_max": int(max(0, eff_max)),
            "forced_out": tgt,
            "forced_tot": None,
            "eff_ignore_eos": eff_ignore_eos,
            "meta": meta,
        }

    if eff_mode == "target-total":
        tgt = getattr(cfg, "TARGET_TOTAL_TOKENS", None)
        if tgt is None:
            return {
                "eff_mode": "legacy",
                "eff_max": int(max(0, base_cap)),
                "forced_out": None,
                "forced_tot": None,
                "eff_ignore_eos": eff_ignore_eos,
                "meta": meta,
            }
        tgt = int(tgt)
        eff_max = tgt if base_cap == 0 else min(tgt, base_cap)
        meta["_target_total_tokens"] = int(tgt)
        return {
            "eff_mode": eff_mode,
            "eff_max": int(max(0, eff_max)),
            "forced_out": None,
            "forced_tot": tgt,
            "eff_ignore_eos": eff_ignore_eos,
            "meta": meta,
        }

    if eff_mode == "dist-output":
        seed_base = int(
            getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0
        )
        by_prompt = bool(getattr(cfg, "LENGTH_DIST_BY_PROMPT", True))
        rng = rng_for_prompt(seed_base, (plain_prompt if by_prompt else None), by_prompt=by_prompt)

        # KEY: feed req_id so STRICT_HIST picks the same bin as predictor/real
        sampled_out = int(sample_out_tokens_from_cfg(rng, req_id=req_id))
        est_in = int(estimate_in_tokens_from_chars(plain_prompt))
        out_budget = sampled_out if base_cap == 0 else min(sampled_out, base_cap)

        meta.update(
            {
                "_dist_mode": True,
                "_dist_seed_base": int(seed_base),
                "_dist_by_prompt": bool(by_prompt),
                "_dist_prompt_tokens_est": int(est_in),
                "_dist_completion_tokens_target": int(out_budget),
                "_dist_total_tokens_target": int(est_in + out_budget),
                "_dist_sampled_out_raw": int(sampled_out),
                "_req_id": (None if req_id is None else int(req_id)),
            }
        )

        return {
            "eff_mode": eff_mode,
            "eff_max": int(max(0, out_budget)),
            "forced_out": sampled_out,
            "forced_tot": None,
            "eff_ignore_eos": eff_ignore_eos,
            "meta": meta,
        }

    return {
        "eff_mode": "legacy",
        "eff_max": int(max(0, base_cap)),
        "forced_out": None,
        "forced_tot": None,
        "eff_ignore_eos": eff_ignore_eos,
        "meta": meta,
    }


# ---------------------------------------------------------------------
# Side-effect-free prediction used by Oracle predictor — accepts req_id
# ---------------------------------------------------------------------
def preview_out_tokens_for_prompt(*, plain_prompt: str, req_id: int) -> int:
    """
    Return the predicted completion tokens for this prompt under the current
    SIM/length policy WITHOUT mutating any global state. Uses req_id for
    STRICT histogram so predictor and simulator align exactly.
    """
    cfg = get_config()
    mode = str(getattr(cfg, "LENGTH_MODE", "legacy") or "legacy").lower().strip()

    # Explicit targets
    if mode == "target-output":
        tgt = getattr(cfg, "TARGET_OUTPUT_TOKENS", None)
        if tgt is not None:
            return int(tgt)
    if mode == "target-total":
        tgt = getattr(cfg, "TARGET_TOTAL_TOKENS", None)
        if tgt is not None:
            return int(tgt)

    # Distribution-based
    if mode == "dist-output":
        d = dict(getattr(cfg, "SIM_OUT_DIST", {}) or {})
        kind = str(d.get("kind", "lognormal")).lower()

        if kind == "hist":
            values = list(map(int, d.get("values", [])))
            probs = d.get("probs") or ([1.0 / max(1, len(values))] * len(values))
            if not values:
                return int(getattr(cfg, "SIM_OUT_TOKENS", 0) or 0)

            if bool(getattr(cfg, "LENGTH_DIST_STRICT_HIST", False)):
                label = str(getattr(cfg, "LENGTH_HIST_SERIES_LABEL", "default"))
                seed_base = int(
                    getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0
                )
                idx = get_hist_index_for_req(
                    label,
                    int(req_id),
                    lambda: _next_hist_index_prng(label, [float(p) for p in probs], seed_base),
                )
                idx = max(0, min(idx, len(values) - 1))
                return int(values[idx])
            else:
                seed_base = int(
                    getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0
                )
                by_prompt = bool(getattr(cfg, "LENGTH_DIST_BY_PROMPT", True))
                rng = rng_for_prompt(
                    seed_base, (plain_prompt if by_prompt else None), by_prompt=by_prompt
                )
                total = sum(float(p) for p in probs) or 1.0
                u, cum = rng.random(), 0.0
                for v, w in zip(values, probs):
                    cum += float(w) / total
                    if u <= cum:
                        return int(v)
                return int(values[-1])

        # non-hist distributions
        seed_base = int(
            getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0
        )
        by_prompt = bool(getattr(cfg, "LENGTH_DIST_BY_PROMPT", True))
        rng = rng_for_prompt(
            seed_base, (plain_prompt if by_prompt else None), by_prompt=by_prompt
        )
        return int(sample_out_tokens_from_cfg(rng))

    cap = int(getattr(cfg, "MAX_TOKENS", 0) or 0)
    return int(cap if cap > 0 else 0)
