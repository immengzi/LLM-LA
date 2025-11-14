# -*- coding: utf-8 -*-
"""
length_backend.py

Shared length-backend helpers used by BOTH:
- simulator paths (output length sampling)
- utils.send_chat_request (real vLLM path that may mirror/simulate)
- oracle predictor preview (side-effect-free)

Modes supported:
- legacy
- target-output
- target-total
- replay-output  (uses register_replay_out_len(req_id, out_len))
- dist-output    (lognormal/gamma/pareto/hist; optional STRICT hist)
"""

import math
import random
import hashlib
import threading
from typing import Optional, Dict, Any, List, Callable

from config import get_config

# ---------------------------------------------------------------------
# Deterministic per-run STRICT plan (size = number of enqueues)
# ---------------------------------------------------------------------
_plan_state: Dict[str, Dict[str, Any]] = {}
_plan_lock = threading.Lock()


def set_hist_plan(label: str, probs: List[float], n: int, seed_base: int):
    """Precompute a deterministic histogram plan of exact size n."""
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
        _plan_state[label] = {"seq": seq}


def get_hist_index_for_req(label: str, req_id: int, default_fn: Callable[[], int]) -> int:
    """Return planned class index for a given req_id, or default_fn() if no plan."""
    with _plan_lock:
        st = _plan_state.get(label)
        if not st:
            return default_fn()
        seq: List[int] = st.get("seq") or []
        if not seq:
            return default_fn()
        return seq[req_id % len(seq)]


# ---------------------------------------------------------------------
# Deterministic PRNG fallback (non-STRICT)
# ---------------------------------------------------------------------
_prng_state: Dict[str, Dict[str, Any]] = {}
_prng_lock = threading.Lock()


def _next_hist_index_prng(label: str, probs: List[float], seed_base: int) -> int:
    """Stable categorical draw using a per-label seeded RNG."""
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
    h = hashlib.sha256(f"{seed_base}:{name}".encode("utf-8")).hexdigest()
    return int(h[:16], 16)


def rng_for_prompt(seed_base: int, prompt: Optional[str], by_prompt: bool = True) -> random.Random:
    s = seed_for_name(seed_base, prompt) if (by_prompt and prompt) else int(seed_base)
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
# Replay registry: req_id -> out_len (dataset-driven)
# ---------------------------------------------------------------------
_replay_lock = threading.Lock()
_replay_out_len: Dict[int, int] = {}


def register_replay_out_len(req_id: int, out_len: int) -> None:
    """Called at enqueue time if dataset-provided reply length is known."""
    with _replay_lock:
        _replay_out_len[int(req_id)] = int(max(0, out_len))


def _get_replay_out_len(req_id: Optional[int]) -> Optional[int]:
    if req_id is None:
        return None
    with _replay_lock:
        return _replay_out_len.get(int(req_id))


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
            seed_base = int(getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0)
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
# Compute per-request cap/targets
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
    eff_mode = (length_mode or getattr(cfg, "LENGTH_MODE", "legacy") or "legacy").lower().strip()
    eff_ignore_eos = bool(ignore_eos if ignore_eos is not None else getattr(cfg, "IGNORE_EOS", False))

    cfg_cap = int(getattr(cfg, "MAX_TOKENS", 0) or 0)
    if base_cap is None or base_cap < 0:
        base_cap = 0
    if base_cap == 0 and cfg_cap > 0:
        base_cap = cfg_cap

    # Explicit override modes
    if target_output_tokens is not None or target_total_tokens is not None:
        forced_out = forced_tot = None
        if target_output_tokens is not None:
            forced_out = max(0, int(target_output_tokens))
        if target_total_tokens is not None:
            forced_tot = max(0, int(target_total_tokens))
        eff_max = forced_out or forced_tot or base_cap
        return {
            "eff_mode": eff_mode,
            "eff_max": int(max(0, eff_max)),
            "forced_out": forced_out,
            "forced_tot": forced_tot,
            "eff_ignore_eos": eff_ignore_eos,
            "meta": {"_length_mode": eff_mode},
        }

    meta: Dict[str, Any] = {"_length_mode": eff_mode}

    # Target-output
    if eff_mode == "target-output":
        tgt = getattr(cfg, "TARGET_OUTPUT_TOKENS", None)
        if tgt is not None:
            tgt = int(tgt)
            eff_max = min(tgt, base_cap)
            meta["_target_completion_tokens"] = tgt
            return {
                "eff_mode": eff_mode,
                "eff_max": eff_max,
                "forced_out": tgt,
                "forced_tot": None,
                "eff_ignore_eos": eff_ignore_eos,
                "meta": meta,
            }

    # Target-total
    if eff_mode == "target-total":
        tgt = getattr(cfg, "TARGET_TOTAL_TOKENS", None)
        if tgt is not None:
            tgt = int(tgt)
            eff_max = min(tgt, base_cap)
            meta["_target_total_tokens"] = tgt
            return {
                "eff_mode": eff_mode,
                "eff_max": eff_max,
                "forced_out": None,
                "forced_tot": tgt,
                "eff_ignore_eos": eff_ignore_eos,
                "meta": meta,
            }

    # replay-output
    if eff_mode == "replay-output":
        v = _get_replay_out_len(req_id)
        if v is not None:
            est_in = int(estimate_in_tokens_from_chars(plain_prompt))
            out_budget = v if base_cap == 0 else min(int(v), base_cap)
            meta.update(
                {
                    "_replay_mode": True,
                    "_replay_out_len": int(v),
                    "_replay_total_tokens_target": int(est_in + out_budget),
                    "_req_id": (None if req_id is None else int(req_id)),
                }
            )
            return {
                "eff_mode": eff_mode,
                "eff_max": int(max(0, out_budget)),
                "forced_out": int(v),
                "forced_tot": None,
                "eff_ignore_eos": eff_ignore_eos,
                "meta": meta,
            }
        # fallback to legacy if no replay length
        return {
            "eff_mode": "legacy",
            "eff_max": base_cap,
            "forced_out": None,
            "forced_tot": None,
            "eff_ignore_eos": eff_ignore_eos,
            "meta": meta,
        }

    # Distribution-based
    if eff_mode == "dist-output":
        seed_base = int(getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0)
        by_prompt = bool(getattr(cfg, "LENGTH_DIST_BY_PROMPT", True))
        rng = rng_for_prompt(seed_base, (plain_prompt if by_prompt else None), by_prompt=by_prompt)

        sampled_out = int(sample_out_tokens_from_cfg(rng, req_id=req_id))
        est_in = int(estimate_in_tokens_from_chars(plain_prompt))
        out_budget = min(sampled_out, base_cap)
        meta.update(
            {
                "_dist_mode": True,
                "_dist_seed_base": seed_base,
                "_dist_by_prompt": by_prompt,
                "_dist_prompt_tokens_est": est_in,
                "_dist_completion_tokens_target": out_budget,
                "_dist_total_tokens_target": est_in + out_budget,
                "_req_id": req_id,
            }
        )
        return {
            "eff_mode": eff_mode,
            "eff_max": out_budget,
            "forced_out": sampled_out,
            "forced_tot": None,
            "eff_ignore_eos": eff_ignore_eos,
            "meta": meta,
        }

    # legacy fallback
    return {
        "eff_mode": "legacy",
        "eff_max": base_cap,
        "forced_out": None,
        "forced_tot": None,
        "eff_ignore_eos": eff_ignore_eos,
        "meta": meta,
    }


# ---------------------------------------------------------------------
# Predictor preview — side-effect-free
# ---------------------------------------------------------------------
def preview_out_tokens_for_prompt(*, plain_prompt: str, req_id: int) -> int:
    cfg = get_config()
    mode = str(getattr(cfg, "LENGTH_MODE", "legacy") or "legacy").lower().strip()

    if mode == "target-output":
        tgt = getattr(cfg, "TARGET_OUTPUT_TOKENS", None)
        if tgt is not None:
            return int(tgt)
    if mode == "target-total":
        tgt = getattr(cfg, "TARGET_TOTAL_TOKENS", None)
        if tgt is not None:
            return int(tgt)

    if mode == "replay-output":
        v = _get_replay_out_len(req_id)
        if v is not None:
            return int(v)
        cap = int(getattr(cfg, "MAX_TOKENS", 0) or 0)
        return int(cap if cap > 0 else 0)

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
                seed_base = int(getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0)
                idx = get_hist_index_for_req(
                    label,
                    int(req_id),
                    lambda: _next_hist_index_prng(label, [float(p) for p in probs], seed_base),
                )
                idx = max(0, min(idx, len(values) - 1))
                return int(values[idx])
            else:
                seed_base = int(getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0)
                by_prompt = bool(getattr(cfg, "LENGTH_DIST_BY_PROMPT", True))
                rng = rng_for_prompt(seed_base, (plain_prompt if by_prompt else None), by_prompt=by_prompt)
                total = sum(float(p) for p in probs) or 1.0
                u, cum = rng.random(), 0.0
                for v, w in zip(values, probs):
                    cum += float(w) / total
                    if u <= cum:
                        return int(v)
                return int(values[-1])
        seed_base = int(getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0)
        by_prompt = bool(getattr(cfg, "LENGTH_DIST_BY_PROMPT", True))
        rng = rng_for_prompt(seed_base, (plain_prompt if by_prompt else None), by_prompt=by_prompt)
        return int(sample_out_tokens_from_cfg(rng))

    cap = int(getattr(cfg, "MAX_TOKENS", 0) or 0)
    return int(cap if cap > 0 else 0)


def count_input_tokens(plain_prompt: str, req_id: Optional[int] = 0) -> int:
    """
    Return input token count using the SAME tokenizer/path as compute_length_plan.
    We call compute_length_plan and read the input-length field it returns.

    Tries several common keys to stay compatible with your plan dict:
      - "input_tokens", "prompt_tokens", "in_tokens"
    Falls back to a minimal whitespace heuristic if none are present.
    """
    try:
        cfg = get_config()
        plan = compute_length_plan(
            plain_prompt=plain_prompt,
            base_cap=int(getattr(cfg, "MAX_TOKENS", 0) or 0),
            target_output_tokens=getattr(cfg, "TARGET_OUTPUT_TOKENS", None),
            target_total_tokens=getattr(cfg, "TARGET_TOTAL_TOKENS", None),
            ignore_eos=getattr(cfg, "IGNORE_EOS", False),
            length_mode=getattr(cfg, "LENGTH_MODE", "legacy"),
            req_id=int(req_id or 0),
        )
        for k in ("input_tokens", "prompt_tokens", "in_tokens"):
            v = plan.get(k)
            if v is not None:
                return int(v)
    except Exception:
        pass
    return max(1, len((plain_prompt or "").split()))
