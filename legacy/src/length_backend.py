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

import os
try:
    from transformers import AutoTokenizer  # type: ignore
except Exception:
    AutoTokenizer = None  # type: ignore

from config import get_config


# ---------------------------------------------------------------------
# Mode "enum" (just names; all values still come from config)
# ---------------------------------------------------------------------
MODE_LEGACY = "legacy"
MODE_TARGET_OUTPUT = "target-output"
MODE_TARGET_TOTAL = "target-total"
MODE_REPLAY_OUTPUT = "replay-output"
MODE_DIST_OUTPUT = "dist-output"


# ---------------------------------------------------------------------
# Shared tokenizer for logging real input token counts
# ---------------------------------------------------------------------
_logging_tokenizer = None
_logging_tokenizer_lock = threading.Lock()


def _get_logging_tokenizer():
    """
    Lazily load a tokenizer used for logging input token counts.

    Preference:
      1) cfg.HF_TOKENIZER_NAME (if set)
      2) cfg.MODEL_NAME

    Falls back to None if transformers is unavailable or loading fails.
    """
    global _logging_tokenizer
    if _logging_tokenizer is not None:
        return _logging_tokenizer

    if AutoTokenizer is None:
        return None

    with _logging_tokenizer_lock:
        if _logging_tokenizer is not None:
            return _logging_tokenizer

        cfg = get_config()
        name = getattr(cfg, "HF_TOKENIZER_NAME", None) or getattr(cfg, "MODEL_NAME", "gpt2")

        try:
            if isinstance(name, str) and os.path.isdir(name):
                print(f"[LENGTH_BACKEND] Using local tokenizer path for logging: {name}")
                tok = AutoTokenizer.from_pretrained(name, local_files_only=True)
            else:
                print(f"[LENGTH_BACKEND] Loading tokenizer for logging from HF Hub: {name}")
                tok = AutoTokenizer.from_pretrained(name)

            try:
                tok.model_max_length = int(1e9)
            except Exception:
                pass

            _logging_tokenizer = tok
            return _logging_tokenizer
        except Exception as e:
            print(
                f"[LENGTH_BACKEND] WARNING: failed to load tokenizer ({e}); "
                "falling back to whitespace lengths for input_tokens"
            )
            _logging_tokenizer = None
            return None


# ---------------------------------------------------------------------
# Deterministic per-run STRICT histogram plan (size = number of enqueues)
# ---------------------------------------------------------------------
_plan_state: Dict[str, Dict[str, Any]] = {}
_plan_lock = threading.Lock()


def set_hist_plan(label: str, probs: List[float], n: int, seed_base: int):
    """
    Precompute a deterministic histogram plan of exact size n.

    Produces a shuffled sequence of class indices (0..len(probs)-1) of
    length n, approximately respecting the target probabilities.
    """
    with _plan_lock:
        # rough counts
        counts = [int(round(float(p) * n)) for p in probs]
        delta = n - sum(counts)
        order = sorted(range(len(probs)), key=lambda i: -probs[i])
        i = 0
        while delta != 0 and order:
            j = order[i % len(order)]
            counts[j] += 1 if delta > 0 else -1
            delta += -1 if delta > 0 else 1
            i += 1

        # expand counts to sequence
        seq = [i for i, c in enumerate(counts) for _ in range(max(0, c))]
        if len(seq) < n:
            j = max(range(len(probs)), key=lambda k: probs[k])
            seq.extend([j] * (n - len(seq)))
        elif len(seq) > n:
            seq = seq[:n]

        # shuffle deterministically
        rng = random.Random(seed_for_name(seed_base, f"hist_plan::{label}"))
        rng.shuffle(seq)
        _plan_state[label] = {"seq": seq}


def get_hist_index_for_req(label: str, req_id: int, default_fn: Callable[[], int]) -> int:
    """
    Return planned class index for a given req_id, or default_fn() if no plan.

    Uses the per-label precomputed plan; if it doesn't exist or is empty,
    falls back to default_fn() (usually a PRNG-based draw).
    """
    with _plan_lock:
        st = _plan_state.get(label)
        if not st:
            return default_fn()
        seq: List[int] = st.get("seq") or []
        if not seq:
            return default_fn()
        return seq[req_id % len(seq)]


# ---------------------------------------------------------------------
# Deterministic PRNG fallback (non-STRICT histogram sampling)
# ---------------------------------------------------------------------
_prng_state: Dict[str, Dict[str, Any]] = {}
_prng_lock = threading.Lock()


def _next_hist_index_prng(label: str, probs: List[float], seed_base: int) -> int:
    """
    Stable categorical draw using a per-label seeded RNG.

    This is used when STRICT hist is off, or as a fallback if no plan exists.
    """
    key = f"prng:{label}"
    with _prng_lock:
        st = _prng_state.get(key)
        if st is None:
            s = seed_for_name(seed_base, f"hist_prng::{label}")
            st = {"rng": random.Random(s)}
            _prng_state[key] = st
        rng: random.Random = st["rng"]

        total = sum(float(p) for p in probs) or 1.0
        u = rng.random()
        cum = 0.0
        for i, p in enumerate(probs):
            cum += float(p) / total
            if u <= cum:
                return i
        return len(probs) - 1


# ---------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------
def seed_for_name(seed_base: int, name: str) -> int:
    """Turn (seed_base, name) into a deterministic int seed."""
    h = hashlib.sha256(f"{seed_base}:{name}".encode("utf-8")).hexdigest()
    return int(h[:16], 16)


def rng_for_prompt(seed_base: int, prompt: Optional[str], by_prompt: bool = True) -> random.Random:
    """
    Deterministic RNG keyed either by:
      - (seed_base, prompt) if by_prompt and prompt is non-empty
      - seed_base only otherwise
    """
    s = seed_for_name(seed_base, prompt) if (by_prompt and prompt) else int(seed_base)
    return random.Random(s)


def estimate_in_tokens_from_chars(prompt: str) -> int:
    """
    Rough heuristic for input tokens from character length.

    Used only in SIM / replay metadata. Real token counts come from
    count_input_tokens().
    """
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
    """
    Sample a completion length (in tokens) from SIM_OUT_DIST, respecting:

      - SIM_VARY_OUT_TOKENS
      - kind: lognormal / gamma / pareto / hist
      - LENGTH_DIST_STRICT_HIST (strict vs PRNG)
      - MAX_TOKENS as an upper cap

    All config comes from get_config() / RouterConfig.
    """
    cfg = get_config()
    if not cfg.SIM_VARY_OUT_TOKENS:
        return int(cfg.SIM_OUT_TOKENS)

    dist_cfg = dict(cfg.SIM_OUT_DIST or {})
    kind = str(dist_cfg.get("kind", "lognormal")).lower()
    lo = int(dist_cfg.get("min", 1))
    hi = int(dist_cfg.get("max", cfg.MAX_TOKENS or 4096))

    x = None

    # ---- Continuous distributions ------------------------------------
    if kind == "lognormal":
        mu = float(dist_cfg.get("mu", 4.8))
        sigma = float(dist_cfg.get("sigma", 0.8))
        x = rng.lognormvariate(mu, sigma)

    elif kind == "gamma":
        k = float(dist_cfg.get("k", 2.0))
        theta = float(dist_cfg.get("theta", 64.0 / max(k, 1e-9)))
        x = rng.gammavariate(k, theta)

    elif kind == "pareto":
        alpha = float(dist_cfg.get("alpha", 1.5))
        xm = float(dist_cfg.get("xm", 16.0))
        x = xm * rng.paretovariate(alpha)

    # ---- Discrete histogram -----------------------------------------
    elif kind == "hist":
        values = list(map(int, dist_cfg.get("values", [])))
        probs = dist_cfg.get("probs") or [1.0 / max(1, len(values))] * max(1, len(values))

        if not values:
            # No histogram values ⇒ fall back to fixed SIM_OUT_TOKENS
            return int(cfg.SIM_OUT_TOKENS)

        strict = bool(getattr(cfg, "LENGTH_DIST_STRICT_HIST", False))

        if strict and req_id is not None:
            # STRICT hist: precomputed plan if available, otherwise deterministic PRNG
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
            # Non-strict: simple categorical draw with the provided RNG
            p = float(rng.random())
            cum = 0.0
            for v, w in zip(values, probs):
                cum += float(w)
                if p <= cum:
                    x = int(v)
                    break
            if x is None:
                x = int(values[-1])

    # ---- Fallback: fixed length -------------------------------------
    else:
        return int(cfg.SIM_OUT_TOKENS)

    # Clamp inside [lo, hi]
    return max(lo, min(hi, int(round(x if x is not None else cfg.SIM_OUT_TOKENS))))


# ---------------------------------------------------------------------
# Plan helpers (small, linear, mode-specific)
# ---------------------------------------------------------------------
def _make_plan(
    *,
    eff_mode: str,
    eff_max: int,
    forced_out: Optional[int],
    forced_tot: Optional[int],
    eff_ignore_eos: bool,
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Single place for the plan dict, so structure is obvious."""
    return {
        "eff_mode": eff_mode,
        "eff_max": int(max(0, eff_max)),
        "forced_out": forced_out,
        "forced_tot": forced_tot,
        "eff_ignore_eos": eff_ignore_eos,
        "meta": meta,
    }


def _plan_explicit_overrides(
    *,
    eff_mode: str,
    base_cap: int,
    eff_ignore_eos: bool,
    target_output_tokens: Optional[int],
    target_total_tokens: Optional[int],
) -> Optional[Dict[str, Any]]:
    """
    If per-request target_output/target_total are set, use them directly.

    This behaves exactly like your original "Explicit override modes" block:
    it ignores LENGTH_MODE and returns immediately.
    """
    if target_output_tokens is None and target_total_tokens is None:
        return None

    forced_out: Optional[int] = None
    forced_tot: Optional[int] = None

    if target_output_tokens is not None:
        forced_out = max(0, int(target_output_tokens))
    if target_total_tokens is not None:
        forced_tot = max(0, int(target_total_tokens))

    eff_max = forced_out or forced_tot or base_cap

    meta = {"_length_mode": eff_mode}
    return _make_plan(
        eff_mode=eff_mode,
        eff_max=eff_max,
        forced_out=forced_out,
        forced_tot=forced_tot,
        eff_ignore_eos=eff_ignore_eos,
        meta=meta,
    )


def _plan_target_output(
    *,
    cfg,
    base_cap: int,
    eff_ignore_eos: bool,
    meta: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Handle LENGTH_MODE == target-output."""
    tgt = getattr(cfg, "TARGET_OUTPUT_TOKENS", None)
    if tgt is None:
        return None

    tgt = int(tgt)
    eff_max = min(tgt, base_cap)
    meta["_target_completion_tokens"] = tgt

    return _make_plan(
        eff_mode=MODE_TARGET_OUTPUT,
        eff_max=eff_max,
        forced_out=tgt,
        forced_tot=None,
        eff_ignore_eos=eff_ignore_eos,
        meta=meta,
    )


def _plan_target_total(
    *,
    cfg,
    base_cap: int,
    eff_ignore_eos: bool,
    meta: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Handle LENGTH_MODE == target-total."""
    tgt = getattr(cfg, "TARGET_TOTAL_TOKENS", None)
    if tgt is None:
        return None

    tgt = int(tgt)
    eff_max = min(tgt, base_cap)
    meta["_target_total_tokens"] = tgt

    return _make_plan(
        eff_mode=MODE_TARGET_TOTAL,
        eff_max=eff_max,
        forced_out=None,
        forced_tot=tgt,
        eff_ignore_eos=eff_ignore_eos,
        meta=meta,
    )


def _plan_replay_output(
    *,
    plain_prompt: str,
    base_cap: int,
    eff_ignore_eos: bool,
    meta: Dict[str, Any],
    req_id: Optional[int],
) -> Dict[str, Any]:
    """
    Handle LENGTH_MODE == replay-output (including fallback to legacy).

    Behaviour matches your original code: if we don't find a replay length,
    we fall back to 'legacy' in eff_mode but keep meta['_length_mode'] as
    whatever the requested mode was.
    """
    v = _get_replay_out_len(req_id)
    if v is None:
        # No replay length known → behave like legacy with same cap
        return _make_plan(
            eff_mode=MODE_LEGACY,
            eff_max=base_cap,
            forced_out=None,
            forced_tot=None,
            eff_ignore_eos=eff_ignore_eos,
            meta=meta,
        )

    v = int(v)
    est_in = int(estimate_in_tokens_from_chars(plain_prompt))
    out_budget = v if base_cap == 0 else min(v, base_cap)

    meta.update(
        {
            "_replay_mode": True,
            "_replay_out_len": v,
            "_replay_total_tokens_target": int(est_in + out_budget),
            "_req_id": (None if req_id is None else int(req_id)),
        }
    )

    return _make_plan(
        eff_mode=MODE_REPLAY_OUTPUT,
        eff_max=out_budget,
        forced_out=v,
        forced_tot=None,
        eff_ignore_eos=eff_ignore_eos,
        meta=meta,
    )


def _plan_dist_output(
    *,
    cfg,
    plain_prompt: str,
    base_cap: int,
    eff_ignore_eos: bool,
    meta: Dict[str, Any],
    req_id: Optional[int],
) -> Dict[str, Any]:
    """Handle LENGTH_MODE == dist-output."""
    seed_base = int(
        getattr(cfg, "LENGTH_DIST_SEED", None)
        or getattr(cfg, "SEED", 0)
        or 0
    )
    by_prompt = bool(getattr(cfg, "LENGTH_DIST_BY_PROMPT", True))

    rng = rng_for_prompt(
        seed_base,
        (plain_prompt if by_prompt else None),
        by_prompt=by_prompt,
    )

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

    return _make_plan(
        eff_mode=MODE_DIST_OUTPUT,
        eff_max=out_budget,
        forced_out=sampled_out,
        forced_tot=None,
        eff_ignore_eos=eff_ignore_eos,
        meta=meta,
    )


def _plan_legacy(
    *,
    base_cap: int,
    eff_ignore_eos: bool,
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Handle legacy / fallback."""
    return _make_plan(
        eff_mode=MODE_LEGACY,
        eff_max=base_cap,
        forced_out=None,
        forced_tot=None,
        eff_ignore_eos=eff_ignore_eos,
        meta=meta,
    )


# ---------------------------------------------------------------------
# Compute per-request cap/targets (refactored using helpers)
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
    """
    Compute the effective length plan for a single request.

    Returns a dict with keys:
      - eff_mode:        effective mode (may be 'legacy' even if replay-output fell back)
      - eff_max:         max completion tokens to allow
      - forced_out:      if not None, fixed completion length
      - forced_tot:      if not None, fixed total tokens (in + out)
      - eff_ignore_eos:  final ignore_eos flag
      - meta:            extra metadata for logging/debugging

    Behaviour matches the original implementation; all values come from
    RouterConfig via get_config() plus the function arguments.
    """
    cfg = get_config()

    # ------------------------------
    # Step 1: normalize mode & caps
    # ------------------------------
    # EXACTLY the same pattern you used originally:
    #   (length_mode or cfg.LENGTH_MODE or "legacy").lower().strip()
    eff_mode = (
        length_mode
        or getattr(cfg, "LENGTH_MODE", MODE_LEGACY)
        or MODE_LEGACY
    )
    eff_mode = eff_mode.lower().strip()

    eff_ignore_eos = bool(
        ignore_eos if ignore_eos is not None else getattr(cfg, "IGNORE_EOS", False)
    )

    # base_cap: if <=0 or None, use cfg.MAX_TOKENS if >0
    cfg_cap = int(getattr(cfg, "MAX_TOKENS", 0) or 0)
    if base_cap is None or base_cap < 0:
        base_cap = 0
    if base_cap == 0 and cfg_cap > 0:
        base_cap = cfg_cap
    base_cap = int(max(0, base_cap))

    # ------------------------------
    # Step 2: explicit overrides
    # ------------------------------
    override_plan = _plan_explicit_overrides(
        eff_mode=eff_mode,
        base_cap=base_cap,
        eff_ignore_eos=eff_ignore_eos,
        target_output_tokens=target_output_tokens,
        target_total_tokens=target_total_tokens,
    )
    if override_plan is not None:
        return override_plan

    # mode-specific metadata starts with just the requested mode
    meta: Dict[str, Any] = {"_length_mode": eff_mode}

    # ------------------------------
    # Step 3: target-output mode
    # ------------------------------
    if eff_mode == MODE_TARGET_OUTPUT:
        plan = _plan_target_output(
            cfg=cfg,
            base_cap=base_cap,
            eff_ignore_eos=eff_ignore_eos,
            meta=meta,
        )
        if plan is not None:
            return plan

    # ------------------------------
    # Step 4: target-total mode
    # ------------------------------
    if eff_mode == MODE_TARGET_TOTAL:
        plan = _plan_target_total(
            cfg=cfg,
            base_cap=base_cap,
            eff_ignore_eos=eff_ignore_eos,
            meta=meta,
        )
        if plan is not None:
            return plan

    # ------------------------------
    # Step 5: replay-output mode
    # ------------------------------
    if eff_mode == MODE_REPLAY_OUTPUT:
        return _plan_replay_output(
            plain_prompt=plain_prompt,
            base_cap=base_cap,
            eff_ignore_eos=eff_ignore_eos,
            meta=meta,
            req_id=req_id,
        )

    # ------------------------------
    # Step 6: dist-output mode
    # ------------------------------
    if eff_mode == MODE_DIST_OUTPUT:
        return _plan_dist_output(
            cfg=cfg,
            plain_prompt=plain_prompt,
            base_cap=base_cap,
            eff_ignore_eos=eff_ignore_eos,
            meta=meta,
            req_id=req_id,
        )

    # ------------------------------
    # Step 7: legacy / fallback
    # ------------------------------
    return _plan_legacy(
        base_cap=base_cap,
        eff_ignore_eos=eff_ignore_eos,
        meta=meta,
    )


# ---------------------------------------------------------------------
# Predictor preview — side-effect-free
# ---------------------------------------------------------------------
def preview_out_tokens_for_prompt(*, plain_prompt: str, req_id: int) -> int:
    """
    Side-effect-free preview of completion length.

    Behaviour matches the original implementation:
    - honours LENGTH_MODE, TARGET_*_TOKENS, replay-output, dist-output;
    - falls back to MAX_TOKENS for legacy/unknown modes.
    """
    cfg = get_config()
    # Same pattern as original:
    #   str(cfg.LENGTH_MODE or "legacy").lower().strip()
    mode = str(getattr(cfg, "LENGTH_MODE", MODE_LEGACY) or MODE_LEGACY).lower().strip()

    # target-output → fixed completion length
    if mode == MODE_TARGET_OUTPUT:
        tgt = getattr(cfg, "TARGET_OUTPUT_TOKENS", None)
        if tgt is not None:
            return int(tgt)

    # target-total → just return the total target as a rough preview
    if mode == MODE_TARGET_TOTAL:
        tgt = getattr(cfg, "TARGET_TOTAL_TOKENS", None)
        if tgt is not None:
            return int(tgt)

    # replay-output → use recorded length if available, else fall back to cap
    if mode == MODE_REPLAY_OUTPUT:
        v = _get_replay_out_len(req_id)
        if v is not None:
            return int(v)
        cap = int(getattr(cfg, "MAX_TOKENS", 0) or 0)
        return int(cap if cap > 0 else 0)

    # dist-output → mirror SIM_OUT_DIST behaviour
    if mode == MODE_DIST_OUTPUT:
        d = dict(getattr(cfg, "SIM_OUT_DIST", {}) or {})
        kind = str(d.get("kind", "lognormal")).lower()

        # Histogram handled explicitly
        if kind == "hist":
            values = list(map(int, d.get("values", [])))
            probs = d.get("probs") or ([1.0 / max(1, len(values))] * len(values))
            if not values:
                return int(getattr(cfg, "SIM_OUT_TOKENS", 0) or 0)

            strict = bool(getattr(cfg, "LENGTH_DIST_STRICT_HIST", False))

            if strict:
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
                # non-strict hist: draw once from a deterministic RNG
                seed_base = int(getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0)
                by_prompt = bool(getattr(cfg, "LENGTH_DIST_BY_PROMPT", True))
                rng = rng_for_prompt(
                    seed_base,
                    (plain_prompt if by_prompt else None),
                    by_prompt=by_prompt,
                )
                total = sum(float(p) for p in probs) or 1.0
                u, cum = rng.random(), 0.0
                for v, w in zip(values, probs):
                    cum += float(w) / total
                    if u <= cum:
                        return int(v)
                return int(values[-1])

        # Non-hist distributions: just reuse the same sampler as SIM
        seed_base = int(getattr(cfg, "LENGTH_DIST_SEED", None) or getattr(cfg, "SEED", 0) or 0)
        by_prompt = bool(getattr(cfg, "LENGTH_DIST_BY_PROMPT", True))
        rng = rng_for_prompt(
            seed_base,
            (plain_prompt if by_prompt else None),
            by_prompt=by_prompt,
        )
        return int(sample_out_tokens_from_cfg(rng))

    # legacy / anything else → just use the cap
    cap = int(getattr(cfg, "MAX_TOKENS", 0) or 0)
    return int(cap if cap > 0 else 0)


# ---------------------------------------------------------------------
# Input token counting
# ---------------------------------------------------------------------
def count_input_tokens(plain_prompt: str, req_id: Optional[int] = 0) -> int:
    """
    Return input token count using the same tokenizer as the model
    (HF_TOKENIZER_NAME if set, otherwise MODEL_NAME).

    Falls back to a simple whitespace-based length if no tokenizer
    is available or tokenization fails.
    """
    prompt = plain_prompt or ""
    try:
        tok = _get_logging_tokenizer()
        if tok is None:
            raise RuntimeError("no tokenizer available")
        ids = tok.encode(prompt, add_special_tokens=False)
        return max(1, len(ids))
    except Exception:
        return max(1, len(prompt.split()))
