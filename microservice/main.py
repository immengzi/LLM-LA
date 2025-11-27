# -*- coding: utf-8 -*-
"""
main.py – Unified load-testing client for vLLM.

Supports:
  • PROMPTS_FILE_PATH (JSON/JSONL)
  • LMSYS dataset (HF or local)
  • LENGTH_MODE replay-output
  • All load patterns (det, poisson, bursty, steps, rand, dump)

Produces:
  • results/client/<run_id>/output.jsonl     – per-request logs
  • results/client/<run_id>/load.jsonl       – arrival schedule logs
  • results/client/<run_id>/configs.json     – snapshot of config

Config keys used:
  CLIENT_ENDPOINT
  CLIENT_MODE / CLIENT_USE_LMSYS
  PROMPTS_FILE_PATH, PROMPTS_LIMIT
  LOAD_PATTERN, LOAD_RATE_RPS, LOAD_WARMUP_S, LOAD_DURATION_S
  LOAD_BURST_ON_S, LOAD_BURST_OFF_S, LOAD_BURST_RPS_ON, LOAD_BURST_RPS_OFF
  LOAD_STEP_SCHEDULE
  LOAD_RAND_RPS_MIN, LOAD_RAND_RPS_MAX, LOAD_RAND_EPOCH_S, LOAD_RAND_KIND
  LENGTH_MODE
  HF_DATASET_NAME, HF_DATASET_SPLIT, HF_TOKENIZER_NAME, HF_STREAMING
  LMSYS_MIN_INPUT_TOKENS, LMSYS_MAX_INPUT_TOKENS, LMSYS_REPEAT_EACH
"""

from __future__ import annotations

import argparse
import time
from collections import deque
from typing import List, Tuple, Optional

from config import get_config
from utils import load_prompts, log_result, send_chat_request
from lmsys_loader import iter_lmsys_pairs
from length_backend import register_replay_out_len
from loadgen import drive_load


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _build_lmsys_payloads(max_items: Optional[int]) -> Tuple[List[str], List[int]]:
    """
    Convert LMSYS (prompt,out_len) iterator into aligned lists.
    Respects PROMPTS_LIMIT unless explicitly overridden.
    """
    cfg = get_config()
    limit = max_items or getattr(cfg, "PROMPTS_LIMIT", None) or 10000

    prompts: List[str] = []
    out_lens: List[int] = []

    for prompt, out_len in iter_lmsys_pairs(
        max_n=limit,
        progress=True,
        progress_desc="LMSYS→client"
    ):
        prompts.append(prompt)
        out_lens.append(int(max(0, out_len)))

    return prompts, out_lens


def _build_plain_prompts(max_items: Optional[int]) -> List[str]:
    """
    Load prompts from PROMPTS_FILE_PATH using utils.load_prompts().
    """
    cfg = get_config()
    dq = load_prompts(cfg.PROMPTS_FILE_PATH)
    items = list(dq)

    if max_items is not None and max_items >= 0:
        items = items[:max_items]

    return items


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> int:
    cfg = get_config()

    # ---------------------------------------------------------
    # CLI
    # ---------------------------------------------------------
    parser = argparse.ArgumentParser(
        description="vLLM load-testing client – prompts or LMSYS dataset."
    )

    parser.add_argument("--mode", choices=["prompts", "lmsys"], default=None,
                        help="Force mode. If omitted, uses CLIENT_MODE or CLIENT_USE_LMSYS.")

    parser.add_argument("--endpoint", default=None,
                        help="Override CLIENT_ENDPOINT (base URL, no /v1/chat/completions).")

    parser.add_argument("--pattern", default=None,
                        help="Load pattern: det | poisson | bursty | steps | rand | dump")

    parser.add_argument("--rate", type=float, default=None,
                        help="Base RPS for det/poisson/steps/rand patterns.")

    parser.add_argument("--duration", type=float, default=None,
                        help="Main duration in seconds (excluding warmup).")

    parser.add_argument("--warmup", type=float, default=None,
                        help="Warmup duration in seconds.")

    parser.add_argument("--max-requests", type=int, default=None,
                        help="Optional cap on request count (overrides PROMPTS_LIMIT).")

    parser.add_argument("--verbose", action="store_true",
                        help="Verbose stdout.")

    args = parser.parse_args(argv)

    # ---------------------------------------------------------
    # Determine mode (prompts vs LMSYS)
    # ---------------------------------------------------------
    cfg_mode = getattr(cfg, "CLIENT_MODE", None)
    cfg_use_lmsys = bool(getattr(cfg, "CLIENT_USE_LMSYS", False))

    if args.mode:
        mode = args.mode
    else:
        if cfg_use_lmsys:
            mode = "lmsys"
        elif cfg_mode in ("prompts", "lmsys"):
            mode = cfg_mode
        else:
            mode = "prompts"

    using_lmsys = (mode == "lmsys")

    # ---------------------------------------------------------
    # Load pattern parameters
    # ---------------------------------------------------------
    pattern = args.pattern or getattr(cfg, "LOAD_PATTERN", "det")
    rate_rps = float(args.rate or getattr(cfg, "LOAD_RATE_RPS", 5.0))
    warmup_s = float(args.warmup or getattr(cfg, "LOAD_WARMUP_S", 0.0))
    duration_s = float(args.duration or getattr(cfg, "LOAD_DURATION_S", 60.0))

    burst_on_s = float(getattr(cfg, "LOAD_BURST_ON_S", 2.0))
    burst_off_s = float(getattr(cfg, "LOAD_BURST_OFF_S", 2.0))
    burst_rps_on = float(getattr(cfg, "LOAD_BURST_RPS_ON", 10.0))
    burst_rps_off = float(getattr(cfg, "LOAD_BURST_RPS_OFF", 0.0))
    step_schedule = str(getattr(cfg, "LOAD_STEP_SCHEDULE", "") or "")

    rand_rps_min = getattr(cfg, "LOAD_RAND_RPS_MIN", None)
    rand_rps_max = getattr(cfg, "LOAD_RAND_RPS_MAX", None)
    rand_epoch_s = float(getattr(cfg, "LOAD_RAND_EPOCH_S", 5.0))
    rand_kind = str(getattr(cfg, "LOAD_RAND_KIND", "poisson") or "poisson")

    # ---------------------------------------------------------
    # Endpoint
    # ---------------------------------------------------------
    endpoint = args.endpoint or getattr(cfg, "CLIENT_ENDPOINT", None)
    if not endpoint:
        endpoint = "http://127.0.0.1:8200"

    # ---------------------------------------------------------
    # Load prompts
    # ---------------------------------------------------------
    max_reqs = args.max_requests or getattr(cfg, "PROMPTS_LIMIT", None)

    if using_lmsys:
        prompts, out_lens = _build_lmsys_payloads(max_reqs)
    else:
        prompts = _build_plain_prompts(max_reqs)
        out_lens = []

    total = len(prompts)
    if total == 0:
        print("[CLIENT] No prompts found. Exiting.")
        return 0

    # ---------------------------------------------------------
    # LENGTH_MODE: replay-output
    # ---------------------------------------------------------
    length_mode = str(getattr(cfg, "LENGTH_MODE", "legacy")).lower().strip()
    use_replay = using_lmsys and length_mode == "replay-output"

    if use_replay and len(out_lens) != len(prompts):
        raise RuntimeError("Replay-output requires aligned LMSYS output lengths.")

    # ---------------------------------------------------------
    # Enqueue callback
    # ---------------------------------------------------------
    next_req_id = 0

    def enqueue_one(prompt: str, t_enq_client: float) -> None:
        nonlocal next_req_id
        req_id = next_req_id
        next_req_id += 1

        if use_replay:
            register_replay_out_len(req_id, out_lens[req_id])

        messages = [{"role": "user", "content": prompt}]
        t0 = time.time()

        try:
            resp = send_chat_request(
                endpoint=endpoint,
                messages=messages,
                max_tokens=None,
                length_mode=None,
                req_id=req_id,
            )
            t1 = time.time()

            # extract text
            text = None
            try:
                choices = resp.get("choices") or []
                if choices:
                    msg = (choices[0] or {}).get("message") or {}
                    text = msg.get("content")
            except Exception:
                pass

            log_result(
                mode="client",
                endpoint=endpoint,
                model=getattr(cfg, "MODEL_NAME", "unknown"),
                status="ok",
                prompt=prompt,
                response=text,
                latency_s=t1 - t0,
                extra={
                    "req_id": req_id,
                    "using_lmsys": using_lmsys,
                    "length_mode": length_mode,
                },
            )

        except Exception as e:
            t1 = time.time()
            log_result(
                mode="client",
                endpoint=endpoint,
                model=getattr(cfg, "MODEL_NAME", "unknown"),
                status="error",
                prompt=prompt,
                response=None,
                latency_s=t1 - t0,
                error=str(e),
                extra={"req_id": req_id},
            )

    # ---------------------------------------------------------
    # Run load generation
    # ---------------------------------------------------------
    print(
        f"[CLIENT] mode={mode}, total_prompts={total}, "
        f"pattern={pattern}, endpoint={endpoint}, "
        f"LENGTH_MODE={length_mode}, replay={use_replay}"
    )

    drive_load(
        pattern=pattern,
        prompts=deque(prompts),
        enqueue_one=enqueue_one,
        rate_rps=rate_rps,
        warmup_s=warmup_s,
        duration_s=duration_s,
        burst_on_s=burst_on_s,
        burst_off_s=burst_off_s,
        burst_rps_on=burst_rps_on,
        burst_rps_off=burst_rps_off,
        step_schedule=step_schedule,
        rand_rps_min=rand_rps_min,
        rand_rps_max=rand_rps_max,
        rand_epoch_s=rand_epoch_s,
        rand_kind=rand_kind,
        router_mode="client",
        log_every=1,
        verbose=args.verbose,
    )

    print("[CLIENT] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
