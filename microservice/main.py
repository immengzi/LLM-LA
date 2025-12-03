# main.py
# Entry point: read YAML config, build prompts, build schedule, send requests.

from __future__ import annotations

import argparse
import time

import requests

from config import load_config
from prompts import load_prompts_from_file, build_prompts_from_lmsys
from scheduler import build_schedule
from http_client import send_one


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to YAML config file",
    )
    # Optional quick override for total_requests (handy for ad-hoc runs)
    parser.add_argument(
        "--n",
        type=int,
        default=None,
        help="Override total_requests from config (optional)",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.n is not None:
        cfg.total_requests = int(args.n)

    # ---------------------------
    # Build prompts
    # ---------------------------
    if cfg.prompt_source == "file":
        prompts = load_prompts_from_file(
            path=cfg.file_prompts.path,
            variant=cfg.file_prompts.variant,
            n=cfg.total_requests,
        )
    elif cfg.prompt_source == "hf-lmsys":
        prompts = build_prompts_from_lmsys(
            cfg.hf_lmsys,
            n=cfg.total_requests,
        )
    else:
        raise ValueError(f"Unknown prompt_source '{cfg.prompt_source}'")

    total = len(prompts)
    if total == 0:
        print("[client] No prompts available; exiting.")
        return

    print(
        f"[client] router-url={cfg.router_url}, "
        f"n={total}, pattern={cfg.load_pattern.pattern}, "
        f"prompt-source={cfg.prompt_source}"
    )

    # ---------------------------
    # Build schedule
    # ---------------------------
    lp = cfg.load_pattern
    plan_times = build_schedule(
        pattern=lp.pattern,
        total_items=total,
        rate_rps=lp.rate_rps,
        warmup_s=lp.warmup_s,
        duration_s=lp.duration_s,
        burst_on_s=lp.burst_on_s,
        burst_off_s=lp.burst_off_s,
        burst_rps_on=lp.burst_rps_on,
        burst_rps_off=lp.burst_rps_off,
        step_schedule=lp.step_schedule,
        rand_rps_min=lp.rand_rps_min,
        rand_rps_max=lp.rand_rps_max,
        rand_epoch_s=lp.rand_epoch_s,
        seed=lp.loadgen_seed,
    )

    if len(plan_times) < total:
        prompts = prompts[:len(plan_times)]
        total = len(prompts)
        print(f"[client] schedule shorter than prompts; trimming to {total} events")

    # ---------------------------
    # Execute schedule (single-thread open-loop)
    # ---------------------------
    gen = cfg.generation
    session = requests.Session()
    t_start_wall = time.time()
    sent = 0

    for idx, (ts_mono, prompt) in enumerate(zip(plan_times, prompts)):
        now_mono = time.monotonic()
        if ts_mono > now_mono:
            time.sleep(ts_mono - now_mono)

        meta = {
            "max_tokens": int(gen.max_tokens),
            "temperature": float(gen.temperature),
            "length_mode": gen.length_mode,
        }
        if gen.target_output_tokens is not None:
            meta["target_output_tokens"] = int(gen.target_output_tokens)
        if gen.target_total_tokens is not None:
            meta["target_total_tokens"] = int(gen.target_total_tokens)

        try:
            rid = send_one(session, cfg.router_url, prompt, meta)
            sent += 1
            print(f"[client] sent idx={idx} req_id={rid}")
        except Exception as e:
            print(f"[client] ✗ ERROR idx={idx}: {e}")

    session.close()
    dt = time.time() - t_start_wall
    print(f"[client] done. Sent {sent} requests in {dt:.3f}s")


if __name__ == "__main__":
    main()
