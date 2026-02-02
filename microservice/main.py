# main.py
# Entry point: read YAML config, build prompts, build schedule, run open-loop load.
#
# NOTE: Helm knobs live in cfg.helm but are used by sweep_methods.py (cluster lifecycle),
# not by main.py. main.py behavior remains identical.

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from config import load_config
from prompts import load_prompts_from_file, build_prompts_from_lmsys
from scheduler import build_schedule
from load_runner import run_open_loop_load
from experiment_io import init_experiment

# Optional metrics
try:
    from metrics_prom import start_metrics_collection, stop_metrics_collection
except Exception:
    start_metrics_collection = None  # type: ignore
    stop_metrics_collection = None   # type: ignore


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help=(
            "Config selector. "
            "If it is a bare name (no directory, no suffix), the client loads ./configs/<name>.yaml. "
            "If it has a directory or suffix, it is treated as a path."
        ),
    )
    parser.add_argument(
        "--n",
        type=int,
        default=None,
        help="Override total_requests from config (optional)",
    )
    args = parser.parse_args()

    # Resolve config path
    if args.config:
        raw = Path(args.config)
        if raw.parent != Path(".") or raw.suffix:
            config_path = raw
        else:
            config_path = Path("configs") / f"{args.config}.yaml"
    else:
        config_path = Path("configs") / "example_config.yaml"

    cfg = load_config(str(config_path))

    if args.n is not None:
        cfg.total_requests = int(args.n)

    # Build prompts
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

    # Transport summary (non-breaking)
    tcfg = getattr(cfg, "transport", None)
    transport_mode = getattr(tcfg, "mode", "sync") if tcfg is not None else "sync"
    submit_path = getattr(tcfg, "submit_path", "/submit") if tcfg is not None else "/submit"
    results_zmq = getattr(tcfg, "results_zmq", None) if tcfg is not None else None
    topic = getattr(tcfg, "topic", "") if tcfg is not None else ""
    run_id = getattr(tcfg, "run_id", None) if tcfg is not None else None
    grace_s = getattr(tcfg, "grace_s", 30.0) if tcfg is not None else 30.0

    print(
        f"[client] router-url={cfg.router_url}, "
        f"n={total}, pattern={cfg.load_pattern.pattern}, "
        f"prompt-source={cfg.prompt_source}, "
        f"warmup_reqs={cfg.load_pattern.warmup_reqs}, "
        f"output_log_mode={cfg.output_log_mode}, "
        f"print_trace={cfg.print_trace}, "
        f"metrics_enabled={bool(cfg.metrics.enabled)}, "
        f"transport_mode={transport_mode}"
    )

    if str(transport_mode).lower() == "async_pubsub":
        print(
            f"[client] async_pubsub: submit_path={submit_path} "
            f"results_zmq={results_zmq} topic={topic!r} run_id={run_id!r} grace_s={grace_s}"
        )

    # Initialize experiment directory + logger
    exp_dir, exp_logger = init_experiment(
        cfg,
        config_path=str(config_path),
        config_name=None,
    )
    print(f"[client] experiment_dir={exp_dir}")

    # Build schedule
    lp = cfg.load_pattern
    plan_times = build_schedule(
        pattern=lp.pattern,
        total_items=total,
        rate_rps=lp.rate_rps,
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
        prompts = prompts[: len(plan_times)]
        total = len(prompts)
        print(f"[client] schedule shorter than prompts; trimming to {total} events")

    # Start metrics (best-effort)
    metrics_started = False
    if cfg.metrics.enabled:
        if start_metrics_collection is None:
            print("[metrics] enabled but metrics_prom.py not available; skipping.")
        else:
            try:
                start_metrics_collection(
                    run_dir=str(exp_dir),
                    cfg=cfg.metrics,
                )
                metrics_started = True
                print("[metrics] collection started")
            except Exception as e:
                print(f"[metrics] failed to start metrics collection: {e}")

    t_start_wall = time.time()
    t_start_load = time.time()
    t_end_load = None

    try:
        run_open_loop_load(
            router_url=cfg.router_url,
            prompts=prompts,
            plan_times=plan_times,
            gen_cfg=cfg.generation,
            warmup_reqs=cfg.load_pattern.warmup_reqs,
            logger=exp_logger,
            output_log_mode=cfg.output_log_mode,
            print_trace=cfg.print_trace,
            transport=getattr(cfg, "transport", None),
        )
    finally:
        t_end_load = time.time()

        # Stop metrics first (flush), then close request logger
        if metrics_started and stop_metrics_collection is not None:
            try:
                stop_metrics_collection()
                print("[metrics] collection stopped")
            except Exception as e:
                print(f"[metrics] failed to stop metrics collection: {e}")

        exp_logger.close()

    dt_wall = time.time() - t_start_wall
    dt_load = (t_end_load - t_start_load) if t_end_load is not None else None

    # Persist a small, machine-readable run summary for reproducibility
    run_summary = {
        "total_requests": int(total),
        "load_runner_duration_s": round(float(dt_load), 3) if dt_load is not None else None,
        "wall_time_s": round(float(dt_wall), 3),
        "transport_mode": str(transport_mode),
        "submit_path": str(submit_path),
        "results_zmq": results_zmq,
        "topic": str(topic),
        "run_id": run_id,
        "grace_s": float(grace_s) if grace_s is not None else None,
    }
    try:
        with (Path(exp_dir) / "run_summary.json").open("w", encoding="utf-8") as f:
            json.dump(run_summary, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[client] WARN: failed to write run_summary.json: {e}")

    print(f"[client] done. Total elapsed wall time = {dt_wall:.3f}s")


if __name__ == "__main__":
    main()
