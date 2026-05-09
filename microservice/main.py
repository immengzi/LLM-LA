# main.py

from __future__ import annotations

import argparse
import json
import time
import os
from pathlib import Path

from config import load_config
from prompts import load_prompts_from_file, build_prompts_from_lmsys, load_replay_output_lengths
from scheduler import build_schedule
from load_runner import run_open_loop_load
from experiment_io import init_experiment

# event-driven pod->node mapping snapshots (autoscaler / churn)
from k8s_event_podmap import EventDrivenPodMapLogger

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
    output_tokens_per_request = None
    if cfg.prompt_source == "file":
        prompts = load_prompts_from_file(
            path=cfg.file_prompts.path,
            variant=cfg.file_prompts.variant,
            n=cfg.total_requests,
        )
    elif cfg.prompt_source == "hf-lmsys":
        pairs = build_prompts_from_lmsys(
            cfg.hf_lmsys,
            n=cfg.total_requests,
        )
        prompts = [p for p, _ in pairs]
        if cfg.generation.use_dataset_output_len:
            output_tokens_per_request = [ot for _, ot in pairs]
            print(
                f"[client] Using dataset output lengths: "
                f"min={min(output_tokens_per_request)}, "
                f"max={max(output_tokens_per_request)}, "
                f"avg={sum(output_tokens_per_request)/len(output_tokens_per_request):.0f}"
            )
    else:
        raise ValueError(f"Unknown prompt_source '{cfg.prompt_source}'")

    # Replay output lengths from a previous experiment (overrides dataset lengths)
    replay_ref = getattr(cfg.generation, "replay_output_lengths_from", None)
    if replay_ref is not None:
        replay_ref = str(replay_ref).strip()
    if replay_ref:
        replay_path = replay_ref
        if not replay_path.endswith(".json"):
            replay_path = str(Path("experiments") / replay_ref / "logs.json")
        output_tokens_per_request = load_replay_output_lengths(
            replay_path, n=len(prompts),
        )
        cfg.generation.use_dataset_output_len = True
        cfg.generation.ignore_eos = True
        print(
            f"[client] Replay mode: output lengths from {replay_path} "
            f"(use_dataset_output_len=True, ignore_eos=True, "
            f"finish_reason will be 'length' not 'stop')"
        )

    total = len(prompts)
    if total == 0:
        print("[client] No prompts available; exiting.")
        return

    backend = str(getattr(cfg, "backend", "router") or "router").strip().lower()

    # Transport summary
    tcfg = getattr(cfg, "transport", None)
    transport_mode = getattr(tcfg, "mode", "sync") if tcfg is not None else "sync"
    submit_path = getattr(tcfg, "submit_path", "/submit") if tcfg is not None else "/submit"
    results_zmq = getattr(tcfg, "results_zmq", None) if tcfg is not None else None
    topic = getattr(tcfg, "topic", "") if tcfg is not None else ""
    run_id = getattr(tcfg, "run_id", None) if tcfg is not None else None

    print(
        f"[client] backend={backend}, "
        f"router-url={cfg.router_url}, "
        f"n={total}, pattern={cfg.load_pattern.pattern}, "
        f"prompt-source={cfg.prompt_source}, "
        f"warmup_reqs={cfg.load_pattern.warmup_reqs}, "
        f"output_log_mode={cfg.output_log_mode}, "
        f"print_trace={cfg.print_trace}, "
        f"metrics_enabled={bool(cfg.metrics.enabled)}, "
        f"transport_mode={transport_mode}"
    )

    if backend == "router" and str(transport_mode).lower() == "async_pubsub":
        print(
            f"[client] async_pubsub: submit_path={submit_path} "
            f"results_zmq={results_zmq} topic={topic!r} run_id={run_id!r}"
        )

    if backend == "aibrix":
        print(
            f"[client] aibrix: base_url={cfg.aibrix.base_url} "
            f"chat_path={cfg.aibrix.chat_path} "
            f"model={cfg.aibrix.model!r} "
            f"routing_strategy={cfg.aibrix.routing_strategy!r} "
            f"stream={bool(cfg.aibrix.stream)}"
        )

    if backend == "litellm":
        print(
            f"[client] litellm: base_url={cfg.litellm.base_url} "
            f"chat_path={cfg.litellm.chat_path} "
            f"model={cfg.litellm.model!r} "
            f"stream={bool(cfg.litellm.stream)}"
        )
        print(
            "[client] NOTE: backend=litellm routes through LiteLLM proxy "
            "(production auth/spend validation). Use backend=router for benchmarking."
        )

    if backend == "boom":
        print(
            f"[client] boom: base_url={cfg.boom.base_url} "
            f"chat_path={cfg.boom.chat_path} "
            f"model={cfg.boom.model!r} "
            f"timeout_s={cfg.boom.timeout_s} "
            f"(HTTP read timeout; must cover longest generation) "
            f"stream={bool(cfg.boom.stream)}"
        )
        print(
            "[client] NOTE: backend=boom routes through BooM Gateway "
            "(production auth/spend validation). Use backend=router for benchmarking."
        )

    # Initialize experiment directory + logger
    exp_dir, exp_logger = init_experiment(
        cfg,
        config_path=str(config_path),
        config_name=None,
    )
    print(f"[client] experiment_dir={exp_dir}")

    # start event-driven pod->node mapping watcher (writes JSONL beside other logs)
    podmap_logger = None
    try:
        podmap_logger = EventDrivenPodMapLogger(
            out_path=Path(exp_dir) / "pod_node_mapping_events.jsonl",
            namespace=os.environ.get("PODMAP_NAMESPACE", "vllm"),
            deployment_name=os.environ.get("PODMAP_DEPLOYMENT", "vllm-qwen"),
            kubectl=os.environ.get("PODMAP_KUBECTL", "kubectl"),
            quiet_window_s=float(os.environ.get("PODMAP_QUIET_S", "10") or "10"),
            snapshot_timeout_s=float(os.environ.get("PODMAP_TIMEOUT_S", "5") or "5"),
        )
        podmap_logger.start()
        print(
            f"[client] event podmap logger started -> "
            f"{Path(exp_dir) / 'pod_node_mapping_events.jsonl'}"
        )
    except Exception as e:
        print(f"[client] WARN: event podmap logger failed to start: {e}")
        podmap_logger = None

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
        if output_tokens_per_request is not None:
            output_tokens_per_request = output_tokens_per_request[: len(plan_times)]
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
            output_tokens_per_request=output_tokens_per_request,
            warmup_reqs=cfg.load_pattern.warmup_reqs,
            logger=exp_logger,
            output_log_mode=cfg.output_log_mode,
            print_trace=cfg.print_trace,
            transport=getattr(cfg, "transport", None),
            backend=backend,
            aibrix=getattr(cfg, "aibrix", None),
            litellm=getattr(cfg, "litellm", None),
            boom=getattr(cfg, "boom", None),
            slo=getattr(cfg, "slo", None),
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

        # stop event-driven podmap logger
        if podmap_logger is not None:
            try:
                podmap_logger.stop()
                print("[client] event podmap logger stopped")
            except Exception:
                pass

        exp_logger.close()

    dt_wall = time.time() - t_start_wall
    dt_load = (t_end_load - t_start_load) if t_end_load is not None else None

    # Persist a small, machine-readable run summary for reproducibility.
    # NEW: litellm fields added alongside existing aibrix fields.
    run_summary = {
        "total_requests": int(total),
        "backend": backend,
        "load_runner_duration_s": round(float(dt_load), 3) if dt_load is not None else None,
        "wall_time_s": round(float(dt_wall), 3),
        "transport_mode": str(transport_mode),
        "submit_path": str(submit_path),
        "results_zmq": results_zmq,
        "topic": str(topic),
        "run_id": run_id,
        # aibrix fields
        "aibrix_base_url": getattr(getattr(cfg, "aibrix", None), "base_url", None),
        "aibrix_chat_path": getattr(getattr(cfg, "aibrix", None), "chat_path", None),
        "aibrix_model": getattr(getattr(cfg, "aibrix", None), "model", None),
        "aibrix_routing_strategy": getattr(getattr(cfg, "aibrix", None), "routing_strategy", None),
        "aibrix_stream": getattr(getattr(cfg, "aibrix", None), "stream", None),
        # litellm fields
        "litellm_base_url": getattr(getattr(cfg, "litellm", None), "base_url", None),
        "litellm_chat_path": getattr(getattr(cfg, "litellm", None), "chat_path", None),
        "litellm_model": getattr(getattr(cfg, "litellm", None), "model", None),
        "litellm_stream": getattr(getattr(cfg, "litellm", None), "stream", None),
        # boom fields
        "boom_base_url": getattr(getattr(cfg, "boom", None), "base_url", None),
        "boom_chat_path": getattr(getattr(cfg, "boom", None), "chat_path", None),
        "boom_model": getattr(getattr(cfg, "boom", None), "model", None),
        "boom_stream": getattr(getattr(cfg, "boom", None), "stream", None),
    }
    try:
        with (Path(exp_dir) / "run_summary.json").open("w", encoding="utf-8") as f:
            json.dump(run_summary, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[client] WARN: failed to write run_summary.json: {e}")

    print(f"[client] done. Total elapsed wall time = {dt_wall:.3f}s")


if __name__ == "__main__":
    main()