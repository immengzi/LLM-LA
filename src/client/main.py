# main.py

from __future__ import annotations

import argparse
import json
import time
import os
from pathlib import Path

from config import load_config
from prompts import (
    load_prompts_from_file,
    build_prompts_from_lmsys,
    load_replay_output_lengths,
    build_conversations_from_lmsys,
    build_conversations_from_codeflowbench,
    build_claude_conversations_from_codeflowbench,
    load_replay_conversation_lengths,
)
from scheduler import build_schedule
from load_runner import run_open_loop_load, run_users_claude_load
from experiment_io import init_experiment
from trace_utils import summarize_endpoint_tokens

# Optional Redis KV-block ownership watcher (writes redis_kv_watch.jsonl)
try:
    from redis_watch import RedisKVWatcher
except Exception:
    RedisKVWatcher = None  # type: ignore

# Optional Redis block-hash verifier (writes redis_verify.json)
try:
    from redis_verify import verify_router_logs
except Exception:
    verify_router_logs = None  # type: ignore

# Optional router /latency_log collector (BooM-proof endpoint + prefix/KV logging)
try:
    from router_log_collector import (
        RouterLogCollector,
        EnrichingLogger,
        join_logs_with_router,
        build_claude_logs_from_router,
        write_logs_full,
        summarize_routing,
    )
except Exception:
    RouterLogCollector = None  # type: ignore
    EnrichingLogger = None     # type: ignore
    join_logs_with_router = None  # type: ignore
    build_claude_logs_from_router = None  # type: ignore
    write_logs_full = None     # type: ignore
    summarize_routing = None      # type: ignore

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

    # Detect the integrated claude-CLI transport (closed-loop "users" load).
    _tcfg = getattr(cfg, "transport", None)
    claude_mode = (
        str(getattr(_tcfg, "mode", "sync") if _tcfg is not None else "sync").lower()
        == "claude"
    )

    # Build prompts
    output_tokens_per_request = None
    conversations = None
    conv_output_tokens = None
    claude_conversations = None  # list of per-conversation user-turn strings

    if claude_mode:
        # Closed-loop users model: build N*K deterministic conversations of raw
        # user turns (Claude Code supplies its own system prompt per turn).
        ucfg = cfg.users
        n_convs = int(ucfg.num_users) * int(ucfg.convs_per_user)
        claude_conversations = build_claude_conversations_from_codeflowbench(
            cfg.hf_lmsys, n_conversations=n_convs,
        )
        prompts = [c[0] for c in claude_conversations if c]  # first-turn view for counts
        total_turns = sum(len(c) for c in claude_conversations)
        print(
            f"[client] claude users: {len(claude_conversations)} conversations "
            f"({ucfg.num_users} users x {ucfg.convs_per_user}), "
            f"{total_turns} total user turns"
        )
    elif cfg.prompt_source == "file":
        prompts = load_prompts_from_file(
            path=cfg.file_prompts.path,
            variant=cfg.file_prompts.variant,
            n=cfg.total_requests,
        )
    elif cfg.prompt_source == "hf-lmsys":
        if cfg.multi_turn:
            conversations = build_conversations_from_lmsys(
                cfg.hf_lmsys,
                n=cfg.total_requests,
                min_rounds=2,
            )
            prompts = [c.turns[0].content for c in conversations]
            total_turns = sum(c.num_rounds for c in conversations)
            print(
                f"[client] Multi-turn mode: {len(conversations)} conversations, "
                f"{total_turns} total user turns, "
                f"avg_rounds={total_turns/len(conversations):.1f}"
            )
        else:
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
    elif cfg.prompt_source == "codeflow":
        if cfg.multi_turn:
            conversations = build_conversations_from_codeflowbench(
                cfg.hf_lmsys,
                n=cfg.total_requests,
                min_rounds=1,
            )
            prompts = [c.turns[0].content for c in conversations]
            total_turns = sum(c.num_rounds for c in conversations)
            print(
                f"[client] CodeFlowBench: {len(conversations)} conversations, "
                f"{total_turns} total user turns"
            )
        else:
            conversations = build_conversations_from_codeflowbench(
                cfg.hf_lmsys,
                n=cfg.total_requests,
                min_rounds=1,
            )
            prompts = [c.turns[0].content for c in conversations]
            if cfg.generation.use_dataset_output_len:
                output_tokens_per_request = [c.turns[1].output_tokens for c in conversations]
                print(
                    f"[client] CodeFlowBench using dataset output lengths: "
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
            replay_path = str(Path(cfg.experiments_root) / replay_ref / "logs.json")

        if cfg.multi_turn and conversations is not None:
            conv_output_tokens = load_replay_conversation_lengths(
                replay_path, n_conversations=len(conversations),
            )
        else:
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

    if cfg.multi_model is not None:
        mm = cfg.multi_model
        weight_sum = sum(t.weight for t in mm.targets)
        targets_desc = ", ".join(
            f"{t.model} ({t.weight/weight_sum:.0%})" for t in mm.targets
        )
        print(
            f"[client] multi_model: strategy={mm.strategy}, "
            f"targets=[{targets_desc}]"
        )

    # Initialize experiment directory + logger
    exp_dir, exp_logger = init_experiment(
        cfg,
        config_path=str(config_path),
        config_name=None,
        experiments_root=cfg.experiments_root,
    )
    print(f"[client] experiment_dir={exp_dir}")

    # Optional router /latency_log collector: persists router_logs.json and
    # live-enriches each request record with the serving endpoint + prefix/KV
    # fields (works through BooM since it reads the router directly).
    router_collector = None
    run_logger = exp_logger
    if getattr(cfg, "collect_router_log", False):
        if RouterLogCollector is None:
            print("[router-log] enabled but router_log_collector.py not available; skipping.")
        else:
            try:
                router_log_url = (getattr(cfg, "router_log_url", "") or cfg.router_url)
                router_collector = RouterLogCollector(
                    router_url=router_log_url,
                    out_path=Path(exp_dir) / "router_logs.json",
                )
                router_collector.start()
                run_logger = EnrichingLogger(exp_logger, router_collector)
                print(f"[router-log] collector started -> {Path(exp_dir) / 'router_logs.json'}")
            except Exception as e:
                print(f"[router-log] failed to start collector: {e}")
                router_collector = None
                run_logger = exp_logger

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

    # Build schedule (open-loop only; the claude "users" model is closed-loop and
    # does not use an arrival schedule).
    plan_times = []
    if not claude_mode:
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
            if conversations is not None:
                conversations = conversations[: len(plan_times)]
            if conv_output_tokens is not None:
                conv_output_tokens = conv_output_tokens[: len(plan_times)]
            total = len(prompts)
            print(f"[client] schedule shorter than prompts; trimming to {total} events")

    # Start Redis KV-block ownership watcher (best-effort; opt-in via config).
    redis_watcher = None
    rw = getattr(cfg, "redis_watch", None)
    if rw is not None and getattr(rw, "enabled", False):
        if RedisKVWatcher is None:
            print("[redis-watch] enabled but redis_watch.py not available; skipping.")
        else:
            try:
                redis_watcher = RedisKVWatcher(
                    out_path=Path(exp_dir) / "redis_kv_watch.jsonl",
                    host=rw.node_ip,
                    port=rw.port,
                    model=rw.model,
                    db=rw.db,
                    password=rw.password,
                    interval_s=rw.interval_s,
                    max_keys=rw.max_keys,
                    scan_count=rw.scan_count,
                    snapshot_max_blocks=getattr(rw, "snapshot_max_blocks", 200),
                    snapshot_mode=getattr(rw, "snapshot_mode", "full"),
                    snapshot_full_every_n_ticks=getattr(rw, "snapshot_full_every_n_ticks", 0),
                )
                if not redis_watcher.start():
                    redis_watcher = None
            except Exception as e:
                print(f"[redis-watch] failed to start: {e}")
                redis_watcher = None

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

    # Start container log capture (vLLM/router/sidecar) -> exp_dir/vllm-logs/
    # (in-process replacement for the manual container-logs.sh).
    pod_log_streamer = None
    if os.environ.get("CAPTURE_POD_LOGS", "1") not in ("0", "false", "False"):
        try:
            from pod_log_streamer import PodLogStreamer

            pod_log_streamer = PodLogStreamer(
                out_dir=Path(exp_dir) / "vllm-logs",
                namespace=os.environ.get("PODMAP_NAMESPACE", "vllm"),
            )
            if not pod_log_streamer.start():
                pod_log_streamer = None
            else:
                print(f"[pod-logs] capturing container logs -> {Path(exp_dir) / 'vllm-logs'}")
        except Exception as e:
            print(f"[pod-logs] failed to start: {e}")
            pod_log_streamer = None

    t_start_wall = time.time()
    t_start_load = time.time()
    t_end_load = None

    try:
        if claude_mode:
            run_users_claude_load(
                conversations=claude_conversations,
                claude_cfg=cfg.claude,
                users_cfg=cfg.users,
                logger=run_logger,
                output_log_mode=cfg.output_log_mode,
            )
        else:
            run_open_loop_load(
                router_url=cfg.router_url,
                prompts=prompts,
                plan_times=plan_times,
                gen_cfg=cfg.generation,
                output_tokens_per_request=output_tokens_per_request,
                warmup_reqs=cfg.load_pattern.warmup_reqs,
                logger=run_logger,
                output_log_mode=cfg.output_log_mode,
                print_trace=cfg.print_trace,
                log_request_body=getattr(cfg, "log_request_body", False),
                request_body_max_bytes=getattr(cfg, "request_body_max_bytes", 16384),
                transport=getattr(cfg, "transport", None),
                backend=backend,
                aibrix=getattr(cfg, "aibrix", None),
                litellm=getattr(cfg, "litellm", None),
                boom=getattr(cfg, "boom", None),
                slo=getattr(cfg, "slo", None),
                conversations=conversations,
                conv_output_tokens=conv_output_tokens,
                multi_model=cfg.multi_model,
                claude_code_injection=getattr(cfg, "claude_code_injection", None),
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

        # Stop Redis KV watcher (does a final scan) before closing logs.
        if redis_watcher is not None:
            try:
                redis_watcher.stop()
                print("[redis-watch] stopped")
            except Exception as e:
                print(f"[redis-watch] failed to stop: {e}")

        # stop event-driven podmap logger
        if podmap_logger is not None:
            try:
                podmap_logger.stop()
                print("[client] event podmap logger stopped")
            except Exception:
                pass

        # Stop container log capture (flush + terminate kubectl streams).
        if pod_log_streamer is not None:
            try:
                pod_log_streamer.stop()
            except Exception as e:
                print(f"[pod-logs] failed to stop: {e}")

        # Stop router-log collector (does a final poll) before closing logs.json.
        if router_collector is not None:
            try:
                router_collector.stop()
                print(f"[router-log] collector stopped ({router_collector.count} records)")
            except Exception as e:
                print(f"[router-log] failed to stop collector: {e}")

        exp_logger.close()

    logs_path = str(Path(exp_dir) / "logs.json")
    router_logs_path = str(Path(exp_dir) / "router_logs.json")

    # Authoritative end-of-run join: rewrite logs.json so every record carries
    # the serving endpoint + prefix/KV fields recorded by the router.
    #
    # The claude "users" transport drives the real CLI, which does not surface
    # the router's request id, so there is no req_id to join on. In that mode the
    # router /latency_log (router_logs.json) is the standalone KV-truth source, so
    # we summarize routing directly from it instead of joining into logs.json.
    routing_summary = None
    if router_collector is not None and join_logs_with_router is not None:
        if claude_mode:
            # The claude CLI does not surface the router rid, so logs.json is
            # rebuilt straight from the router /latency_log truth -- the same
            # records prod_latency_collector.py emits (endpoint + kv_hits/
            # total_blocks/matched_tokens/kv_hit/block_hashes). Live client turn
            # records are preserved to claude_client_logs.json; claude per-turn
            # extras are attached by content match. Then summarize from logs.json.
            try:
                if build_claude_logs_from_router is not None:
                    stats = build_claude_logs_from_router(logs_path, router_logs_path)
                    print(
                        f"[router-log] claude logs.json rebuilt from router truth: "
                        f"router_records={stats['router_records']} "
                        f"client_records={stats['client_records']} "
                        f"extras_attached={stats['extras_attached']}"
                    )
                    if stats["router_records"] == 0:
                        print(
                            "[router-log] WARN: no router records -- ensure "
                            "collect_router_log=true and the router /latency_log "
                            "is reachable."
                        )
                routing_summary = summarize_routing(logs_path)
            except Exception as e:
                print(f"[router-log] WARN: claude logs rebuild failed: {e}")
                try:
                    routing_summary = summarize_routing(router_logs_path)
                except Exception:
                    pass
        else:
            try:
                # When we also emit logs_full.json, keep logs.json lean: the
                # bulky block-hash list + request body ride only the full variant.
                emit_full = getattr(cfg, "emit_logs_full", False)
                drop_fields = {"block_hashes", "request_body"} if emit_full else None
                stats = join_logs_with_router(
                    logs_path, router_logs_path, drop_fields=drop_fields
                )
                print(
                    f"[router-log] joined logs.json: matched={stats['matched']}/"
                    f"{stats['total']} (missing={stats['missing']})"
                )
                routing_summary = summarize_routing(logs_path)
            except Exception as e:
                print(f"[router-log] WARN: end-of-run join failed: {e}")

    # Optional superset log: logs_full.json = every logs.json record PLUS the full
    # request body and prefix block-hash list, from the router truth. logs.json
    # itself stays lean/body-free. Opt-in via emit_logs_full (needs the router to
    # emit bodies via router_log_request_body + hashes via router_log_block_hashes).
    if (
        getattr(cfg, "emit_logs_full", False)
        and router_collector is not None
        and write_logs_full is not None
    ):
        try:
            full_logs_path = str(Path(exp_dir) / "logs_full.json")
            fstats = write_logs_full(logs_path, router_logs_path, full_logs_path)
            print(
                f"[router-log] wrote logs_full.json (block hashes + full request): "
                f"matched={fstats['matched']}/{fstats['total']} "
                f"(missing={fstats['missing']})"
            )
        except Exception as e:
            print(f"[router-log] WARN: logs_full.json write failed: {e}")

    # Confirm Redis stores the block hashes the router computed (writes
    # redis_verify.json). Runs against the router's own truth (router_logs.json).
    # Scoped to the claude "users" comparison so existing configs are unchanged.
    redis_verify_summary = None
    if claude_mode and verify_router_logs is not None and Path(router_logs_path).is_file():
        try:
            rw = getattr(cfg, "redis_watch", None)
            model_default = ""
            if rw is not None and getattr(rw, "model", ""):
                model_default = rw.model
            elif getattr(cfg, "boom", None) is not None:
                model_default = getattr(cfg.boom, "model", "") or ""
            redis_verify_summary = verify_router_logs(
                router_logs_path,
                str(Path(exp_dir) / "redis_verify.json"),
                namespace=os.environ.get("PODMAP_NAMESPACE", "vllm"),
                model_default=model_default,
            )
            print(
                f"[redis-verify] checked={redis_verify_summary.get('checked')} "
                f"hashes_match={redis_verify_summary.get('hashes_match')} "
                f"missing={redis_verify_summary.get('missing')}"
            )
        except Exception as e:
            print(f"[redis-verify] WARN: verification failed: {e}")

    token_summary_path = str(Path(exp_dir) / "endpoint_tokens.json")
    summarize_endpoint_tokens(logs_path, save_path=token_summary_path)

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
        # multi-model fields
        "multi_model_strategy": cfg.multi_model.strategy if cfg.multi_model else None,
        "multi_model_targets": [
            {"model": t.model, "weight": t.weight}
            for t in cfg.multi_model.targets
        ] if cfg.multi_model else None,
    }
    if routing_summary is not None:
        run_summary["routing"] = routing_summary
    if redis_verify_summary is not None:
        run_summary["redis_verify"] = redis_verify_summary
    if claude_mode:
        run_summary["users"] = {
            "num_users": cfg.users.num_users,
            "convs_per_user": cfg.users.convs_per_user,
            "interval_between_convs_s": cfg.users.interval_between_convs_s,
            "ramp_s": cfg.users.ramp_s,
        }
        run_summary["claude_model"] = getattr(cfg.claude, "model", None)
    try:
        with (Path(exp_dir) / "run_summary.json").open("w", encoding="utf-8") as f:
            json.dump(run_summary, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[client] WARN: failed to write run_summary.json: {e}")

    print(f"[client] done. Total elapsed wall time = {dt_wall:.3f}s")


if __name__ == "__main__":
    main()