# sim/runner.py
from __future__ import annotations

from dataclasses import asdict, replace
from typing import Any, Dict, List, Tuple
import json
import random
from pathlib import Path

import yaml

from config import ClientConfig
from sim.config import DEFAULT_SIM_CONFIG, SimConfig
from sim.engine import Engine
from sim.models import Endpoint, SimRequest
from sim.router import Router
from sim.schedule import build_schedule_virtual
from sim.prompt_source import build_requests
from sim.service_model import ServiceModel
from sim.stats import summarize
from sim.experiment_io import init_experiment_sim, write_metrics_summary, write_run_summary


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def _build_sim_config(raw: Dict[str, Any]) -> SimConfig:
    """
    Single source of defaults: sim/config.py

    Optional override:
      client YAML may include:
        sim:
          decode_tps: 500
          ...
    """
    over = raw.get("sim") or {}
    if not isinstance(over, dict):
        over = {}

    allowed = set(asdict(DEFAULT_SIM_CONFIG).keys())
    filtered: Dict[str, Any] = {k: v for k, v in over.items() if k in allowed}

    # apply overrides to the dataclass (no defaults here, defaults are in SimConfig)
    cfg = replace(DEFAULT_SIM_CONFIG, **filtered)

    # minimal type normalization / coercion (NOT defaulting)
    cfg.enabled = bool(cfg.enabled)
    cfg.methods = list(cfg.methods)
    cfg.seed = int(cfg.seed)

    cfg.n_endpoints = int(cfg.n_endpoints)
    cfg.max_inflight_per_ep = int(cfg.max_inflight_per_ep)

    cfg.prefill_tps = float(cfg.prefill_tps)
    cfg.decode_tps = float(cfg.decode_tps)

    cfg.batch_n_sat = int(cfg.batch_n_sat)
    cfg.noise_sigma = float(cfg.noise_sigma)

    cfg.output_log_mode = str(cfg.output_log_mode)

    return cfg


def _run_one_method(
    cfg: ClientConfig,
    sim_cfg: SimConfig,
    method: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:

    rng = random.Random(int(sim_cfg.seed) + (hash(method) % 10_000_000))

    # Build requests (prompt, in_tokens, out_tokens)
    req_specs = build_requests(cfg)
    total = len(req_specs)

    # Virtual schedule identical patterns
    lp = cfg.load_pattern
    plan_times = build_schedule_virtual(
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

    # Endpoints
    endpoints = [
        Endpoint(
            endpoint_id=f"ep{i}",
            max_inflight=int(sim_cfg.max_inflight_per_ep),
            prefill_tps=float(sim_cfg.prefill_tps),
            decode_tps=float(sim_cfg.decode_tps),
        )
        for i in range(int(sim_cfg.n_endpoints))
    ]

    service = ServiceModel(
        batch_n_sat=int(sim_cfg.batch_n_sat),
        noise_sigma=float(sim_cfg.noise_sigma),
    )
    router = Router(mode=method, endpoints=endpoints, service=service, rng=rng)

    eng = Engine()

    # Schedule arrivals
    for idx, ((prompt, in_tok, out_tok), t) in enumerate(zip(req_specs, plan_times)):
        req = SimRequest(
            idx=idx,
            req_id=f"{method}-{idx}",
            prompt=prompt,
            in_tokens=int(in_tok),
            out_tokens=int(out_tok),
        )
        eng.schedule(float(t), "ARRIVAL", req)

    records: List[Dict[str, Any]] = []
    lat_end_to_end: List[float] = []
    qwait: List[float] = []
    server: List[float] = []

    ep_map = {e.endpoint_id: e for e in endpoints}

    def handle(ev):
        now = eng.t

        if ev.kind == "ARRIVAL":
            req: SimRequest = ev.payload
            router.enqueue(req, now)
            router.step(eng)

        elif ev.kind == "COMPLETE":
            ep_id, req = ev.payload
            ep = ep_map[ep_id]
            ep.inflight -= 1
            ep.ok += 1

            req.t_response_router = now

            tq = (req.t_dispatch_router - req.t_arrival_router) if (req.t_dispatch_router is not None and req.t_arrival_router is not None) else 0.0
            ts = (req.t_response_router - req.t_dispatch_router) if (req.t_response_router is not None and req.t_dispatch_router is not None) else 0.0
            te = (req.t_response_router - req.t_arrival_router) if (req.t_response_router is not None and req.t_arrival_router is not None) else 0.0

            lat_end_to_end.append(float(te))
            qwait.append(float(tq))
            server.append(float(ts))

            record: Dict[str, Any] = {
                "idx": req.idx,
                "req_id": req.req_id,
                "prompt": req.prompt if sim_cfg.output_log_mode == "full" else req.prompt[:200],
                "planned_ts_mono": req.t_arrival_router,
                "actual_send_ts_mono": req.t_dispatch_router,
                "end_to_end_s": None,
                "model_latency_s": ts,
                "finish_reason": "stop",
                "prompt_tokens": req.in_tokens,
                "completion_tokens": req.out_tokens,
                "total_tokens": req.in_tokens + req.out_tokens,
                "trace": {
                    "endpoint": req.endpoint,
                    "router_mode": method,
                    "router_queue_wait_s": tq,
                    "server_roundtrip_s": ts,
                    "end_to_end_latency_s": te,
                    "t_prefill_s": req.t_prefill_s,
                    "t_decode_s": req.t_decode_s,
                },
            }
            records.append(record)

            router.step(eng)

    eng.run(until=None, handler=handle)

    summary = summarize(lat_end_to_end, qwait, server, sim_end_time_s=eng.t)
    metrics_summary = {
        "method": method,
        "n": summary.n,
        "sim_end_time_s": summary.sim_end_time_s,
        "throughput_rps": summary.throughput_rps,
        "latency_ms": summary.latency_ms,
        "queue_wait_ms": summary.queue_wait_ms,
        "server_ms": summary.server_ms,
        "per_endpoint": {e.endpoint_id: {"ok": e.ok, "err": e.err} for e in endpoints},
    }

    return records, metrics_summary


def run_sim_from_yaml(config_path: str, cfg: ClientConfig):
    raw = _load_yaml(config_path)
    sim_cfg = _build_sim_config(raw)

    if not sim_cfg.enabled:
        print("[main_sim] sim.enabled=false; exiting.")
        return

    exp_dir = init_experiment_sim(cfg, config_path=config_path, sim_cfg=asdict(sim_cfg))
    print(f"[main_sim] experiment_dir={exp_dir}")

    all_metrics: Dict[str, Any] = {
        "methods": {},
        "sim_cfg": asdict(sim_cfg),
        "client_cfg": asdict(cfg),
    }
    run_summary: Dict[str, Any] = {"methods": {}, "total_requests": int(cfg.total_requests)}

    for m in sim_cfg.methods:
        print(f"[main_sim] running method={m}")
        records, metrics = _run_one_method(cfg, sim_cfg, method=str(m))

        (Path(exp_dir) / f"logs_{m}.json").write_text(
            json.dumps(records, indent=2),
            encoding="utf-8",
        )

        all_metrics["methods"][m] = metrics
        run_summary["methods"][m] = {
            "n": metrics.get("n"),
            "sim_end_time_s": metrics.get("sim_end_time_s"),
            "throughput_rps": metrics.get("throughput_rps"),
        }

    write_metrics_summary(exp_dir, all_metrics)
    write_run_summary(exp_dir, run_summary)

    print("[main_sim] done. Wrote:")
    print(f"  - {Path(exp_dir) / 'metrics_summary.json'}")
    print(f"  - {Path(exp_dir) / 'run_summary.json'}")
    for m in sim_cfg.methods:
        print(f"  - {Path(exp_dir) / f'logs_{m}.json'}")
