#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import click
import json
from collections import deque
from itertools import islice

from kubernetes import client

from utils_k8s import load_kube
from config import load_config, set_config, dump_config_dict

# shim for discovery in SIM_MODE=only
from sim_backend_http_shim import (
    configure as sim_http_configure,
    endpoints as sim_http_endpoints,
)


@click.command()
@click.option(
    "--mode",
    type=click.Choice(
        [
            "rr-batching",
            "random-batching",
            "least-queue-batching",
            "pull-batching",
        ],
        case_sensitive=False,
    ),
    default="rr-batching",
    show_default=True,
)
@click.option("--prompts-file", type=str, default="mix", show_default=False)
@click.option(
    "--metrics-interval",
    type=float,
    default=float(os.getenv("METRICS_LOG_INTERVAL", "1.0")),
    show_default=True,
)
@click.option(
    "--config",
    "config_path",
    type=str,
    default="real_distro",
    help="Path to experiment config.(yaml|yml|json)",
)
@click.option(
    "--length-mode",
    type=click.Choice(
        ["legacy", "target-output", "target-total", "dist-output"], case_sensitive=False
    ),
    default=None,
    show_default=False,
)
@click.option("--target-output", type=int, default=None, show_default=False)
@click.option("--target-total", type=int, default=None, show_default=False)
@click.option("--ignore-eos/--no-ignore-eos", default=None)
@click.option("--prompts-limit", type=int, default=None, show_default=False)
@click.option(
    "--predictor-name",
    type=click.Choice(["none", "oracle"], case_sensitive=False),
    default=None,
    help="Optional: choose token length predictor (none|oracle).",
    show_default=False,
)
def cli(
    mode: str,
    prompts_file: str,
    metrics_interval: float,
    config_path: str,
    length_mode: str,
    target_output: int,
    target_total: int,
    ignore_eos: bool,
    prompts_limit: int,
    predictor_name: str,
):
    if not load_kube():
        return

    cfg = load_config(config_path)

    # apply CLI overrides
    if prompts_file:
        cfg.PROMPTS_FILE = prompts_file
    if prompts_limit is not None:
        cfg.PROMPTS_LIMIT = int(prompts_limit)
    if length_mode is not None:
        cfg.LENGTH_MODE = length_mode
    if target_output is not None:
        cfg.TARGET_OUTPUT_TOKENS = int(target_output)
    if target_total is not None:
        cfg.TARGET_TOTAL_TOKENS = int(target_total)
    if ignore_eos is not None:
        cfg.IGNORE_EOS = bool(ignore_eos)
    if predictor_name is not None:
        cfg.PREDICTOR_NAME = predictor_name

    set_config(cfg)

    # Lazy imports
    from utils import load_prompts, DEFAULT_PROMPTS_FILE
    import router_modes  # uses discover_endpoints internally

    # SIM shim
    if str(cfg.SIM_MODE).lower() == "only":
        sim_http_configure(config_path)

        def _shim_discover_endpoints(_core, _ns, _label, _port):
            return sim_http_endpoints()

        router_modes.discover_endpoints = _shim_discover_endpoints

    # Load prompts
    path = DEFAULT_PROMPTS_FILE
    try:
        prompts = load_prompts(path)
        if cfg.PROMPTS_LIMIT is not None:
            prompts = deque(islice(prompts, int(cfg.PROMPTS_LIMIT)))
        print(
            f"[PROMPTS] Loaded {len(prompts)} prompts from {path} (limit={cfg.PROMPTS_LIMIT})"
        )
    except Exception as e:
        print(f"[ERROR] Failed to load prompts: {e}")
        return

    print("[CONFIG] Effective config:\n" + json.dumps(dump_config_dict(), indent=2))

    core = client.CoreV1Api()
    mode = mode.lower().strip()

    if mode == "rr-batching":
        router_modes.run_rr_batching(core, prompts, metrics_interval)
    elif mode == "random-batching":
        router_modes.run_random_batching(core, prompts, metrics_interval)
    elif mode == "least-queue-batching":
        router_modes.run_least_queue_batching(core, prompts, metrics_interval)
    elif mode == "pull-batching":
        router_modes.run_pull_batching(core, prompts, metrics_interval)


if __name__ == "__main__":
    cli()
