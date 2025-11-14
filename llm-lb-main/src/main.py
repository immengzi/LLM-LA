#!/usr/bin/env python
# -*- coding: utf-8 -*-
# main.py
import os
import click
import json
import itertools
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
        ["legacy", "target-output", "target-total", "dist-output", "replay-output"],
        case_sensitive=False,
    ),
    default=None,
    show_default=False,
)
@click.option("--target-output", type=int, default=None, show_default=False)
@click.option("--target-total", type=int, default=None, show_default=False)
@click.option("--ignore-eos/--no-ignore-eos", default=None)
@click.option(
    "--prompts-limit",
    type=int,
    default=None,
    show_default=False,
    help="Max prompts to use. For PROMPTS_SOURCE=file this slices the local list. "
         "For PROMPTS_SOURCE=hf-lmsys this is passed as the on-the-fly cap.",
)
@click.option(
    "--predictor-name",
    type=click.Choice(["none", "oracle"], case_sensitive=False),
    default=None,
    help="Optional: choose token length predictor (none|oracle).",
    show_default=False,
)
# ---- HF live dataset knobs (backward compatible; defaults still 'file') ----
@click.option(
    "--prompts-source",
    type=click.Choice(["file", "hf-lmsys"], case_sensitive=False),
    default=None,
    help='Prompt source. If omitted or "file", loads local prompts JSON. '
         'If "hf-lmsys", pulls prompts on-the-fly from LMSYS.',
    show_default=False,
)
@click.option("--hf-name", type=str, default=None, help='HF dataset name (e.g. "lmsys/lmsys-chat-1m").')
@click.option("--hf-split", type=str, default=None, help='HF dataset split (e.g. "train").')
@click.option("--hf-tokenizer", type=str, default=None, help='Tokenizer used to estimate reply length (e.g. "gpt2").')
@click.option("--hf-streaming/--no-hf-streaming", default=None, help="Use HF streaming loader (config default).")
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
    # NEW:
    prompts_source: str,
    hf_name: str,
    hf_split: str,
    hf_tokenizer: str,
    hf_streaming: bool,
):
    if not load_kube():
        return

    cfg = load_config(config_path)

    # apply CLI overrides (existing)
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

    # ---- HF loader overrides (only applied if provided) ----
    if prompts_source is not None:
        cfg.PROMPTS_SOURCE = prompts_source  # "file" or "hf-lmsys"
    if hf_name is not None:
        cfg.HF_DATASET_NAME = hf_name
    if hf_split is not None:
        cfg.HF_DATASET_SPLIT = hf_split
    if hf_tokenizer is not None:
        cfg.HF_TOKENIZER_NAME = hf_tokenizer
    if hf_streaming is not None:
        cfg.HF_STREAMING = bool(hf_streaming)

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
    source_kind = str(getattr(cfg, "PROMPTS_SOURCE", "file")).lower()
    try:
        if source_kind == "hf-lmsys":
            # On-the-fly HF loader (first user turn of LMSYS convos) — LAZY
            from utils_lmsys_loader import iter_lmsys_pairs  # helper module

            ds_name   = getattr(cfg, "HF_DATASET_NAME", "lmsys/lmsys-chat-1m")
            ds_split  = getattr(cfg, "HF_DATASET_SPLIT", "train")
            tok_name  = getattr(cfg, "HF_TOKENIZER_NAME", "gpt2")
            streaming = bool(getattr(cfg, "HF_STREAMING", False))
            limit     = getattr(cfg, "PROMPTS_LIMIT", None)
            max_n     = int(limit) if limit is not None else None  # None => unbounded

            # Build a generator of (prompt, out_len) — do NOT materialize
            def _stream_prompts():
                it = iter_lmsys_pairs(
                    dataset_name=ds_name,
                    split=ds_split,
                    tokenizer_name=tok_name,
                    max_n=max_n if max_n is not None else 10**12,  # guard upper-bound
                    streaming=streaming,
                )
                for utext, out_len in it:
                    yield (utext, int(out_len))

            prompts = _stream_prompts() if max_n is None else itertools.islice(_stream_prompts(), max_n)
            # Note: leave as iterator; loadgen & router_modes handle iterables now.
            print(
                f"[PROMPTS/HF] Streaming prompts from {ds_name}:{ds_split} "
                f"(limit={limit}, streaming={streaming})"
            )
        else:
            # Original file-based path (kept as before; this is finite so deque is fine)
            path = DEFAULT_PROMPTS_FILE
            prompts = load_prompts(path)
            limit = getattr(cfg, "PROMPTS_LIMIT", None)
            if limit is not None:
                prompts = deque(islice(prompts, int(limit)))
            print(
                f"[PROMPTS] Loaded {len(prompts)} prompts from {path} (limit={limit})"
            )
    except Exception as e:
        print(f"[ERROR] Failed to load prompts ({source_kind}): {e}")
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
