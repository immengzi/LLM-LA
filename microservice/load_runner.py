# load_runner.py
# Per-request load runner:
# - Optional request-based warmup before the main load.
# - Router backend:
#     - sync: thread-per-request, POST /enqueue
#     - async_pubsub: single submit scheduler + single ZMQ SUB listener
# - AIBrix backend:
#     - threaded_http: thread-per-request, OpenAI-compatible HTTP calls to AIBrix gateway
# - LiteLLM backend:
#     - threaded_http: thread-per-request, OpenAI-compatible HTTP calls to LiteLLM proxy
#       (production auth/spend validation only — NOT for benchmarking)
# - BooM Gateway backend:
#     - identical to LiteLLM (same protocol), reuses LiteLLM worker with label="BooM"
#
# Termination policy for router async_pubsub (preferred + backstop):
#   1) Preferred: Prometheus fleet-idle detection:
#        - If vLLM reports requests_running==0 (and requests_waiting==0 when available)
#          continuously for idle_zero_running_s seconds => stop early (mark remaining as LOST).
#   2) Backstop: idle_timeout_s since last EMITTED completion event => stop (mark remaining LOST).
#
# No reconciliation polling / retry logic.

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple
import threading
import time
import json

import requests

from http_client import send_one, submit_one, send_one_aibrix, send_one_litellm
from config import (
    GenerationConfig,
    AIBrixConfig,
    LiteLLMConfig,
    BooMConfig,
    SLOConfig,
    generation_effective_ignore_eos,
)
from trace_utils import print_trace_block, compute_trace_metrics
from experiment_io import ExperimentLogger

# TransportConfig is new; keep import backward-friendly.
try:
    from config import TransportConfig
except Exception:  # pragma: no cover
    TransportConfig = None  # type: ignore

# Metrics tick accessor (optional at runtime if metrics disabled)
try:
    from metrics_prom import get_last_metrics_tick
except Exception:  # pragma: no cover
    get_last_metrics_tick = None  # type: ignore


@dataclass
class RequestTask:
    idx: int
    prompt: str
    ts_mono: float  # absolute monotonic timestamp for sending
    output_tokens: Optional[int] = None  # per-request output len from dataset


# Shared state for per-second send statistics
_sec_lock = threading.Lock()
_sec_counts: Dict[int, int] = {}


def _extract_result_fields(
    result: Optional[Dict[str, Any]],
) -> Tuple[
    Optional[float],  # latency_s
    Optional[str],  # finish_reason
    Optional[str],  # output_preview
    Optional[str],  # output_full
    Optional[int],  # usage_prompt_tokens
    Optional[int],  # usage_completion_tokens
    Optional[int],  # usage_total_tokens
    Optional[Dict[str, Any]],  # trace_dict
    Optional[Dict[str, float]],  # trace_metrics
]:
    latency_s: Optional[float] = None
    finish_reason: Optional[str] = None
    output_preview: Optional[str] = None
    output_full: Optional[str] = None

    usage_prompt_tokens: Optional[int] = None
    usage_completion_tokens: Optional[int] = None
    usage_total_tokens: Optional[int] = None

    trace_dict: Optional[Dict[str, Any]] = None
    trace_metrics: Optional[Dict[str, float]] = None

    if isinstance(result, dict):
        # --- main fields ---
        if isinstance(result.get("latency_s"), (int, float)):
            latency_s = float(result["latency_s"])
        if isinstance(result.get("finish_reason"), str):
            finish_reason = result["finish_reason"]
        if isinstance(result.get("output"), str):
            output_full = result["output"]
            output_preview = output_full.replace("\n", " ")
            if len(output_preview) > 120:
                output_preview = output_preview[:117] + "..."

        # --- usage (tokens) ---
        usage_dict: Optional[Dict[str, Any]] = None
        u_top = result.get("usage")
        if isinstance(u_top, dict):
            usage_dict = u_top
        else:
            raw = result.get("raw")
            if isinstance(raw, dict):
                u_raw = raw.get("usage")
                if isinstance(u_raw, dict):
                    usage_dict = u_raw

        if isinstance(usage_dict, dict):
            pt = usage_dict.get("prompt_tokens")
            ct = usage_dict.get("completion_tokens")
            tt = usage_dict.get("total_tokens")
            try:
                if pt is not None:
                    usage_prompt_tokens = int(pt)
            except Exception:
                pass
            try:
                if ct is not None:
                    usage_completion_tokens = int(ct)
            except Exception:
                pass
            try:
                if tt is not None:
                    usage_total_tokens = int(tt)
            except Exception:
                pass

        # --- trace fields ---
        trace = result.get("trace")
        if isinstance(trace, dict):
            trace_dict = trace
            trace_metrics = compute_trace_metrics(trace)

    return (
        latency_s,
        finish_reason,
        output_preview,
        output_full,
        usage_prompt_tokens,
        usage_completion_tokens,
        usage_total_tokens,
        trace_dict,
        trace_metrics,
    )


def _choose_log_output(
    *,
    output_log_mode: str,
    output_full: Optional[str],
    output_preview: Optional[str],
) -> Optional[str]:
    mode = output_log_mode or "preview"
    if mode == "full":
        if output_full is not None:
            return output_full
        return output_preview
    if output_preview is not None:
        return output_preview
    return output_full


def _emit_completion_from_result(
    *,
    rid: str,
    result: Any,
    info: Dict[str, Any],
    done_counter: Dict[str, int],
    output_log_mode: str,
    print_trace: bool,
):
    """
    Emit stdout + optional logger record, using the same schema as sync mode.
    Shared by:
      - pubsub listener (normal)
      - async submitter loop (race-fix path: orphan result arrived before pending)
    """
    t0 = float(info["t0_wall"])
    t1 = time.time()
    end_to_end_s = t1 - t0

    (
        latency_s,
        finish_reason,
        output_preview,
        output_full,
        usage_prompt_tokens,
        usage_completion_tokens,
        usage_total_tokens,
        trace_dict,
        trace_metrics,
    ) = _extract_result_fields(result if isinstance(result, dict) else None)

    idx = int(info["idx"])

    if latency_s is not None:
        print(
            f"[client][RECV][T{idx}] idx={idx} req_id={rid} "
            f"wait_wall={end_to_end_s:.3f}s model_latency={latency_s:.3f}s"
        )
    else:
        print(
            f"[client][RECV][T{idx}] idx={idx} req_id={rid} "
            f"wait_wall={end_to_end_s:.3f}s"
        )

    if finish_reason is not None:
        print(f"[client][T{idx}]   finish_reason={finish_reason}")

    if print_trace and output_preview is not None:
        print(f"[client][T{idx}]   output_preview={output_preview!r}")

    if print_trace and isinstance(result, dict):
        tr = result.get("trace")
        if isinstance(tr, dict):
            print_trace_block(idx, {"req_id": rid, "result": result})

    logger: Optional[ExperimentLogger] = info.get("logger")
    if logger is not None:
        record: Dict[str, Any] = {
            "idx": idx,
            "req_id": rid,
            "prompt": info.get("prompt"),
            "planned_ts_mono": info.get("planned_ts_mono"),
            "actual_send_ts_mono": info.get("actual_send_ts_mono"),
            "t0_wall": t0,
            "t1_wall": t1,
            "end_to_end_s": end_to_end_s,
            "model_latency_s": latency_s,
            "finish_reason": finish_reason,
        }

        if usage_prompt_tokens is not None:
            record["prompt_tokens"] = usage_prompt_tokens
        if usage_completion_tokens is not None:
            record["completion_tokens"] = usage_completion_tokens
        if usage_total_tokens is not None:
            record["total_tokens"] = usage_total_tokens

        log_output = _choose_log_output(
            output_log_mode=output_log_mode,
            output_full=output_full,
            output_preview=output_preview,
        )
        if log_output is not None:
            record["output"] = log_output

        if trace_dict is not None:
            record["trace"] = trace_dict
        if trace_metrics is not None:
            record["trace_metrics"] = trace_metrics

        logger.log_request(record)

    done_counter["done"] = int(done_counter.get("done", 0)) + 1


def _note_last_recv(progress: Dict[str, float], progress_lock: threading.Lock) -> None:
    now = time.time()
    with progress_lock:
        progress["last_recv_wall"] = now


def _replace_gen_cfg(gen_cfg: GenerationConfig, output_tokens: int) -> GenerationConfig:
    from dataclasses import replace
    return replace(gen_cfg, max_tokens=output_tokens, min_tokens=output_tokens)


def _build_generation_meta(
    gen_cfg: GenerationConfig,
    output_tokens_override: Optional[int] = None,
) -> Dict[str, Any]:
    max_tok = int(gen_cfg.max_tokens)
    min_tok = int(gen_cfg.min_tokens) if gen_cfg.min_tokens is not None else None

    if output_tokens_override is not None:
        max_tok = output_tokens_override
        min_tok = output_tokens_override

    meta = {
        "max_tokens": max_tok,
        "temperature": float(gen_cfg.temperature),
        "length_mode": gen_cfg.length_mode,
        "enable_thinking": bool(gen_cfg.think),
    }
    if min_tok is not None:
        meta["min_tokens"] = min_tok
    if gen_cfg.target_output_tokens is not None:
        meta["target_output_tokens"] = int(gen_cfg.target_output_tokens)
    if gen_cfg.target_total_tokens is not None:
        meta["target_total_tokens"] = int(gen_cfg.target_total_tokens)
    if generation_effective_ignore_eos(gen_cfg):
        meta["ignore_eos"] = True
    return meta


import random as _random

_slo_rng = _random.Random(42)


def _build_slo_fields(slo_cfg: Optional[SLOConfig], idx: int) -> Optional[Dict[str, Any]]:
    """
    Resolve per-request SLO annotations based on the mix distribution.
    Returns None when SLO is disabled or the request falls in the no-SLO remainder.
    """
    if slo_cfg is None or not slo_cfg.enabled:
        return None
    mix = slo_cfg.mix
    if not mix:
        return None

    roll = _slo_rng.random()
    cumulative = 0.0
    for entry in mix:
        if not isinstance(entry, dict):
            continue
        frac = float(entry.get("fraction", 0))
        cumulative += frac
        if roll < cumulative:
            fields: Dict[str, Any] = {}
            slo_type = entry.get("slo_type", slo_cfg.default_slo_type)
            fields["slo_type"] = slo_type
            if entry.get("slo_ttft_ms") is not None:
                fields["slo_ttft_ms"] = float(entry["slo_ttft_ms"])
            if entry.get("slo_tpot_ms") is not None:
                fields["slo_tpot_ms"] = float(entry["slo_tpot_ms"])
            if entry.get("slo_e2e_ms") is not None:
                fields["slo_e2e_ms"] = float(entry["slo_e2e_ms"])
            if entry.get("task_type") is not None:
                fields["task_type"] = str(entry["task_type"])
            if entry.get("output_len_hint") is not None:
                fields["output_len_hint"] = int(entry["output_len_hint"])
            return fields
    return None


def _log_send_tick(now_send: float, t0_mono: float) -> None:
    rel = now_send - t0_mono
    sec = int(rel)
    with _sec_lock:
        _sec_counts[sec] = _sec_counts.get(sec, 0) + 1
        count_in_sec = _sec_counts[sec]
    print(f"[load_runner][SEC] t=[{sec},{sec+1}) sent_so_far_in_sec={count_in_sec}")


def _request_thread_router_sync(
    task: RequestTask,
    router_url: str,
    gen_cfg: GenerationConfig,
    t0_mono: float,
    logger: Optional[ExperimentLogger] = None,
    output_log_mode: str = "preview",
    print_trace: bool = True,
    slo_cfg: Optional[SLOConfig] = None,
):
    """
    Router sync worker: blocks on /enqueue.
    """
    session = requests.Session()
    try:
        now = time.monotonic()
        delay = task.ts_mono - now
        if delay > 0:
            time.sleep(delay)

        now_send = time.monotonic()
        _log_send_tick(now_send, t0_mono)

        meta = _build_generation_meta(gen_cfg, output_tokens_override=task.output_tokens)
        slo_fields = _build_slo_fields(slo_cfg, task.idx)

        print(f"[client][SEND][T{task.idx}] idx={task.idx} planned_ts={task.ts_mono:.6f}")

        t0 = time.time()
        try:
            rid, result = send_one(session, router_url, task.prompt, meta, slo_fields=slo_fields)
            t1 = time.time()
            end_to_end_s = t1 - t0

            (
                latency_s,
                finish_reason,
                output_preview,
                output_full,
                usage_prompt_tokens,
                usage_completion_tokens,
                usage_total_tokens,
                trace_dict,
                trace_metrics,
            ) = _extract_result_fields(result)

            if latency_s is not None:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={end_to_end_s:.3f}s model_latency={latency_s:.3f}s"
                )
            else:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={end_to_end_s:.3f}s"
                )

            if finish_reason is not None:
                print(f"[client][T{task.idx}]   finish_reason={finish_reason}")

            if print_trace and output_preview is not None:
                print(f"[client][T{task.idx}]   output_preview={output_preview!r}")

            if isinstance(result, dict):
                trace = result.get("trace")
                if isinstance(trace, dict) and print_trace:
                    print_trace_block(task.idx, {"req_id": rid, "result": result})

            if logger is not None:
                record: Dict[str, Any] = {
                    "idx": task.idx,
                    "req_id": rid,
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "actual_send_ts_mono": now_send,
                    "t0_wall": t0,
                    "t1_wall": t1,
                    "end_to_end_s": end_to_end_s,
                    "model_latency_s": latency_s,
                    "finish_reason": finish_reason,
                }

                if usage_prompt_tokens is not None:
                    record["prompt_tokens"] = usage_prompt_tokens
                if usage_completion_tokens is not None:
                    record["completion_tokens"] = usage_completion_tokens
                if usage_total_tokens is not None:
                    record["total_tokens"] = usage_total_tokens

                log_output = _choose_log_output(
                    output_log_mode=output_log_mode,
                    output_full=output_full,
                    output_preview=output_preview,
                )
                if log_output is not None:
                    record["output"] = log_output

                if trace_dict is not None:
                    record["trace"] = trace_dict
                if trace_metrics is not None:
                    record["trace_metrics"] = trace_metrics

                logger.log_request(record)

        except Exception as e:
            print(f"[client][RECV][T{task.idx}] ✗ ERROR idx={task.idx}: {e}")
            if logger is not None:
                err_record: Dict[str, Any] = {
                    "idx": task.idx,
                    "error": str(e),
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "send_failed": True,
                }
                logger.log_request(err_record)
    finally:
        session.close()


def _request_thread_aibrix_http(
    task: RequestTask,
    aibrix_cfg: AIBrixConfig,
    gen_cfg: GenerationConfig,
    t0_mono: float,
    logger: Optional[ExperimentLogger] = None,
    output_log_mode: str = "preview",
    print_trace: bool = True,
):
    """
    AIBrix worker: one open HTTP request per thread.
    The connection stays open until the AIBrix/vLLM response completes.
    """
    session = requests.Session()
    session.headers.update({"Connection": "close"})
    try:
        now = time.monotonic()
        delay = task.ts_mono - now
        if delay > 0:
            time.sleep(delay)

        now_send = time.monotonic()
        _log_send_tick(now_send, t0_mono)

        print(f"[client][SEND][T{task.idx}] idx={task.idx} planned_ts={task.ts_mono:.6f}")

        t0 = time.time()
        try:
            rid, result = send_one_aibrix(session, aibrix_cfg, task.prompt, gen_cfg)
            t1 = time.time()
            end_to_end_s = t1 - t0

            (
                latency_s,
                finish_reason,
                output_preview,
                output_full,
                usage_prompt_tokens,
                usage_completion_tokens,
                usage_total_tokens,
                trace_dict,
                trace_metrics,
            ) = _extract_result_fields(result)

            if latency_s is not None:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={end_to_end_s:.3f}s model_latency={latency_s:.3f}s"
                )
            else:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={end_to_end_s:.3f}s"
                )

            if finish_reason is not None:
                print(f"[client][T{task.idx}]   finish_reason={finish_reason}")

            if print_trace and output_preview is not None:
                print(f"[client][T{task.idx}]   output_preview={output_preview!r}")

            if isinstance(result, dict):
                trace = result.get("trace")
                if isinstance(trace, dict) and print_trace:
                    print_trace_block(task.idx, {"req_id": rid, "result": result})

            if logger is not None:
                record: Dict[str, Any] = {
                    "idx": task.idx,
                    "req_id": rid,
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "actual_send_ts_mono": now_send,
                    "t0_wall": t0,
                    "t1_wall": t1,
                    "end_to_end_s": end_to_end_s,
                    "model_latency_s": latency_s,
                    "finish_reason": finish_reason,
                }

                if usage_prompt_tokens is not None:
                    record["prompt_tokens"] = usage_prompt_tokens
                if usage_completion_tokens is not None:
                    record["completion_tokens"] = usage_completion_tokens
                if usage_total_tokens is not None:
                    record["total_tokens"] = usage_total_tokens

                log_output = _choose_log_output(
                    output_log_mode=output_log_mode,
                    output_full=output_full,
                    output_preview=output_preview,
                )
                if log_output is not None:
                    record["output"] = log_output

                if trace_dict is not None:
                    record["trace"] = trace_dict
                if trace_metrics is not None:
                    record["trace_metrics"] = trace_metrics

                logger.log_request(record)

        except Exception as e:
            print(f"[client][RECV][T{task.idx}] ✗ ERROR idx={task.idx}: {e}")
            if logger is not None:
                err_record: Dict[str, Any] = {
                    "idx": task.idx,
                    "error": str(e),
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "send_failed": True,
                }
                logger.log_request(err_record)
    finally:
        session.close()


# ============================================================
# OpenAI-compatible proxy worker thread (LiteLLM / BooM Gateway)
#
# Mirrors _request_thread_aibrix_http exactly — same threading
# model, same logging schema, same result parsing. Shared by
# both backend="litellm" and backend="boom" via the label param.
#
# Connection: "close" per thread — same rationale as AIBrix
# (single request per thread, no benefit from keep-alive).
# ============================================================

def _request_thread_litellm_http(
    task: RequestTask,
    litellm_cfg: LiteLLMConfig,
    gen_cfg: GenerationConfig,
    t0_mono: float,
    logger: Optional[ExperimentLogger] = None,
    output_log_mode: str = "preview",
    print_trace: bool = True,
    label: str = "LiteLLM",
):
    """
    OpenAI-compatible proxy worker: one open HTTP request per thread.
    Used by both LiteLLM and BooM Gateway backends (same wire protocol).
    NOT for benchmarking — use backend=router for clean measurements.
    """
    session = requests.Session()
    session.headers.update({"Connection": "close"})
    try:
        now = time.monotonic()
        delay = task.ts_mono - now
        if delay > 0:
            time.sleep(delay)

        now_send = time.monotonic()
        _log_send_tick(now_send, t0_mono)

        print(f"[client][SEND][T{task.idx}] idx={task.idx} planned_ts={task.ts_mono:.6f}")

        t0 = time.time()
        try:
            effective_gen = gen_cfg
            if task.output_tokens is not None:
                effective_gen = _replace_gen_cfg(gen_cfg, task.output_tokens)
            rid, result = send_one_litellm(session, litellm_cfg, task.prompt, effective_gen, label=label)
            t1 = time.time()
            end_to_end_s = t1 - t0

            (
                latency_s,
                finish_reason,
                output_preview,
                output_full,
                usage_prompt_tokens,
                usage_completion_tokens,
                usage_total_tokens,
                trace_dict,
                trace_metrics,
            ) = _extract_result_fields(result)

            if latency_s is not None:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={end_to_end_s:.3f}s model_latency={latency_s:.3f}s"
                )
            else:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={end_to_end_s:.3f}s"
                )

            if finish_reason is not None:
                print(f"[client][T{task.idx}]   finish_reason={finish_reason}")

            if print_trace and output_preview is not None:
                print(f"[client][T{task.idx}]   output_preview={output_preview!r}")

            if isinstance(result, dict):
                trace = result.get("trace")
                if isinstance(trace, dict) and print_trace:
                    print_trace_block(task.idx, {"req_id": rid, "result": result})

            if logger is not None:
                record: Dict[str, Any] = {
                    "idx": task.idx,
                    "req_id": rid,
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "actual_send_ts_mono": now_send,
                    "t0_wall": t0,
                    "t1_wall": t1,
                    "end_to_end_s": end_to_end_s,
                    "model_latency_s": latency_s,
                    "finish_reason": finish_reason,
                }

                if usage_prompt_tokens is not None:
                    record["prompt_tokens"] = usage_prompt_tokens
                if usage_completion_tokens is not None:
                    record["completion_tokens"] = usage_completion_tokens
                if usage_total_tokens is not None:
                    record["total_tokens"] = usage_total_tokens

                log_output = _choose_log_output(
                    output_log_mode=output_log_mode,
                    output_full=output_full,
                    output_preview=output_preview,
                )
                if log_output is not None:
                    record["output"] = log_output

                if trace_dict is not None:
                    record["trace"] = trace_dict
                if trace_metrics is not None:
                    record["trace_metrics"] = trace_metrics

                logger.log_request(record)

        except Exception as e:
            print(f"[client][RECV][T{task.idx}] ✗ ERROR idx={task.idx}: {e}")
            if logger is not None:
                err_record: Dict[str, Any] = {
                    "idx": task.idx,
                    "error": str(e),
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "send_failed": True,
                }
                logger.log_request(err_record)
    finally:
        session.close()


# ============================================================
# Anthropic Messages API worker thread (boom-claude backend)
#
# Sends Anthropic-format /v1/messages requests to BooM Gateway.
# Same threading model as LiteLLM/AIBrix but uses Anthropic wire
# format (x-api-key header, content blocks, stop_reason).
# ============================================================

def _request_thread_anthropic_http(
    task: RequestTask,
    boom_cfg: BooMConfig,
    gen_cfg: GenerationConfig,
    t0_mono: float,
    logger: Optional[ExperimentLogger] = None,
    output_log_mode: str = "preview",
    print_trace: bool = True,
):
    """
    BooM Claude worker: Anthropic /v1/messages per thread.
    Validates the exact wire path a live Claude Code agent uses.
    """
    session = requests.Session()
    session.headers.update({"Connection": "close"})
    try:
        now = time.monotonic()
        delay = task.ts_mono - now
        if delay > 0:
            time.sleep(delay)

        now_send = time.monotonic()
        _log_send_tick(now_send, t0_mono)

        print(f"[client][SEND][T{task.idx}] idx={task.idx} planned_ts={task.ts_mono:.6f}")

        t0 = time.time()
        try:
            rid, result = send_one_anthropic(session, boom_cfg, task.prompt, gen_cfg)
            t1 = time.time()
            end_to_end_s = t1 - t0

            (
                latency_s,
                finish_reason,
                output_preview,
                output_full,
                usage_prompt_tokens,
                usage_completion_tokens,
                usage_total_tokens,
                trace_dict,
                trace_metrics,
            ) = _extract_result_fields(result)

            if latency_s is not None:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={end_to_end_s:.3f}s model_latency={latency_s:.3f}s"
                )
            else:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={end_to_end_s:.3f}s"
                )

            if finish_reason is not None:
                print(f"[client][T{task.idx}]   finish_reason={finish_reason}")

            if print_trace and output_preview is not None:
                print(f"[client][T{task.idx}]   output_preview={output_preview!r}")

            if isinstance(result, dict):
                trace = result.get("trace")
                if isinstance(trace, dict) and print_trace:
                    print_trace_block(task.idx, {"req_id": rid, "result": result})

            if logger is not None:
                record: Dict[str, Any] = {
                    "idx": task.idx,
                    "req_id": rid,
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "actual_send_ts_mono": now_send,
                    "t0_wall": t0,
                    "t1_wall": t1,
                    "end_to_end_s": end_to_end_s,
                    "model_latency_s": latency_s,
                    "finish_reason": finish_reason,
                }

                if usage_prompt_tokens is not None:
                    record["prompt_tokens"] = usage_prompt_tokens
                if usage_completion_tokens is not None:
                    record["completion_tokens"] = usage_completion_tokens
                if usage_total_tokens is not None:
                    record["total_tokens"] = usage_total_tokens

                log_output = _choose_log_output(
                    output_log_mode=output_log_mode,
                    output_full=output_full,
                    output_preview=output_preview,
                )
                if log_output is not None:
                    record["output"] = log_output

                if trace_dict is not None:
                    record["trace"] = trace_dict
                if trace_metrics is not None:
                    record["trace_metrics"] = trace_metrics

                logger.log_request(record)

        except Exception as e:
            print(f"[client][RECV][T{task.idx}] ✗ ERROR idx={task.idx}: {e}")
            if logger is not None:
                err_record: Dict[str, Any] = {
                    "idx": task.idx,
                    "error": str(e),
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "send_failed": True,
                }
                logger.log_request(err_record)
    finally:
        session.close()


def _pubsub_listener_thread(
    *,
    results_zmq: str,
    topic: str,
    run_id: Optional[str],
    pending: Dict[str, Dict[str, Any]],
    pending_lock: threading.Lock,
    orphans: Dict[str, Dict[str, Any]],
    orphans_lock: threading.Lock,
    orphan_ttl_s: float,
    orphan_max: int,
    done_counter: Dict[str, int],
    done_evt: threading.Event,
    progress: Dict[str, float],
    progress_lock: threading.Lock,
    output_log_mode: str,
    print_trace: bool,
):
    """
    Single SUB connection that receives completion events.

    Race to fix:
      Result may arrive on SUB before the submitter loop has inserted pending[rid].
      We stash it in `orphans` and let the submitter consume it immediately.

    Expected wire format:
      - multipart: [topic, json_bytes] OR single-frame json_bytes
      - payload json includes at least: {"req_id": "...", "result": {...}}
      - optionally includes "run_id" (we filter if provided)
    """
    try:
        import zmq  # type: ignore
    except Exception as e:
        print(f"[client][pubsub] ✗ pyzmq not available: {e}")
        return

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    try:
        sock.connect(results_zmq)

        if topic:
            sock.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
        else:
            sock.setsockopt(zmq.SUBSCRIBE, b"")

        while not done_evt.is_set():
            try:
                if sock.poll(timeout=200) == 0:
                    continue
                msg = sock.recv_multipart(flags=0)
            except Exception:
                continue

            payload_bytes: Optional[bytes] = None
            if isinstance(msg, list) and len(msg) >= 2:
                payload_bytes = msg[1]
            elif isinstance(msg, list) and len(msg) == 1:
                payload_bytes = msg[0]

            if not payload_bytes:
                continue

            try:
                data = json.loads(payload_bytes.decode("utf-8"))
            except Exception:
                continue

            if run_id is not None:
                if data.get("run_id") != run_id:
                    continue

            rid = data.get("req_id")
            result = data.get("result")

            if not rid or not isinstance(rid, str):
                continue

            with pending_lock:
                info = pending.pop(rid, None)

            if info is None:
                now = time.time()
                with orphans_lock:
                    if len(orphans) > 0 and (len(orphans) % 256 == 0):
                        dead = [
                            k
                            for k, v in orphans.items()
                            if (now - float(v.get("t_recv_wall", 0.0))) > float(orphan_ttl_s)
                        ]
                        for k in dead:
                            orphans.pop(k, None)

                    if len(orphans) >= int(orphan_max):
                        dead = [
                            k
                            for k, v in orphans.items()
                            if (now - float(v.get("t_recv_wall", 0.0))) > float(orphan_ttl_s)
                        ]
                        for k in dead:
                            orphans.pop(k, None)
                        if len(orphans) >= int(orphan_max):
                            try:
                                orphans.pop(next(iter(orphans.keys())), None)
                            except Exception:
                                pass

                    orphans[rid] = {"t_recv_wall": now, "result": result}
                continue

            _emit_completion_from_result(
                rid=rid,
                result=result,
                info=info,
                done_counter=done_counter,
                output_log_mode=output_log_mode,
                print_trace=print_trace,
            )
            _note_last_recv(progress, progress_lock)

    finally:
        try:
            sock.close(0)
        except Exception:
            pass


def _drain_threads_with_fleet_idle(
    threads: List[threading.Thread],
    total: int,
    t0_wall: float,
    idle_zero_running_s: float,
    idle_timeout_s: float,
    logger: Optional[ExperimentLogger] = None,
    backend_label: str = "boom",
) -> None:
    """
    Wait for all request threads to finish, but abort early when the vLLM
    fleet is idle (requests_running==0) for idle_zero_running_s — meaning
    there are no inflight requests on the engine and any remaining threads
    must be stuck on a lost response (BooM 502, connection hang, etc.).

    Also applies a backstop idle_timeout_s after the last thread completed.

    This replicates the fleet-idle safeguard from the async_pubsub drain loop
    for backends where each request is a blocking HTTP call in its own thread
    (BooM, LiteLLM, router-sync, aibrix).
    """
    alive_at_start = sum(1 for t in threads if t.is_alive())
    completed = total - alive_at_start
    zero_run_start_wall: Optional[float] = None
    last_fleet_debug: Dict[str, Any] = {}
    last_completion_wall = time.time()
    next_progress_print = time.time() + 5.0

    can_fleet_idle = (
        idle_zero_running_s > 0
        and get_last_metrics_tick is not None
    )

    if can_fleet_idle:
        print(
            f"[load_runner] {backend_label}: fleet-idle safeguard enabled "
            f"(idle_zero_running_s={idle_zero_running_s}s)"
        )
    else:
        if idle_zero_running_s > 0 and get_last_metrics_tick is None:
            print(
                f"[load_runner] {backend_label}: WARNING: metrics_prom.get_last_metrics_tick "
                "not available; fleet-idle safeguard disabled"
            )

    timed_out = False
    timeout_reason: Optional[str] = None

    while True:
        still_alive = [t for t in threads if t.is_alive()]
        new_completed = total - len(still_alive)
        if new_completed > completed:
            last_completion_wall = time.time()
            completed = new_completed
        if not still_alive:
            break

        now = time.time()

        fleet_idle_ok: Optional[bool] = None
        if can_fleet_idle:
            tick = get_last_metrics_tick()
            fleet_idle_ok, last_fleet_debug = _fleet_idle_from_metrics_tick(tick)

            if fleet_idle_ok is True:
                if zero_run_start_wall is None:
                    zero_run_start_wall = now
                elif (now - zero_run_start_wall) >= float(idle_zero_running_s):
                    timed_out = True
                    timeout_reason = (
                        f"fleet_idle (requests_running==0 for "
                        f"{idle_zero_running_s}s while {len(still_alive)} threads stuck)"
                    )
                    break
            else:
                zero_run_start_wall = None

        idle_since_last = now - last_completion_wall
        if idle_since_last > float(idle_timeout_s):
            timed_out = True
            timeout_reason = (
                f"idle_timeout_after_last_completion ({idle_timeout_s}s, "
                f"{len(still_alive)} threads stuck)"
            )
            break

        if now >= next_progress_print:
            waited = now - t0_wall
            extra = ""
            if can_fleet_idle:
                if fleet_idle_ok is True and zero_run_start_wall is not None:
                    zero_idle_s = now - zero_run_start_wall
                    zero_left = max(0.0, float(idle_zero_running_s) - zero_idle_s)
                    extra = (
                        f" fleet_idle=True zero_idle={zero_idle_s:.1f}s "
                        f"zero_left≈{zero_left:.1f}s"
                    )
                elif fleet_idle_ok is False:
                    mr = last_fleet_debug.get("max_running")
                    mw = last_fleet_debug.get("max_waiting")
                    extra = f" fleet_idle=False max_running={mr} max_waiting={mw}"
                else:
                    extra = f" fleet_idle=unknown reason={last_fleet_debug.get('reason')}"

            idle_left = max(0.0, float(idle_timeout_s) - idle_since_last)
            print(
                f"[load_runner] {backend_label} drain-wait: "
                f"inflight={len(still_alive)} completed={completed}/{total} "
                f"waited={waited:.1f}s idle_since_last_completion={idle_since_last:.1f}s "
                f"idle_left≈{idle_left:.1f}s{extra}"
            )
            next_progress_print = now + 5.0

        time.sleep(0.5)

    lost = 0
    if timed_out:
        # Pipeline drain grace: give BooM/proxy time to relay in-transit
        # responses before declaring threads LOST.  Up to 30s, but stop
        # early if all threads finish or no further progress.
        grace_s = 30.0
        grace_start = time.time()
        still_alive = [t for t in threads if t.is_alive()]
        alive_at_grace_start = len(still_alive)
        print(
            f"[load_runner] {backend_label}: fleet/idle triggered — {timeout_reason}. "
            f"{alive_at_grace_start} thread(s) still alive; "
            f"waiting up to {grace_s:.0f}s for pipeline drain..."
        )
        last_progress_at = time.time()
        while True:
            still_alive = [t for t in threads if t.is_alive()]
            if not still_alive:
                print(
                    f"[load_runner] {backend_label}: all threads completed "
                    f"during grace period ({time.time() - grace_start:.1f}s)"
                )
                break
            now = time.time()
            new_alive = len(still_alive)
            if new_alive < alive_at_grace_start:
                last_progress_at = now
                alive_at_grace_start = new_alive
            if (now - grace_start) >= grace_s:
                print(
                    f"[load_runner] {backend_label}: grace period exhausted "
                    f"({grace_s:.0f}s), {new_alive} thread(s) still stuck"
                )
                break
            if (now - last_progress_at) >= 10.0:
                print(
                    f"[load_runner] {backend_label}: no progress for 10s "
                    f"during grace, {new_alive} thread(s) stuck"
                )
                break
            time.sleep(0.5)

        still_alive = [t for t in threads if t.is_alive()]
        lost = len(still_alive)
        if lost > 0:
            reason_str = timeout_reason or "unknown"
            print(
                f"[load_runner] {backend_label}: ABORTING — {reason_str}. "
                f"Marking {lost} inflight request(s) as LOST."
            )
            for t in still_alive:
                idx_hint = getattr(t, "name", "?")
                print(f"[client][RECV] ✗ LOST thread={idx_hint} ({reason_str})")
            if logger is not None:
                for t in still_alive:
                    err_record: Dict[str, Any] = {
                        "error": f"lost ({reason_str})",
                        "send_failed": True,
                    }
                    logger.log_request(err_record)
    else:
        for t in threads:
            t.join(timeout=2.0)

    elapsed = time.time() - t0_wall
    print(
        f"[load_runner] Done. Sent {total} requests in {elapsed:.3f}s. "
        f"completed={completed} lost={lost}"
    )


def _fleet_idle_from_metrics_tick(tick: Optional[Dict[str, Any]]) -> Tuple[Optional[bool], Dict[str, Any]]:
    """
    Determine fleet-idle from the last metrics tick:
      - Consider vLLM-ish rows: those that have requests_running or requests_waiting fields.
      - Fleet idle => max(requests_running) == 0 AND (if waiting values exist) max(requests_waiting) == 0.
    Returns:
      (idle_bool_or_none, debug_dict)
    """
    if not isinstance(tick, dict):
        return None, {"reason": "no_tick"}

    samples = tick.get("samples")
    if not isinstance(samples, list) or not samples:
        return None, {"reason": "no_samples"}

    running_vals: List[float] = []
    waiting_vals: List[float] = []

    for rec in samples:
        if not isinstance(rec, dict):
            continue
        if ("requests_running" not in rec) and ("requests_waiting" not in rec):
            continue

        rv = rec.get("requests_running")
        wv = rec.get("requests_waiting")

        try:
            if rv is not None:
                running_vals.append(float(rv))
        except Exception:
            pass
        try:
            if wv is not None:
                waiting_vals.append(float(wv))
        except Exception:
            pass

    if not running_vals and not waiting_vals:
        return None, {"reason": "no_vllm_rows"}

    max_running = max(running_vals) if running_vals else None
    max_waiting = max(waiting_vals) if waiting_vals else None

    if max_running is None:
        return None, {"reason": "no_running_metric", "max_waiting": max_waiting}

    if max_running != 0.0:
        return False, {"max_running": max_running, "max_waiting": max_waiting}

    if max_waiting is not None and max_waiting != 0.0:
        return False, {"max_running": max_running, "max_waiting": max_waiting}

    return True, {"max_running": max_running, "max_waiting": max_waiting}


def run_open_loop_load(
    *,
    router_url: str,
    prompts: List[str],
    plan_times: List[float],
    gen_cfg: GenerationConfig,
    output_tokens_per_request: Optional[List[int]] = None,
    warmup_reqs: int = 0,
    logger: Optional[ExperimentLogger] = None,
    output_log_mode: str = "preview",
    print_trace: bool = True,
    transport: Any = None,
    backend: str = "router",
    aibrix: Optional[AIBrixConfig] = None,
    litellm: Optional[LiteLLMConfig] = None,
    boom: Optional[BooMConfig] = None,
    slo: Optional[SLOConfig] = None,
):
    """
    Execute a precomputed schedule.

    backend:
      - "router":
          - transport.mode == "sync"         -> existing /enqueue behavior
          - transport.mode == "async_pubsub" -> /submit + ZMQ completion events
      - "aibrix":
          - concurrent threaded HTTP requests to AIBrix gateway
          - no ZMQ
      - "litellm":
          - concurrent threaded HTTP requests to LiteLLM proxy
          - identical threading model to aibrix
          - for production/demo validation only, NOT benchmarking
      - "boom":
          - concurrent threaded HTTP requests to BooM Gateway
          - identical wire protocol to litellm (reuses same worker)
          - for production/demo validation only, NOT benchmarking
    """
    if len(prompts) != len(plan_times):
        raise ValueError("prompts and plan_times length mismatch")

    total = len(prompts)
    if total == 0:
        print("[load_runner] No jobs to send.")
        return

    backend = str(backend or "router").strip().lower()
    if backend not in ("router", "aibrix", "litellm", "boom"):
        raise ValueError(f"Invalid backend '{backend}'")

    mode = "sync"
    submit_path = "/submit"
    results_zmq = None
    topic = ""
    run_id = None

    idle_timeout_s = 60.0
    idle_zero_running_s = 10.0
    orphan_ttl_s = 300.0
    orphan_max = 100000

    if transport is not None:
        try:
            mode = str(getattr(transport, "mode", "sync") or "sync")
            submit_path = str(getattr(transport, "submit_path", "/submit") or "/submit")
            results_zmq = getattr(transport, "results_zmq", None)
            topic = str(getattr(transport, "topic", "") or "")
            run_id = getattr(transport, "run_id", None)

            if hasattr(transport, "idle_timeout_s"):
                idle_timeout_s = float(getattr(transport, "idle_timeout_s") or idle_timeout_s)

            if hasattr(transport, "idle_zero_running_s"):
                idle_zero_running_s = float(getattr(transport, "idle_zero_running_s"))

            if hasattr(transport, "orphan_ttl_s"):
                orphan_ttl_s = float(getattr(transport, "orphan_ttl_s") or orphan_ttl_s)
            if hasattr(transport, "orphan_max"):
                orphan_max = int(getattr(transport, "orphan_max") or orphan_max)
        except Exception:
            mode = "sync"

    try:
        idle_timeout_s = float(idle_timeout_s)
    except Exception:
        idle_timeout_s = 60.0
    if idle_timeout_s < 1.0:
        print(f"[load_runner] WARNING: idle_timeout_s={idle_timeout_s} too small; clamping to 1.0s")
        idle_timeout_s = 1.0
    if idle_timeout_s > 3600.0:
        print(f"[load_runner] WARNING: idle_timeout_s={idle_timeout_s} very large; clamping to 3600s")
        idle_timeout_s = 3600.0

    try:
        idle_zero_running_s = float(idle_zero_running_s)
    except Exception:
        idle_zero_running_s = 10.0

    warmup_reqs = int(warmup_reqs or 0)
    if warmup_reqs > 0:
        print(f"[load_runner] warmup: sending {warmup_reqs} dummy requests before timed load")

        session = requests.Session()
        meta = _build_generation_meta(gen_cfg)
        warm_prompt = "WARMUP: dummy request to warm up vLLM workers, router, and caches."

        t_w0 = time.time()
        for i in range(warmup_reqs):
            try:
                if backend == "aibrix":
                    if aibrix is None:
                        raise RuntimeError("backend='aibrix' requires aibrix config")
                    rid, _result = send_one_aibrix(session, aibrix, warm_prompt, gen_cfg)
                elif backend == "litellm":
                    if litellm is None:
                        raise RuntimeError("backend='litellm' requires litellm config")
                    rid, _result = send_one_litellm(session, litellm, warm_prompt, gen_cfg)
                elif backend == "boom":
                    if boom is None:
                        raise RuntimeError("backend='boom' requires boom config")
                    rid, _result = send_one_litellm(session, boom, warm_prompt, gen_cfg, label="BooM")
                else:
                    rid, _result = send_one(session, router_url, warm_prompt, meta)
                print(f"[client][warmup] i={i} req_id={rid}")
            except Exception as e:
                print(f"[client][warmup] ERROR i={i}: {e}")
        session.close()
        t_w1 = time.time()
        print(f"[load_runner] warmup done in {t_w1 - t_w0:.3f}s")

    print(f"[load_runner] Starting load for {total} requests (backend={backend}, transport_mode={mode})")

    if not plan_times:
        print("[load_runner] Empty schedule, nothing to send.")
        return

    t0_mono = time.monotonic()
    t0_plan = plan_times[0]
    adj_plan_times = [t0_mono + (ts - t0_plan) for ts in plan_times]

    # -------------------------------------------------------
    # AIBrix backend: threaded open-loop HTTP requests
    # -------------------------------------------------------
    if backend == "aibrix":
        if aibrix is None:
            raise RuntimeError("backend='aibrix' requires aibrix config")

        threads: List[threading.Thread] = []
        t0_wall = time.time()

        for idx, (ts_mono, prompt) in enumerate(zip(adj_plan_times, prompts)):
            now = time.monotonic()
            sleep_until = ts_mono - 0.005
            if sleep_until > now:
                time.sleep(sleep_until - now)

            task = RequestTask(idx=idx, prompt=prompt, ts_mono=ts_mono)
            t = threading.Thread(
                target=_request_thread_aibrix_http,
                args=(task, aibrix, gen_cfg, t0_mono, logger, output_log_mode, print_trace),
                daemon=True,
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        elapsed = time.time() - t0_wall
        print(f"[load_runner] Done. Sent {total} requests in {elapsed:.3f}s")
        return

    # -------------------------------------------------------
    # NEW: LiteLLM backend — identical threading model to AIBrix
    # -------------------------------------------------------
    if backend == "litellm":
        if litellm is None:
            raise RuntimeError("backend='litellm' requires litellm config")

        print(
            f"[load_runner] LiteLLM proxy: {litellm.base_url}{litellm.chat_path} "
            f"model={litellm.model}"
        )
        print(
            "[load_runner] NOTE: backend=litellm routes through the LiteLLM proxy "
            "for production auth/spend validation. Use backend=router for benchmarking."
        )

        threads: List[threading.Thread] = []
        t0_wall = time.time()

        for idx, (ts_mono, prompt) in enumerate(zip(adj_plan_times, prompts)):
            now = time.monotonic()
            sleep_until = ts_mono - 0.005
            if sleep_until > now:
                time.sleep(sleep_until - now)

            ot = output_tokens_per_request[idx] if output_tokens_per_request else None
            task = RequestTask(idx=idx, prompt=prompt, ts_mono=ts_mono, output_tokens=ot)
            t = threading.Thread(
                target=_request_thread_litellm_http,
                args=(task, litellm, gen_cfg, t0_mono, logger, output_log_mode, print_trace),
                daemon=True,
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        elapsed = time.time() - t0_wall
        print(f"[load_runner] Done. Sent {total} requests in {elapsed:.3f}s")
        return

    # -------------------------------------------------------
    # BooM Gateway backend — reuses LiteLLM worker (same protocol)
    # -------------------------------------------------------
    if backend == "boom":
        if boom is None:
            raise RuntimeError("backend='boom' requires boom config")

        print(
            f"[load_runner] BooM Gateway: {boom.base_url}{boom.chat_path} "
            f"model={boom.model}"
        )
        print(
            "[load_runner] NOTE: backend=boom routes through BooM Gateway "
            "for production auth/spend validation. Use backend=router for benchmarking."
        )

        threads: List[threading.Thread] = []
        t0_wall = time.time()

        for idx, (ts_mono, prompt) in enumerate(zip(adj_plan_times, prompts)):
            now = time.monotonic()
            sleep_until = ts_mono - 0.005
            if sleep_until > now:
                time.sleep(sleep_until - now)

            ot = output_tokens_per_request[idx] if output_tokens_per_request else None
            task = RequestTask(idx=idx, prompt=prompt, ts_mono=ts_mono, output_tokens=ot)
            t = threading.Thread(
                target=_request_thread_litellm_http,
                args=(task, boom, gen_cfg, t0_mono, logger, output_log_mode, print_trace, "BooM"),
                daemon=True,
                name=f"boom-T{idx}",
            )
            t.start()
            threads.append(t)

        _drain_threads_with_fleet_idle(
            threads=threads,
            total=total,
            t0_wall=t0_wall,
            idle_zero_running_s=idle_zero_running_s,
            idle_timeout_s=idle_timeout_s,
            logger=logger,
            backend_label="boom",
        )
        return

    # -------------------------------------------------------
    # Router sync path: thread-per-request /enqueue
    # -------------------------------------------------------
    if mode != "async_pubsub":
        threads: List[threading.Thread] = []
        t0_wall = time.time()

        for idx, (ts_mono, prompt) in enumerate(zip(adj_plan_times, prompts)):
            ot = output_tokens_per_request[idx] if output_tokens_per_request else None
            task = RequestTask(idx=idx, prompt=prompt, ts_mono=ts_mono, output_tokens=ot)
            t = threading.Thread(
                target=_request_thread_router_sync,
                args=(task, router_url, gen_cfg, t0_mono, logger, output_log_mode, print_trace, slo),
                daemon=True,
                name=f"router-sync-T{idx}",
            )
            t.start()
            threads.append(t)

        _drain_threads_with_fleet_idle(
            threads=threads,
            total=total,
            t0_wall=t0_wall,
            idle_zero_running_s=idle_zero_running_s,
            idle_timeout_s=idle_timeout_s,
            logger=logger,
            backend_label="router-sync",
        )
        return

    # -------------------------------------------------------
    # Router async_pubsub path
    # -------------------------------------------------------
    if not results_zmq:
        raise RuntimeError("transport.mode=async_pubsub requires transport.results_zmq to be set")

    print(f"[load_runner] async_pubsub: idle_timeout_s(after last recv)={idle_timeout_s}")
    if idle_zero_running_s > 0:
        print(f"[load_runner] async_pubsub: idle_zero_running_s(fleet idle shortcut)={idle_zero_running_s}")
    else:
        print("[load_runner] async_pubsub: fleet-idle shortcut disabled (idle_zero_running_s<=0)")

    if get_last_metrics_tick is None and idle_zero_running_s > 0:
        print("[load_runner] WARNING: metrics_prom.get_last_metrics_tick not available; fleet-idle shortcut disabled")
        idle_zero_running_s = 0.0

    pending: Dict[str, Dict[str, Any]] = {}
    pending_lock = threading.Lock()

    orphans: Dict[str, Dict[str, Any]] = {}
    orphans_lock = threading.Lock()

    done_evt = threading.Event()
    done_counter: Dict[str, int] = {"done": 0}

    progress = {"last_recv_wall": time.time()}
    progress_lock = threading.Lock()

    sub_t = threading.Thread(
        target=_pubsub_listener_thread,
        kwargs={
            "results_zmq": str(results_zmq),
            "topic": topic,
            "run_id": run_id,
            "pending": pending,
            "pending_lock": pending_lock,
            "orphans": orphans,
            "orphans_lock": orphans_lock,
            "orphan_ttl_s": float(orphan_ttl_s),
            "orphan_max": int(orphan_max),
            "done_counter": done_counter,
            "done_evt": done_evt,
            "progress": progress,
            "progress_lock": progress_lock,
            "output_log_mode": output_log_mode,
            "print_trace": print_trace,
        },
        daemon=True,
    )
    sub_t.start()

    t0_wall = time.time()
    session = requests.Session()
    try:
        for idx, (ts_mono, prompt) in enumerate(zip(adj_plan_times, prompts)):
            now = time.monotonic()
            delay = ts_mono - now
            if delay > 0:
                time.sleep(delay)

            now_send = time.monotonic()
            _log_send_tick(now_send, t0_mono)

            ot = output_tokens_per_request[idx] if output_tokens_per_request else None
            meta = _build_generation_meta(gen_cfg, output_tokens_override=ot)
            slo_fields = _build_slo_fields(slo, idx)

            print(f"[client][SEND][T{idx}] idx={idx} planned_ts={ts_mono:.6f}")

            t0 = time.time()
            try:
                rid = submit_one(session, router_url, submit_path, prompt, meta, run_id=run_id, slo_fields=slo_fields)
            except Exception as e:
                print(f"[client][RECV][T{idx}] ✗ ERROR idx={idx}: {e}")
                if logger is not None:
                    err_record: Dict[str, Any] = {
                        "idx": idx,
                        "error": str(e),
                        "prompt": prompt,
                        "planned_ts_mono": ts_mono,
                        "send_failed": True,
                    }
                    logger.log_request(err_record)
                continue

            info = {
                "idx": idx,
                "req_id": rid,
                "prompt": prompt,
                "planned_ts_mono": ts_mono,
                "actual_send_ts_mono": now_send,
                "t0_wall": t0,
                "logger": logger,
            }

            with pending_lock:
                pending[rid] = info

            orphan_entry = None
            with orphans_lock:
                orphan_entry = orphans.pop(rid, None)

            if orphan_entry is not None:
                t_recv = float(orphan_entry.get("t_recv_wall", 0.0))
                if (time.time() - t_recv) <= float(orphan_ttl_s):
                    with pending_lock:
                        pending.pop(rid, None)

                    _emit_completion_from_result(
                        rid=rid,
                        result=orphan_entry.get("result"),
                        info=info,
                        done_counter=done_counter,
                        output_log_mode=output_log_mode,
                        print_trace=print_trace,
                    )
                    _note_last_recv(progress, progress_lock)

    finally:
        session.close()

    wait_start = time.time()
    next_progress_print = wait_start + 5.0

    timed_out = False
    timeout_reason: Optional[str] = None
    zero_run_start_wall: Optional[float] = None
    last_fleet_debug: Dict[str, Any] = {}

    while True:
        with pending_lock:
            remaining = len(pending)

        if remaining == 0:
            break

        now = time.time()
        with progress_lock:
            last_recv_wall = float(progress.get("last_recv_wall", now))
        idle_s = now - last_recv_wall

        fleet_idle_ok: Optional[bool] = None
        if idle_zero_running_s > 0 and get_last_metrics_tick is not None:
            tick = get_last_metrics_tick()
            fleet_idle_ok, last_fleet_debug = _fleet_idle_from_metrics_tick(tick)

            if fleet_idle_ok is True:
                if zero_run_start_wall is None:
                    zero_run_start_wall = now
                elif (now - zero_run_start_wall) >= float(idle_zero_running_s):
                    timed_out = True
                    timeout_reason = f"fleet_idle (requests_running==0 for {idle_zero_running_s}s)"
                    break
            else:
                zero_run_start_wall = None

        if idle_s > float(idle_timeout_s):
            timed_out = True
            timeout_reason = f"idle_timeout_after_last_recv ({idle_timeout_s}s)"
            break

        if now >= next_progress_print:
            waited = now - wait_start
            left = max(0.0, float(idle_timeout_s) - idle_s)

            extra = ""
            if idle_zero_running_s > 0:
                if fleet_idle_ok is True and zero_run_start_wall is not None:
                    zero_idle_s = now - zero_run_start_wall
                    zero_left = max(0.0, float(idle_zero_running_s) - zero_idle_s)
                    extra = f" fleet_idle=True zero_idle={zero_idle_s:.1f}s zero_left≈{zero_left:.1f}s"
                elif fleet_idle_ok is False:
                    mr = last_fleet_debug.get("max_running")
                    mw = last_fleet_debug.get("max_waiting")
                    extra = f" fleet_idle=False max_running={mr} max_waiting={mw}"
                else:
                    extra = f" fleet_idle=unknown reason={last_fleet_debug.get('reason')}"

            print(
                f"[load_runner] drain-wait: remaining={remaining} "
                f"waited={waited:.1f}s idle={idle_s:.1f}s idle_left≈{left:.1f}s{extra}"
            )
            next_progress_print = now + 5.0

        time.sleep(0.1)

    done_evt.set()
    try:
        sub_t.join(timeout=1.0)
    except Exception:
        pass

    lost = 0
    if timed_out:
        with pending_lock:
            leftovers = list(pending.values())
            pending.clear()

        reason_str = timeout_reason or f"idle_timeout_after_last_recv ({idle_timeout_s}s)"
        for info in leftovers:
            idx = int(info["idx"])
            rid = str(info["req_id"])
            print(
                f"[client][RECV][T{idx}] ✗ LOST idx={idx} req_id={rid} "
                f"({reason_str})"
            )
            lost += 1
            if logger is not None:
                err_record: Dict[str, Any] = {
                    "idx": idx,
                    "req_id": rid,
                    "error": f"lost ({reason_str})",
                    "prompt": info.get("prompt"),
                    "planned_ts_mono": info.get("planned_ts_mono"),
                    "actual_send_ts_mono": info.get("actual_send_ts_mono"),
                    "t0_wall": info.get("t0_wall"),
                    "send_failed": True,
                }
                logger.log_request(err_record)

    elapsed = time.time() - t0_wall
    print(
        f"[load_runner] Done. Submitted {total} requests in {elapsed:.3f}s. "
        f"completed={int(done_counter.get('done', 0))} lost={lost}"
    )