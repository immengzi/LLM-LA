# load_runner.py
# Per-request threaded load runner:
# - Optional request-based warmup before the main load.
# - For each (plan_time, prompt), spawn a thread.
#
# TRANSPORT MODES:
#   - sync (default): each thread calls POST /enqueue and blocks for the response.
#   - async_pubsub: each thread calls POST /submit (submit+ack) and returns immediately.
#                   A single ZMQ SUB listener receives completion events and emits the
#                   SAME per-request log schema as sync mode (so analysis stays unchanged).
#
# (idle-timeout termination policy for async_pubsub):
#   - After the last RECEIVED completion (last RECV), wait at most idle_timeout_s seconds.
#   - If no new completion arrives in that idle window, mark ALL remaining pending requests
#     as LOST and terminate the experiment.
#   - No reconciliation polling / retry logic.

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple
import threading
import time
import json

import requests

from http_client import send_one, submit_one
from config import GenerationConfig
from trace_utils import print_trace_block, compute_trace_metrics
from experiment_io import ExperimentLogger

# TransportConfig is new; keep import backward-friendly.
try:
    from config import TransportConfig
except Exception:  # pragma: no cover
    TransportConfig = None  # type: ignore


@dataclass
class RequestTask:
    idx: int
    prompt: str
    ts_mono: float  # absolute monotonic timestamp for sending


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
    # preview mode
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
    Emit stdout + optional logger record, using the EXACT SAME schema as sync mode.
    This is shared by:
      - pubsub listener (normal)
      - async submit thread (race-fix path: orphan result arrived before pending)
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
            # keep old print_trace_block signature happy (expects {"req_id":..., "result":...})
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


def _request_thread_sync(
    task: RequestTask,
    router_url: str,
    gen_cfg: GenerationConfig,
    t0_mono: float,
    logger: Optional[ExperimentLogger] = None,
    output_log_mode: str = "preview",
    print_trace: bool = True,
):
    """
    Sync worker: blocks on /enqueue.
    (This is your existing behavior, kept intact.)
    """
    session = requests.Session()
    try:
        now = time.monotonic()
        delay = task.ts_mono - now
        if delay > 0:
            time.sleep(delay)

        # Per-second logging at actual send time
        now_send = time.monotonic()
        rel = now_send - t0_mono
        sec = int(rel)
        with _sec_lock:
            _sec_counts[sec] = _sec_counts.get(sec, 0) + 1
            count_in_sec = _sec_counts[sec]
        print(f"[load_runner][SEC] t=[{sec},{sec+1}) sent_so_far_in_sec={count_in_sec}")

        meta = {
            "max_tokens": int(gen_cfg.max_tokens),
            "temperature": float(gen_cfg.temperature),
            "length_mode": gen_cfg.length_mode,
            "enable_thinking": bool(gen_cfg.think),
        }
        if gen_cfg.target_output_tokens is not None:
            meta["target_output_tokens"] = int(gen_cfg.target_output_tokens)
        if gen_cfg.target_total_tokens is not None:
            meta["target_total_tokens"] = int(gen_cfg.target_total_tokens)

        print(f"[client][SEND][T{task.idx}] idx={task.idx} planned_ts={task.ts_mono:.6f}")

        t0 = time.time()
        try:
            rid, result = send_one(session, router_url, task.prompt, meta)
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


def _request_thread_async_submit(
    task: RequestTask,
    router_url: str,
    submit_path: str,
    gen_cfg: GenerationConfig,
    t0_mono: float,
    pending: Dict[str, Dict[str, Any]],
    pending_lock: threading.Lock,
    # orphan buffer for race where SUB result arrives before pending[rid] is set
    orphans: Dict[str, Dict[str, Any]],
    orphans_lock: threading.Lock,
    orphan_ttl_s: float,
    done_counter: Dict[str, int],
    output_log_mode: str,
    print_trace: bool,
    # run_id forwarding so router can publish to results.<run_id>
    run_id: Optional[str],
    # idle-timeout progress tracking
    progress: Dict[str, float],
    progress_lock: threading.Lock,
    logger: Optional[ExperimentLogger] = None,
):
    """
    Async_pubsub worker:
      - Sleep until scheduled timestamp
      - Submit to /submit (ack immediately)
      - Store local bookkeeping by req_id so the SUB listener can emit the SAME logs later.
      - Race fix: if result arrived early (in orphans), emit immediately.
    """
    session = requests.Session()
    try:
        now = time.monotonic()
        delay = task.ts_mono - now
        if delay > 0:
            time.sleep(delay)

        # Per-second logging at actual send time
        now_send = time.monotonic()
        rel = now_send - t0_mono
        sec = int(rel)
        with _sec_lock:
            _sec_counts[sec] = _sec_counts.get(sec, 0) + 1
            count_in_sec = _sec_counts[sec]
        print(f"[load_runner][SEC] t=[{sec},{sec+1}) sent_so_far_in_sec={count_in_sec}")

        meta = {
            "max_tokens": int(gen_cfg.max_tokens),
            "temperature": float(gen_cfg.temperature),
            "length_mode": gen_cfg.length_mode,
            "enable_thinking": bool(gen_cfg.think),
        }
        if gen_cfg.target_output_tokens is not None:
            meta["target_output_tokens"] = int(gen_cfg.target_output_tokens)
        if gen_cfg.target_total_tokens is not None:
            meta["target_total_tokens"] = int(gen_cfg.target_total_tokens)

        print(f"[client][SEND][T{task.idx}] idx={task.idx} planned_ts={task.ts_mono:.6f}")

        t0 = time.time()
        rid = submit_one(session, router_url, submit_path, task.prompt, meta, run_id=run_id)

        info = {
            "idx": task.idx,
            "req_id": rid,
            "prompt": task.prompt,
            "planned_ts_mono": task.ts_mono,
            "actual_send_ts_mono": now_send,
            "t0_wall": t0,
            "logger": logger,  # stored for convenience (could be None)
        }

        # Store pending first
        with pending_lock:
            pending[rid] = info

        # Race fix: if we already received this rid, consume it and emit now.
        orphan_entry = None
        with orphans_lock:
            orphan_entry = orphans.pop(rid, None)

        if orphan_entry is not None:
            # orphan_entry shape: {"t_recv_wall": float, "result": Any}
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
                # IMPORTANT: update idle-timeout progress only when we actually EMIT a completion
                _note_last_recv(progress, progress_lock)

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
    # orphan buffer for race where SUB result arrives before pending[rid] exists
    orphans: Dict[str, Dict[str, Any]],
    orphans_lock: threading.Lock,
    orphan_ttl_s: float,
    orphan_max: int,
    done_counter: Dict[str, int],
    done_evt: threading.Event,
    # idle-timeout progress tracking
    progress: Dict[str, float],
    progress_lock: threading.Lock,
    output_log_mode: str,
    print_trace: bool,
):
    """
    Single SUB connection that receives completion events.

    Race to fix:
      Result may arrive on SUB before the submit thread has inserted pending[rid].
      Previously you dropped it (info is None), permanently losing that completion.
      Now we stash it in `orphans` and let submit threads pick it up.

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

        # Subscribe: if topic is empty, subscribe to everything.
        if topic:
            sock.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
        else:
            sock.setsockopt(zmq.SUBSCRIBE, b"")

        while not done_evt.is_set():
            try:
                # Use poll so we can exit promptly.
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

            # Normal path: try to match pending
            with pending_lock:
                info = pending.pop(rid, None)

            if info is None:
                # RACE FIX: stash as orphan, best-effort bounded by ttl+max
                now = time.time()
                with orphans_lock:
                    # cleanup old entries occasionally
                    if len(orphans) > 0 and (len(orphans) % 256 == 0):
                        dead = [
                            k
                            for k, v in orphans.items()
                            if (now - float(v.get("t_recv_wall", 0.0))) > float(orphan_ttl_s)
                        ]
                        for k in dead:
                            orphans.pop(k, None)

                    # enforce max size (drop expired first; if still full, drop arbitrary one)
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

            # Matched: emit completion
            _emit_completion_from_result(
                rid=rid,
                result=result,
                info=info,
                done_counter=done_counter,
                output_log_mode=output_log_mode,
                print_trace=print_trace,
            )
            # IMPORTANT: update idle-timeout progress only when we actually EMIT a completion
            _note_last_recv(progress, progress_lock)

    finally:
        try:
            sock.close(0)
        except Exception:
            pass


def run_open_loop_load(
    *,
    router_url: str,
    prompts: List[str],
    plan_times: List[float],
    gen_cfg: GenerationConfig,
    warmup_reqs: int = 0,
    logger: Optional[ExperimentLogger] = None,
    output_log_mode: str = "preview",
    print_trace: bool = True,
    # optional: transport config
    transport: Any = None,
):
    """
    Execute a precomputed schedule using one thread per request.

    transport:
      - None or TransportConfig(mode="sync") => existing /enqueue behavior.
      - TransportConfig(mode="async_pubsub") => /submit + ZMQ completion events
        with an end-of-run policy of idle_timeout_s AFTER LAST RECEIVED completion.
    """
    if len(prompts) != len(plan_times):
        raise ValueError("prompts and plan_times length mismatch")

    total = len(prompts)
    if total == 0:
        print("[load_runner] No jobs to send.")
        return

    # Normalize transport
    mode = "sync"
    submit_path = "/submit"
    results_zmq = None
    topic = ""
    run_id = None

    # async_pubsub end condition
    idle_timeout_s = 60.0

    # Orphan buffering knobs (hardcoded safe defaults)
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

            if hasattr(transport, "orphan_ttl_s"):
                orphan_ttl_s = float(getattr(transport, "orphan_ttl_s") or orphan_ttl_s)
            if hasattr(transport, "orphan_max"):
                orphan_max = int(getattr(transport, "orphan_max") or orphan_max)
        except Exception:
            mode = "sync"

    # Safety clamp for idle_timeout_s
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

    # Warmup (kept synchronous even in async_pubsub mode)
    warmup_reqs = int(warmup_reqs or 0)
    if warmup_reqs > 0:
        print(f"[load_runner] warmup: sending {warmup_reqs} dummy requests before timed load")

        session = requests.Session()
        meta = {
            "max_tokens": int(gen_cfg.max_tokens),
            "temperature": float(gen_cfg.temperature),
            "length_mode": gen_cfg.length_mode,
            "enable_thinking": bool(gen_cfg.think),
        }
        if gen_cfg.target_output_tokens is not None:
            meta["target_output_tokens"] = int(gen_cfg.target_output_tokens)
        if gen_cfg.target_total_tokens is not None:
            meta["target_total_tokens"] = int(gen_cfg.target_total_tokens)

        warm_prompt = "WARMUP: dummy request to warm up vLLM workers, router, and caches."

        t_w0 = time.time()
        for i in range(warmup_reqs):
            try:
                rid, _result = send_one(session, router_url, warm_prompt, meta)
                print(f"[client][warmup] i={i} req_id={rid}")
            except Exception as e:
                print(f"[client][warmup] ERROR i={i}: {e}")
        session.close()
        t_w1 = time.time()
        print(f"[load_runner] warmup done in {t_w1 - t_w0:.3f}s")

    print(f"[load_runner] Starting thread-per-request mode for {total} requests (transport_mode={mode})")

    if not plan_times:
        print("[load_runner] Empty schedule, nothing to send.")
        return

    # Re-anchor schedule so first planned time starts now.
    t0_mono = time.monotonic()
    t0_plan = plan_times[0]
    adj_plan_times = [t0_mono + (ts - t0_plan) for ts in plan_times]

    # SYNC path: unchanged behavior
    if mode != "async_pubsub":
        threads: List[threading.Thread] = []
        t0_wall = time.time()

        for idx, (ts_mono, prompt) in enumerate(zip(adj_plan_times, prompts)):
            task = RequestTask(idx=idx, prompt=prompt, ts_mono=ts_mono)
            t = threading.Thread(
                target=_request_thread_sync,
                args=(task, router_url, gen_cfg, t0_mono, logger, output_log_mode, print_trace),
                daemon=True,
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        elapsed = time.time() - t0_wall
        print(f"[load_runner] Done. Sent {total} requests in {elapsed:.3f}s")
        return

    # ASYNC_PUBSUB path
    if not results_zmq:
        raise RuntimeError("transport.mode=async_pubsub requires transport.results_zmq to be set")

    print(f"[load_runner] async_pubsub: idle_timeout_s(after last recv)={idle_timeout_s}")

    pending: Dict[str, Dict[str, Any]] = {}
    pending_lock = threading.Lock()

    # orphan buffer (rid -> {"t_recv_wall": ..., "result": ...})
    orphans: Dict[str, Dict[str, Any]] = {}
    orphans_lock = threading.Lock()

    done_evt = threading.Event()
    done_counter: Dict[str, int] = {"done": 0}

    # progress tracking for idle-timeout termination
    progress = {"last_recv_wall": time.time()}
    progress_lock = threading.Lock()

    # Start one subscriber thread (single long-lived connection)
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

    # Submit threads (short-lived connections)
    threads = []
    t0_wall = time.time()

    for idx, (ts_mono, prompt) in enumerate(zip(adj_plan_times, prompts)):
        task = RequestTask(idx=idx, prompt=prompt, ts_mono=ts_mono)
        t = threading.Thread(
            target=_request_thread_async_submit,
            args=(
                task,
                router_url,
                submit_path,
                gen_cfg,
                t0_mono,
                pending,
                pending_lock,
                orphans,
                orphans_lock,
                float(orphan_ttl_s),
                done_counter,
                output_log_mode,
                print_trace,
                run_id,
                progress,
                progress_lock,
                logger,
            ),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # After all submits: wait while completions keep arriving.
    # Stop condition: pending drains OR idle_timeout_s since last emitted completion.
    wait_start = time.time()
    next_progress_print = wait_start + 5.0

    timed_out = False
    while True:
        with pending_lock:
            remaining = len(pending)

        if remaining == 0:
            break

        now = time.time()
        with progress_lock:
            last_recv_wall = float(progress.get("last_recv_wall", now))
        idle_s = now - last_recv_wall

        if idle_s > float(idle_timeout_s):
            timed_out = True
            break

        if now >= next_progress_print:
            waited = now - wait_start
            left = max(0.0, float(idle_timeout_s) - idle_s)
            print(
                f"[load_runner] drain-wait: remaining={remaining} "
                f"waited={waited:.1f}s idle={idle_s:.1f}s idle_left≈{left:.1f}s"
            )
            next_progress_print = now + 5.0

        time.sleep(0.1)

    # Stop listener
    done_evt.set()
    try:
        sub_t.join(timeout=1.0)
    except Exception:
        pass

    # If timed out: mark remaining pending as LOST and log with backward-compatible schema
    lost = 0
    if timed_out:
        with pending_lock:
            leftovers = list(pending.values())
            pending.clear()

        for info in leftovers:
            idx = int(info["idx"])
            rid = str(info["req_id"])
            print(
                f"[client][RECV][T{idx}] ✗ LOST idx={idx} req_id={rid} "
                f"(idle_timeout after last recv: {idle_timeout_s}s)"
            )
            lost += 1
            if logger is not None:
                err_record: Dict[str, Any] = {
                    "idx": idx,
                    "req_id": rid,
                    "error": f"lost (idle_timeout after last recv: {idle_timeout_s}s)",
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
