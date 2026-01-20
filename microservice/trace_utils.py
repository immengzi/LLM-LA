# trace_utils.py
# Helpers for computing and printing timing/trace metrics.

from __future__ import annotations

from typing import Dict, Any, Optional


def _get_ts(trace: Dict[str, Any], name: str) -> Optional[float]:
    v = trace.get(name)
    if isinstance(v, (int, float)):
        return float(v)
    return None


def compute_trace_metrics(trace: Dict[str, Any]) -> Dict[str, float]:
    """
    Compute derived latencies (in seconds) from a trace dict.

    Returned keys are *_s, and only include metrics whose underlying
    timestamps are present and numeric.
    """
    metrics: Dict[str, float] = {}

    t_enq_client = _get_ts(trace, "t_enq_client")
    t_arrive_router = _get_ts(trace, "t_arrive_router")
    t_enq_router_queue = _get_ts(trace, "t_enq_router_queue") or t_arrive_router
    t_dispatch_router = _get_ts(trace, "t_dispatch_router")

    t_arrive_sidecar_push = _get_ts(trace, "t_arrive_sidecar_push")
    t_arrive_sidecar_pull = _get_ts(trace, "t_arrive_sidecar_pull")
    t_dequeue_sidecar = _get_ts(trace, "t_dequeue_sidecar")

    t_vllm_send = _get_ts(trace, "t_vllm_send")
    t_vllm_recv = _get_ts(trace, "t_vllm_recv")

    t_post_result_sidecar = _get_ts(trace, "t_post_result_sidecar")
    t_router_result_recv = _get_ts(trace, "t_router_result_recv")

    # NEW router post-result split points (added in router/api.py)
    t_router_result_store = _get_ts(trace, "t_router_result_store")
    t_enqueue_unblocked = _get_ts(trace, "t_enqueue_unblocked")
    t_enqueue_about_to_return = _get_ts(trace, "t_enqueue_about_to_return")

    # Existing final router timestamp
    t_enqueue_response = _get_ts(trace, "t_enqueue_response")

    # End-to-end as seen by client
    if t_enq_client is not None and t_enqueue_response is not None:
        metrics["end_to_end_s"] = t_enqueue_response - t_enq_client

    # Client → router ingress
    if t_enq_client is not None and t_arrive_router is not None:
        metrics["client_to_router_s"] = t_arrive_router - t_enq_client

    # Router queueing (client enqueue → dispatch or router-queue enqueue → dispatch)
    if t_enq_router_queue is not None and t_dispatch_router is not None:
        metrics["router_queue_s"] = t_dispatch_router - t_enq_router_queue

    # Router → sidecar (pick whichever arrival we have)
    t_arrive_sidecar = t_arrive_sidecar_pull or t_arrive_sidecar_push
    if t_dispatch_router is not None and t_arrive_sidecar is not None:
        metrics["router_to_sidecar_s"] = t_arrive_sidecar - t_dispatch_router

    # Sidecar local queue waiting
    if t_arrive_sidecar is not None and t_dequeue_sidecar is not None:
        metrics["sidecar_queue_s"] = t_dequeue_sidecar - t_arrive_sidecar

    # vLLM compute time
    if t_vllm_send is not None and t_vllm_recv is not None:
        metrics["vllm_compute_s"] = t_vllm_recv - t_vllm_send

    # Sidecar post-processing before callback
    if t_vllm_recv is not None and t_post_result_sidecar is not None:
        metrics["sidecar_post_s"] = t_post_result_sidecar - t_vllm_recv

    # Sidecar → router on result
    if t_post_result_sidecar is not None and t_router_result_recv is not None:
        metrics["sidecar_to_router_s"] = t_router_result_recv - t_post_result_sidecar

    # Router post-result overhead (coarse; kept for backwards compat)
    if t_router_result_recv is not None and t_enqueue_response is not None:
        metrics["router_post_result_s"] = t_enqueue_response - t_router_result_recv

    # ------------------------------------------------------------------
    # break router_post_result_s into sub-stages (when timestamps exist)
    # ------------------------------------------------------------------
    # 1) /result handler work + store_result path (router result handling)
    if t_router_result_recv is not None and t_router_result_store is not None:
        metrics["router_result_handler_s"] = t_router_result_store - t_router_result_recv

    # 2) Wake-up + scheduling delay until /enqueue unblocks
    # (event.set() -> waiter thread returns -> asyncio resumes)
    if t_router_result_store is not None and t_enqueue_unblocked is not None:
        metrics["router_wakeup_s"] = t_enqueue_unblocked - t_router_result_store
    elif t_router_result_recv is not None and t_enqueue_unblocked is not None:
        # fallback if store ts not present
        metrics["router_wakeup_s"] = t_enqueue_unblocked - t_router_result_recv

    # 3) Python-side response assembly / trace merge / rename before returning
    if t_enqueue_unblocked is not None and t_enqueue_about_to_return is not None:
        metrics["router_response_build_s"] = t_enqueue_about_to_return - t_enqueue_unblocked

    # 4) Any remaining gap until the final router timestamp
    # (depending on where you stamp t_enqueue_response, this may represent
    # late processing or be ~0; it helps detect "mystery gap".)
    if t_enqueue_about_to_return is not None and t_enqueue_response is not None:
        metrics["router_after_return_stamp_s"] = t_enqueue_response - t_enqueue_about_to_return
    elif t_enqueue_unblocked is not None and t_enqueue_response is not None:
        metrics["router_after_unblock_s"] = t_enqueue_response - t_enqueue_unblocked

    # Router-centric "server roundtrip" (excluding client net)
    if t_arrive_router is not None and t_enqueue_response is not None:
        metrics["server_roundtrip_s"] = t_enqueue_response - t_arrive_router

    return metrics


def print_trace_block(idx: int, result: Dict[str, Any]) -> None:
    """
    Given a per-request result dict, print derived latency metrics and
    non-timestamp trace extras for a single request index.

    This assumes stdout logging (same style as load_runner).
    """
    if not isinstance(result, dict):
        return

    trace = result.get("trace")
    if not isinstance(trace, dict):
        return

    # Endpoint identity
    endpoint = trace.get("pod")
    print(f"[client][T{idx}]   trace_endpoint={endpoint}")

    # Router mode (if present)
    router_mode = trace.get("router_mode")
    if isinstance(router_mode, str):
        print(f"[client][T{idx}]   router_mode={router_mode}")

    # Derived latency metrics (no raw timestamps)
    metrics = compute_trace_metrics(trace)
    for name, value in metrics.items():
        print(f"[client][T{idx}]   {name}={value:.6f}s")

    # Non-timestamp extras (queue lengths, inflight counts, etc.)
    for k, v in trace.items():
        if k in ("pod", "router_mode"):
            continue
        if isinstance(k, str) and k.startswith("t_"):
            # Skip raw timestamps here – we only expose derived latencies.
            continue
        print(f"[client][T{idx}]   {k}={v}")
