# trace_utils.py
# Helpers for computing and printing timing/trace metrics.

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Any, List, Optional, Tuple


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

    # router post-result split points (added in router/api.py)
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

    # Sidecar-measured TTFT (streaming path: sidecar → vLLM first token latency)
    ttft_sidecar_s = _get_ts(trace, "ttft_sidecar_s")
    if ttft_sidecar_s is not None:
        metrics["ttft_sidecar_s"] = ttft_sidecar_s

    # Client-measured TTFT (from trace if present, e.g. streaming proxy path)
    ttft_s = _get_ts(trace, "ttft_s")
    if ttft_s is not None:
        metrics["ttft_s"] = ttft_s

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


# ============================================================
# Per-endpoint token accounting
# ============================================================

class EndpointTokenTracker:
    """
    Accumulates prefill (prompt) and decode (completion) token counts
    per serving endpoint.  Thread-safe — can be called from request
    callback threads in load_runner.

    Usage:
        tracker = EndpointTokenTracker()
        # after each request completes:
        tracker.record(endpoint="vllm-pod-0", prompt_tokens=512, completion_tokens=128)
        ...
        tracker.print_summary()
    """

    def __init__(self) -> None:
        import threading
        self._lock = threading.Lock()
        self._prefill: Dict[str, int] = defaultdict(int)
        self._decode: Dict[str, int] = defaultdict(int)
        self._count: Dict[str, int] = defaultdict(int)

    def record(
        self,
        endpoint: Optional[str],
        prompt_tokens: Optional[int] = None,
        completion_tokens: Optional[int] = None,
    ) -> None:
        ep = endpoint or "_unknown_"
        with self._lock:
            self._count[ep] += 1
            if prompt_tokens is not None:
                self._prefill[ep] += int(prompt_tokens)
            if completion_tokens is not None:
                self._decode[ep] += int(completion_tokens)

    def snapshot(self) -> List[Dict[str, Any]]:
        """Return per-endpoint totals as a list of dicts (sorted by endpoint)."""
        with self._lock:
            endpoints = sorted(
                set(self._count) | set(self._prefill) | set(self._decode)
            )
            return [
                {
                    "endpoint": ep,
                    "requests": self._count.get(ep, 0),
                    "prefill_tokens": self._prefill.get(ep, 0),
                    "decode_tokens": self._decode.get(ep, 0),
                    "total_tokens": self._prefill.get(ep, 0) + self._decode.get(ep, 0),
                }
                for ep in endpoints
            ]

    def totals(self) -> Tuple[int, int, int]:
        """Return (total_prefill, total_decode, total_requests) across all endpoints."""
        with self._lock:
            return (
                sum(self._prefill.values()),
                sum(self._decode.values()),
                sum(self._count.values()),
            )

    def print_summary(self, label: str = "Token summary") -> None:
        rows = self.snapshot()
        if not rows:
            print(f"[{label}] no token data recorded")
            return

        total_pf, total_dc, total_req = self.totals()
        print(f"\n[{label}] per-endpoint token counts:")
        print(f"  {'endpoint':<40s}  {'reqs':>6s}  {'prefill':>10s}  {'decode':>10s}  {'total':>10s}")
        print(f"  {'-'*40}  {'-'*6}  {'-'*10}  {'-'*10}  {'-'*10}")
        for r in rows:
            print(
                f"  {r['endpoint']:<40s}  {r['requests']:>6d}  "
                f"{r['prefill_tokens']:>10d}  {r['decode_tokens']:>10d}  "
                f"{r['total_tokens']:>10d}"
            )
        print(f"  {'TOTAL':<40s}  {total_req:>6d}  {total_pf:>10d}  {total_dc:>10d}  {total_pf + total_dc:>10d}")
        print()


# ============================================================
# Post-experiment per-endpoint token summary from logs.json
# ============================================================

def summarize_endpoint_tokens(logs_path: str, save_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Read a completed experiment's logs.json and produce a per-endpoint
    summary of prefill (prompt) and decode (completion) tokens served.

    Each line in logs.json is a JSON record with optional fields:
      endpoint_id, prompt_tokens, completion_tokens

    Returns the summary rows and optionally writes them to save_path as JSON.
    """
    import json as _json

    tracker = EndpointTokenTracker()
    total_records = 0
    errors = 0

    try:
        with open(logs_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = _json.loads(line)
                except Exception:
                    errors += 1
                    continue

                if not isinstance(rec, dict):
                    continue
                if rec.get("send_failed") or rec.get("error"):
                    continue

                total_records += 1
                tracker.record(
                    endpoint=rec.get("endpoint_id"),
                    prompt_tokens=rec.get("prompt_tokens"),
                    completion_tokens=rec.get("completion_tokens"),
                )
    except FileNotFoundError:
        print(f"[token_summary] logs file not found: {logs_path}")
        return []

    rows = tracker.snapshot()
    total_pf, total_dc, total_req = tracker.totals()

    print(f"\n[token_summary] Analyzed {total_records} records from {logs_path}")
    if errors:
        print(f"[token_summary] ({errors} unparseable lines skipped)")

    print(f"  {'endpoint':<40s}  {'reqs':>6s}  {'prefill':>10s}  {'decode':>10s}  {'total':>10s}")
    print(f"  {'-'*40}  {'-'*6}  {'-'*10}  {'-'*10}  {'-'*10}")
    for r in rows:
        print(
            f"  {r['endpoint']:<40s}  {r['requests']:>6d}  "
            f"{r['prefill_tokens']:>10d}  {r['decode_tokens']:>10d}  "
            f"{r['total_tokens']:>10d}"
        )
    print(f"  {'TOTAL':<40s}  {total_req:>6d}  {total_pf:>10d}  {total_dc:>10d}  {total_pf + total_dc:>10d}")
    print()

    if save_path:
        summary = {
            "total_records": total_records,
            "total_prefill_tokens": total_pf,
            "total_decode_tokens": total_dc,
            "total_requests": total_req,
            "per_endpoint": rows,
        }
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                _json.dump(summary, f, indent=2)
            print(f"[token_summary] saved to {save_path}")
        except Exception as e:
            print(f"[token_summary] WARN: failed to save: {e}")

    return rows
