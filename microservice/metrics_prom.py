# metrics_prom.py
#
# Prometheus-backed metrics sampler for vLLM research + debugging.
#
# Outputs (inside experiment dir):
#   - metrics.jsonl         (tick-by-tick samples; includes error ticks)
#   - metrics_summary.json  (rollups computed on stop)
#
# Notes:
# - Prometheus does NOT accept float durations in range selectors (e.g. [10.0s]).
#   We always format window_s as an integer duration string like "10s".
# - GPU/DCGM metrics are optional and off by default.
#
# Router + sidecar metrics:
#   Router metrics (router service / likely :8080):
#     - router_central_queue_length (gauge)               -> router_queue_length
#     - router_admission_requests_total (counter -> rate) -> router_admission_rps
#     - router_dispatch_requests_total (counter -> rate)  -> router_outgoing_rps
#
#   Sidecar metrics (kv-sidecar / likely :9000):
#     - sidecar_queue_length (gauge)                      -> sidecar_queue_length
#     - sidecar_received_requests_total (counter -> rate) -> sidecar_received_rps
#     - sidecar_completed_requests_total (counter -> rate)-> sidecar_completed_rps
#
# (thread/worker metrics):
#   Sidecar (kv-sidecar / :9000):
#     - sidecar_python_threads (gauge)                    -> sidecar_python_threads
#     - sidecar_workers_total (gauge)                     -> sidecar_workers_total
#     - sidecar_workers_busy (gauge)                      -> sidecar_workers_busy
#
#   vLLM wrapper exporter (vllm container / :9101):
#     - vllm_threads (gauge)                              -> vllm_threads
#
# Key behaviors:
#   1) vLLM metrics: logged per vLLM instance (:8200) row
#   2) sidecar metrics: endpoint=<pod name> remapped into vLLM (:8200) rows via kube_pod_info
#      - AND pod-labeled sidecar gauges are also remapped into :8200 rows via pod->instance mapping.
#   3) router metrics:
#       - logged per router instance (:8080) row
#       - also broadcast as aggregated scalars into every vLLM (:8200) row
#
# Critical fixes:
#   - If endpoint discovery hasn't populated yet, fallback-discover vLLM instances from Prometheus
#   - If Prometheus returns instances not in `keys`, auto-add them so they appear in output
#   - sampler stores last successful tick and exposes get_last_metrics_tick()
#     so load_runner can reuse the existing Prometheus scraping to detect
#     vllm:num_requests_running == 0 fleet-idle condition.

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import threading
import time

import requests


# ----------------------------
# JSONL logger
# ----------------------------

class JsonlLogger:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._fh: Optional[Any] = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")

    def write(self, obj: Dict[str, Any]) -> None:
        if self._fh is None:
            return
        line = json.dumps(obj, ensure_ascii=False)
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


# ----------------------------
# Prometheus client
# ----------------------------

class PrometheusHTTP:
    def __init__(self, base_url: str, timeout_s: float = 5.0):
        self.base_url = str(base_url).rstrip("/")
        self.timeout_s = float(timeout_s)

    def instant(self, promql: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v1/query"
        r = requests.get(url, params={"query": promql}, timeout=self.timeout_s)
        r.raise_for_status()
        payload = r.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus error: {payload}")
        return payload.get("data", {}).get("result", []) or []


# ----------------------------
# Helpers
# ----------------------------

def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _endpoint_to_instance(ep: str) -> str:
    """
    Convert an endpoint URL into the Prometheus 'instance' label value (host:port).
    Examples:
      - http://10.244.0.243:8200 -> 10.244.0.243:8200
      - 10.244.0.243:8200        -> 10.244.0.243:8200
    """
    try:
        from urllib.parse import urlparse
        u = urlparse(ep)
        return (u.netloc or u.path).strip("/")
    except Exception:
        return ep.replace("http://", "").replace("https://", "").strip("/")


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        f = float(x)
        if f != f:  # NaN
            return None
        return f
    except Exception:
        return None


def _vec_values(vec: List[Dict[str, Any]]) -> List[float]:
    vals: List[float] = []
    for it in vec or []:
        v = it.get("value")
        if isinstance(v, list) and len(v) >= 2:
            f = _safe_float(v[1])
            if f is not None:
                vals.append(float(f))
    return vals


def _scalar_max(vec: List[Dict[str, Any]]) -> Optional[float]:
    vals = _vec_values(vec)
    return max(vals) if vals else None


def _scalar_sum(vec: List[Dict[str, Any]]) -> Optional[float]:
    vals = _vec_values(vec)
    return sum(vals) if vals else None


def _vec_to_map_by_label(
    vec: List[Dict[str, Any]],
    label_key: str,
    allowed: Optional[set[str]] = None,
    model_name: Optional[str] = None,
) -> Dict[str, float]:
    """
    Convert a Prometheus vector response into {label_value: float_value}.
    If allowed is provided, only keep those label values.
    If model_name is provided, and the series has a model_name label, enforce it.
    """
    out: Dict[str, float] = {}
    for it in vec or []:
        labels = it.get("metric", {}) or {}
        k = labels.get(label_key)
        if not k:
            continue

        k = str(k)
        if allowed is not None and k not in allowed:
            continue

        if (
            model_name
            and (labels.get("model_name") is not None)
            and labels.get("model_name") != model_name
        ):
            continue

        v = it.get("value")
        val = None
        if isinstance(v, list) and len(v) >= 2:
            val = _safe_float(v[1])
        if val is None:
            continue
        out[k] = float(val)
    return out


def _divide_maps(num: Dict[str, float], den: Dict[str, float]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k, n in num.items():
        d = den.get(k)
        if d is None or d == 0.0:
            continue
        out[k] = n / d
    return out


def _format_prom_window_s(window_s: Any, *, default_s: int = 10) -> str:
    """
    Prometheus range selectors require integer durations like '10s', not '10.0s'.
    """
    try:
        s = int(round(float(window_s)))
        if s <= 0:
            s = int(default_s)
    except Exception:
        s = int(default_s)
    return f"{s}s"


def _parse_extra_instances(cfg: Any) -> List[str]:
    """
    Optional: let the experiment config supply additional Prometheus instances
    that should be present in the output even if not in router-discovered endpoints.

    Supported config attrs (any of these):
      - extra_instances: "10.244.0.17:8080,10.244.0.17:9000"
      - router_instance: "10.244.0.17:8080"
      - sidecar_instance: "10.244.0.17:9000"
    """
    out: List[str] = []

    extra = getattr(cfg, "extra_instances", None)
    if isinstance(extra, str) and extra.strip():
        for part in extra.split(","):
            s = part.strip()
            if s:
                out.append(s)

    r = getattr(cfg, "router_instance", None)
    if isinstance(r, str) and r.strip():
        out.append(r.strip())

    sc = getattr(cfg, "sidecar_instance", None)
    if isinstance(sc, str) and sc.strip():
        out.append(sc.strip())

    # de-dup preserving order
    seen = set()
    deduped: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


# ----------------------------
# Metrics catalogs
# ----------------------------

# vLLM core metrics (existing)
CPU_ONLY_METRICS_CATALOG: List[Dict[str, str]] = [
    # Core
    {"name": "vllm:num_requests_running", "kind": "gauge", "field": "requests_running"},
    {"name": "vllm:num_requests_waiting", "kind": "gauge", "field": "requests_waiting"},
    {"name": "vllm:request_success_total", "kind": "counter_rate", "field": "request_success_per_sec"},
    {"name": "vllm:request_failure_total", "kind": "counter_rate", "field": "request_failure_per_sec"},
    {"name": "vllm:num_preemptions_total", "kind": "counter_rate", "field": "preemptions_per_sec"},

    # Token rates
    {"name": "vllm:generation_tokens_total", "kind": "counter_rate", "field": "gen_tokens_per_sec"},
    {"name": "vllm:prompt_tokens_total", "kind": "counter_rate", "field": "prefill_tokens_per_sec"},

    # KV cache (may be absent depending on vLLM build/config; ok if null)
    {"name": "vllm:gpu_cache_usage_perc", "kind": "gauge", "field": "gpu_kv_cache_usage_frac"},
    {"name": "vllm:cpu_cache_usage_perc", "kind": "gauge", "field": "cpu_kv_cache_usage_frac"},

    # Latency hist avgs
    {"name": "vllm:time_to_first_token_seconds", "kind": "hist_avg", "field": "ttft_seconds_avg"},
    {"name": "vllm:time_per_output_token_seconds", "kind": "hist_avg", "field": "tpot_seconds_avg"},
    {"name": "vllm:e2e_request_latency_seconds", "kind": "hist_avg", "field": "e2e_latency_seconds_avg"},
    {"name": "vllm:request_queue_time_seconds", "kind": "hist_avg", "field": "queue_time_seconds_avg"},
    {"name": "vllm:request_inference_time_seconds", "kind": "hist_avg", "field": "inference_time_seconds_avg"},
    {"name": "vllm:request_prefill_time_seconds", "kind": "hist_avg", "field": "prefill_time_seconds_avg"},
    {"name": "vllm:request_decode_time_seconds", "kind": "hist_avg", "field": "decode_time_seconds_avg"},

    # Token distribution (hists)
    {"name": "vllm:request_prompt_tokens", "kind": "hist_avg", "field": "request_prompt_tokens_avg"},
    {"name": "vllm:request_generation_tokens", "kind": "hist_avg", "field": "request_generation_tokens_avg"},
    {"name": "vllm:request_max_num_generation_tokens", "kind": "hist_avg", "field": "request_max_generation_tokens_avg"},

    # Spec decode (optional at vLLM level; okay if missing)
    {"name": "vllm:spec_decode_num_accepted_tokens_total", "kind": "counter_rate", "field": "spec_tokens_accepted_per_sec"},
    {"name": "vllm:spec_decode_num_draft_tokens_total", "kind": "counter_rate", "field": "spec_tokens_draft_per_sec"},
    {"name": "vllm:spec_decode_num_emitted_tokens_total", "kind": "counter_rate", "field": "spec_tokens_emitted_per_sec"},
]

# Router + Sidecar + vLLM thread metrics
ROUTER_SIDECAR_METRICS_CATALOG: List[Dict[str, str]] = [
    # Router (labeled by instance=:8080)
    {"name": "router_central_queue_length", "kind": "gauge", "field": "router_queue_length", "label": "instance"},
    {"name": "router_admission_requests_total", "kind": "counter_rate", "field": "router_admission_rps", "label": "instance"},
    {"name": "router_dispatch_requests_total", "kind": "counter_rate", "field": "router_outgoing_rps", "label": "instance"},

    # Sidecar (labeled by endpoint=<pod name>)
    {"name": "sidecar_queue_length", "kind": "gauge", "field": "sidecar_queue_length", "label": "endpoint"},
    {"name": "sidecar_received_requests_total", "kind": "counter_rate", "field": "sidecar_received_rps", "label": "endpoint"},
    {"name": "sidecar_completed_requests_total", "kind": "counter_rate", "field": "sidecar_completed_rps", "label": "endpoint"},

    # sidecar worker metrics (endpoint=<pod name>)
    {"name": "sidecar_workers_total", "kind": "gauge", "field": "sidecar_workers_total", "label": "endpoint"},
    {"name": "sidecar_workers_busy", "kind": "gauge", "field": "sidecar_workers_busy", "label": "endpoint"},

    # sidecar python thread count (usually has 'pod' label via Prometheus Operator relabeling)
    # We remap pod -> vLLM (:8200) instance.
    {"name": "sidecar_python_threads", "kind": "gauge", "field": "sidecar_python_threads", "label": "pod"},

    # vLLM wrapper exporter thread count (metric itself includes pod="..."; remap pod -> :8200 instance)
    {"name": "vllm_threads", "kind": "gauge", "field": "vllm_threads", "label": "pod"},
]


GPU_DCGM_METRICS_CATALOG: List[Dict[str, str]] = [
    # These only work if dcgm-exporter is deployed and scraped by Prometheus.
    {"name": "avg by (pod) (max_over_time(DCGM_FI_DEV_GPU_UTIL[3s]))", "kind": "gauge", "field": "gpu_util_percent", "label": "pod"},
    {"name": "sum by (pod) (DCGM_FI_DEV_FB_USED)", "kind": "gauge", "field": "gpu_mem_used_mib", "label": "pod"},
    {"name": "(sum by (pod) (DCGM_FI_DEV_FB_USED)) / (sum by (pod) (DCGM_FI_DEV_FB_TOTAL))", "kind": "gauge", "field": "gpu_mem_util_frac", "label": "pod"},
    {"name": "avg by (pod) (DCGM_FI_DEV_GPU_TEMP)", "kind": "gauge", "field": "gpu_temp_c", "label": "pod"},
    {"name": "avg by (pod) (DCGM_FI_DEV_POWER_USAGE)", "kind": "gauge", "field": "gpu_power_watts", "label": "pod"},
]


# ----------------------------
# Sampler thread
# ----------------------------

class _MetricsSampler(threading.Thread):
    def __init__(
        self,
        *,
        run_dir: Path,
        prom_url: str,
        prom_timeout_s: float,
        interval_s: float,
        rate_window: str,
        model_name: Optional[str],
        metrics_catalog: List[Dict[str, str]],
        mode_name: str = "client",
        max_instances: Optional[int] = None,
        only_filter_to_endpoints: bool = False,
        extra_instances: Optional[List[str]] = None,
    ):
        super().__init__(daemon=True)
        self.run_dir = Path(run_dir)
        self.mode_name = str(mode_name)

        self.interval_s = max(0.2, float(interval_s))
        self.rate_window = str(rate_window or "10s")
        self.model_name = (model_name or "").strip() or None
        self.max_instances = int(max_instances) if max_instances is not None else None

        # If true: router/sidecar metrics are filtered to discovered endpoints too.
        # Default false to avoid dropping :8080/:9000 targets.
        self.only_filter_to_endpoints = bool(only_filter_to_endpoints)

        self._extra_instances = list(extra_instances or [])

        self._prom = PrometheusHTTP(prom_url, timeout_s=prom_timeout_s)
        self._stop_ev = threading.Event()
        self._eps_lock = threading.Lock()
        self._endpoints: List[str] = []

        self._catalog = list(metrics_catalog)

        self._jsonl = JsonlLogger(self.run_dir / "metrics.jsonl")
        self._summary_path = self.run_dir / "metrics_summary.json"

        # last successful tick snapshot for reuse by load_runner
        self._last_lock = threading.Lock()
        self._last_tick: Optional[Dict[str, Any]] = None

        # rollups
        self._samples = 0
        self._tick_errors = 0

        # existing rollups
        self._sum_gpu_kv = 0.0
        self._sum_gen_tps = 0.0
        self._sum_prefill_tps = 0.0
        self._sum_reqs_running = 0.0
        self._sum_reqs_waiting = 0.0

        # router + sidecar rollups
        self._sum_router_q = 0.0
        self._sum_router_adm_rps = 0.0
        self._sum_router_out_rps = 0.0

        self._sum_sidecar_q = 0.0
        self._sum_sidecar_recv_rps = 0.0
        self._sum_sidecar_comp_rps = 0.0

        # rollups: threads / workers
        self._sum_vllm_threads = 0.0
        self._sum_sidecar_py_threads = 0.0
        self._sum_sidecar_workers_total = 0.0
        self._sum_sidecar_workers_busy = 0.0

    def set_endpoints(self, endpoints: List[str]) -> None:
        with self._eps_lock:
            self._endpoints = list(endpoints or [])

    def stop(self) -> None:
        self._stop_ev.set()

    # allow readers to retrieve last successful tick
    def get_last_tick(self) -> Optional[Dict[str, Any]]:
        with self._last_lock:
            if self._last_tick is None:
                return None
            # return a shallow copy so callers can't mutate internal state
            return dict(self._last_tick)

    def _q_gauge(self, name: str) -> List[Dict[str, Any]]:
        return self._prom.instant(name)

    def _q_rate(self, name: str) -> List[Dict[str, Any]]:
        return self._prom.instant(f"rate({name}[{self.rate_window}])")

    def _q_hist_pair(self, base: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        s = self._prom.instant(f"rate({base}_sum[{self.rate_window}])")
        c = self._prom.instant(f"rate({base}_count[{self.rate_window}])")
        return s, c

    def _build_pod2instance(self, raw: Dict[str, Any], instances: List[str]) -> Dict[str, str]:
        """
        When GPU/DCGM series are labeled by pod, try to map pod -> instance.
        We do this in two passes:
          1) infer from vLLM series if they contain both pod+instance labels
          2) fallback: kube_pod_info (pod_ip), then pod_ip:port
        """
        pod2inst: Dict[str, str] = {}

        # pass 1: infer from vLLM series
        for name, vec in raw.items():
            if not isinstance(name, str):
                continue
            if not name.startswith("vllm:"):
                continue
            for it in vec or []:
                labels = it.get("metric", {}) or {}
                pod = labels.get("pod")
                inst = labels.get("instance")
                if pod and inst and str(pod) not in pod2inst:
                    pod2inst[str(pod)] = str(inst)

        if pod2inst:
            return pod2inst

        # pass 2: kube_pod_info fallback
        port: Optional[str] = None
        if instances:
            try:
                _h, p = instances[0].rsplit(":", 1)
                if p.isdigit():
                    port = p
            except Exception:
                port = None
        if port is None:
            return pod2inst

        try:
            vec = self._prom.instant("kube_pod_info")
        except Exception:
            vec = []

        allowed = set(instances) if instances else None
        for it in vec or []:
            labels = it.get("metric", {}) or {}
            pod = labels.get("pod")
            pod_ip = labels.get("pod_ip")
            if not pod or not pod_ip:
                continue
            inst = f"{pod_ip}:{port}"
            if allowed is not None and inst not in allowed:
                continue
            if str(pod) not in pod2inst:
                pod2inst[str(pod)] = inst

        return pod2inst

    def _build_inst2pod(self, raw: Dict[str, Any]) -> Dict[str, str]:
        """
        Try to infer instance -> pod mapping from vLLM series that include both labels.
        This lets us join client trace logs (pod name) with metrics samples (instance).
        """
        inst2pod: Dict[str, str] = {}
        for name, vec in raw.items():
            if not isinstance(name, str):
                continue
            if not name.startswith("vllm:"):
                continue
            for it in vec or []:
                labels = it.get("metric", {}) or {}
                inst = labels.get("instance")
                pod = labels.get("pod")
                if inst and pod:
                    inst_s = str(inst)
                    if inst_s not in inst2pod:
                        inst2pod[inst_s] = str(pod)
        return inst2pod

    def _allowed_for_metric(self, metric_name: str, vllm_allowed: Optional[set[str]]) -> Optional[set[str]]:
        """
        Default behavior:
          - vLLM series are filtered to vllm_allowed (discovered endpoints)
          - router/sidecar series are NOT filtered (otherwise they get dropped)
        If only_filter_to_endpoints=True, everything uses vllm_allowed.
        """
        if self.only_filter_to_endpoints:
            return vllm_allowed
        if metric_name.startswith("vllm:"):
            return vllm_allowed
        return None

    def _vllm_port_from_instances(self, vllm_instances: List[str]) -> str:
        """
        Best-effort extract port from vLLM instances (default 8200).
        """
        if not vllm_instances:
            return "8200"
        try:
            _h, p = str(vllm_instances[0]).rsplit(":", 1)
            if p.isdigit():
                return p
        except Exception:
            pass
        return "8200"

    def _podname_to_instance_map(self, pod_names: List[str], port: str) -> Dict[str, str]:
        """
        Map pod name -> instance (pod_ip:port) using kube_pod_info.
        Used to remap sidecar series labeled by endpoint=<pod name>.
        """
        want = set(str(x) for x in (pod_names or []) if x)
        if not want:
            return {}

        try:
            vec = self._prom.instant("kube_pod_info")
        except Exception:
            vec = []

        out: Dict[str, str] = {}
        for it in vec or []:
            labels = it.get("metric", {}) or {}
            pod = labels.get("pod")
            pod_ip = labels.get("pod_ip")
            if not pod or not pod_ip:
                continue
            pod_s = str(pod)
            if pod_s in want and pod_s not in out:
                out[pod_s] = f"{pod_ip}:{port}"
        return out

    def _discover_router_instances(self) -> List[str]:
        """
        Auto-discover router :8080 instances by asking Prometheus for a router gauge.
        """
        try:
            vec = self._prom.instant("router_central_queue_length")
        except Exception:
            vec = []
        insts: List[str] = []
        seen = set()
        for it in vec or []:
            labels = it.get("metric", {}) or {}
            inst = labels.get("instance")
            if not inst:
                continue
            s = str(inst)
            if s not in seen:
                seen.add(s)
                insts.append(s)
        return insts

    def _fallback_discover_vllm_instances(self, *, vllm_port: str = "8200") -> List[str]:
        """
        If endpoint discovery isn't ready, discover vLLM instances directly from Prometheus.
        """
        try:
            vec = self._prom.instant("vllm:num_requests_running")
        except Exception:
            vec = []
        insts: List[str] = []
        seen = set()
        for it in vec or []:
            labels = it.get("metric", {}) or {}
            inst = labels.get("instance")
            if not inst:
                continue
            s = str(inst)
            if ":" in s:
                try:
                    _h, p = s.rsplit(":", 1)
                    if p.isdigit() and p != str(vllm_port):
                        continue
                except Exception:
                    pass
            if s not in seen:
                seen.add(s)
                insts.append(s)
        return insts

    def _collect_one_tick(self) -> Dict[str, Any]:
        # endpoints are the router-discovered vLLM pod endpoints (typically :8200)
        with self._eps_lock:
            endpoints = list(self._endpoints)

        vllm_instances = [_endpoint_to_instance(e) for e in endpoints]
        if self.max_instances is not None and len(vllm_instances) > self.max_instances:
            vllm_instances = vllm_instances[: self.max_instances]

        # Fallback discover from Prometheus if discovery is empty at this tick
        if not vllm_instances:
            vllm_instances = self._fallback_discover_vllm_instances(vllm_port="8200")

        vllm_allowed = set(vllm_instances) if vllm_instances else None
        vllm_port = self._vllm_port_from_instances(vllm_instances)

        raw: Dict[str, Any] = {}
        for spec in self._catalog:
            name = spec["name"]
            kind = spec["kind"]
            if kind == "gauge":
                raw[name] = self._q_gauge(name)
            elif kind == "counter_rate":
                raw[name] = self._q_rate(name)
            elif kind == "hist_avg":
                s, c = self._q_hist_pair(name)
                raw[name + "_sum"] = s
                raw[name + "_count"] = c

        # join helpers primarily from vLLM series
        pod2inst = self._build_pod2instance(raw, vllm_instances)
        inst2pod = self._build_inst2pod(raw)

        per_inst: Dict[str, Dict[str, Any]] = {}

        # ---- include router :8080 instances as first-class rows ----
        router_instances = self._discover_router_instances()

        # keys in output: vLLM + router + any user-specified extras
        keys: List[str] = []
        for inst in vllm_instances:
            if inst not in keys:
                keys.append(inst)
        for inst in router_instances:
            if inst not in keys:
                keys.append(inst)
        for inst in self._extra_instances:
            if inst not in keys:
                keys.append(inst)

        def _ensure(inst: str) -> Dict[str, Any]:
            rec = per_inst.get(inst)
            if rec is None:
                rec = {"instance": inst}
                per_inst[inst] = rec
            if "pod" not in rec:
                pod = inst2pod.get(inst)
                if pod:
                    rec["pod"] = pod
            return rec

        def _add_key(inst: str) -> None:
            """
            IMPORTANT: if Prometheus returns instances we didn't pre-list in `keys`,
            add them so they show up in output.
            """
            if not inst:
                return
            if inst not in keys:
                keys.append(inst)
            _ensure(inst)

        for inst in keys:
            _ensure(inst)

        # ----------------------------
        # Router metrics: per-instance maps
        # ----------------------------
        try:
            rq_vec = self._prom.instant("router_central_queue_length")
            ra_vec = self._prom.instant(f"rate(router_admission_requests_total[{self.rate_window}])")
            rd_vec = self._prom.instant(f"rate(router_dispatch_requests_total[{self.rate_window}])")

            router_q_by_inst = _vec_to_map_by_label(rq_vec, "instance", allowed=None)
            router_adm_by_inst = _vec_to_map_by_label(ra_vec, "instance", allowed=None)
            router_out_by_inst = _vec_to_map_by_label(rd_vec, "instance", allowed=None)
        except Exception:
            router_q_by_inst = {}
            router_adm_by_inst = {}
            router_out_by_inst = {}

        # write per-router rows
        for inst in router_instances:
            _add_key(inst)
            rec = _ensure(inst)
            rec["router_queue_length"] = router_q_by_inst.get(inst)
            rec["router_admission_rps"] = router_adm_by_inst.get(inst)
            rec["router_outgoing_rps"] = router_out_by_inst.get(inst)

        # aggregated scalars for broadcast into vLLM rows
        try:
            router_q_scalar = max(router_q_by_inst.values()) if router_q_by_inst else None
            router_adm_scalar = sum(router_adm_by_inst.values()) if router_adm_by_inst else None
            router_out_scalar = sum(router_out_by_inst.values()) if router_out_by_inst else None
        except Exception:
            router_q_scalar = None
            router_adm_scalar = None
            router_out_scalar = None

        # ----------------------------
        # Catalog loop for all other metrics (vLLM + sidecar + dcgm + threads)
        # ----------------------------
        for spec in self._catalog:
            name = spec["name"]
            kind = spec["kind"]
            field = spec["field"]
            label_key = spec.get("label", "instance")

            # router_* handled above
            if isinstance(name, str) and name.startswith("router_"):
                continue

            allowed_for_this = self._allowed_for_metric(name, vllm_allowed)

            if kind == "hist_avg":
                m_sum = _vec_to_map_by_label(
                    raw.get(name + "_sum", []),
                    label_key,
                    allowed=None if label_key in ("pod", "exported_endpoint", "endpoint") else allowed_for_this,
                    model_name=self.model_name,
                )
                m_cnt = _vec_to_map_by_label(
                    raw.get(name + "_count", []),
                    label_key,
                    allowed=None if label_key in ("pod", "exported_endpoint", "endpoint") else allowed_for_this,
                    model_name=self.model_name,
                )
                m_val = _divide_maps(m_sum, m_cnt)
            else:
                m_val = _vec_to_map_by_label(
                    raw.get(name, []),
                    label_key,
                    allowed=None if label_key in ("pod", "exported_endpoint", "endpoint") else allowed_for_this,
                    model_name=self.model_name,
                )

            # If metric is labeled by pod, remap pod->instance
            if label_key == "pod":
                remapped: Dict[str, float] = {}
                for pod, val in m_val.items():
                    inst = pod2inst.get(pod)
                    if inst is None:
                        continue
                    if allowed_for_this is not None and inst not in allowed_for_this:
                        continue
                    remapped[inst] = val
                m_val = remapped

            # Sidecar metrics keyed by endpoint/exported_endpoint=<pod name> -> map to instance (pod_ip:port)
            if label_key in ("exported_endpoint", "endpoint"):
                pod_names = list(m_val.keys())
                podname2inst = self._podname_to_instance_map(pod_names, vllm_port)
                remapped = {}
                for pod_name, val in m_val.items():
                    inst = podname2inst.get(pod_name)
                    if inst is None:
                        continue
                    remapped[inst] = val
                m_val = remapped

            # IMPORTANT: auto-add instances observed in Prometheus results so they show up in output
            for inst, val in m_val.items():
                _add_key(inst)
                per_inst[inst][field] = val

        # Broadcast aggregated router scalars into every vLLM row
        for inst in vllm_instances:
            _add_key(inst)
            rec = _ensure(inst)
            rec["router_queue_length"] = router_q_scalar
            rec["router_admission_rps"] = router_adm_scalar
            rec["router_outgoing_rps"] = router_out_scalar

        return {
            "ts": _now_iso_utc(),
            "mode": self.mode_name,
            "instances": keys,
            "samples": [per_inst[k] for k in keys if k in per_inst],
        }

    def _update_rollups(self, tick: Dict[str, Any]) -> None:
        samples = tick.get("samples")
        if not isinstance(samples, list) or not samples:
            return

        def _avg(key: str) -> Optional[float]:
            vals: List[float] = []
            for rec in samples:
                if not isinstance(rec, dict):
                    continue
                v = _safe_float(rec.get(key))
                if v is not None:
                    vals.append(v)
            if not vals:
                return None
            return sum(vals) / len(vals)

        def _sum(key: str) -> Optional[float]:
            vals: List[float] = []
            for rec in samples:
                if not isinstance(rec, dict):
                    continue
                v = _safe_float(rec.get(key))
                if v is not None:
                    vals.append(v)
            if not vals:
                return None
            return sum(vals)

        # existing rollups
        a_kv = _avg("gpu_kv_cache_usage_frac")
        a_run = _avg("requests_running")
        a_wait = _avg("requests_waiting")
        s_gen = _sum("gen_tokens_per_sec")
        s_pre = _sum("prefill_tokens_per_sec")

        if a_kv is not None:
            self._sum_gpu_kv += a_kv
        if a_run is not None:
            self._sum_reqs_running += a_run
        if a_wait is not None:
            self._sum_reqs_waiting += a_wait
        if s_gen is not None:
            self._sum_gen_tps += s_gen
        if s_pre is not None:
            self._sum_prefill_tps += s_pre

        # router + sidecar rollups
        a_router_q = _avg("router_queue_length")
        s_router_adm = _sum("router_admission_rps")
        s_router_out = _sum("router_outgoing_rps")

        a_sidecar_q = _avg("sidecar_queue_length")
        s_sidecar_recv = _sum("sidecar_received_rps")
        s_sidecar_comp = _sum("sidecar_completed_rps")

        if a_router_q is not None:
            self._sum_router_q += a_router_q
        if s_router_adm is not None:
            self._sum_router_adm_rps += s_router_adm
        if s_router_out is not None:
            self._sum_router_out_rps += s_router_out

        if a_sidecar_q is not None:
            self._sum_sidecar_q += a_sidecar_q
        if s_sidecar_recv is not None:
            self._sum_sidecar_recv_rps += s_sidecar_recv
        if s_sidecar_comp is not None:
            self._sum_sidecar_comp_rps += s_sidecar_comp

        # rollups: threads / workers
        a_vllm_thr = _avg("vllm_threads")
        a_sc_py_thr = _avg("sidecar_python_threads")
        a_sc_w_total = _avg("sidecar_workers_total")
        a_sc_w_busy = _avg("sidecar_workers_busy")

        if a_vllm_thr is not None:
            self._sum_vllm_threads += a_vllm_thr
        if a_sc_py_thr is not None:
            self._sum_sidecar_py_threads += a_sc_py_thr
        if a_sc_w_total is not None:
            self._sum_sidecar_workers_total += a_sc_w_total
        if a_sc_w_busy is not None:
            self._sum_sidecar_workers_busy += a_sc_w_busy

        self._samples += 1

    def _write_summary_file(self) -> Dict[str, Any]:
        if self._samples <= 0:
            summary: Dict[str, Any] = {"samples": 0, "tick_errors": self._tick_errors}
        else:
            summary = {
                "samples": self._samples,
                "tick_errors": self._tick_errors,
                "overall": {
                    # existing
                    "avg_gpu_kv_cache_usage_frac": (self._sum_gpu_kv / self._samples) if self._samples else None,
                    "avg_requests_running": self._sum_reqs_running / self._samples,
                    "avg_requests_waiting": self._sum_reqs_waiting / self._samples,
                    "sum_generation_tokens_per_sec": self._sum_gen_tps / self._samples,
                    "sum_prefill_tokens_per_sec": self._sum_prefill_tps / self._samples,

                    # router + sidecar
                    "avg_router_queue_length": self._sum_router_q / self._samples,
                    "sum_router_admission_rps": self._sum_router_adm_rps / self._samples,
                    "sum_router_outgoing_rps": self._sum_router_out_rps / self._samples,

                    "avg_sidecar_queue_length": self._sum_sidecar_q / self._samples,
                    "sum_sidecar_received_rps": self._sum_sidecar_recv_rps / self._samples,
                    "sum_sidecar_completed_rps": self._sum_sidecar_comp_rps / self._samples,

                    # threads / workers
                    "avg_vllm_threads": self._sum_vllm_threads / self._samples,
                    "avg_sidecar_python_threads": self._sum_sidecar_py_threads / self._samples,
                    "avg_sidecar_workers_total": self._sum_sidecar_workers_total / self._samples,
                    "avg_sidecar_workers_busy": self._sum_sidecar_workers_busy / self._samples,
                },
            }

        self._summary_path.parent.mkdir(parents=True, exist_ok=True)
        with self._summary_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, sort_keys=True)
        return summary

    def run(self) -> None:
        self._jsonl.open()
        try:
            while not self._stop_ev.is_set():
                try:
                    tick = self._collect_one_tick()

                    # publish last successful tick for other threads
                    with self._last_lock:
                        self._last_tick = tick

                    self._jsonl.write(tick)
                    self._update_rollups(tick)
                except Exception as e:
                    self._tick_errors += 1
                    self._jsonl.write(
                        {
                            "ts": _now_iso_utc(),
                            "type": "metrics_error",
                            "error": str(e),
                        }
                    )
                time.sleep(self.interval_s)
        finally:
            try:
                self._write_summary_file()
            except Exception:
                pass
            self._jsonl.close()


# ----------------------------
# Public registry API
# ----------------------------

_lock = threading.Lock()
_sampler: Optional[_MetricsSampler] = None


def start_metrics_collection(*, run_dir: str | Path, cfg: Any) -> None:
    """
    Start metrics sampling in the background.

    run_dir: experiment directory path (e.g. experiments/5)
    cfg: PrometheusMetricsConfig (cfg.enabled, cfg.prometheus_base_url, ...)
    """
    from config import PrometheusMetricsConfig  # ensure class exists / importable

    global _sampler
    with _lock:
        if _sampler is not None:
            _sampler.stop()
            _sampler = None

        if not isinstance(cfg, PrometheusMetricsConfig) or not bool(cfg.enabled):
            return

        prom_url = str(cfg.prometheus_base_url)
        interval_s = float(cfg.scrape_interval_s)

        # IMPORTANT:
        # rate() needs >=2 scrapes in the lookback window, otherwise Prom returns empty vectors.
        # So ensure window >= ~2 scrapes and also >= 30s (common cluster scrape interval).
        window_s = float(getattr(cfg, "window_s", 10))
        safe_window_s = max(window_s, 2.2 * interval_s, 30.0)
        rate_window = _format_prom_window_s(safe_window_s, default_s=30)

        model_name = getattr(cfg, "model_name", None)

        # Catalog selection:
        catalog = list(CPU_ONLY_METRICS_CATALOG) + list(ROUTER_SIDECAR_METRICS_CATALOG)

        # keep existing debug option (still optional)
        if bool(getattr(cfg, "include_debug_metrics", False)):
            catalog += [
                {"name": "vllm:request_total", "kind": "counter_rate", "field": "request_total_per_sec"},
                {"name": "vllm:request_timeout_total", "kind": "counter_rate", "field": "request_timeout_per_sec"},
            ]

        if bool(getattr(cfg, "include_gpu_metrics", False)):
            catalog += list(GPU_DCGM_METRICS_CATALOG)

        only_filter_to_endpoints = bool(getattr(cfg, "only_filter_to_endpoints", False))
        extra_instances = _parse_extra_instances(cfg)

        _sampler = _MetricsSampler(
            run_dir=Path(run_dir),
            prom_url=prom_url,
            prom_timeout_s=float(getattr(cfg, "prometheus_timeout_s", 5.0)),
            interval_s=interval_s,
            rate_window=rate_window,
            model_name=model_name,
            metrics_catalog=catalog,
            mode_name="client",
            max_instances=getattr(cfg, "max_instances", None),
            only_filter_to_endpoints=only_filter_to_endpoints,
            extra_instances=extra_instances,
        )
        _sampler.start()
        print(f"[metrics] started -> {Path(run_dir) / 'metrics.jsonl'}")


def update_metrics_endpoints(endpoints: List[str]) -> None:
    """
    Call this from your router loop whenever endpoint discovery changes.

    NOTE: These endpoints are assumed to be vLLM endpoints (typically :8200).
    Sidecar metrics are remapped into :8200 rows using kube_pod_info + endpoint label.
    Router metrics are logged as router rows and also broadcast into :8200 rows.
    """
    global _sampler
    with _lock:
        s = _sampler
    if s is None:
        return
    try:
        s.set_endpoints(endpoints or [])
    except Exception:
        pass


def get_last_metrics_tick() -> Optional[Dict[str, Any]]:
    """
    Return the last successful metrics tick collected by the background sampler.
    Used by async_pubsub drain logic to detect fleet-idle (requests_running==0).

    Returns:
      - dict tick (shallow copy), or
      - None if metrics are disabled / not started / no tick yet.
    """
    global _sampler
    with _lock:
        s = _sampler
    if s is None:
        return None
    try:
        return s.get_last_tick()
    except Exception:
        return None


def stop_metrics_collection() -> Dict[str, Any]:
    global _sampler
    with _lock:
        s = _sampler
        _sampler = None

    if s is None:
        return {}

    s.stop()
    s.join(timeout=2)

    try:
        with (s.run_dir / "metrics_summary.json").open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}
