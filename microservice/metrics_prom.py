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

        if model_name and (labels.get("model_name") is not None) and labels.get("model_name") != model_name:
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


# ----------------------------
# Metrics catalogs
# ----------------------------

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

    # Spec decode (optional)
    {"name": "vllm:spec_decode_num_accepted_tokens_total", "kind": "counter_rate", "field": "spec_tokens_accepted_per_sec"},
    {"name": "vllm:spec_decode_num_draft_tokens_total", "kind": "counter_rate", "field": "spec_tokens_draft_per_sec"},
    {"name": "vllm:spec_decode_num_emitted_tokens_total", "kind": "counter_rate", "field": "spec_tokens_emitted_per_sec"},
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
    ):
        super().__init__(daemon=True)
        self.run_dir = Path(run_dir)
        self.mode_name = str(mode_name)

        self.interval_s = max(0.2, float(interval_s))
        self.rate_window = str(rate_window or "10s")
        self.model_name = (model_name or "").strip() or None
        self.max_instances = int(max_instances) if max_instances is not None else None

        self._prom = PrometheusHTTP(prom_url, timeout_s=prom_timeout_s)
        self._stop_ev = threading.Event()
        self._eps_lock = threading.Lock()
        self._endpoints: List[str] = []

        self._catalog = list(metrics_catalog)

        self._jsonl = JsonlLogger(self.run_dir / "metrics.jsonl")
        self._summary_path = self.run_dir / "metrics_summary.json"

        # rollups
        self._samples = 0
        self._tick_errors = 0

        self._sum_gpu_kv = 0.0
        self._sum_gen_tps = 0.0
        self._sum_prefill_tps = 0.0
        self._sum_reqs_running = 0.0
        self._sum_reqs_waiting = 0.0

    def set_endpoints(self, endpoints: List[str]) -> None:
        with self._eps_lock:
            self._endpoints = list(endpoints or [])

    def stop(self) -> None:
        self._stop_ev.set()

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

    def _collect_one_tick(self) -> Dict[str, Any]:
        # endpoints are the router-discovered pod endpoints (whatever your discover_endpoints returns)
        with self._eps_lock:
            endpoints = list(self._endpoints)

        # instances are the IP:port "instance" labels used by Prometheus (from endpoints if present)
        instances = [_endpoint_to_instance(e) for e in endpoints]

        if self.max_instances is not None and len(instances) > self.max_instances:
            instances = instances[: self.max_instances]

        allowed = set(instances) if instances else None

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

        pod2inst = self._build_pod2instance(raw, instances)
        inst2pod = self._build_inst2pod(raw)

        per_inst: Dict[str, Dict[str, Any]] = {}
        keys = instances[:] if instances else []

        def _ensure(inst: str) -> Dict[str, Any]:
            rec = per_inst.get(inst)
            if rec is None:
                rec = {"instance": inst}
                per_inst[inst] = rec
            # attach pod if known (helps join with router trace endpoint)
            if "pod" not in rec:
                pod = inst2pod.get(inst)
                if pod:
                    rec["pod"] = pod
            return rec

        for spec in self._catalog:
            name = spec["name"]
            kind = spec["kind"]
            field = spec["field"]
            label_key = spec.get("label", "instance")

            if kind == "hist_avg":
                m_sum = _vec_to_map_by_label(
                    raw.get(name + "_sum", []),
                    label_key,
                    allowed=None if label_key == "pod" else allowed,
                    model_name=self.model_name,
                )
                m_cnt = _vec_to_map_by_label(
                    raw.get(name + "_count", []),
                    label_key,
                    allowed=None if label_key == "pod" else allowed,
                    model_name=self.model_name,
                )
                m_val = _divide_maps(m_sum, m_cnt)
            else:
                m_val = _vec_to_map_by_label(
                    raw.get(name, []),
                    label_key,
                    allowed=None if label_key == "pod" else allowed,
                    model_name=self.model_name,
                )

            # If the metric is labeled by pod, remap pod->instance for joinability
            if label_key == "pod":
                remapped: Dict[str, float] = {}
                for pod, val in m_val.items():
                    inst = pod2inst.get(pod)
                    if inst is None:
                        continue
                    if allowed is not None and inst not in allowed:
                        continue
                    remapped[inst] = val
                m_val = remapped

            if not keys:
                keys = list(m_val.keys())

            for inst in keys:
                rec = _ensure(inst)
                rec[field] = m_val.get(inst, None)

        # If endpoints were not supplied, let Prometheus define the instance set and
        # also provide a deterministic endpoints list so the JSON is consistent/usable.
        if not endpoints:
            instances = [
                rec.get("instance")
                for rec in per_inst.values()
                if isinstance(rec, dict) and rec.get("instance")
            ]
            instances = sorted(set(str(x) for x in instances))

        return {
            "ts": _now_iso_utc(),
            "mode": self.mode_name,
            "instances": instances,   # instance labels (host:port). If endpoints empty, derived from samples[].instance
            "samples": list(per_inst.values()),
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

        self._samples += 1

    def _write_summary_file(self) -> Dict[str, Any]:
        if self._samples <= 0:
            summary: Dict[str, Any] = {"samples": 0, "tick_errors": self._tick_errors}
        else:
            summary = {
                "samples": self._samples,
                "tick_errors": self._tick_errors,
                "overall": {
                    "avg_gpu_kv_cache_usage_frac": (self._sum_gpu_kv / self._samples) if self._samples else None,
                    "avg_requests_running": self._sum_reqs_running / self._samples,
                    "avg_requests_waiting": self._sum_reqs_waiting / self._samples,
                    "sum_generation_tokens_per_sec": self._sum_gen_tps / self._samples,
                    "sum_prefill_tokens_per_sec": self._sum_prefill_tps / self._samples,
                },
            }

        self._summary_path.parent.mkdir(parents=True, exist_ok=True)
        with self._summary_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, sort_keys=True)
        return summary

    def run(self) -> None:
        self._jsonl.open()
        try:
            # NOTE: removed the "metrics_start" JSONL record (user request)

            while not self._stop_ev.is_set():
                try:
                    tick = self._collect_one_tick()
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

        # IMPORTANT: Prometheus requires integer duration like "10s", not "10.0s".
        rate_window = _format_prom_window_s(getattr(cfg, "window_s", 10), default_s=10)

        model_name = getattr(cfg, "model_name", None)

        # Catalog selection:
        catalog = list(CPU_ONLY_METRICS_CATALOG)

        if bool(getattr(cfg, "include_debug_metrics", False)):
            catalog += [
                {"name": "vllm:request_total", "kind": "counter_rate", "field": "request_total_per_sec"},
                {"name": "vllm:request_timeout_total", "kind": "counter_rate", "field": "request_timeout_per_sec"},
            ]

        if bool(getattr(cfg, "include_gpu_metrics", False)):
            catalog += list(GPU_DCGM_METRICS_CATALOG)

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
        )
        _sampler.start()
        print(f"[metrics] started -> {Path(run_dir) / 'metrics.jsonl'}")


def update_metrics_endpoints(endpoints: List[str]) -> None:
    """
    Call this from your router loop whenever endpoint discovery changes.
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


def stop_metrics_collection() -> Dict[str, Any]:
    global _sampler
    with _lock:
        s = _sampler
        _sampler = None

    if s is None:
        return {}

    s.stop()
    time.sleep(min(0.25, float(s.interval_s)))

    try:
        with (s.run_dir / "metrics_summary.json").open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}
