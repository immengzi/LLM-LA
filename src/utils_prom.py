# prom_utils.py

from __future__ import annotations

import re
import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone
from time import monotonic
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import get_config
from utils import JsonlLogger, get_run_dir, register_metrics_getter


# =======================
# Low-level helpers
# =======================

_POD_UID_RE = re.compile(r"pod([0-9a-fA-F\-]{36})")
_CGROUP_HINTS = ("/proc/{pid}/cgroup", "/proc/{pid}/mountinfo")


def _read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return ""


def _pid_to_pod_uid(pid: int) -> Optional[str]:
    """Try to extract k8s pod UID from cgroup v1/v2 notations."""
    for p in _CGROUP_HINTS:
        txt = _read_text(p.format(pid=pid))
        if not txt:
            continue
        m = _POD_UID_RE.search(txt)
        if m:
            return m.group(1).lower()
    return None


def _bin_exists(bin_name: str) -> bool:
    return shutil.which(bin_name) is not None


def _endpoint_to_instance(ep: str) -> str:
    try:
        from urllib.parse import urlparse

        u = urlparse(ep)
        return u.netloc or u.path
    except Exception:
        return ep.replace("http://", "").replace("https://", "").strip("/")


# =======================
# Local GPU util mapper
# =======================

class _LocalGPUMapper:
    """
    Resolves:
      - pod_uid -> pod_name
      - pod_name -> [gpu_idx,...]
      - gpu_idx -> util%
    Uses: nvidia-smi pmon/csv and Kubernetes API (if available),
    and Prom series to bind endpoint->pod.
    """

    def __init__(self):
        self._last_map_ts = 0.0
        self._poduid_to_name: Dict[str, str] = {}
        self._podname_to_gpus: Dict[str, List[int]] = {}
        self._gpu_util: Dict[int, float] = {}
        self._last_gpu_ts = 0.0

        # try to init k8s (optional)
        self._k8s = None
        try:
            from kubernetes import client, config as kcfg

            try:
                kcfg.load_incluster_config()
            except Exception:
                kcfg.load_kube_config()
            self._k8s = client.CoreV1Api()
        except Exception:
            self._k8s = None

    def _refresh_gpu_util(self):
        cfg = get_config()
        now = monotonic()
        if now - self._last_gpu_ts < float(cfg.LOCAL_GPU_REFRESH_S):
            return
        self._last_gpu_ts = now
        util: Dict[int, float] = {}

        # Fast path: CSV query for per-GPU util (no colors, no units)
        try:
            out = subprocess.check_output(
                [
                    "nvidia-smi",
                    "--query-gpu=index,utilization.gpu",
                    "--format=csv,noheader,nounits",
                ],
                text=True,
                timeout=1.5,
            )
            for line in out.strip().splitlines():
                parts = [t.strip() for t in line.split(",")]
                if len(parts) >= 2:
                    idx = int(parts[0])
                    val = float(parts[1])
                    util[idx] = max(0.0, min(100.0, val))
        except Exception:
            pass

        self._gpu_util = util

    def _refresh_pod_maps(self):
        cfg = get_config()
        now = monotonic()
        if now - self._last_map_ts < float(cfg.LOCAL_GPU_POD_CACHE_TTL_S):
            return
        self._last_map_ts = now
        poduid_to_name: Dict[str, str] = {}
        podname_to_gpus: Dict[str, List[int]] = {}

        # (1) discover running GPU processes
        pids_by_gpu: Dict[int, List[int]] = {}
        try:
            # pmon supports one-shot: collect once
            pmon = subprocess.check_output(
                ["nvidia-smi", "pmon", "-c", "1"], text=True, timeout=1.5
            )
            # lines like: "# gpu        pid   type    sm   mem   enc   dec   command"
            for line in pmon.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = re.split(r"\s+", line)
                if len(parts) < 2:
                    continue
                try:
                    gpu_idx = int(parts[0])
                    pid = int(parts[1])
                except Exception:
                    continue
                pids_by_gpu.setdefault(gpu_idx, []).append(pid)
        except Exception:
            pass

        # (2) map pid -> pod_uid via /proc
        for gpu_idx, pid_list in pids_by_gpu.items():
            for pid in pid_list:
                puid = _pid_to_pod_uid(pid)
                if puid:
                    podname_to_gpus.setdefault(puid, [])
                    if gpu_idx not in podname_to_gpus[puid]:
                        podname_to_gpus[puid].append(gpu_idx)

        # (3) convert pod_uid -> pod_name (optional k8s API)
        if self._k8s and podname_to_gpus:
            try:
                pods = self._k8s.list_pod_for_all_namespaces(limit=500).items
                uid_to_name = {
                    p.metadata.uid.lower(): p.metadata.name
                    for p in pods
                    if p.metadata and p.metadata.uid
                }
                for puid in list(podname_to_gpus.keys()):
                    name = uid_to_name.get(puid)
                    if name:
                        poduid_to_name[puid] = name
            except Exception:
                pass

        # fallback: if k8s unavailable, we keep keys as UIDs and treat them as "names"
        self._poduid_to_name = poduid_to_name
        # reindex podname_to_gpus by "display name": prefer real name else UID
        final_map: Dict[str, List[int]] = {}
        for puid, gpus in podname_to_gpus.items():
            key = poduid_to_name.get(puid, puid)
            final_map[key] = gpus
        self._podname_to_gpus = final_map

    def get_pod_gpu_indices(self, pod_name: str) -> List[int]:
        self._refresh_pod_maps()
        return self._podname_to_gpus.get(pod_name, [])

    def get_gpu_util(self, idx: int) -> Optional[float]:
        self._refresh_gpu_util()
        return self._gpu_util.get(idx)

    def avg_util_for_pod(self, pod_name: str) -> Optional[float]:
        gpus = self.get_pod_gpu_indices(pod_name)
        if not gpus:
            return None
        vals = [self.get_gpu_util(i) for i in gpus]
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        return sum(vals) / len(vals)


_LOCAL_GPU = _LocalGPUMapper() if _bin_exists("nvidia-smi") else None


# =======================
# Prometheus client
# =======================

class PrometheusHTTP:
    def __init__(self, base_url: str, timeout_s: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout_s

    def instant(self, promql: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v1/query"
        r = requests.get(url, params={"query": promql}, timeout=self.timeout)
        r.raise_for_status()
        payload = r.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus error: {payload}")
        return payload.get("data", {}).get("result", []) or []


# =======================
# GPU util probe
# =======================

def probe_gpu_util(endpoint: str, metrics_path: str, timeout: float = 1.5) -> Optional[float]:
    """
    Prefer local nvidia-smi if enabled; fallback to Prometheus/DCGM.
    Signature unchanged; metrics_path ignored.
    """
    cfg = get_config()
    inst = _endpoint_to_instance(endpoint)

    # Prefer local path when available/enabled
    if bool(cfg.USE_LOCAL_GPU_UTIL) and _LOCAL_GPU is not None:
        try:
            prom = PrometheusHTTP(cfg.PROMETHEUS_URL, timeout_s=timeout)
            pod: Optional[str] = None
            for q in (
                f'vllm:num_requests_running{{instance="{inst}"}}',
                f'vllm:request_success_total{{instance="{inst}"}}',
                f'vllm:generation_tokens_total{{instance="{inst}"}}',
            ):
                try:
                    vec = prom.instant(q)
                except Exception:
                    vec = []
                if vec:
                    labels = vec[0].get("metric", {}) or {}
                    pod = labels.get("pod")
                    if pod:
                        break
        except Exception:
            pod = None

        if pod:
            v = _LOCAL_GPU.avg_util_for_pod(pod)
            if v is not None:
                return max(0.0, min(100.0, float(v)))

    # Fallback: Prom/DCGM
    try:
        prom = PrometheusHTTP(cfg.PROMETHEUS_URL, timeout_s=timeout)
        pod: Optional[str] = None
        for q in (
            f'vllm:num_requests_running{{instance="{inst}"}}',
            f'vllm:request_success_total{{instance="{inst}"}}',
            f'vllm:generation_tokens_total{{instance="{inst}"}}',
        ):
            try:
                vec = prom.instant(q)
            except Exception:
                vec = []
            if vec:
                labels = vec[0].get("metric", {}) or {}
                pod = labels.get("pod")
                if pod:
                    break

        if pod:
            vec = prom.instant(f'avg(DCGM_FI_DEV_GPU_UTIL{{pod="{pod}"}})')
            if vec:
                v = vec[0].get("value", [None, None])
                return max(0.0, min(100.0, float(v[1])))

        vec = prom.instant("avg(DCGM_FI_DEV_GPU_UTIL)")
        if vec:
            v = vec[0].get("value", [None, None])
            return max(0.0, min(100.0, float(v[1])))
        return None
    except Exception:
        return None


# =======================
# Vector mapping helpers
# =======================

def _filter_by_instance_and_model(
    vec: List[Dict[str, Any]], instances: List[str], model: str
) -> Dict[str, float]:
    """
    Filter a Prom vector locally and map -> {instance: value_float}.
    - If 'instances' is empty, keep all.
    - If 'model' is non-empty, require metric['model_name'] == model *only if present*.
    """
    allowed = set(instances) if instances else None
    out: Dict[str, float] = {}
    for it in vec:
        labels = it.get("metric", {}) or {}
        inst = labels.get("instance")
        if not inst:
            continue
        if allowed is not None and inst not in allowed:
            continue
        if model and (labels.get("model_name") is not None) and labels.get("model_name") != model:
            continue
        ts_val = it.get("value", [None, None])
        val = ts_val[1] if isinstance(ts_val, list) and len(ts_val) >= 2 else None
        if val is None:
            continue
        try:
            f = float(val)
            if not (f != f):  # NaN
                out[inst] = f
        except Exception:
            continue
    return out


# =======================
# Metrics catalog
# =======================

METRICS_CATALOG: List[Dict[str, str]] = [
    # Gauges
    {"name": "vllm:num_requests_running", "kind": "gauge", "field": "requests_running"},
    {"name": "vllm:num_requests_waiting", "kind": "gauge", "field": "requests_waiting"},
    {"name": "vllm:gpu_cache_usage_perc", "kind": "gauge", "field": "gpu_kv_cache_usage_frac"},
    {"name": "vllm:cpu_cache_usage_perc", "kind": "gauge", "field": "cpu_kv_cache_usage_frac"},
    # Counters → rates
    {"name": "vllm:generation_tokens_total", "kind": "counter_rate", "field": "gen_tokens_per_sec"},
    {"name": "vllm:prompt_tokens_total", "kind": "counter_rate", "field": "prefill_tokens_per_sec"},
    {"name": "vllm:request_success_total", "kind": "counter_rate", "field": "request_success_per_sec"},
    {"name": "vllm:num_preemptions_total", "kind": "counter_rate", "field": "preemptions_per_sec"},
    # Speculative decoding counters → rates (present when SD enabled)
    {"name": "vllm:spec_decode_num_accepted_tokens_total", "kind": "counter_rate", "field": "spec_tokens_accepted_per_sec"},
    {"name": "vllm:spec_decode_num_draft_tokens_total", "kind": "counter_rate", "field": "spec_tokens_draft_per_sec"},
    {"name": "vllm:spec_decode_num_emitted_tokens_total", "kind": "counter_rate", "field": "spec_tokens_emitted_per_sec"},
    # Histograms (averages over window) — rate(_sum)/rate(_count)
    {"name": "vllm:time_to_first_token_seconds", "kind": "hist_avg", "field": "ttft_seconds_avg"},
    {"name": "vllm:time_per_output_token_seconds", "kind": "hist_avg", "field": "tpot_seconds_avg"},
    {"name": "vllm:e2e_request_latency_seconds", "kind": "hist_avg", "field": "e2e_latency_seconds_avg"},
    {"name": "vllm:request_queue_time_seconds", "kind": "hist_avg", "field": "queue_time_seconds_avg"},
    {"name": "vllm:request_inference_time_seconds", "kind": "hist_avg", "field": "inference_time_seconds_avg"},
    {"name": "vllm:request_prefill_time_seconds", "kind": "hist_avg", "field": "prefill_time_seconds_avg"},
    {"name": "vllm:request_decode_time_seconds", "kind": "hist_avg", "field": "decode_time_seconds_avg"},
    {"name": "vllm:request_prompt_tokens", "kind": "hist_avg", "field": "request_prompt_tokens_avg"},
    {"name": "vllm:request_generation_tokens", "kind": "hist_avg", "field": "request_generation_tokens_avg"},
    {"name": "vllm:request_max_num_generation_tokens", "kind": "hist_avg", "field": "request_max_generation_tokens_avg"},
    # DCGM by (pod)
    {"name": "avg by (pod) (max_over_time(DCGM_FI_DEV_GPU_UTIL[3s]))", "kind": "gauge", "field": "gpu_util_percent", "label": "pod"},
    {"name": "sum by (pod) (DCGM_FI_DEV_FB_USED)", "kind": "gauge", "field": "gpu_mem_used_mib", "label": "pod"},
    {"name": "(sum by (pod) (DCGM_FI_DEV_FB_USED)) / (sum by (pod) (DCGM_FI_DEV_FB_TOTAL))", "kind": "gauge", "field": "gpu_mem_util_frac", "label": "pod"},
    {"name": "avg by (pod) (DCGM_FI_DEV_GPU_TEMP)", "kind": "gauge", "field": "gpu_temp_c", "label": "pod"},
    {"name": "avg by (pod) (DCGM_FI_DEV_POWER_USAGE)", "kind": "gauge", "field": "gpu_power_watts", "label": "pod"},
]


# =======================
# Sampler thread
# =======================

class _MetricsSampler(threading.Thread):
    def __init__(self, mode: str, interval_s: float):
        super().__init__(daemon=True)
        self.mode = mode
        self.interval_s = max(0.2, float(interval_s))
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._eps: List[str] = []

        cfg = get_config()
        self._prom = PrometheusHTTP(cfg.PROMETHEUS_URL, cfg.PROM_TIMEOUT_S)

        run_dir = get_run_dir(mode)
        self._jsonl = JsonlLogger(f"{run_dir}/metrics.jsonl")

        # aggregates for a few key rollups (extend as needed)
        self._samples = 0
        self._sum_gpu_kv = 0.0
        self._sum_gen_tps = 0.0
        self._sum_prefill_tps = 0.0

    def set_endpoints(self, endpoints: List[str]):
        with self._lock:
            self._eps = list(endpoints)

    def stop(self):
        self._stop.set()

    # ---- low-level query helpers (no label filters in query) ----
    def _q_gauge(self, name: str) -> List[Dict[str, Any]]:
        return self._prom.instant(name)

    def _q_rate(self, name: str) -> List[Dict[str, Any]]:
        cfg = get_config()
        return self._prom.instant(f"rate({name}[{cfg.RATE_INTERVAL}])")

    def _q_hist_avg_pair(self, base: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        cfg = get_config()
        sum_vec = self._prom.instant(f"rate({base}_sum[{cfg.RATE_INTERVAL}])")
        cnt_vec = self._prom.instant(f"rate({base}_count[{cfg.RATE_INTERVAL}])")
        return sum_vec, cnt_vec

    @staticmethod
    def _map_vec(vec: List[Dict[str, Any]], instances: List[str]) -> Dict[str, float]:
        cfg = get_config()
        return _filter_by_instance_and_model(vec, instances, cfg.MODEL_NAME)

    def _map_vec_by_label(
        self,
        vec: List[Dict[str, Any]],
        instances: List[str],
        label_key: str,
        pod2inst: Dict[str, str],
    ) -> Dict[str, float]:
        """
        Map a Prometheus vector into {instance: value}.
        - If label_key == "instance": behaves like _map_vec (with relaxed model filter).
        - If label_key == "pod": translate via pod2inst (skip if we can't map).
        """
        cfg = get_config()
        if label_key == "instance":
            return _filter_by_instance_and_model(vec, instances, cfg.MODEL_NAME)

        allowed = set(instances) if instances else None
        out: Dict[str, float] = {}
        for it in vec:
            labels = it.get("metric", {}) or {}
            # Enforce model filter only if the series actually has model_name
            if (
                cfg.MODEL_NAME
                and (labels.get("model_name") is not None)
                and labels.get("model_name") != cfg.MODEL_NAME
            ):
                continue

            if label_key == "pod":
                pod = labels.get("pod")
                inst = pod2inst.get(pod) if pod else None
            else:
                inst = labels.get(label_key)

            if not inst:
                continue
            if allowed is not None and inst not in allowed:
                continue

            ts_val = it.get("value", [None, None])
            val = ts_val[1] if isinstance(ts_val, list) and len(ts_val) >= 2 else None
            if val is None:
                continue
            try:
                f = float(val)
                if not (f != f):  # NaN
                    out[inst] = f
            except Exception:
                continue
        return out

    @staticmethod
    def _divide_maps(num: Dict[str, float], den: Dict[str, float]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for k, n in num.items():
            d = den.get(k)
            if d and d != 0.0:
                out[k] = n / d
        return out

    def _build_pod2instance(self, raw_results: Dict[str, Any], instances: List[str]) -> Dict[str, str]:
        """
        Build a pod -> instance(ip:port) map.
        Priority:
          1) Any vLLM series present in raw_results that carry both 'pod' and 'instance' labels.
          2) Fallback: kube_pod_info (pod_ip) + infer port from the provided instances (use the first one's port).
        """
        pod2inst: Dict[str, str] = {}

        # Pass 1: infer from any vLLM metric vectors we already fetched this tick
        for name, vec in raw_results.items():
            if not isinstance(name, str) or not name.startswith("vllm:"):
                continue
            for it in vec or []:
                labels = it.get("metric", {}) or {}
                pod = labels.get("pod")
                inst = labels.get("instance")
                if pod and inst and pod not in pod2inst:
                    pod2inst[pod] = inst

        if pod2inst:
            return pod2inst

        # Pass 2 (fallback): use kube_pod_info to get pod_ip and combine with a known port
        port = None
        if instances:
            try:
                parts = instances[0].rsplit(":", 1)
                if len(parts) == 2 and parts[1].isdigit():
                    port = parts[1]
            except Exception:
                port = None

        if port is None:
            return pod2inst  # give up; we'll skip pod-keyed metrics this tick

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
            if pod not in pod2inst:
                pod2inst[pod] = inst

        return pod2inst

    def run(self):
        while not self._stop.is_set():
            with self._lock:
                instances = [_endpoint_to_instance(e) for e in self._eps]
            ts = datetime.now(timezone.utc).isoformat()
            samples_by_instance: Dict[str, Dict[str, Any]] = {}

            try:
                # 1) Fetch all vectors once per tick
                raw_results: Dict[str, Any] = {}
                for spec in METRICS_CATALOG:
                    name, kind = spec["name"], spec["kind"]
                    if kind == "gauge":
                        raw_results[name] = self._q_gauge(name)
                    elif kind == "counter_rate":
                        raw_results[name] = self._q_rate(name)
                    elif kind == "hist_avg":
                        raw_results[name + "_sum"], raw_results[name + "_count"] = self._q_hist_avg_pair(name)
                    else:
                        continue

                # Build pod->instance map for this tick (used by DCGM specs with label='pod')
                pod2inst = self._build_pod2instance(raw_results, instances)

                # 2) Filter and materialize per-instance fields
                for spec in METRICS_CATALOG:
                    name, kind, field = spec["name"], spec["kind"], spec["field"]
                    label_key = spec.get("label", "instance")

                    if kind == "hist_avg":
                        m_sum = self._map_vec_by_label(raw_results.get(name + "_sum", []), instances, label_key, pod2inst)
                        m_cnt = self._map_vec_by_label(raw_results.get(name + "_count", []), instances, label_key, pod2inst)
                        m_val = self._divide_maps(m_sum, m_cnt)
                    else:
                        m_val = self._map_vec_by_label(raw_results.get(name, []), instances, label_key, pod2inst)

                    # attach to per-instance record
                    keys = instances or list(m_val.keys())
                    for inst in keys:
                        rec = samples_by_instance.setdefault(inst, {"instance": inst})
                        if inst in m_val:
                            rec[field] = m_val[inst]
                        else:
                            rec.setdefault(field, None)

                # Override gpu_util_percent with local nvidia-smi if enabled
                cfg = get_config()
                if bool(cfg.USE_LOCAL_GPU_UTIL) and _LOCAL_GPU is not None:
                    # try to map instance -> pod using the raw vLLM metrics we just fetched
                    inst2pod: Dict[str, str] = {}
                    for name, vec in raw_results.items():
                        if not isinstance(name, str) or not name.startswith("vllm:"):
                            continue
                        for it in vec or []:
                            labels = it.get("metric", {}) or {}
                            inst_lbl = labels.get("instance")
                            pod_lbl = labels.get("pod")
                            if inst_lbl and pod_lbl and inst_lbl not in inst2pod:
                                inst2pod[inst_lbl] = pod_lbl

                    for inst, rec in samples_by_instance.items():
                        pod = inst2pod.get(inst)
                        if not pod:
                            continue
                        u = _LOCAL_GPU.avg_util_for_pod(pod)
                        if u is not None:
                            rec["gpu_util_percent"] = u

                # 3) Emit one line
                samples = list(samples_by_instance.values())
                self._jsonl.write({"ts": ts, "mode": self.mode, "samples": samples})

                # 4) Update a few rollups
                kv_vals = [rec.get("gpu_kv_cache_usage_frac") for rec in samples if rec.get("gpu_kv_cache_usage_frac") is not None]
                gen_vals = [rec.get("gen_tokens_per_sec") for rec in samples if rec.get("gen_tokens_per_sec") is not None]
                pre_vals = [rec.get("prefill_tokens_per_sec") for rec in samples if rec.get("prefill_tokens_per_sec") is not None]

                if kv_vals:
                    self._sum_gpu_kv += sum(kv_vals) / len(kv_vals)
                if gen_vals:
                    self._sum_gen_tps += sum(gen_vals)
                if pre_vals:
                    self._sum_prefill_tps += sum(pre_vals)
                self._samples += 1

            except Exception:
                # swallow one tick and continue
                pass

            time.sleep(self.interval_s)

    def summary(self) -> Dict[str, Any]:
        if self._samples == 0:
            return {}
        return {
            "vllm": {
                "overall_avg_gpu_kv_cache_usage_frac": self._sum_gpu_kv / self._samples,
                "overall_avg_generation_tps": self._sum_gen_tps / self._samples,
                "overall_avg_prefill_tps": self._sum_prefill_tps / self._samples,
                "samples": self._samples,
            }
        }


# =======================
# Registry + public API
# =======================

_samplers: Dict[str, _MetricsSampler] = {}
_lock = threading.Lock()


def start_metrics_collection(mode: str, metrics_path: str, interval_s: float) -> None:
    """
    Public API — signature unchanged.
    'metrics_path' is ignored in the Prometheus-backed sampler.
    """
    with _lock:
        s = _samplers.get(mode)
        if s:
            s.stop()
        s = _MetricsSampler(mode, interval_s)
        _samplers[mode] = s
        s.start()
        print(f"[METRICS] Started for mode={mode}, interval={interval_s}s")


def update_metrics_endpoints(mode: str, endpoints: List[str]):
    with _lock:
        if mode in _samplers:
            _samplers[mode].set_endpoints(endpoints)


def stop_metrics_collection(mode: str) -> Dict[str, Any]:
    with _lock:
        s = _samplers.pop(mode, None)
        if not s:
            return {}
        s.stop()
        time.sleep(min(0.05, s.interval_s))
        return s.summary()


def _get_metrics_summary(mode: str) -> Dict[str, Any]:
    with _lock:
        s = _samplers.get(mode)
        return s.summary() if s else {}


# Let util.save_summary() include current aggregates
register_metrics_getter(_get_metrics_summary)
