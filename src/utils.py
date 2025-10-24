# util.py
import json
import os
import time
import threading
import random
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests
from urllib.parse import urlparse
from config import get_config, dump_config_dict
from length_backend import compute_length_plan, rng_for_prompt, sample_out_tokens_from_cfg

_cfg = get_config()

# ---------------- Results directories + JSONL logging ----------------

# Use central config for results layout (ENV/JSON already handled there)
RESULTS_ROOT = _cfg.RESULTS_DIR
QUEUE_LOG_FILENAME = _cfg.QUEUE_LOG_FILENAME


def _mode_dir(mode: str) -> str:
    path = os.path.join(RESULTS_ROOT, mode)
    os.makedirs(path, exist_ok=True)
    return path


def _next_run_dir(mode: str) -> str:
    mode_path = _mode_dir(mode)
    existing = [
        int(d)
        for d in os.listdir(mode_path)
        if d.isdigit() and os.path.isdir(os.path.join(mode_path, d))
    ]
    next_id = (max(existing) + 1) if existing else 1
    run_path = os.path.join(mode_path, str(next_id))
    os.makedirs(run_path, exist_ok=True)

    #  snapshot effective config next to other artifacts
    cfg_path = os.path.join(run_path, "configs.json")
    try:
        if not os.path.exists(cfg_path):
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump(dump_config_dict(), f, indent=2)
    except Exception as e:
        print(f"[WARN] Could not write configs snapshot at {cfg_path}: {e}")

    return run_path


_run_dirs: Dict[str, str] = {}
_run_dirs_guard = threading.Lock()


def get_run_dir(mode: str) -> str:
    with _run_dirs_guard:
        if mode not in _run_dirs:
            _run_dirs[mode] = _next_run_dir(mode)
        return _run_dirs[mode]


class JsonlLogger:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(self.path, "a", encoding="utf-8"):
            pass

    def write(self, record: dict):
        line = json.dumps(record, ensure_ascii=False)
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line + "\n")


_loggers: Dict[str, JsonlLogger] = {}
_loggers_guard = threading.Lock()


def _get_logger_for_mode(mode: str) -> JsonlLogger:
    run_dir = get_run_dir(mode)
    path = os.path.join(run_dir, "output.jsonl")
    with _loggers_guard:
        if mode not in _loggers:
            _loggers[mode] = JsonlLogger(path)
        return _loggers[mode]


def log_result(
    *,
    mode: str,
    endpoint: str,
    model: str,
    prompt: str,
    status: str,
    response_preview: str | None = None,
    latency_s: float | None = None,
    error: str | None = None,
    extra: Dict[str, Any] | None = None,
) -> None:
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "endpoint": endpoint,
        "model": model,
        "status": status,
        "latency_s": latency_s,
        "prompt": prompt,
        "response_preview": response_preview,
        "error": error,
    }
    if extra:
        rec.update(extra)
    _get_logger_for_mode(mode).write(rec)


# ---- queue/batching telemetry helper (same folder as mode logs) ----

_queue_loggers: Dict[str, JsonlLogger] = {}
_queue_loggers_guard = threading.Lock()


def _get_queue_logger_for_mode(router_mode: str) -> JsonlLogger:
    """
    Returns a logger that writes to <results>/<router_mode>/<run_id>/<QUEUE_LOG_FILENAME>.
    This keeps queue telemetry colocated with the router's regular logs.
    """
    run_dir = get_run_dir(router_mode)
    path = os.path.join(run_dir, QUEUE_LOG_FILENAME)
    key = f"{router_mode}::{path}"
    with _queue_loggers_guard:
        if key not in _queue_loggers:
            _queue_loggers[key] = JsonlLogger(path)
        return _queue_loggers[key]


def log_queue(
    *,
    router_mode: str,
    endpoint: str,
    event: str,
    extra: Dict[str, Any] | None = None,
) -> None:
    """
    Structured queue/batching telemetry writer.

    Location:
      results/<router_mode>/<run_id>/{QUEUE_LOG_FILENAME}

    Fields:
      - router_mode: which router emitted the event (stored in 'model')
      - endpoint: target endpoint/pod
      - event: short string like 'pull-batch', 'push-step', 'route-rr', etc.
      - extra: dict of numeric counters (want, pulled, q_before, q_after, inflight_*,
               util_pct, busy, added, etc.)
    """
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "mode": "queue-log",  # logical stream name
        "endpoint": endpoint,
        "model": router_mode,  # which router emitted the event
        "status": "queue",
        "prompt": event,  # event type
    }
    if extra:
        rec.update(extra)
    _get_queue_logger_for_mode(router_mode).write(rec)


# ---- metrics summary registry (hooked by prom_utils) ----
_metrics_summary_getter: Optional[callable] = None


def register_metrics_getter(getter: callable) -> None:
    global _metrics_summary_getter
    _metrics_summary_getter = getter


def get_metrics_summary(mode: str) -> Dict[str, Any]:
    if _metrics_summary_getter is None:
        return {}
    return _metrics_summary_getter(mode) or {}


def save_summary(mode: str, summary: dict) -> None:
    metrics = get_metrics_summary(mode)
    if metrics:
        summary = {**summary, "metrics": metrics}
    run_dir = get_run_dir(mode)
    path = os.path.join(run_dir, "results.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"[SUMMARY] Saved {path}")


# ---------------- HTTP helpers ----------------

# --- helpers: detect HTTP sim endpoints from config ---
def _count_sim_eps_from_cfg(cfg) -> int:
    sim = cfg.SIM_ENDPOINTS
    total = 0
    if isinstance(sim, dict):
        for k in sim.keys():
            spec = sim[k] or {}
            total += int(spec.get("count", 1))
    elif isinstance(sim, list):
        for item in sim:
            if isinstance(item, str) and "@" in item:
                total += 1
    return total


def _is_http_sim_endpoint(endpoint: str, cfg) -> bool:
    """
    True if endpoint is one of http://SIM_HTTP_HOST:(SIM_HTTP_PORT_BASE .. base+count-1)
    derived from SIM_ENDPOINTS.
    """
    try:
        host = getattr(cfg, "SIM_HTTP_HOST", "127.0.0.1")
        base = int(getattr(cfg, "SIM_HTTP_PORT_BASE", 9101))
        total = _count_sim_eps_from_cfg(cfg)
        if total <= 0:
            return False

        p = urlparse(endpoint)
        if p.scheme != "http":
            return False
        # p.hostname can be None for malformed URLs
        if (p.hostname or "") != host:
            return False
        port = p.port or 80
        return base <= port < (base + total)
    except Exception:
        return False


# --- REPLACE your healthy() with this version ---
def healthy(
    endpoint: str, health_path: str = "/health", timeout_s: float | None = None
) -> bool:
    """
    Returns True if endpoint responds 2xx on /health.

    In SIM_MODE=only:
      - sim:// endpoints -> True (assume OK)
      - HTTP sim endpoints (from SIM_HTTP_HOST + SIM_HTTP_PORT_BASE range) -> allowed
      - anything else (real) -> return False (DO NOT raise) so workers skip gracefully.
    """
    cfg = get_config()
    timeout = float(timeout_s if timeout_s is not None else cfg.HEALTH_TIMEOUT_S)

    # in-process sim endpoints, if you still use them anywhere
    if endpoint.startswith("sim://"):
        return True

    # allow HTTP-sim endpoints in SIM-only mode
    is_http_sim = _is_http_sim_endpoint(endpoint, cfg)

    # Block true-real endpoints *silently* in SIM-only mode
    if str(cfg.SIM_MODE).lower() == "only" and cfg.SIM_ENDPOINTS and not is_http_sim:
        return False

    # For allowed endpoints, probe /health
    try:
        r = requests.get(endpoint.rstrip("/") + health_path, timeout=timeout)
        return bool(r.ok)
    except Exception:
        return False

def send_chat_request(
    *,
    endpoint: str,
    model: Optional[str] = None,
    messages: Optional[List[Dict[str, Any]]] = None,
    max_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    request_timeout_s: Optional[int] = None,
    # Optional per-call overrides (fall back to config if None)
    top_p: Optional[float] = None,
    top_k: Optional[bool] = None,
    do_sample: Optional[bool] = None,
    n: Optional[int] = None,
    stream: Optional[bool] = None,
    presence_penalty: Optional[float] = None,
    frequency_penalty: Optional[float] = None,
    seed: Optional[float] = None,
    stop: Optional[List[str]] = None,
    logprobs: Optional[int] = None,
    logit_bias: Optional[Dict[str, float]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    # NEW: persistent HTTP connection (if provided, use keep-alive & pooling)
    session: "requests.Session | None" = None,
    # Unified length control — let the policy compute the plan.
    target_output_tokens: Optional[int] = None,
    target_total_tokens: Optional[int] = None,
    ignore_eos: Optional[bool] = None,
    length_mode: Optional[str] = None,
    # Keep req_id for determinism/traceability (strict-hist etc.)
    req_id: Optional[int] = None,
) -> dict:
    cfg = get_config()

    # Extract a plain prompt (best effort; prefer last user message)
    plain_prompt = ""
    if messages and len(messages) > 0:
        try:
            last_user = None
            for m in reversed(messages):
                if (m or {}).get("role") == "user":
                    last_user = m
                    break
            src = last_user if last_user is not None else (messages[0] or {})
            plain_prompt = src.get("content", "") or ""
        except Exception:
            plain_prompt = ""

    # Resolve base cap
    base_cap = int(max_tokens if max_tokens is not None else getattr(cfg, "MAX_TOKENS", 0) or 0)
    if base_cap < 0:
        base_cap = 0

    # Build unified plan (keeps new features) + carry req_id
    plan = compute_length_plan(
        plain_prompt=plain_prompt,
        base_cap=base_cap,
        target_output_tokens=target_output_tokens,
        target_total_tokens=target_total_tokens,
        ignore_eos=ignore_eos,
        length_mode=length_mode,
        req_id=req_id,
    )

    # --- SIM guard (no local synth; we want to POST to HTTP-sim) ----------------
    is_http_sim = _is_http_sim_endpoint(endpoint, cfg)
    sim_flag = str(getattr(cfg, "SIM_MODE", "")).lower().strip()
    if sim_flag == "only" and cfg.SIM_ENDPOINTS and not is_http_sim:
        # In SIM-only runs, disallow *real* endpoints, but allow HTTP-sim endpoints
        raise RuntimeError(f"SIM_MODE=only but router tried real endpoint: {endpoint}")
    # ---------------------------------------------------------------------------

    # Compose OpenAI-compatible payload for the server
    payload = {
        "model": model if model is not None else cfg.MODEL_NAME,
        "messages": messages if messages is not None else [],
        "max_tokens": int(plan["eff_max"]),
        "temperature": temperature if temperature is not None else cfg.TEMPERATURE,
        "top_p": top_p if top_p is not None else cfg.TOP_P,
        "top_k": top_k if top_k is not None else cfg.TOP_K,
        "presence_penalty": (presence_penalty if presence_penalty is not None else cfg.PRESENCE_PENALTY),
        "frequency_penalty": (frequency_penalty if frequency_penalty is not None else cfg.FREQUENCY_PENALTY),
        "seed": seed if seed is not None else cfg.SEED,
        "n": n if n is not None else cfg.N,
        "stream": stream if stream is not None else cfg.STREAM,
        "do_sample": do_sample if do_sample is not None else cfg.DO_SAMPLE,
    }
    if plan["eff_ignore_eos"]:
        payload["ignore_eos"] = True

    eff_stop = stop if stop is not None else (cfg.STOP or None)
    if eff_stop:
        payload["stop"] = eff_stop
    eff_logprobs = logprobs if logprobs is not None else cfg.LOGPROBS
    if isinstance(eff_logprobs, int) and eff_logprobs > 0:
        payload["logprobs"] = eff_logprobs
    eff_logit_bias = logit_bias if logit_bias is not None else (cfg.LOGIT_BIAS or None)
    if eff_logit_bias:
        payload["logit_bias"] = eff_logit_bias

    headers = {}
    if getattr(cfg, "VLLM_AUTH_BEARER", ""):
        headers["Authorization"] = f"Bearer {cfg.VLLM_AUTH_BEARER}"
    if extra_headers:
        headers.update(extra_headers)

    url = endpoint.rstrip("/") + cfg.VLLM_CHAT_PATH
    timeout = (request_timeout_s if request_timeout_s is not None else cfg.REQUEST_TIMEOUT_S)

    # Optional: uncomment for quick visibility
    # print(f"[SEND*HTTP] POST {url} timeout={timeout} req_id={req_id}")

    # Reuse a persistent session if provided (prevents bursty new TCP handshakes)
    if session is not None:
        r = session.post(url, json=payload, headers=(headers or None), timeout=timeout)
    else:
        r = requests.post(url, json=payload, headers=(headers or None), timeout=timeout)

    # Optional: nicer error body for debugging
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code} {r.reason}: {r.text[:500]}")

    resp = r.json()

    # Attach usage-style fields if server returns them
    usage = resp.get("usage") or {}
    resp["_prompt_tokens"] = usage.get("prompt_tokens")
    resp["_completion_tokens"] = usage.get("completion_tokens")
    resp["_total_tokens"] = usage.get("total_tokens")

    # Attach plan meta + req_id (for analysis / strict-hist trace)
    resp.update(plan.get("meta", {}))
    if req_id is not None:
        resp["_req_id"] = int(req_id)
    return resp




DEFAULT_PROMPTS_FILE = _cfg.PROMPTS_FILE_PATH


def _read_json_or_jsonl(path: str) -> List[str]:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".jsonl":
        items: List[str] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, str):
                        items.append(obj)
                    elif isinstance(obj, dict) and "prompt" in obj:
                        items.append(str(obj["prompt"]))
                    else:
                        items.append(str(obj))
                except Exception:
                    items.append(line)  # tolerate raw text
        return items

    # JSON
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return [str(x) for x in data]
    if (
        isinstance(data, dict)
        and "prompts" in data
        and isinstance(data["prompts"], list)
    ):
        return [str(x) for x in data["prompts"]]
    raise ValueError(
        "Prompts file must be a JSON list / JSONL, or an object with key 'prompts'."
    )


def load_prompts(path: Optional[str] = None) -> deque:
    cfg = get_config()
    path = path or cfg.PROMPTS_FILE_PATH
    items = _read_json_or_jsonl(path)

    if cfg.PROMPTS_SHUFFLE:
        rnd = random.Random(cfg.PROMPTS_SEED)
        rnd.shuffle(items)

    if cfg.PROMPTS_LIMIT is not None and cfg.PROMPTS_LIMIT >= 0:
        items = items[: cfg.PROMPTS_LIMIT]

    if not items:
        raise ValueError(f"No prompts found after applying settings (file={path}).")

    return deque(items)


def _count_sim_eps_from_cfg(cfg) -> int:
    sim = cfg.SIM_ENDPOINTS
    total = 0
    if isinstance(sim, dict):
        for k in sim.keys():
            spec = sim[k] or {}
            total += int(spec.get("count", 1))
    elif isinstance(sim, list):
        for item in sim:
            if isinstance(item, str) and "@" in item:
                total += 1
    return total


def _is_http_sim_endpoint(endpoint: str, cfg) -> bool:
    host = getattr(cfg, "SIM_HTTP_HOST", "127.0.0.1")
    base = int(getattr(cfg, "SIM_HTTP_PORT_BASE", 9101))
    total = _count_sim_eps_from_cfg(cfg)
    if total <= 0:
        return False
    try:
        parsed = urlparse(endpoint)
        if parsed.scheme != "http":
            return False
        hostport = parsed.netloc.split(":")
        if len(hostport) != 2:
            return False
        ep_host, ep_port = hostport[0], int(hostport[1])
        return (ep_host == host) and (base <= ep_port < base + total)
    except Exception:
        return False

# ---- AUTOSCALE LOGGER ----

_autoscale_loggers: Dict[str, JsonlLogger] = {}
_autoscale_loggers_guard = threading.Lock()
AUTOSCALE_LOG_FILENAME = getattr(_cfg, "AUTOSCALE_LOG_FILENAME", "autoscale.jsonl")

def _get_autoscale_logger_for_mode(router_mode: str) -> JsonlLogger:
    """
    Writes to results/<router_mode>/<run_id>/<AUTOSCALE_LOG_FILENAME>.
    """
    run_dir = get_run_dir(router_mode)
    path = os.path.join(run_dir, AUTOSCALE_LOG_FILENAME)
    key = f"{router_mode}::{path}"
    with _autoscale_loggers_guard:
        if key not in _autoscale_loggers:
            _autoscale_loggers[key] = JsonlLogger(path)
        return _autoscale_loggers[key]


def log_autoscale(
    *,
    router_mode: str,
    desired_servers: int,
    realized_servers: int,
    total_eps: int,
    active_eps: int,
    draining_eps: int,
    queue_len: int,
    inflight: int,
    reason: str,
):
    """
    Writes autoscale telemetry both to stdout and to:
      results/<router_mode>/<run_id>/<AUTOSCALE_LOG_FILENAME>

    Each record is a single JSON line with clear fields.
    """
    now = datetime.now(timezone.utc).isoformat()
    record = {
        "ts": now,
        "router_mode": router_mode,
        "reason": reason,
        "desired_servers": int(desired_servers),
        "realized_servers": int(realized_servers),
        "total_eps": int(total_eps),
        "active_eps": int(active_eps),
        "draining_eps": int(draining_eps),
        "queue_len": int(queue_len),
        "inflight": int(inflight),
    }

    # Print live to stdout (unbuffered)
    print(json.dumps(record, ensure_ascii=False), flush=True)

    # Write to the per-run autoscale.jsonl in results/<mode>/<run_id>/
    try:
        logger = _get_autoscale_logger_for_mode(router_mode)
        logger.write(record)
    except Exception as e:
        print(f"[WARN] autoscale log failed: {e}", flush=True)
