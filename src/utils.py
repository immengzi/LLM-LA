# util.py
import json
import os
import time
from enum import Enum
import re
import threading
import random
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from urllib.parse import urlparse

from config import get_config, dump_config_dict
from length_backend import compute_length_plan, rng_for_prompt, sample_out_tokens_from_cfg

_cfg = get_config()

# ---------------- Results directories + JSONL logging ----------------

# Use central config for results layout (ENV/JSON already handled there)
RESULTS_ROOT = _cfg.RESULTS_DIR
QUEUE_LOG_FILENAME = _cfg.QUEUE_LOG_FILENAME


class _PayloadMode(str, Enum):
    OFF = "off"
    HEAD = "head"
    FULL = "full"


def _redact_text(text: str | None) -> str | None:
    # TODO: add patterns for secrets, emails, tokens, etc.
    return text


def _clip_by_mode(text: str | None, mode: str, head_chars: int) -> tuple[str | None, bool]:
    """
    Returns (text_to_log, was_truncated).
    - OFF  => (None, False)
    - HEAD => first head_chars with ellipsis if needed
    - FULL => full text
    """
    if not text:
        return text, False
    m = (mode or "").lower()
    if m == _PayloadMode.OFF:
        return None, False
    if m == _PayloadMode.FULL or len(text) <= head_chars:
        return text, False
    return text[:max(0, int(head_chars))] + "… [truncated]", True


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


# ---- unified request/response logger ----
def log_result(
    *,
    mode: str,
    endpoint: str,
    model: str,
    status: str,
    prompt: str | None = None,
    response: str | None = None,
    response_preview: str | None = None,  # kept for backward compat; used only if response is None
    latency_s: float | None = None,
    error: str | None = None,
    extra: Dict[str, Any] | None = None,
    **_ignored,
) -> None:
    """
    Writes one JSONL record to results/<mode>/<run_id>/output.jsonl

    Controls:
      - LOG_PAYLOAD_MODE: "off" | "head" | "full"
      - LOG_HEAD_CHARS: int  (used when mode == "head")

    Behavior:
      - FULL  -> emit 'prompt' + 'response' only
      - HEAD  -> emit 'prompt' (clipped) + 'response_preview' only
      - OFF   -> emit neither prompt nor response fields
      - 'truncated' is present only for FULL/HEAD; in FULL it's all False, in HEAD reflects clipping.
    """
    cfg = get_config()
    payload_mode = _PayloadMode(str(getattr(cfg, "LOG_PAYLOAD_MODE", "head")).lower())
    head_chars = int(getattr(cfg, "LOG_HEAD_CHARS", 512))

    # Prefer explicit response; else accept legacy response_preview once
    if response is None and response_preview is not None:
        response = response_preview

    # Redact first
    prompt = _redact_text(prompt)
    response = _redact_text(response)

    # Clip according to mode for prompt/response separately
    # NOTE: _clip_by_mode expects a str for 'mode', so pass payload_mode.value
    clipped_prompt, prompt_trunc = _clip_by_mode(prompt, payload_mode.value, head_chars)
    clipped_resp, resp_trunc = _clip_by_mode(response, payload_mode.value, head_chars)

    record: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "endpoint": endpoint,
        "model": model,
        "status": status,
        "latency_s": latency_s,
        "error": error,
    }
    if extra:
        record.update(extra)

    if payload_mode == _PayloadMode.FULL:
        # Only 'response' + full (or untruncated) prompt
        record["prompt"] = clipped_prompt
        record["response"] = clipped_resp
        record["truncated"] = {
            "prompt": False,
            "response": False,
        }

    elif payload_mode == _PayloadMode.HEAD:
        # Only 'response_preview' + clipped prompt
        record["prompt"] = clipped_prompt
        record["response_preview"] = clipped_resp or ""
        record["truncated"] = {
            "prompt": bool(prompt_trunc),
            "response": bool(resp_trunc),
        }

    else:  # _PayloadMode.OFF
        # No payloads
        pass

    # Final invariant guard
    assert not (("response" in record) and ("response_preview" in record)), \
        "Invariant violated: both response and response_preview set."

    _get_logger_for_mode(mode).write(record)


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
        "mode": "queue-log",
        "endpoint": endpoint,
        "model": router_mode,
        "status": "queue",
        "prompt": event,
    }
    if extra:
        rec.update(extra)
    _get_queue_logger_for_mode(router_mode).write(rec)


# ---- LOAD LOGGER (unified: summary + per-arrival timing) ----

_load_loggers: Dict[str, JsonlLogger] = {}
_load_loggers_guard = threading.Lock()
LOAD_LOG_FILENAME = getattr(_cfg, "LOAD_LOG_FILENAME", "load.jsonl")


def _get_load_logger_for_mode(router_mode: str) -> JsonlLogger:
    """
    Writes to results/<router_mode>/<run_id>/load.jsonl (or cfg.LOAD_LOG_FILENAME).
    """
    run_dir = get_run_dir(router_mode)
    path = os.path.join(run_dir, LOAD_LOG_FILENAME)
    key = f"{router_mode}::{path}"
    with _load_loggers_guard:
        if key not in _load_loggers:
            _load_loggers[key] = JsonlLogger(path)
        return _load_loggers[key]


def log_load(
    *,
    router_mode: str,
    event: str,                      # "start" | "arrival" | "done" | optional others
    pattern: str | None = None,
    phase: str | None = None,        # "warmup" | "dump" | "main"
    rps: float | None = None,        # coarse rps for the event (if applicable)
    extra: Dict[str, Any] | None = None,
    # per-arrival timing (optional)
    idx: int | None = None,
    uid: str | None = None,
    second: int | None = None,
    rps_effective: float | None = None,
    planned_at: float | None = None,
    woke_at: float | None = None,
    enq_at: float | None = None,
) -> None:
    """
    Unified load telemetry.

    Location:
      results/<router_mode>/<run_id>/load.jsonl

    Fields per record:
      - ts, router_mode, event
      - pattern, phase, rps (optional)
      - extra (free-form)
      - idx, uid, second, rps_effective, planned_at, woke_at, enq_at (optional per-arrival timing)
    """
    rec: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "router_mode": router_mode,
        "event": event,
    }
    if pattern is not None:
        rec["pattern"] = pattern
    if phase is not None:
        rec["phase"] = phase
    if rps is not None:
        rec["rps"] = float(rps)
    if extra:
        rec.update(extra)

    if idx is not None:
        rec["idx"] = int(idx)
    if uid is not None:
        rec["uid"] = str(uid)
    if second is not None:
        rec["second"] = int(second)
    if rps_effective is not None:
        rec["rps_effective"] = float(rps_effective)
    if planned_at is not None:
        rec["planned_at"] = float(planned_at)
    if woke_at is not None:
        rec["woke_at"] = float(woke_at)
    if enq_at is not None:
        rec["enq_at"] = float(enq_at)

    try:
        _get_load_logger_for_mode(router_mode).write(rec)
    except Exception as e:
        print(f"[WARN] load log failed: {e}", flush=True)


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
        if (p.hostname or "") != host:
            return False
        port = p.port or 80
        return base <= port < (base + total)
    except Exception:
        return False


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

    if endpoint.startswith("sim://"):
        return True

    is_http_sim = _is_http_sim_endpoint(endpoint, cfg)

    if str(cfg.SIM_MODE).lower() == "only" and cfg.SIM_ENDPOINTS and not is_http_sim:
        return False

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
    session: "requests.Session | None" = None,
    target_output_tokens: Optional[int] = None,
    target_total_tokens: Optional[int] = None,
    ignore_eos: Optional[bool] = None,
    length_mode: Optional[str] = None,
    req_id: Optional[int] = None,
) -> dict:
    cfg = get_config()

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

    base_cap = int(max_tokens if max_tokens is not None else getattr(cfg, "MAX_TOKENS", 0) or 0)
    if base_cap < 0:
        base_cap = 0

    plan = compute_length_plan(
        plain_prompt=plain_prompt,
        base_cap=base_cap,
        target_output_tokens=target_output_tokens,
        target_total_tokens=target_total_tokens,
        ignore_eos=ignore_eos,
        length_mode=length_mode,
        req_id=req_id,
    )

    is_http_sim = _is_http_sim_endpoint(endpoint, cfg)
    sim_flag = str(getattr(cfg, "SIM_MODE", "")).lower().strip()
    if sim_flag == "only" and cfg.SIM_ENDPOINTS and not is_http_sim:
        raise RuntimeError(f"SIM_MODE=only but router tried real endpoint: {endpoint}")

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
        "chat_template_kwargs": {"enable_thinking": cfg.THINK},
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

    if session is not None:
        r = session.post(url, json=payload, headers=(headers or None), timeout=timeout)
    else:
        r = requests.post(url, json=payload, headers=(headers or None), timeout=timeout)

    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code} {r.reason}: {r.text[:500]}")

    resp = r.json()

    usage = resp.get("usage") or {}
    resp["_prompt_tokens"] = usage.get("prompt_tokens")
    resp["_completion_tokens"] = usage.get("completion_tokens")
    resp["_total_tokens"] = usage.get("total_tokens")

    resp.update(plan.get("meta", {}))
    if req_id is not None:
        resp["_req_id"] = int(req_id)
    return resp


# ---- KV-prefix hash client ----

def compute_hashes_for_prompt(
    prompt: str,
    timeout: float = 10.0,
) -> Tuple[List[int], List[int]]:
    """
    Call the CPU hash service and return (block_hashes, token_ids).

    The service is expected to accept:
        POST HASH_SERVICE_URL
        {
            "messages": [{"role": "user", "content": "<prompt>"}]
        }

    And return:
        {
            "block_hashes": [int, ...],
            "token_ids": [int, ...]
        }

    URL is taken from get_config().HASH_SERVICE_URL, with a safe default.
    """
    cfg = get_config()
    url = getattr(cfg, "HASH_SERVICE_URL", "http://127.0.0.1:30095/compute_hashes")

    payload = {
        "messages": [
            {"role": "user", "content": prompt},
        ]
    }

    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json() or {}

    block_hashes = [int(bh) for bh in (data.get("block_hashes") or [])]
    token_ids = [int(t) for t in (data.get("token_ids") or [])]

    return block_hashes, token_ids


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
                    items.append(line)
        return items

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

    print(json.dumps(record, ensure_ascii=False), flush=True)

    try:
        logger = _get_autoscale_logger_for_mode(router_mode)
        logger.write(record)
    except Exception as e:
        print(f"[WARN] autoscale log failed: {e}", flush=True)
