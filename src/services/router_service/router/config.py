# router/config.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional
import os
import sys

# Global singleton to avoid recomputing config multiple times
_CONFIG = None


# -----------------------------------------------------------------------
# Multi-model registry (loaded from a shared ConfigMap-mounted YAML file)
# -----------------------------------------------------------------------

@dataclass
class ModelEntry:
    """One entry from model_list in the shared models.yaml."""
    name: str
    label_selector: str = ""
    batch_size: int = 0


_MODEL_REGISTRY: Optional[Dict[str, ModelEntry]] = None


def load_model_registry(path: str) -> Dict[str, ModelEntry]:
    """
    Parse models.yaml (shared ConfigMap format) and build a registry
    keyed by model_name.  Each entry's router_params is extracted;
    litellm_params is ignored (consumed by BooM/LiteLLM only).
    """
    import yaml

    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}

    model_list = data.get("model_list") or []
    registry: Dict[str, ModelEntry] = {}
    for item in model_list:
        name = item.get("model_name", "").strip()
        if not name:
            continue
        rp = item.get("router_params") or {}
        registry[name] = ModelEntry(
            name=name,
            label_selector=str(rp.get("label_selector", "")),
            batch_size=int(rp.get("batch_size", 0)),
        )
    return registry


def get_model_registry() -> Optional[Dict[str, ModelEntry]]:
    """
    Return the loaded model registry, or None if multi-model is not enabled.
    """
    return _MODEL_REGISTRY


def get_known_models() -> List[str]:
    """Return sorted list of registered model names (empty if single-model mode)."""
    if _MODEL_REGISTRY is None:
        return []
    return sorted(_MODEL_REGISTRY.keys())


@dataclass
class RouterConfig:
    # Basic
    HOST: str = "0.0.0.0"
    PORT: int = 8080

    # API key for /v1/chat/completions (empty = no auth required)
    API_KEY: str = ""

    # Redis (for KV watcher)
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    MODEL_NAME: str = "served-model"

    # Inline KV-block hash computation.
    # The tokenizer path must point at the same model directory vLLM uses.
    KV_TOKENIZER_PATH: str = "/model"
    KV_BLOCK_SIZE: int = 128

    # KV-block hash source:
    #   "inline"   -> compute in-process via prefix_hash.py (default).
    #   "external" -> call the legacy vllm-cpu-hash service over HTTP.
    KV_HASH_SOURCE: str = "inline"
    HASH_SERVICE_URL: str = "http://vllm-cpu-hash:9095"

    # vLLM discovery (for KV watcher to map pod -> endpoint)
    NAMESPACE: str = "vllm"
    LABEL_SELECTOR: str = "app=vllm-qwen"
    VLLM_PORT: int = 8200

    # KV watcher controls
    KV_WATCH_INTERVAL_S: float = 1.0
    KV_WATCH_MAX_KEYS: int = 200
    KV_DISCOVERY_INTERVAL_S: float = 5.0
    KV_LOG_KEYS: str = "off"  # off | summary | full

    # --------------------------------------------------------------------
    # UNIFIED ROUTING STRATEGY (single selector for the two KV mechanisms)
    # "" (unset) -> use the legacy KV_AWARE / AFFINITY_ENABLED flags as-is.
    #   none      -> prefix off, affinity off
    #   prefix    -> prefix on,  affinity off
    #   affinity  -> prefix off, affinity on
    #   both      -> prefix on,  affinity on
    # When non-empty this OVERRIDES KV_AWARE / AFFINITY_ENABLED. AFFINITY_MODE
    # (soft|hard) remains a separate modifier applied when affinity is on.
    # --------------------------------------------------------------------
    ROUTER_STRATEGY: str = ""

    # Routing knobs
    KV_AWARE: bool = True
    LEN_AWARE: bool = True
    LEN_POLICY: str = "short_first"  # short_first | long_first

    # --------------------------------------------------------------------
    # KEY AFFINITY ROUTING (conversation stickiness)
    # Keep all turns of one conversation on the same vLLM pod. Off by
    # default; fully router-side (no BooM/sidecar/client changes).
    # --------------------------------------------------------------------
    AFFINITY_ENABLED: bool = False
    AFFINITY_MODE: str = "soft"          # soft (preference) | hard (time-bounded pin)
    AFFINITY_TTL_S: float = 300.0        # conversation->endpoint mapping lifetime
    AFFINITY_HARD_TIMEOUT_S: float = 5.0  # hard mode: max hold before releasing to any pod

    # how many items to scan vs want
    POOL_FACTOR: int = 4
    POOL_BIDIRECTIONAL: bool = False

    # batch size proxy: expected output tokens (for predictor)
    DEFAULT_MAX_TOKENS: int = 1024

    # --------------------------------------------------------------------
    # Router mode + sidecar port
    # --------------------------------------------------------------------
    ROUTER_MODE: str = "pull"        # "pull", "push-rr", "push-random", "push-leastq"
    SIDECAR_PORT: int = 9000         # sidecar FastAPI port

    # --------------------------------------------------------------------
    # Synchronous response flow (existing /enqueue)
    # --------------------------------------------------------------------
    RESULT_TIMEOUT_S: float = 60.0
    RESULT_POLL_INTERVAL_S: float = 0.02  # (kept for compatibility; not used by ACK+poll)

    # --------------------------------------------------------------------
    # ACK+POLL / retention support (used by router_state cleanup too)
    # --------------------------------------------------------------------
    POLL_RESULT_TTL_S: float = 300.0
    POLL_CLEANUP_INTERVAL_S: float = 1.0

    # --------------------------------------------------------------------
    # Sidecar -> router result ingestion transport
    # --------------------------------------------------------------------
    RESULT_TRANSPORT_MODE: str = "sync"         # sync | submit_ack
    RESULT_SUBMIT_PATH: str = "/result_submit"  # only used when RESULT_TRANSPORT_MODE=submit_ack

    # --------------------------------------------------------------------
    # ASYNC PUBSUB transport (client submit + ZMQ publish results)
    # --------------------------------------------------------------------
    TRANSPORT_MODE: str = "sync"          # "sync" | "async_pubsub"
    SUBMIT_PATH: str = "/submit"          # client submit endpoint (FastAPI path)
    RESULTS_ZMQ_BIND: str = "tcp://0.0.0.0:5559"  # router PUB bind
    RESULTS_ZMQ_TOPIC: str = "results"    # pubsub topic prefix
    RESULTS_ZMQ_HWM: int = 100000         # high-water mark (best-effort safety)
    RESULTS_GRACE_S: float = 30.0         # client-side wait-after-submit; router doesn't enforce

    # --------------------------------------------------------------------
    # HTTP timeouts
    # --------------------------------------------------------------------
    PUSH_HTTP_TIMEOUT_S: float = 2.0     # push-mode health + push

    # --------------------------------------------------------------------
    # HTTP connection pooling / keepalive knobs (NO semantic changes)
    # --------------------------------------------------------------------
    PUSH_MAX_KEEPALIVE: int = 200
    PUSH_KEEPALIVE_EXPIRY_S: float = 30.0

    # --------------------------------------------------------------------
    # Push least-queue behavior
    # --------------------------------------------------------------------
    PUSH_LEASTQ_MODE: str = "health"     # health | local

    # --------------------------------------------------------------------
    # Push-mode decoupling (ACK fast; dispatch in background)
    # --------------------------------------------------------------------
    PUSH_DECOUPLE_DISPATCH: bool = True
    PUSH_DISPATCH_QUEUE_MAX: int = 100000
    PUSH_DISPATCH_WORKERS: int = 32
    PUSH_DISPATCH_MAX_DELAY_S: float = 60.0

    # --------------------------------------------------------------------
    # Per-request routing logs
    # --------------------------------------------------------------------
    REQ_LOG_MODE: str = "off"   # off | summary | full

    # --------------------------------------------------------------------
    # TRACE SYSTEM
    # --------------------------------------------------------------------
    TRACE_ENABLED: bool = False
    TRACE_SAMPLING_RATE: float = 1.0

    # When true, the /latency_log ring includes the full per-request prefix
    # block-hash list (bulky). Counts (kv_hits_len/total_blocks) are always
    # included; this only controls the raw hash list.
    ROUTER_LOG_BLOCK_HASHES: bool = False

    # --------------------------------------------------------------------
    # SLO-AWARE ROUTING
    # --------------------------------------------------------------------
    SLO_AWARE: bool = False
    SLO_WITH_KV: bool = True
    ADMISSION_THROTTLE: bool = False
    FIXED_BATCH_SIZE: int = 0               # 0 = no cap

    # Output length predictor
    OUTPUT_LEN_PREDICTOR: str = "simple"    # simple | distribution | regression | hint_only
    # Batch size estimation
    BATCH_SIZE_ESTIMATE: str = "fixed"      # fixed | inflight | reported
    FIXED_BATCH_ESTIMATE: int = 8

    # Latency predictor
    LATENCY_PREDICTOR: str = "linear"       # linear | piecewise | bayesian | hybrid
    LATENCY_ONLINE_UPDATE: bool = False
    LATENCY_PROFILE_PATH: str = ""          # path to latency_profile.json

    # Queue wait model
    QUEUE_WAIT_MODEL: str = "none"          # none | simple | drain_rate
    # Chunked prefill correction
    CHUNKED_PREFILL_AWARE: bool = False
    MAX_NUM_BATCHED_TOKENS: int = 0

    # Multi-model: path to shared models.yaml (empty = single-model legacy)
    MODEL_CONFIG_PATH: str = ""


def _norm_mode(s: str) -> str:
    return str(s or "").strip().lower()


def _norm_log_mode(s: str) -> str:
    s = str(s or "").strip().lower()
    if s not in ("off", "summary", "full"):
        return "off"
    return s


def get_config() -> RouterConfig:
    """
    Return a singleton RouterConfig with env overrides applied.
    Does NOT print anything (printing is done explicitly by callers).
    """
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG

    cfg = RouterConfig()

    # Basic overrides
    cfg.HOST = os.getenv("HOST", cfg.HOST)
    cfg.PORT = int(os.getenv("PORT", cfg.PORT))
    cfg.API_KEY = os.getenv("API_KEY", cfg.API_KEY)

    cfg.REDIS_HOST = os.getenv("REDIS_HOST", cfg.REDIS_HOST)
    cfg.REDIS_PORT = int(os.getenv("REDIS_PORT", cfg.REDIS_PORT))
    cfg.MODEL_NAME = os.getenv("MODEL_NAME", cfg.MODEL_NAME)
    cfg.NAMESPACE = os.getenv("NAMESPACE", cfg.NAMESPACE)
    cfg.LABEL_SELECTOR = os.getenv("LABEL_SELECTOR", cfg.LABEL_SELECTOR)
    cfg.VLLM_PORT = int(os.getenv("VLLM_PORT", cfg.VLLM_PORT))
    cfg.KV_LOG_KEYS = os.getenv("KV_LOG_KEYS", cfg.KV_LOG_KEYS)

    # Routing knobs
    cfg.KV_AWARE = os.getenv("KV_AWARE", str(cfg.KV_AWARE)).lower() == "true"
    cfg.LEN_AWARE = os.getenv("LEN_AWARE", str(cfg.LEN_AWARE)).lower() == "true"
    cfg.LEN_POLICY = os.getenv("LEN_POLICY", cfg.LEN_POLICY)
    cfg.POOL_FACTOR = int(os.getenv("POOL_FACTOR", cfg.POOL_FACTOR))

    # Key affinity routing
    cfg.AFFINITY_ENABLED = os.getenv("AFFINITY_ENABLED", str(cfg.AFFINITY_ENABLED)).lower() == "true"
    cfg.AFFINITY_MODE = os.getenv("AFFINITY_MODE", cfg.AFFINITY_MODE).lower()
    if cfg.AFFINITY_MODE not in ("soft", "hard"):
        cfg.AFFINITY_MODE = "soft"
    cfg.AFFINITY_TTL_S = float(os.getenv("AFFINITY_TTL_S", cfg.AFFINITY_TTL_S))
    cfg.AFFINITY_HARD_TIMEOUT_S = float(
        os.getenv("AFFINITY_HARD_TIMEOUT_S", cfg.AFFINITY_HARD_TIMEOUT_S)
    )

    # Unified routing strategy: when set, derives KV_AWARE / AFFINITY_ENABLED
    # from a single knob and overrides the individual flags above.
    cfg.ROUTER_STRATEGY = os.getenv("ROUTER_STRATEGY", cfg.ROUTER_STRATEGY).strip().lower()
    _strategy_map = {
        "none": (False, False),
        "prefix": (True, False),
        "affinity": (False, True),
        "both": (True, True),
    }
    if cfg.ROUTER_STRATEGY in _strategy_map:
        cfg.KV_AWARE, cfg.AFFINITY_ENABLED = _strategy_map[cfg.ROUTER_STRATEGY]
    cfg.POOL_BIDIRECTIONAL = os.getenv("POOL_BIDIRECTIONAL", "false").lower() in ("1", "true", "yes")
    cfg.DEFAULT_MAX_TOKENS = int(os.getenv("DEFAULT_MAX_TOKENS", cfg.DEFAULT_MAX_TOKENS))

    # Inline KV-block hash computation
    cfg.KV_TOKENIZER_PATH = os.getenv("KV_TOKENIZER_PATH", cfg.KV_TOKENIZER_PATH)
    cfg.KV_BLOCK_SIZE = int(os.getenv("KV_BLOCK_SIZE", cfg.KV_BLOCK_SIZE))
    cfg.KV_HASH_SOURCE = os.getenv("KV_HASH_SOURCE", cfg.KV_HASH_SOURCE).strip().lower()
    if cfg.KV_HASH_SOURCE not in ("inline", "external"):
        cfg.KV_HASH_SOURCE = "inline"
    cfg.HASH_SERVICE_URL = os.getenv("HASH_SERVICE_URL", cfg.HASH_SERVICE_URL)

    # Router operation mode
    cfg.ROUTER_MODE = os.getenv("ROUTER_MODE", cfg.ROUTER_MODE)
    cfg.SIDECAR_PORT = int(os.getenv("SIDECAR_PORT", cfg.SIDECAR_PORT))

    # Sync flow
    cfg.RESULT_TIMEOUT_S = float(os.getenv("RESULT_TIMEOUT_S", cfg.RESULT_TIMEOUT_S))
    cfg.RESULT_POLL_INTERVAL_S = float(
        os.getenv("RESULT_POLL_INTERVAL_S", cfg.RESULT_POLL_INTERVAL_S)
    )

    # Poll retention knobs (also used by router_state cleanup loop)
    cfg.POLL_RESULT_TTL_S = float(os.getenv("POLL_RESULT_TTL_S", cfg.POLL_RESULT_TTL_S))
    cfg.POLL_CLEANUP_INTERVAL_S = float(
        os.getenv("POLL_CLEANUP_INTERVAL_S", cfg.POLL_CLEANUP_INTERVAL_S)
    )

    # Harden against bad envs (avoid disabling cleanup accidentally)
    if cfg.POLL_RESULT_TTL_S <= 0:
        cfg.POLL_RESULT_TTL_S = 300.0
    if cfg.POLL_CLEANUP_INTERVAL_S <= 0:
        cfg.POLL_CLEANUP_INTERVAL_S = 1.0
    cfg.POLL_CLEANUP_INTERVAL_S = max(0.1, float(cfg.POLL_CLEANUP_INTERVAL_S))
    cfg.POLL_RESULT_TTL_S = max(1.0, float(cfg.POLL_RESULT_TTL_S))

    # sidecar -> router result transport knobs
    cfg.RESULT_TRANSPORT_MODE = os.getenv("RESULT_TRANSPORT_MODE", cfg.RESULT_TRANSPORT_MODE)
    cfg.RESULT_SUBMIT_PATH = os.getenv("RESULT_SUBMIT_PATH", cfg.RESULT_SUBMIT_PATH)
    if cfg.RESULT_SUBMIT_PATH and not str(cfg.RESULT_SUBMIT_PATH).startswith("/"):
        cfg.RESULT_SUBMIT_PATH = "/" + str(cfg.RESULT_SUBMIT_PATH)

    # Normalize/validate result transport mode
    rtm = _norm_mode(cfg.RESULT_TRANSPORT_MODE)
    if rtm not in ("sync", "submit_ack"):
        rtm = "sync"
    cfg.RESULT_TRANSPORT_MODE = rtm

    # Async pubsub transport knobs
    cfg.TRANSPORT_MODE = os.getenv("TRANSPORT_MODE", cfg.TRANSPORT_MODE)
    cfg.SUBMIT_PATH = os.getenv("SUBMIT_PATH", cfg.SUBMIT_PATH)
    if cfg.SUBMIT_PATH and not str(cfg.SUBMIT_PATH).startswith("/"):
        cfg.SUBMIT_PATH = "/" + str(cfg.SUBMIT_PATH)

    cfg.RESULTS_ZMQ_BIND = os.getenv("RESULTS_ZMQ_BIND", cfg.RESULTS_ZMQ_BIND)
    cfg.RESULTS_ZMQ_TOPIC = os.getenv("RESULTS_ZMQ_TOPIC", cfg.RESULTS_ZMQ_TOPIC)
    cfg.RESULTS_ZMQ_HWM = int(os.getenv("RESULTS_ZMQ_HWM", cfg.RESULTS_ZMQ_HWM))
    cfg.RESULTS_GRACE_S = float(os.getenv("RESULTS_GRACE_S", cfg.RESULTS_GRACE_S))

    tm = _norm_mode(cfg.TRANSPORT_MODE)
    if tm not in ("sync", "async_pubsub"):
        tm = "sync"
    cfg.TRANSPORT_MODE = tm

    # Timeouts
    cfg.PUSH_HTTP_TIMEOUT_S = float(os.getenv("PUSH_HTTP_TIMEOUT_S", cfg.PUSH_HTTP_TIMEOUT_S))

    # Keepalive/pooling knobs
    cfg.PUSH_MAX_KEEPALIVE = int(os.getenv("PUSH_MAX_KEEPALIVE", cfg.PUSH_MAX_KEEPALIVE))
    cfg.PUSH_KEEPALIVE_EXPIRY_S = float(
        os.getenv("PUSH_KEEPALIVE_EXPIRY_S", cfg.PUSH_KEEPALIVE_EXPIRY_S)
    )

    # Push leastq
    cfg.PUSH_LEASTQ_MODE = os.getenv("PUSH_LEASTQ_MODE", cfg.PUSH_LEASTQ_MODE)

    # Push-mode decoupling knobs
    if "PUSH_DECOUPLE_DISPATCH" in os.environ:
        cfg.PUSH_DECOUPLE_DISPATCH = (
            os.getenv("PUSH_DECOUPLE_DISPATCH", "true").lower() == "true"
        )
    if "PUSH_DISPATCH_QUEUE_MAX" in os.environ:
        try:
            cfg.PUSH_DISPATCH_QUEUE_MAX = int(
                os.getenv("PUSH_DISPATCH_QUEUE_MAX", str(cfg.PUSH_DISPATCH_QUEUE_MAX))
            )
        except Exception:
            pass
    if "PUSH_DISPATCH_WORKERS" in os.environ:
        try:
            cfg.PUSH_DISPATCH_WORKERS = int(
                os.getenv("PUSH_DISPATCH_WORKERS", str(cfg.PUSH_DISPATCH_WORKERS))
            )
        except Exception:
            pass
    if "PUSH_DISPATCH_MAX_DELAY_S" in os.environ:
        try:
            cfg.PUSH_DISPATCH_MAX_DELAY_S = float(
                os.getenv("PUSH_DISPATCH_MAX_DELAY_S", str(cfg.PUSH_DISPATCH_MAX_DELAY_S))
            )
        except Exception:
            pass

    # -----------------------------
    # Normalize / sanitize inputs
    # -----------------------------

    # ROUTER_MODE normalization + allowlist
    rm = _norm_mode(cfg.ROUTER_MODE)

    # ✅ Accept common aliases (so you don't silently fall back to pull)
    if rm in ("push-least-queue", "push_least_queue", "push-leastqueue", "push_leastqueue"):
        rm = "push-leastq"

    if rm not in ("pull", "push-rr", "push-random", "push-leastq"):
        rm = "pull"
    cfg.ROUTER_MODE = rm

    # PUSH_LEASTQ_MODE normalization + allowlist
    lqm = _norm_mode(cfg.PUSH_LEASTQ_MODE)
    if lqm not in ("health", "local"):
        lqm = "health"
    cfg.PUSH_LEASTQ_MODE = lqm

    # LEN_POLICY allowlist (when enabled)
    lp = _norm_mode(cfg.LEN_POLICY)
    if lp and lp not in ("short_first", "long_first"):
        lp = "short_first"
    cfg.LEN_POLICY = lp

    # clamp numeric knobs
    cfg.POOL_FACTOR = max(1, int(cfg.POOL_FACTOR))
    cfg.DEFAULT_MAX_TOKENS = max(1, int(cfg.DEFAULT_MAX_TOKENS))

    cfg.RESULTS_ZMQ_HWM = max(1, int(cfg.RESULTS_ZMQ_HWM))
    cfg.RESULTS_GRACE_S = max(0.0, float(cfg.RESULTS_GRACE_S))

    cfg.PUSH_HTTP_TIMEOUT_S = max(0.001, float(cfg.PUSH_HTTP_TIMEOUT_S))

    cfg.PUSH_MAX_KEEPALIVE = max(1, int(cfg.PUSH_MAX_KEEPALIVE))
    cfg.PUSH_KEEPALIVE_EXPIRY_S = max(0.0, float(cfg.PUSH_KEEPALIVE_EXPIRY_S))

    cfg.KV_BLOCK_SIZE = max(1, int(cfg.KV_BLOCK_SIZE))

    cfg.PUSH_DISPATCH_QUEUE_MAX = max(1, int(cfg.PUSH_DISPATCH_QUEUE_MAX))
    cfg.PUSH_DISPATCH_WORKERS = max(1, int(cfg.PUSH_DISPATCH_WORKERS))
    cfg.PUSH_DISPATCH_MAX_DELAY_S = max(0.0, float(cfg.PUSH_DISPATCH_MAX_DELAY_S))

    # Logging verbosity (normalize to avoid surprising behavior)
    cfg.REQ_LOG_MODE = _norm_log_mode(os.getenv("REQ_LOG_MODE", cfg.REQ_LOG_MODE))

    # SLO-aware routing knobs
    cfg.SLO_AWARE = os.getenv("SLO_AWARE", str(cfg.SLO_AWARE)).lower() == "true"
    cfg.SLO_WITH_KV = os.getenv("SLO_WITH_KV", str(cfg.SLO_WITH_KV)).lower() == "true"
    cfg.ADMISSION_THROTTLE = os.getenv("ADMISSION_THROTTLE", str(cfg.ADMISSION_THROTTLE)).lower() == "true"
    cfg.FIXED_BATCH_SIZE = int(os.getenv("FIXED_BATCH_SIZE", cfg.FIXED_BATCH_SIZE))
    cfg.FIXED_BATCH_SIZE = max(0, cfg.FIXED_BATCH_SIZE)

    cfg.OUTPUT_LEN_PREDICTOR = os.getenv("OUTPUT_LEN_PREDICTOR", cfg.OUTPUT_LEN_PREDICTOR)
    olp = _norm_mode(cfg.OUTPUT_LEN_PREDICTOR)
    if olp not in ("simple", "distribution", "regression", "hint_only"):
        olp = "simple"
    cfg.OUTPUT_LEN_PREDICTOR = olp

    cfg.BATCH_SIZE_ESTIMATE = os.getenv("BATCH_SIZE_ESTIMATE", cfg.BATCH_SIZE_ESTIMATE)
    bse = _norm_mode(cfg.BATCH_SIZE_ESTIMATE)
    if bse not in ("fixed", "inflight", "reported"):
        bse = "fixed"
    cfg.BATCH_SIZE_ESTIMATE = bse
    cfg.FIXED_BATCH_ESTIMATE = max(1, int(os.getenv("FIXED_BATCH_ESTIMATE", cfg.FIXED_BATCH_ESTIMATE)))

    cfg.LATENCY_PREDICTOR = os.getenv("LATENCY_PREDICTOR", cfg.LATENCY_PREDICTOR)
    ltp = _norm_mode(cfg.LATENCY_PREDICTOR)
    if ltp not in ("linear", "piecewise", "bayesian", "hybrid"):
        ltp = "linear"
    cfg.LATENCY_PREDICTOR = ltp
    cfg.LATENCY_ONLINE_UPDATE = os.getenv("LATENCY_ONLINE_UPDATE", str(cfg.LATENCY_ONLINE_UPDATE)).lower() == "true"
    cfg.LATENCY_PROFILE_PATH = os.getenv("LATENCY_PROFILE_PATH", cfg.LATENCY_PROFILE_PATH)

    cfg.QUEUE_WAIT_MODEL = os.getenv("QUEUE_WAIT_MODEL", cfg.QUEUE_WAIT_MODEL)
    qwm = _norm_mode(cfg.QUEUE_WAIT_MODEL)
    if qwm not in ("none", "simple", "drain_rate"):
        qwm = "none"
    cfg.QUEUE_WAIT_MODEL = qwm
    cfg.CHUNKED_PREFILL_AWARE = os.getenv("CHUNKED_PREFILL_AWARE", str(cfg.CHUNKED_PREFILL_AWARE)).lower() == "true"
    cfg.MAX_NUM_BATCHED_TOKENS = max(0, int(os.getenv("MAX_NUM_BATCHED_TOKENS", cfg.MAX_NUM_BATCHED_TOKENS)))

    # TRACE overrides
    if "TRACE_ENABLED" in os.environ:
        cfg.TRACE_ENABLED = os.getenv("TRACE_ENABLED", "false").lower() == "true"

    if "ROUTER_LOG_BLOCK_HASHES" in os.environ:
        cfg.ROUTER_LOG_BLOCK_HASHES = os.getenv("ROUTER_LOG_BLOCK_HASHES", "false").lower() == "true"

    if "TRACE_SAMPLING_RATE" in os.environ:
        try:
            r = float(os.getenv("TRACE_SAMPLING_RATE"))
            if 0 < r <= 1.0:
                cfg.TRACE_SAMPLING_RATE = r
        except Exception:
            pass

    # Multi-model config file
    cfg.MODEL_CONFIG_PATH = os.getenv("MODEL_CONFIG_PATH", cfg.MODEL_CONFIG_PATH)

    # Load model registry if config file is provided
    global _MODEL_REGISTRY
    if cfg.MODEL_CONFIG_PATH and os.path.isfile(cfg.MODEL_CONFIG_PATH):
        try:
            _MODEL_REGISTRY = load_model_registry(cfg.MODEL_CONFIG_PATH)
            print(
                f"[router] Loaded model registry from {cfg.MODEL_CONFIG_PATH}: "
                f"{sorted(_MODEL_REGISTRY.keys())}"
            )
            sys.stdout.flush()
        except Exception as e:
            print(f"[router] WARNING: failed to load model registry: {e}")
            sys.stdout.flush()
            _MODEL_REGISTRY = None

    _CONFIG = cfg
    return cfg


def print_config(cfg: RouterConfig) -> None:
    """
    Generic config dumper: prints all fields of RouterConfig.
    Any added/removed fields are automatically reflected.
    """
    import sys
    data = asdict(cfg)

    print("\n================== RouterConfig (effective) ==================")
    for key in sorted(data.keys()):
        value = data[key]
        print(f" {key:22s} = {value}")

    access_log = os.getenv("ACCESS_LOG", "true")
    print(f" {'ACCESS_LOG':22s} = {access_log}")
    print("=============================================================\n")

    sys.stdout.flush()
