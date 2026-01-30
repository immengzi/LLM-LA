# router/config.py
# -*- coding: utf-8 -*-
from dataclasses import dataclass, asdict
import os

# Global singleton to avoid recomputing config multiple times
_CONFIG = None


@dataclass
class RouterConfig:
    # Basic
    HOST: str = "0.0.0.0"
    PORT: int = 8080

    # Redis (for KV watcher)
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    MODEL_NAME: str = "served-model"

    # Hash service
    HASH_SERVICE_URL: str = "http://prefix-hash-service:9095"

    # vLLM discovery (for KV watcher to map pod -> endpoint)
    NAMESPACE: str = "vllm"
    LABEL_SELECTOR: str = "app=vllm-qwen"
    VLLM_PORT: int = 8200

    # KV watcher controls
    KV_WATCH_INTERVAL_S: float = 1.0
    KV_WATCH_MAX_KEYS: int = 200
    KV_DISCOVERY_INTERVAL_S: float = 5.0
    KV_LOG_KEYS: str = "summary"  # off | summary | full

    # Routing knobs
    KV_AWARE: bool = True
    LEN_AWARE: bool = True
    LEN_POLICY: str = "short_first"  # short_first | long_first

    # how many items to scan vs want
    POOL_FACTOR: int = 4

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
    # These are also used to prevent unbounded growth of stored results/waiters
    # under submit_ack / async_pubsub where the client never waits.
    POLL_RESULT_TTL_S: float = 300.0
    POLL_CLEANUP_INTERVAL_S: float = 1.0

    # --------------------------------------------------------------------
    # Sidecar -> router result ingestion transport (NEW; backward compatible)
    # --------------------------------------------------------------------
    # "sync":       sidecar POSTs to /result (old behavior)
    # "submit_ack": sidecar POSTs to RESULT_SUBMIT_PATH and router ACKs immediately
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
    HASH_TIMEOUT_S: float = 2.0          # hash-service compute_hashes
    PUSH_HTTP_TIMEOUT_S: float = 2.0     # push-mode health + push

    # --------------------------------------------------------------------
    # HTTP connection pooling / keepalive knobs (NO semantic changes)
    # --------------------------------------------------------------------
    HASH_MAX_KEEPALIVE: int = 50
    HASH_KEEPALIVE_EXPIRY_S: float = 30.0

    PUSH_MAX_KEEPALIVE: int = 200
    PUSH_KEEPALIVE_EXPIRY_S: float = 30.0

    # --------------------------------------------------------------------
    # Push least-queue behavior
    # --------------------------------------------------------------------
    PUSH_LEASTQ_MODE: str = "health"     # health | local

    # --------------------------------------------------------------------
    # Per-request routing logs
    # --------------------------------------------------------------------
    REQ_LOG_MODE: str = "off"   # off | summary | full

    # --------------------------------------------------------------------
    # TRACE SYSTEM
    # --------------------------------------------------------------------
    TRACE_ENABLED: bool = False
    TRACE_SAMPLING_RATE: float = 1.0


def _norm_mode(s: str) -> str:
    return str(s or "").strip().lower()


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
    cfg.DEFAULT_MAX_TOKENS = int(os.getenv("DEFAULT_MAX_TOKENS", cfg.DEFAULT_MAX_TOKENS))
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
    # don't allow absurdly tight loops
    cfg.POLL_CLEANUP_INTERVAL_S = max(0.1, float(cfg.POLL_CLEANUP_INTERVAL_S))
    # don't allow TTL too tiny (would delete results before any observer sees them)
    cfg.POLL_RESULT_TTL_S = max(1.0, float(cfg.POLL_RESULT_TTL_S))

    # sidecar -> router result transport knobs
    cfg.RESULT_TRANSPORT_MODE = os.getenv("RESULT_TRANSPORT_MODE", cfg.RESULT_TRANSPORT_MODE)
    cfg.RESULT_SUBMIT_PATH = os.getenv("RESULT_SUBMIT_PATH", cfg.RESULT_SUBMIT_PATH)
    if cfg.RESULT_SUBMIT_PATH and not str(cfg.RESULT_SUBMIT_PATH).startswith("/"):
        cfg.RESULT_SUBMIT_PATH = "/" + str(cfg.RESULT_SUBMIT_PATH)

    # Normalize/validate transport mode
    rtm = _norm_mode(cfg.RESULT_TRANSPORT_MODE)
    if rtm not in ("sync", "submit_ack"):
        rtm = "sync"
    cfg.RESULT_TRANSPORT_MODE = rtm

    # Async pubsub transport knobs
    cfg.TRANSPORT_MODE = os.getenv("TRANSPORT_MODE", cfg.TRANSPORT_MODE)
    cfg.SUBMIT_PATH = os.getenv("SUBMIT_PATH", cfg.SUBMIT_PATH)
    cfg.RESULTS_ZMQ_BIND = os.getenv("RESULTS_ZMQ_BIND", cfg.RESULTS_ZMQ_BIND)
    cfg.RESULTS_ZMQ_TOPIC = os.getenv("RESULTS_ZMQ_TOPIC", cfg.RESULTS_ZMQ_TOPIC)
    cfg.RESULTS_ZMQ_HWM = int(os.getenv("RESULTS_ZMQ_HWM", cfg.RESULTS_ZMQ_HWM))
    cfg.RESULTS_GRACE_S = float(os.getenv("RESULTS_GRACE_S", cfg.RESULTS_GRACE_S))

    tm = _norm_mode(cfg.TRANSPORT_MODE)
    if tm not in ("sync", "async_pubsub"):
        tm = "sync"
    cfg.TRANSPORT_MODE = tm

    # Timeouts
    cfg.HASH_TIMEOUT_S = float(os.getenv("HASH_TIMEOUT_S", cfg.HASH_TIMEOUT_S))
    cfg.PUSH_HTTP_TIMEOUT_S = float(os.getenv("PUSH_HTTP_TIMEOUT_S", cfg.PUSH_HTTP_TIMEOUT_S))

    # Keepalive/pooling knobs
    cfg.HASH_MAX_KEEPALIVE = int(os.getenv("HASH_MAX_KEEPALIVE", cfg.HASH_MAX_KEEPALIVE))
    cfg.HASH_KEEPALIVE_EXPIRY_S = float(os.getenv("HASH_KEEPALIVE_EXPIRY_S", cfg.HASH_KEEPALIVE_EXPIRY_S))

    cfg.PUSH_MAX_KEEPALIVE = int(os.getenv("PUSH_MAX_KEEPALIVE", cfg.PUSH_MAX_KEEPALIVE))
    cfg.PUSH_KEEPALIVE_EXPIRY_S = float(os.getenv("PUSH_KEEPALIVE_EXPIRY_S", cfg.PUSH_KEEPALIVE_EXPIRY_S))

    # Push leastq
    cfg.PUSH_LEASTQ_MODE = os.getenv("PUSH_LEASTQ_MODE", cfg.PUSH_LEASTQ_MODE)

    # Logging verbosity
    cfg.REQ_LOG_MODE = os.getenv("REQ_LOG_MODE", cfg.REQ_LOG_MODE)

    # TRACE overrides
    if "TRACE_ENABLED" in os.environ:
        cfg.TRACE_ENABLED = os.getenv("TRACE_ENABLED", "false").lower() == "true"

    if "TRACE_SAMPLING_RATE" in os.environ:
        try:
            r = float(os.getenv("TRACE_SAMPLING_RATE"))
            if 0 < r <= 1.0:
                cfg.TRACE_SAMPLING_RATE = r
        except Exception:
            pass

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
