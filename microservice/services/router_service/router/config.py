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

    # router mode + sidecar port
    ROUTER_MODE: str = "pull"        # "pull", "push-rr", "push-random", "push-leastq"
    SIDECAR_PORT: int = 9000         # sidecar FastAPI port

    # -------------------------------
    # Synchronous response flow
    # -------------------------------
    RESULT_TIMEOUT_S: float = 60.0
    RESULT_POLL_INTERVAL_S: float = 0.02

    # -------------------------------
    # Per-request routing logs
    # -------------------------------
    REQ_LOG_MODE: str = "off"   # off | summary | full


def get_config() -> RouterConfig:
    """
    Return a singleton RouterConfig with env overrides applied.
    Does NOT print anything (printing is done explicitly by callers).
    """
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG

    cfg = RouterConfig()

    # Allow simple env overrides
    cfg.REDIS_HOST = os.getenv("REDIS_HOST", cfg.REDIS_HOST)
    cfg.REDIS_PORT = int(os.getenv("REDIS_PORT", cfg.REDIS_PORT))
    cfg.MODEL_NAME = os.getenv("MODEL_NAME", cfg.MODEL_NAME)
    cfg.NAMESPACE = os.getenv("NAMESPACE", cfg.NAMESPACE)
    cfg.LABEL_SELECTOR = os.getenv("LABEL_SELECTOR", cfg.LABEL_SELECTOR)
    cfg.VLLM_PORT = int(os.getenv("VLLM_PORT", cfg.VLLM_PORT))
    cfg.KV_LOG_KEYS = os.getenv("KV_LOG_KEYS", cfg.KV_LOG_KEYS)

    cfg.KV_AWARE = os.getenv("KV_AWARE", "true").lower() == "true"
    cfg.LEN_AWARE = os.getenv("LEN_AWARE", "true").lower() == "true"
    cfg.LEN_POLICY = os.getenv("LEN_POLICY", cfg.LEN_POLICY)
    cfg.POOL_FACTOR = int(os.getenv("POOL_FACTOR", cfg.POOL_FACTOR))
    cfg.DEFAULT_MAX_TOKENS = int(os.getenv("DEFAULT_MAX_TOKENS", cfg.DEFAULT_MAX_TOKENS))
    cfg.HASH_SERVICE_URL = os.getenv("HASH_SERVICE_URL", cfg.HASH_SERVICE_URL)

    cfg.ROUTER_MODE = os.getenv("ROUTER_MODE", cfg.ROUTER_MODE)
    cfg.SIDECAR_PORT = int(os.getenv("SIDECAR_PORT", cfg.SIDECAR_PORT))

    cfg.RESULT_TIMEOUT_S = float(os.getenv("RESULT_TIMEOUT_S", cfg.RESULT_TIMEOUT_S))
    cfg.RESULT_POLL_INTERVAL_S = float(
        os.getenv("RESULT_POLL_INTERVAL_S", cfg.RESULT_POLL_INTERVAL_S)
    )

    cfg.REQ_LOG_MODE = os.getenv("REQ_LOG_MODE", cfg.REQ_LOG_MODE)

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
    # Sort keys just to have stable ordering
    for key in sorted(data.keys()):
        value = data[key]
        print(f" {key:22s} = {value}")
    # Also show ACCESS_LOG env since it's used outside RouterConfig
    access_log = os.getenv("ACCESS_LOG", "true")
    print(f" {'ACCESS_LOG':22s} = {access_log}")
    print("=============================================================\n")

    sys.stdout.flush()

