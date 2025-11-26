# -*- coding: utf-8 -*-
from dataclasses import dataclass
import os


@dataclass
class RouterConfig:
    # Basic
    HOST: str = "0.0.0.0"
    PORT: int = 8080

    # Redis (for KV watcher)
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    MODEL_NAME: str = "served-model"

    # vLLM discovery (for KV watcher to map pod -> endpoint)
    NAMESPACE: str = "vllm"
    LABEL_SELECTOR: str = "app=vllm-qwen"
    VLLM_PORT: int = 8200

    # KV watcher controls
    KV_WATCH_INTERVAL_S: float = 1.0
    KV_WATCH_MAX_KEYS: int = 200
    KV_DISCOVERY_INTERVAL_S: float = 5.0
    KV_LOG_KEYS: str = "summary"  # off | summary | full

    # Routing knobs (MINIMIZED)
    KV_AWARE: bool = True           # global on/off
    LEN_AWARE: bool = True          # global on/off

    # if both enabled: KV-first, then length
    LEN_POLICY: str = "short_first"  # short_first | long_first

    # how many items to scan vs want
    POOL_FACTOR: int = 4

    # batch size proxy: expected output tokens (for predictor)
    DEFAULT_MAX_TOKENS: int = 1024


def get_config() -> RouterConfig:
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
    return cfg
