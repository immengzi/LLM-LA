# -*- coding: utf-8 -*-
from dataclasses import dataclass
import os


@dataclass
class SidecarConfig:
    # Where the router-service lives
    ROUTER_URL: str = "http://router-service:8080"

    # Where vLLM OpenAI-compatible API lives (inside the same pod)
    VLLM_URL: str = "http://127.0.0.1:8000"

    MODEL_NAME: str = "served-model"

    # Local queue / batching
    TARGET_LOCAL_QUEUE: int = 16   # desired #pending jobs locally
    PULL_INTERVAL_S: float = 0.05  # how often we check for want>0

    # ZMQ → Redis kv sync (same as you had)
    VLLM_HOST: str = "127.0.0.1"
    VLLM_SUB_PORT: int = 5557

    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    CONTAINER_NAME: str = os.getenv("CONTAINER_NAME", "vllm-pod")
    MODEL_NAME_REDIS: str = "served-model"


def get_config() -> SidecarConfig:
    cfg = SidecarConfig()
    cfg.ROUTER_URL = os.getenv("ROUTER_URL", cfg.ROUTER_URL)
    cfg.VLLM_URL = os.getenv("VLLM_URL", cfg.VLLM_URL)
    cfg.MODEL_NAME = os.getenv("MODEL_NAME", cfg.MODEL_NAME)
    cfg.TARGET_LOCAL_QUEUE = int(os.getenv("TARGET_LOCAL_QUEUE", cfg.TARGET_LOCAL_QUEUE))
    cfg.PULL_INTERVAL_S = float(os.getenv("PULL_INTERVAL_S", cfg.PULL_INTERVAL_S))
    cfg.VLLM_HOST = os.getenv("VLLM_HOST", cfg.VLLM_HOST)
    cfg.VLLM_SUB_PORT = int(os.getenv("VLLM_SUB_PORT", cfg.VLLM_SUB_PORT))
    cfg.REDIS_HOST = os.getenv("REDIS_HOST", cfg.REDIS_HOST)
    cfg.REDIS_PORT = int(os.getenv("REDIS_PORT", cfg.REDIS_PORT))
    cfg.CONTAINER_NAME = os.getenv("CONTAINER_NAME", cfg.CONTAINER_NAME)
    cfg.MODEL_NAME_REDIS = os.getenv("MODEL_NAME_REDIS", cfg.MODEL_NAME_REDIS)
    return cfg
