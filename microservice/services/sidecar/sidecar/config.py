# sidecar/config.py
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
    BATCH_SIZE: int = 8
    PULL_INTERVAL_S: float = 0.05

    # ZMQ → Redis kv sync (same as you had)
    VLLM_HOST: str = "127.0.0.1"
    VLLM_SUB_PORT: int = 5557

    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    CONTAINER_NAME: str = os.getenv("CONTAINER_NAME", "vllm-pod")
    MODEL_NAME_REDIS: str = "served-model"

    # sidecar HTTP + concurrency + mode
    SIDECAR_PORT: int = 9000                # where this sidecar FastAPI listens
    SIDECAR_MODE: str = "pull"              # "pull" or "push"

    # HTTP timeouts (seconds)
    ROUTER_PULL_TIMEOUT_S: float = 1.0      # /pull timeout
    VLLM_TIMEOUT_S: float = 30.0            # vLLM generation timeout
    ROUTER_RESULT_TIMEOUT_S: float = 5.0    # /result post timeout

    # ------------------------------------------------
    # Tracing controls (must match router side)
    # ------------------------------------------------
    TRACE_ENABLED: bool = False             # global on/off
    TRACE_SAMPLE_RATE: float = 1.0          # 0.0–1.0, currently unused here but kept for symmetry


def get_config() -> SidecarConfig:
    cfg = SidecarConfig()

    cfg.ROUTER_URL = os.getenv("ROUTER_URL", cfg.ROUTER_URL)
    cfg.VLLM_URL = os.getenv("VLLM_URL", cfg.VLLM_URL)
    cfg.MODEL_NAME = os.getenv("MODEL_NAME", cfg.MODEL_NAME)

    cfg.BATCH_SIZE = int(os.getenv("BATCH_SIZE", getattr(cfg, "BATCH_SIZE", 8)))
    cfg.PULL_INTERVAL_S = float(os.getenv("PULL_INTERVAL_S", cfg.PULL_INTERVAL_S))

    cfg.VLLM_HOST = os.getenv("VLLM_HOST", cfg.VLLM_HOST)
    cfg.VLLM_SUB_PORT = int(os.getenv("VLLM_SUB_PORT", cfg.VLLM_SUB_PORT))
    cfg.REDIS_HOST = os.getenv("REDIS_HOST", cfg.REDIS_HOST)
    cfg.REDIS_PORT = int(os.getenv("REDIS_PORT", cfg.REDIS_PORT))
    cfg.CONTAINER_NAME = os.getenv("CONTAINER_NAME", cfg.CONTAINER_NAME)
    cfg.MODEL_NAME_REDIS = os.getenv("MODEL_NAME_REDIS", cfg.MODEL_NAME_REDIS)

    cfg.SIDECAR_PORT = int(os.getenv("SIDECAR_PORT", cfg.SIDECAR_PORT))
    cfg.SIDECAR_MODE = os.getenv("SIDECAR_MODE", cfg.SIDECAR_MODE)

    cfg.ROUTER_PULL_TIMEOUT_S = float(
        os.getenv("ROUTER_PULL_TIMEOUT_S", cfg.ROUTER_PULL_TIMEOUT_S)
    )
    cfg.VLLM_TIMEOUT_S = float(
        os.getenv("VLLM_TIMEOUT_S", cfg.VLLM_TIMEOUT_S)
    )
    cfg.ROUTER_RESULT_TIMEOUT_S = float(
        os.getenv("ROUTER_RESULT_TIMEOUT_S", cfg.ROUTER_RESULT_TIMEOUT_S)
    )

    # Tracing env overrides
    cfg.TRACE_ENABLED = os.getenv("TRACE_ENABLED", "false").lower() == "true"
    cfg.TRACE_SAMPLE_RATE = float(
        os.getenv("TRACE_SAMPLE_RATE", getattr(cfg, "TRACE_SAMPLE_RATE", 1.0))
    )

    return cfg
