# sidecar/config.py
# -*- coding: utf-8 -*-
from dataclasses import dataclass
import os


@dataclass
class SidecarConfig:
    # ------------------------------------------------
    # Router + vLLM endpoints
    # ------------------------------------------------

    # Where the router-service lives
    ROUTER_URL: str = "http://router-service:8080"

    # Where vLLM OpenAI-compatible API lives (inside the same pod)
    VLLM_URL: str = "http://127.0.0.1:8000"

    MODEL_NAME: str = "served-model"

    # ------------------------------------------------
    # Local queue / batching
    # ------------------------------------------------

    BATCH_SIZE: int = 8
    PREFETCH: int = 0
    PULL_INTERVAL_S: float = 0.05

    # ------------------------------------------------
    # ZMQ → Redis KV sync (unchanged)
    # ------------------------------------------------

    VLLM_HOST: str = "127.0.0.1"
    VLLM_SUB_PORT: int = 5557

    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    CONTAINER_NAME: str = os.getenv("CONTAINER_NAME", "vllm-pod")
    MODEL_NAME_REDIS: str = "served-model"

    # ------------------------------------------------
    # Sidecar HTTP + mode
    # ------------------------------------------------

    SIDECAR_PORT: int = 9000          # where sidecar FastAPI listens
    SIDECAR_MODE: str = "pull"        # pull | push

    # ------------------------------------------------
    # HTTP timeouts
    # ------------------------------------------------

    ROUTER_PULL_TIMEOUT_S: float = 1.0
    VLLM_TIMEOUT_S: float = 30.0

    # ------------------------------------------------
    # Result transport (kept — backward compatible)
    # ------------------------------------------------
    # sync        -> POST /result (old behavior)
    # submit_ack  -> POST /result_submit (immediate ACK)
    RESULT_TRANSPORT_MODE: str = "sync"

    # Path used when RESULT_TRANSPORT_MODE=submit_ack
    RESULT_SUBMIT_PATH: str = "/result_submit"

    # HTTP timeout for result submission (applies to both modes)
    ROUTER_RESULT_TIMEOUT_S: float = 5.0

    # ------------------------------------------------
    # Connection pooling (requests / urllib3)
    # ------------------------------------------------

    ROUTER_POOL_CONNECTIONS: int = 50
    ROUTER_POOL_MAXSIZE: int = 200

    VLLM_POOL_CONNECTIONS: int = 10
    VLLM_POOL_MAXSIZE: int = 50

    # ------------------------------------------------
    # Result posting retry behavior
    # ------------------------------------------------

    RESULT_POST_RETRY: bool = False
    RESULT_POST_MAX_RETRIES: int = 0
    RESULT_POST_BACKOFF_BASE_S: float = 0.05
    RESULT_POST_BACKOFF_CAP_S: float = 2.0

    # ------------------------------------------------
    # Tracing
    # ------------------------------------------------

    TRACE_ENABLED: bool = False
    TRACE_SAMPLE_RATE: float = 1.0

    # ------------------------------------------------
    # Fixed-length / replay mode
    # ------------------------------------------------

    FORCE_IGNORE_EOS: bool = False

    # ------------------------------------------------
    # Streaming
    # ------------------------------------------------

    STREAMING_MODE: bool = False

    # ------------------------------------------------
    # Logging
    # ------------------------------------------------

    LOG_LEVEL: str = "info"


def get_config() -> SidecarConfig:
    cfg = SidecarConfig()

    # ------------------------------------------------
    # Endpoints
    # ------------------------------------------------
    cfg.ROUTER_URL = os.getenv("ROUTER_URL", cfg.ROUTER_URL)
    cfg.VLLM_URL = os.getenv("VLLM_URL", cfg.VLLM_URL)
    cfg.MODEL_NAME = os.getenv("MODEL_NAME", cfg.MODEL_NAME)

    # ------------------------------------------------
    # Queue / batching
    # ------------------------------------------------
    cfg.BATCH_SIZE = int(os.getenv("BATCH_SIZE", cfg.BATCH_SIZE))
    cfg.PREFETCH = int(os.getenv("PREFETCH", cfg.PREFETCH))
    cfg.PULL_INTERVAL_S = float(os.getenv("PULL_INTERVAL_S", cfg.PULL_INTERVAL_S))

    # ------------------------------------------------
    # KV sync
    # ------------------------------------------------
    cfg.VLLM_HOST = os.getenv("VLLM_HOST", cfg.VLLM_HOST)
    cfg.VLLM_SUB_PORT = int(os.getenv("VLLM_SUB_PORT", cfg.VLLM_SUB_PORT))
    cfg.REDIS_HOST = os.getenv("REDIS_HOST", cfg.REDIS_HOST)
    cfg.REDIS_PORT = int(os.getenv("REDIS_PORT", cfg.REDIS_PORT))
    cfg.CONTAINER_NAME = os.getenv("CONTAINER_NAME", cfg.CONTAINER_NAME)
    cfg.MODEL_NAME_REDIS = os.getenv("MODEL_NAME_REDIS", cfg.MODEL_NAME_REDIS)

    # ------------------------------------------------
    # Sidecar mode
    # ------------------------------------------------
    cfg.SIDECAR_PORT = int(os.getenv("SIDECAR_PORT", cfg.SIDECAR_PORT))
    cfg.SIDECAR_MODE = os.getenv("SIDECAR_MODE", cfg.SIDECAR_MODE)

    # ------------------------------------------------
    # Timeouts
    # ------------------------------------------------
    cfg.ROUTER_PULL_TIMEOUT_S = float(os.getenv("ROUTER_PULL_TIMEOUT_S", cfg.ROUTER_PULL_TIMEOUT_S))
    cfg.VLLM_TIMEOUT_S = float(os.getenv("VLLM_TIMEOUT_S", cfg.VLLM_TIMEOUT_S))
    cfg.ROUTER_RESULT_TIMEOUT_S = float(os.getenv("ROUTER_RESULT_TIMEOUT_S", cfg.ROUTER_RESULT_TIMEOUT_S))

    # ------------------------------------------------
    # Result transport (kept)
    # ------------------------------------------------
    cfg.RESULT_TRANSPORT_MODE = os.getenv(
        "RESULT_TRANSPORT_MODE", cfg.RESULT_TRANSPORT_MODE
    ).lower()

    cfg.RESULT_SUBMIT_PATH = os.getenv(
        "RESULT_SUBMIT_PATH", cfg.RESULT_SUBMIT_PATH
    )

    # ------------------------------------------------
    # Pool knobs
    # ------------------------------------------------
    cfg.ROUTER_POOL_CONNECTIONS = int(os.getenv("ROUTER_POOL_CONNECTIONS", cfg.ROUTER_POOL_CONNECTIONS))
    cfg.ROUTER_POOL_MAXSIZE = int(os.getenv("ROUTER_POOL_MAXSIZE", cfg.ROUTER_POOL_MAXSIZE))
    cfg.VLLM_POOL_CONNECTIONS = int(os.getenv("VLLM_POOL_CONNECTIONS", cfg.VLLM_POOL_CONNECTIONS))
    cfg.VLLM_POOL_MAXSIZE = int(os.getenv("VLLM_POOL_MAXSIZE", cfg.VLLM_POOL_MAXSIZE))

    # ------------------------------------------------
    # Result retry knobs
    # ------------------------------------------------
    cfg.RESULT_POST_RETRY = os.getenv("RESULT_POST_RETRY", str(cfg.RESULT_POST_RETRY)).lower() == "true"
    cfg.RESULT_POST_MAX_RETRIES = int(os.getenv("RESULT_POST_MAX_RETRIES", cfg.RESULT_POST_MAX_RETRIES))
    cfg.RESULT_POST_BACKOFF_BASE_S = float(os.getenv("RESULT_POST_BACKOFF_BASE_S", cfg.RESULT_POST_BACKOFF_BASE_S))
    cfg.RESULT_POST_BACKOFF_CAP_S = float(os.getenv("RESULT_POST_BACKOFF_CAP_S", cfg.RESULT_POST_BACKOFF_CAP_S))

    # ------------------------------------------------
    # Tracing
    # ------------------------------------------------
    cfg.TRACE_ENABLED = os.getenv("TRACE_ENABLED", "false").lower() == "true"
    cfg.TRACE_SAMPLE_RATE = float(os.getenv("TRACE_SAMPLE_RATE", cfg.TRACE_SAMPLE_RATE))

    # ------------------------------------------------
    # Fixed-length / replay mode
    # ------------------------------------------------
    cfg.FORCE_IGNORE_EOS = os.getenv("FORCE_IGNORE_EOS", "false").lower() == "true"

    # ------------------------------------------------
    # Streaming
    # ------------------------------------------------
    cfg.STREAMING_MODE = os.getenv("STREAMING_MODE", "false").lower() == "true"

    # ------------------------------------------------
    # Logging
    # ------------------------------------------------
    cfg.LOG_LEVEL = os.getenv("LOG_LEVEL", cfg.LOG_LEVEL).lower()

    return cfg
