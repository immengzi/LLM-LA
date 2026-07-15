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
    # Data Parallel (DP) multi-engine subscription
    # ------------------------------------------------

    DP_SIZE: int = 1
    DP_SIZE_LOCAL: int = 1

    # ------------------------------------------------
    # SLO-driven dynamic pull backpressure (default OFF)
    # ------------------------------------------------
    # When disabled the sidecar behaves *exactly* as before: no monitor thread
    # is started and pull_cap stays BATCH_SIZE + PREFETCH. See slo_backpressure.py.

    # Master switch. false -> zero behavior change / zero extra cost.
    SLO_DYNAMIC_PULL_ENABLED: bool = False

    # Target TPOT SLO in seconds. TPOT above this (windowed) counts as a violation.
    SLO_TPOT_SLO_S: float = 0.05

    # How often the monitor scrapes vLLM /metrics and re-evaluates the cap.
    SLO_EVAL_INTERVAL_S: float = 5.0

    # Sliding window size (number of eval samples) used to smooth TPOT.
    SLO_WINDOW_SAMPLES: int = 6

    # Aggregation over the window: "mean" or "p90".
    SLO_WINDOW_AGG: str = "mean"

    # Hard floor / ceiling for the dynamic pull cap.
    #   min_pull: never starve completely (>= 1).
    #   max_pull: <= 0 means "use BATCH_SIZE + PREFETCH" (the original pull cap).
    SLO_MIN_PULL: int = 1
    SLO_MAX_PULL: int = 0

    # Decrease policy on violation: "additive" (cap - step) or
    # "multiplicative" (floor(cap * factor)). AIMD default = additive.
    SLO_DECREASE_MODE: str = "additive"
    SLO_DECREASE_STEP: int = 1
    SLO_DECREASE_FACTOR: float = 0.5

    # Recovery policy when inside SLO: additive step up.
    SLO_RECOVER_STEP: int = 1

    # Cooldown / hysteresis: minimum seconds between two cap adjustments.
    SLO_COOLDOWN_S: float = 10.0

    # Prometheus metric name exposed by vLLM for TPOT (histogram).
    SLO_TPOT_METRIC: str = "vllm:time_per_output_token_seconds"

    # HTTP timeout for scraping vLLM /metrics.
    SLO_SCRAPE_TIMEOUT_S: float = 2.0

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
    # DP multi-engine
    # ------------------------------------------------
    cfg.DP_SIZE = int(os.getenv("DP_SIZE", cfg.DP_SIZE))
    cfg.DP_SIZE_LOCAL = int(os.getenv("DP_SIZE_LOCAL", cfg.DP_SIZE_LOCAL))

    # ------------------------------------------------
    # SLO-driven dynamic pull backpressure
    # ------------------------------------------------
    cfg.SLO_DYNAMIC_PULL_ENABLED = (
        os.getenv("SLO_DYNAMIC_PULL_ENABLED", str(cfg.SLO_DYNAMIC_PULL_ENABLED)).lower() == "true"
    )
    cfg.SLO_TPOT_SLO_S = float(os.getenv("SLO_TPOT_SLO_S", cfg.SLO_TPOT_SLO_S))
    cfg.SLO_EVAL_INTERVAL_S = float(os.getenv("SLO_EVAL_INTERVAL_S", cfg.SLO_EVAL_INTERVAL_S))
    cfg.SLO_WINDOW_SAMPLES = int(os.getenv("SLO_WINDOW_SAMPLES", cfg.SLO_WINDOW_SAMPLES))
    cfg.SLO_WINDOW_AGG = os.getenv("SLO_WINDOW_AGG", cfg.SLO_WINDOW_AGG).lower()
    cfg.SLO_MIN_PULL = int(os.getenv("SLO_MIN_PULL", cfg.SLO_MIN_PULL))
    cfg.SLO_MAX_PULL = int(os.getenv("SLO_MAX_PULL", cfg.SLO_MAX_PULL))
    cfg.SLO_DECREASE_MODE = os.getenv("SLO_DECREASE_MODE", cfg.SLO_DECREASE_MODE).lower()
    cfg.SLO_DECREASE_STEP = int(os.getenv("SLO_DECREASE_STEP", cfg.SLO_DECREASE_STEP))
    cfg.SLO_DECREASE_FACTOR = float(os.getenv("SLO_DECREASE_FACTOR", cfg.SLO_DECREASE_FACTOR))
    cfg.SLO_RECOVER_STEP = int(os.getenv("SLO_RECOVER_STEP", cfg.SLO_RECOVER_STEP))
    cfg.SLO_COOLDOWN_S = float(os.getenv("SLO_COOLDOWN_S", cfg.SLO_COOLDOWN_S))
    cfg.SLO_TPOT_METRIC = os.getenv("SLO_TPOT_METRIC", cfg.SLO_TPOT_METRIC)
    cfg.SLO_SCRAPE_TIMEOUT_S = float(os.getenv("SLO_SCRAPE_TIMEOUT_S", cfg.SLO_SCRAPE_TIMEOUT_S))

    # ------------------------------------------------
    # Logging
    # ------------------------------------------------
    cfg.LOG_LEVEL = os.getenv("LOG_LEVEL", cfg.LOG_LEVEL).lower()

    return cfg
