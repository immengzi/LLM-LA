# sidecar/metrics.py
# -*- coding: utf-8 -*-

from prometheus_client import Counter, Gauge

# --------------------------------
# Sidecar metrics (existing)
# --------------------------------
# We label by endpoint (pod/container name) so dashboards are easy.

SIDECAR_QUEUE_LENGTH = Gauge(
    "sidecar_queue_length",
    "Current logical length of the sidecar queue (pending + inflight)",
    ["endpoint"],
)

SIDECAR_RECEIVED_REQUESTS_TOTAL = Counter(
    "sidecar_received_requests_total",
    "Total requests received by sidecar (router -> sidecar)",
    ["endpoint"],
)

SIDECAR_COMPLETED_REQUESTS_TOTAL = Counter(
    "sidecar_completed_requests_total",
    "Total requests completed by sidecar (result produced and queued for return)",
    ["endpoint"],
)

# --------------------------------
# Sidecar thread / worker metrics
# --------------------------------

SIDECAR_PYTHON_THREADS = Gauge(
    "sidecar_python_threads",
    "Number of active Python threads in the kv-sidecar process",
)

SIDECAR_WORKERS_TOTAL = Gauge(
    "sidecar_workers_total",
    "Configured number of VLLMWorker threads in the kv-sidecar process",
    ["endpoint"],
)

SIDECAR_WORKERS_BUSY = Gauge(
    "sidecar_workers_busy",
    "Number of VLLMWorker threads currently busy processing a request",
    ["endpoint"],
)

# --------------------------------
# Existing helpers
# --------------------------------


def set_sidecar_queue_length(endpoint: str, n: int) -> None:
    try:
        SIDECAR_QUEUE_LENGTH.labels(endpoint=str(endpoint)).set(int(n))
    except Exception:
        pass


def inc_received(endpoint: str) -> None:
    try:
        SIDECAR_RECEIVED_REQUESTS_TOTAL.labels(endpoint=str(endpoint)).inc()
    except Exception:
        pass


def inc_completed(endpoint: str) -> None:
    try:
        SIDECAR_COMPLETED_REQUESTS_TOTAL.labels(endpoint=str(endpoint)).inc()
    except Exception:
        pass


# --------------------------------
# Helpers
# --------------------------------


def set_sidecar_python_threads(n: int) -> None:
    try:
        SIDECAR_PYTHON_THREADS.set(int(n))
    except Exception:
        pass


def set_sidecar_workers_total(endpoint: str, n: int) -> None:
    try:
        SIDECAR_WORKERS_TOTAL.labels(endpoint=str(endpoint)).set(int(n))
    except Exception:
        pass


def set_sidecar_workers_busy(endpoint: str, n: int) -> None:
    try:
        SIDECAR_WORKERS_BUSY.labels(endpoint=str(endpoint)).set(int(n))
    except Exception:
        pass
