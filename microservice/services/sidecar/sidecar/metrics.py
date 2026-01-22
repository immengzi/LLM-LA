# sidecar/metrics.py
# -*- coding: utf-8 -*-

from prometheus_client import Counter, Gauge

# --------------------------------
# Sidecar metrics (minimal set)
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
