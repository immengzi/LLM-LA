# router/metrics.py
# -*- coding: utf-8 -*-

from prometheus_client import Counter, Gauge

# ----------------------------
# Router metrics (minimal set)
# ----------------------------

ROUTER_CENTRAL_QUEUE_LENGTH = Gauge(
    "router_central_queue_length",
    "Current length of the centralized router queue",
)

ROUTER_ADMISSION_REQUESTS_TOTAL = Counter(
    "router_admission_requests_total",
    "Total requests received at router admission",
)

ROUTER_DISPATCH_REQUESTS_TOTAL = Counter(
    "router_dispatch_requests_total",
    "Total requests dispatched from router to sidecars",
    ["endpoint"],
)


def set_central_queue_length(n: int) -> None:
    try:
        ROUTER_CENTRAL_QUEUE_LENGTH.set(int(n))
    except Exception:
        pass


def inc_admission() -> None:
    try:
        ROUTER_ADMISSION_REQUESTS_TOTAL.inc()
    except Exception:
        pass


def inc_dispatch(endpoint: str) -> None:
    try:
        ROUTER_DISPATCH_REQUESTS_TOTAL.labels(endpoint=str(endpoint)).inc()
    except Exception:
        pass
