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

# -------------------------------------------------
# Push dispatch decoupling metrics
# -------------------------------------------------

ROUTER_PUSH_DISPATCH_QUEUE_LENGTH = Gauge(
    "router_push_dispatch_queue_length",
    "Current number of pending push-dispatch tasks",
)

ROUTER_PUSH_DISPATCH_ENQUEUED_TOTAL = Counter(
    "router_push_dispatch_enqueued_total",
    "Total number of push-dispatch tasks enqueued",
)

ROUTER_PUSH_DISPATCH_STARTED_TOTAL = Counter(
    "router_push_dispatch_started_total",
    "Total number of push-dispatch tasks started by workers",
)

ROUTER_PUSH_DISPATCH_FAILED_TOTAL = Counter(
    "router_push_dispatch_failed_total",
    "Total number of push-dispatch tasks that failed before successful sidecar push",
)

ROUTER_PUSH_DISPATCH_DROPPED_TOTAL = Counter(
    "router_push_dispatch_dropped_total",
    "Total number of push-dispatch tasks dropped due to full dispatch queue",
)


# ----------------------------
# Central queue helpers
# ----------------------------

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


# -------------------------------------------------
# Push-dispatch helpers
# -------------------------------------------------

def set_push_dispatch_queue_length(n: int) -> None:
    try:
        ROUTER_PUSH_DISPATCH_QUEUE_LENGTH.set(int(n))
    except Exception:
        pass


def inc_push_dispatch_enqueued() -> None:
    try:
        ROUTER_PUSH_DISPATCH_ENQUEUED_TOTAL.inc()
    except Exception:
        pass


def inc_push_dispatch_started() -> None:
    try:
        ROUTER_PUSH_DISPATCH_STARTED_TOTAL.inc()
    except Exception:
        pass


def inc_push_dispatch_failed() -> None:
    try:
        ROUTER_PUSH_DISPATCH_FAILED_TOTAL.inc()
    except Exception:
        pass


def inc_push_dispatch_dropped() -> None:
    try:
        ROUTER_PUSH_DISPATCH_DROPPED_TOTAL.inc()
    except Exception:
        pass
