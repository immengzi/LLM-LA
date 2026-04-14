# router/metrics.py
# -*- coding: utf-8 -*-

from prometheus_client import Counter, Gauge, Histogram

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


# -------------------------------------------------
# SLO-aware routing metrics
# -------------------------------------------------

ROUTER_SLO_SLACK_HISTOGRAM = Histogram(
    "router_slo_slack_seconds",
    "Slack (deadline - predicted_completion) at dispatch time",
    buckets=[-5.0, -2.0, -1.0, -0.5, -0.1, 0.0, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, float("inf")],
)

ROUTER_SLO_PREDICTED_MISS_TOTAL = Counter(
    "router_slo_predicted_miss_total",
    "Requests predicted to miss SLO at dispatch (slack < 0)",
)

ROUTER_SLO_ACTUAL_MISS_TOTAL = Counter(
    "router_slo_actual_miss_total",
    "Requests that actually missed SLO (measured on /result)",
)

ROUTER_SLO_ACTUAL_MET_TOTAL = Counter(
    "router_slo_actual_met_total",
    "Requests that met SLO (measured on /result)",
)

ROUTER_OUTPUT_LEN_ERROR_RATIO = Histogram(
    "router_output_len_error_ratio",
    "Output length prediction error: (predicted - actual) / actual",
    buckets=[-2.0, -1.0, -0.5, -0.2, -0.1, 0.0, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0],
)

ROUTER_TTFT_PREDICTION_ERROR = Histogram(
    "router_ttft_prediction_error_seconds",
    "TTFT prediction error: predicted - actual (seconds)",
    buckets=[-2.0, -1.0, -0.5, -0.1, 0.0, 0.1, 0.5, 1.0, 2.0, 5.0],
)

ROUTER_E2E_PREDICTION_ERROR = Histogram(
    "router_e2e_prediction_error_seconds",
    "E2E prediction error: predicted - actual (seconds)",
    buckets=[-5.0, -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 5.0, 10.0],
)

ROUTER_SLO_REGISTRY_SIZE = Gauge(
    "router_slo_registry_size",
    "Current number of entries in the SLO registry",
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


# -------------------------------------------------
# SLO helpers
# -------------------------------------------------

def observe_slo_slack(slack_s: float) -> None:
    try:
        ROUTER_SLO_SLACK_HISTOGRAM.observe(float(slack_s))
    except Exception:
        pass


def inc_slo_predicted_miss() -> None:
    try:
        ROUTER_SLO_PREDICTED_MISS_TOTAL.inc()
    except Exception:
        pass


def inc_slo_actual_miss() -> None:
    try:
        ROUTER_SLO_ACTUAL_MISS_TOTAL.inc()
    except Exception:
        pass


def inc_slo_actual_met() -> None:
    try:
        ROUTER_SLO_ACTUAL_MET_TOTAL.inc()
    except Exception:
        pass


def observe_output_len_error(ratio: float) -> None:
    try:
        ROUTER_OUTPUT_LEN_ERROR_RATIO.observe(float(ratio))
    except Exception:
        pass


def observe_ttft_prediction_error(error_s: float) -> None:
    try:
        ROUTER_TTFT_PREDICTION_ERROR.observe(float(error_s))
    except Exception:
        pass


def observe_e2e_prediction_error(error_s: float) -> None:
    try:
        ROUTER_E2E_PREDICTION_ERROR.observe(float(error_s))
    except Exception:
        pass


def set_slo_registry_size(n: int) -> None:
    try:
        ROUTER_SLO_REGISTRY_SIZE.set(int(n))
    except Exception:
        pass
