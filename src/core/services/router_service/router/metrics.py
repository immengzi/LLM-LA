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

# Per-model breakdown of the central queue length. Additive series: the global
# gauge above is left untouched for backward compatibility (dashboards, alerts,
# legacy autoscaling query). The `model` label is the router's resolved queue
# key (== servedModelName in multi-model mode, MODEL_NAME otherwise) so the
# per-model autoscaling query can select it.
ROUTER_CENTRAL_QUEUE_LENGTH_BY_MODEL = Gauge(
    "router_central_queue_length_by_model",
    "Current length of the centralized router queue, per model",
    ["model"],
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
# Key-affinity routing metrics
# -------------------------------------------------

ROUTER_AFFINITY_HITS_TOTAL = Counter(
    "router_affinity_hits_total",
    "Requests dispatched to their affinity-target endpoint",
)

ROUTER_AFFINITY_HOLDS_TOTAL = Counter(
    "router_affinity_holds_total",
    "Requests withheld from a non-matching endpoint in hard affinity mode",
)

ROUTER_AFFINITY_RELEASES_TOTAL = Counter(
    "router_affinity_releases_total",
    "Requests released to any endpoint after the hard-affinity timeout expired",
)

ROUTER_AFFINITY_MAP_SIZE = Gauge(
    "router_affinity_map_size",
    "Number of live conversation->endpoint affinity mappings",
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

# -------------------------------------------------
# Per-request latency histograms (prod observability)
# -------------------------------------------------

ROUTER_REQUEST_TTFT = Histogram(
    "router_request_ttft_seconds",
    "Time to first token as measured by the router (seconds)",
    ["model"],
    buckets=[0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
)

ROUTER_REQUEST_TPOT_AVG = Histogram(
    "router_request_tpot_avg_seconds",
    "Average time per output token (seconds)",
    ["model"],
    buckets=[0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0],
)

ROUTER_REQUEST_E2E = Histogram(
    "router_request_e2e_seconds",
    "End-to-end request latency as measured by the router (seconds)",
    ["model"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)


# ----------------------------
# Central queue helpers
# ----------------------------

def set_central_queue_length(n: int) -> None:
    try:
        ROUTER_CENTRAL_QUEUE_LENGTH.set(int(n))
    except Exception:
        pass


def set_central_queue_length_by_model(per_model: "dict[str, int]") -> None:
    """Publish the per-model central-queue breakdown.

    Additive only: never touches the global ``router_central_queue_length``
    gauge. ``per_model`` maps the router queue key (resolved model name) to its
    current queued count.
    """
    try:
        for model, n in per_model.items():
            ROUTER_CENTRAL_QUEUE_LENGTH_BY_MODEL.labels(model=str(model)).set(int(n))
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
# Key-affinity helpers
# -------------------------------------------------

def inc_affinity_hit(n: int = 1) -> None:
    try:
        ROUTER_AFFINITY_HITS_TOTAL.inc(int(n))
    except Exception:
        pass


def inc_affinity_hold(n: int = 1) -> None:
    try:
        ROUTER_AFFINITY_HOLDS_TOTAL.inc(int(n))
    except Exception:
        pass


def inc_affinity_release(n: int = 1) -> None:
    try:
        ROUTER_AFFINITY_RELEASES_TOTAL.inc(int(n))
    except Exception:
        pass


def set_affinity_map_size(n: int) -> None:
    try:
        ROUTER_AFFINITY_MAP_SIZE.set(int(n))
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


# -------------------------------------------------
# Per-request latency helpers
# -------------------------------------------------

def observe_request_ttft(ttft_s: float, model: str = "") -> None:
    try:
        ROUTER_REQUEST_TTFT.labels(model=model).observe(float(ttft_s))
    except Exception:
        pass


def observe_request_tpot_avg(tpot_s: float, model: str = "") -> None:
    try:
        ROUTER_REQUEST_TPOT_AVG.labels(model=model).observe(float(tpot_s))
    except Exception:
        pass


def observe_request_e2e(e2e_s: float, model: str = "") -> None:
    try:
        ROUTER_REQUEST_E2E.labels(model=model).observe(float(e2e_s))
    except Exception:
        pass
