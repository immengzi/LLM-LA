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
# SLO-driven dynamic pull backpressure metrics (lazily registered)
# --------------------------------
# These are created only when the feature is enabled (init_slo_metrics() is
# called from the SLO monitor's constructor). Keeping them out of the module
# import path means that when SLO_DYNAMIC_PULL_ENABLED=false the /metrics output
# is byte-for-byte identical to before — not even HELP/TYPE headers appear.

SIDECAR_SLO_DYNAMIC_PULL_CAP: "Gauge | None" = None
SIDECAR_SLO_OBSERVED_TPOT_SECONDS: "Gauge | None" = None
SIDECAR_SLO_VIOLATION: "Gauge | None" = None

_SLO_METRICS_INITED = False


def init_slo_metrics() -> None:
    """Register the SLO backpressure gauges. Idempotent; call when enabled."""
    global _SLO_METRICS_INITED
    global SIDECAR_SLO_DYNAMIC_PULL_CAP
    global SIDECAR_SLO_OBSERVED_TPOT_SECONDS
    global SIDECAR_SLO_VIOLATION
    if _SLO_METRICS_INITED:
        return
    SIDECAR_SLO_DYNAMIC_PULL_CAP = Gauge(
        "sidecar_slo_dynamic_pull_cap",
        "Current dynamic pull cap chosen by the SLO backpressure controller",
        ["endpoint"],
    )
    SIDECAR_SLO_OBSERVED_TPOT_SECONDS = Gauge(
        "sidecar_slo_observed_tpot_seconds",
        "Windowed TPOT observed from vLLM by the SLO backpressure monitor",
        ["endpoint"],
    )
    SIDECAR_SLO_VIOLATION = Gauge(
        "sidecar_slo_violation",
        "1 if the windowed TPOT currently violates the SLO target, else 0",
        ["endpoint"],
    )
    _SLO_METRICS_INITED = True

# --------------------------------
# KV-memory pull gate metrics (lazily registered)
# --------------------------------
# Registered only when KV_PULL_GATE_ENABLED, so the disabled path's /metrics
# output stays byte-for-byte identical (no HELP/TYPE headers), mirroring the SLO
# metrics pattern above.

SIDECAR_KV_PULL_GATE_SCALE: "Gauge | None" = None
SIDECAR_KV_PULL_GATE_KV_USAGE: "Gauge | None" = None

_KV_PULL_GATE_METRICS_INITED = False


def init_kv_pull_gate_metrics() -> None:
    """Register the KV-memory pull gate gauges. Idempotent; call when enabled."""
    global _KV_PULL_GATE_METRICS_INITED
    global SIDECAR_KV_PULL_GATE_SCALE
    global SIDECAR_KV_PULL_GATE_KV_USAGE
    if _KV_PULL_GATE_METRICS_INITED:
        return
    SIDECAR_KV_PULL_GATE_SCALE = Gauge(
        "sidecar_kv_pull_gate_scale",
        "Multiplier the KV-memory pull gate applied to want this tick "
        "(1=no throttle, 0=blocked)",
        ["endpoint"],
    )
    SIDECAR_KV_PULL_GATE_KV_USAGE = Gauge(
        "sidecar_kv_pull_gate_kv_usage",
        "GPU KV cache fill fraction [0,1] the pull gate last acted on",
        ["endpoint"],
    )
    _KV_PULL_GATE_METRICS_INITED = True


def set_kv_pull_gate_state(endpoint: str, scale: float, kv_usage=None) -> None:
    """Publish the KV pull gate scale (and the kv_usage it acted on).

    No-op until init_kv_pull_gate_metrics() has run (feature enabled).
    """
    if not _KV_PULL_GATE_METRICS_INITED:
        return
    try:
        SIDECAR_KV_PULL_GATE_SCALE.labels(endpoint=str(endpoint)).set(float(scale))
        if kv_usage is not None:
            SIDECAR_KV_PULL_GATE_KV_USAGE.labels(endpoint=str(endpoint)).set(float(kv_usage))
    except Exception:
        pass


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


def set_slo_backpressure_state(
    endpoint: str,
    cap: int,
    observed_tpot=None,
    slo_target=None,
) -> None:
    """Publish SLO backpressure observability (cap, observed TPOT, violation).

    No-ops if the gauges have not been registered yet (init_slo_metrics()),
    which only happens when the feature is enabled.
    """
    if not _SLO_METRICS_INITED:
        return
    try:
        SIDECAR_SLO_DYNAMIC_PULL_CAP.labels(endpoint=str(endpoint)).set(int(cap))
        if observed_tpot is not None:
            SIDECAR_SLO_OBSERVED_TPOT_SECONDS.labels(endpoint=str(endpoint)).set(
                float(observed_tpot)
            )
            if slo_target is not None:
                violated = 1 if float(observed_tpot) > float(slo_target) else 0
                SIDECAR_SLO_VIOLATION.labels(endpoint=str(endpoint)).set(violated)
    except Exception:
        pass
