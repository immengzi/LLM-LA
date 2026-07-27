package sidecar

import (
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	QueueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sidecar_queue_length",
		Help: "Logical queue depth (pending + inflight).",
	}, []string{"endpoint"})

	ReceivedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sidecar_received_requests_total",
		Help: "Total requests received from the router.",
	}, []string{"endpoint"})

	CompletedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sidecar_completed_requests_total",
		Help: "Total requests completed by vLLM.",
	}, []string{"endpoint"})

	// PythonThreads mirrors the Python sidecar's sidecar_python_threads gauge.
	// In Go the closest analogue of "active threads" is the goroutine count.
	PythonThreads = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sidecar_python_threads",
		Help: "Number of active worker routines in the kv-sidecar process",
	})

	WorkersTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sidecar_workers_total",
		Help: "Total number of vLLM workers.",
	}, []string{"endpoint"})

	WorkersBusy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sidecar_workers_busy",
		Help: "Number of workers currently processing a request.",
	}, []string{"endpoint"})
)

func StartGoroutineGauge() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			PythonThreads.Set(float64(runtime.NumGoroutine()))
		}
	}()
}

// --------------------------------
// SLO-driven dynamic pull backpressure metrics (lazily registered)
// --------------------------------
// Registered only when the feature is enabled (initSLOMetrics is called from the
// SLO monitor constructor), so a disabled sidecar's /metrics output is unchanged.

var (
	sloMetricsOnce sync.Once

	sloDynamicPullCap      *prometheus.GaugeVec
	sloObservedTpotSeconds *prometheus.GaugeVec
	sloViolation           *prometheus.GaugeVec
)

func initSLOMetrics() {
	sloMetricsOnce.Do(func() {
		sloDynamicPullCap = promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sidecar_slo_dynamic_pull_cap",
			Help: "Current dynamic pull cap chosen by the SLO backpressure controller",
		}, []string{"endpoint"})
		sloObservedTpotSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sidecar_slo_observed_tpot_seconds",
			Help: "Windowed TPOT observed from vLLM by the SLO backpressure monitor",
		}, []string{"endpoint"})
		sloViolation = promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sidecar_slo_violation",
			Help: "1 if the windowed TPOT currently violates the SLO target, else 0",
		}, []string{"endpoint"})
	})
}

// setSLOBackpressureState publishes cap / observed TPOT / violation. No-op if
// the gauges have not been registered yet (feature disabled).
func setSLOBackpressureState(endpoint string, cap int, observedTpot float64, hasTpot bool, sloTarget float64) {
	if sloDynamicPullCap == nil {
		return
	}
	sloDynamicPullCap.WithLabelValues(endpoint).Set(float64(cap))
	if hasTpot {
		sloObservedTpotSeconds.WithLabelValues(endpoint).Set(observedTpot)
		v := 0.0
		if observedTpot > sloTarget {
			v = 1.0
		}
		sloViolation.WithLabelValues(endpoint).Set(v)
	}
}

// --------------------------------
// KV-memory pull gate metrics (lazily registered)
// --------------------------------
// Registered only when KV_PULL_GATE_ENABLED (initKvPullGateMetrics is called
// from RouterPullWorker.Start), so a disabled sidecar's /metrics output is
// unchanged. Mirrors the Python sidecar_kv_pull_gate_* metrics.

var (
	kvPullGateMetricsOnce sync.Once
	kvPullGateScale       *prometheus.GaugeVec
	kvPullGateKvUsage     *prometheus.GaugeVec
)

func initKvPullGateMetrics() {
	kvPullGateMetricsOnce.Do(func() {
		kvPullGateScale = promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sidecar_kv_pull_gate_scale",
			Help: "Multiplier the KV-memory pull gate applied to want this tick (1=no throttle, 0=blocked)",
		}, []string{"endpoint"})
		kvPullGateKvUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sidecar_kv_pull_gate_kv_usage",
			Help: "GPU KV cache fill fraction [0,1] the pull gate last acted on",
		}, []string{"endpoint"})
	})
}

// setKvPullGateState publishes the gate scale and the kv_usage it acted on.
// No-op until initKvPullGateMetrics has run (feature enabled).
func setKvPullGateState(endpoint string, scale, kvUsage float64) {
	if kvPullGateScale == nil {
		return
	}
	kvPullGateScale.WithLabelValues(endpoint).Set(scale)
	kvPullGateKvUsage.WithLabelValues(endpoint).Set(kvUsage)
}
