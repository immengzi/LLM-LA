package gateway

import "github.com/prometheus/client_golang/prometheus"

// Metrics mirror src/services/router_service/router/metrics.py exactly:
// same metric names, types, labels, and histogram buckets, so that the Go
// router's /metrics output is interchangeable with the Python router's.

var (
	RouterCentralQueueLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "router_central_queue_length",
		Help: "Current length of the centralized router queue",
	})

	RouterAdmissionRequestsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_admission_requests_total",
		Help: "Total requests received at router admission",
	})

	RouterDispatchRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "router_dispatch_requests_total",
		Help: "Total requests dispatched from router to sidecars",
	}, []string{"endpoint"})

	// Push dispatch decoupling metrics.
	RouterPushDispatchQueueLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "router_push_dispatch_queue_length",
		Help: "Current number of pending push-dispatch tasks",
	})
	RouterPushDispatchEnqueuedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_push_dispatch_enqueued_total",
		Help: "Total number of push-dispatch tasks enqueued",
	})
	RouterPushDispatchStartedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_push_dispatch_started_total",
		Help: "Total number of push-dispatch tasks started by workers",
	})
	RouterPushDispatchFailedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_push_dispatch_failed_total",
		Help: "Total number of push-dispatch tasks that failed before successful sidecar push",
	})
	RouterPushDispatchDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_push_dispatch_dropped_total",
		Help: "Total number of push-dispatch tasks dropped due to full dispatch queue",
	})

	// SLO-aware routing metrics.
	RouterSLOSlackHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "router_slo_slack_seconds",
		Help:    "Slack (deadline - predicted_completion) at dispatch time",
		Buckets: []float64{-5.0, -2.0, -1.0, -0.5, -0.1, 0.0, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0},
	})
	RouterSLOPredictedMissTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_slo_predicted_miss_total",
		Help: "Requests predicted to miss SLO at dispatch (slack < 0)",
	})
	RouterSLOActualMissTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_slo_actual_miss_total",
		Help: "Requests that actually missed SLO (measured on /result)",
	})
	RouterSLOActualMetTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_slo_actual_met_total",
		Help: "Requests that met SLO (measured on /result)",
	})
	RouterOutputLenErrorRatio = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "router_output_len_error_ratio",
		Help:    "Output length prediction error: (predicted - actual) / actual",
		Buckets: []float64{-2.0, -1.0, -0.5, -0.2, -0.1, 0.0, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0},
	})
	RouterTTFTPredictionError = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "router_ttft_prediction_error_seconds",
		Help:    "TTFT prediction error: predicted - actual (seconds)",
		Buckets: []float64{-2.0, -1.0, -0.5, -0.1, 0.0, 0.1, 0.5, 1.0, 2.0, 5.0},
	})
	RouterE2EPredictionError = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "router_e2e_prediction_error_seconds",
		Help:    "E2E prediction error: predicted - actual (seconds)",
		Buckets: []float64{-5.0, -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 5.0, 10.0},
	})
	RouterSLORegistrySize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "router_slo_registry_size",
		Help: "Current number of entries in the SLO registry",
	})

	// Per-request latency histograms.
	RouterRequestTTFT = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "router_request_ttft_seconds",
		Help:    "Time to first token as measured by the router (seconds)",
		Buckets: []float64{0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0},
	}, []string{"model"})
	RouterRequestTPOTAvg = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "router_request_tpot_avg_seconds",
		Help:    "Average time per output token (seconds)",
		Buckets: []float64{0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0},
	}, []string{"model"})
	RouterRequestE2E = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "router_request_e2e_seconds",
		Help:    "End-to-end request latency as measured by the router (seconds)",
		Buckets: []float64{0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0},
	}, []string{"model"})
)

func RegisterMetrics() {
	prometheus.MustRegister(
		RouterCentralQueueLength,
		RouterAdmissionRequestsTotal,
		RouterDispatchRequestsTotal,
		RouterPushDispatchQueueLength,
		RouterPushDispatchEnqueuedTotal,
		RouterPushDispatchStartedTotal,
		RouterPushDispatchFailedTotal,
		RouterPushDispatchDroppedTotal,
		RouterSLOSlackHistogram,
		RouterSLOPredictedMissTotal,
		RouterSLOActualMissTotal,
		RouterSLOActualMetTotal,
		RouterOutputLenErrorRatio,
		RouterTTFTPredictionError,
		RouterE2EPredictionError,
		RouterSLORegistrySize,
		RouterRequestTTFT,
		RouterRequestTPOTAvg,
		RouterRequestE2E,
	)
}

// ---- helpers mirroring metrics.py ----

func setCentralQueueLength(n int)  { RouterCentralQueueLength.Set(float64(n)) }
func incAdmission()                { RouterAdmissionRequestsTotal.Inc() }
func incDispatch(endpoint string)  { RouterDispatchRequestsTotal.WithLabelValues(endpoint).Inc() }
func setPushDispatchQueueLength(n int) { RouterPushDispatchQueueLength.Set(float64(n)) }
func incPushDispatchEnqueued()     { RouterPushDispatchEnqueuedTotal.Inc() }
func incPushDispatchStarted()      { RouterPushDispatchStartedTotal.Inc() }
func incPushDispatchFailed()       { RouterPushDispatchFailedTotal.Inc() }
func incPushDispatchDropped()      { RouterPushDispatchDroppedTotal.Inc() }
func observeSLOSlack(s float64)    { RouterSLOSlackHistogram.Observe(s) }
func incSLOPredictedMiss()         { RouterSLOPredictedMissTotal.Inc() }
func incSLOActualMiss()            { RouterSLOActualMissTotal.Inc() }
func incSLOActualMet()             { RouterSLOActualMetTotal.Inc() }
func observeOutputLenError(r float64) { RouterOutputLenErrorRatio.Observe(r) }
func observeTTFTPredictionError(e float64) { RouterTTFTPredictionError.Observe(e) }
func observeE2EPredictionError(e float64)  { RouterE2EPredictionError.Observe(e) }
func setSLORegistrySize(n int)     { RouterSLORegistrySize.Set(float64(n)) }
func observeRequestTTFT(s float64, model string) { RouterRequestTTFT.WithLabelValues(model).Observe(s) }
func observeRequestTPOTAvg(s float64, model string) { RouterRequestTPOTAvg.WithLabelValues(model).Observe(s) }
func observeRequestE2E(s float64, model string) { RouterRequestE2E.WithLabelValues(model).Observe(s) }
