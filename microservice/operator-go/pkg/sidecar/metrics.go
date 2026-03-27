package sidecar

import "github.com/prometheus/client_golang/prometheus"

var (
	MetricQueuePending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sidecar_queue_pending",
		Help: "Number of items pending in local queue",
	})
	MetricQueueInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sidecar_queue_inflight",
		Help: "Number of items being processed by vLLM workers",
	})
	MetricQueueLogical = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sidecar_queue_logical",
		Help: "Total logical queue depth (pending + inflight)",
	})
	MetricPullTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sidecar_pull_total",
		Help: "Total items pulled from router",
	})
	MetricResultTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sidecar_result_total",
		Help: "Total results posted back to router",
	})
	MetricVllmErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sidecar_vllm_errors_total",
		Help: "Total vLLM call errors",
	})
)

func init() {
	prometheus.MustRegister(
		MetricQueuePending,
		MetricQueueInflight,
		MetricQueueLogical,
		MetricPullTotal,
		MetricResultTotal,
		MetricVllmErrors,
	)
}
