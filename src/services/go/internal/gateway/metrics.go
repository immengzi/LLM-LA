package gateway

import "github.com/prometheus/client_golang/prometheus"

var (
	RouterCentralQueueLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "router_central_queue_length",
		Help: "Current length of the centralized router queue",
	})

	RouterEnqueueTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_enqueue_total",
		Help: "Total requests enqueued to the router",
	})

	RouterCompletedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_completed_total",
		Help: "Total requests completed (result delivered)",
	})

	RouterPullTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_pull_total",
		Help: "Total pull requests received from sidecars",
	})

	RouterPullItemsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_pull_items_total",
		Help: "Total items pulled from the queue by sidecars",
	})

	RouterResultTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_result_total",
		Help: "Total result callbacks received",
	})

	RouterAdmissionTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_admission_total",
		Help: "Total requests received at router admission",
	})

	RouterV1ChatTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_v1_chat_total",
		Help: "Total /v1/chat/completions requests",
	})

	RouterActiveWaiters = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "router_active_waiters",
		Help: "Number of in-flight requests waiting for results",
	})
)

func RegisterMetrics() {
	prometheus.MustRegister(
		RouterCentralQueueLength,
		RouterEnqueueTotal,
		RouterCompletedTotal,
		RouterPullTotal,
		RouterPullItemsTotal,
		RouterResultTotal,
		RouterAdmissionTotal,
		RouterV1ChatTotal,
		RouterActiveWaiters,
	)
}
