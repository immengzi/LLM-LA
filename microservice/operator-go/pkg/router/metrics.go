package router

import "github.com/prometheus/client_golang/prometheus"

var (
	MetricQueueLen = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "router_central_queue_length",
		Help: "Number of requests in the central queue",
	})
	MetricEnqueueTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_enqueue_total",
		Help: "Total enqueue requests received",
	})
	MetricResultTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_result_total",
		Help: "Total results received from sidecars",
	})
	MetricPullTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_pull_total",
		Help: "Total pull requests from sidecars",
	})
	MetricPullItemsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_pull_items_total",
		Help: "Total items returned via pull",
	})
	MetricPushTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_push_total",
		Help: "Total push dispatches",
	})
	MetricPushErrorTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "router_push_error_total",
		Help: "Total push dispatch errors",
	})
)

func init() {
	prometheus.MustRegister(
		MetricQueueLen,
		MetricEnqueueTotal,
		MetricResultTotal,
		MetricPullTotal,
		MetricPullItemsTotal,
		MetricPushTotal,
		MetricPushErrorTotal,
	)
}
