package sidecar

import (
	"runtime"
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

	Goroutines = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sidecar_goroutines",
		Help: "Current number of goroutines.",
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
			Goroutines.Set(float64(runtime.NumGoroutine()))
		}
	}()
}
