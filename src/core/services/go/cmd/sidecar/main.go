package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/saeid/kv-serving-go/internal/sidecar"
)

func main() {
	cfg := sidecar.LoadConfig()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("[main] invalid configuration: %v", err)
	}
	endpointID := cfg.ContainerName

	log.Printf("=== Go Sidecar ===")
	log.Printf("  mode            = %s", cfg.SidecarMode)
	log.Printf("  endpoint_id     = %s", endpointID)
	log.Printf("  router_url      = %s", cfg.RouterURL)
	log.Printf("  engine          = %s", cfg.InferenceEngine)
	log.Printf("  inference_url   = %s", cfg.InferenceURL)
	log.Printf("  model_name      = %s", cfg.ModelName)
	log.Printf("  batch_size      = %d", cfg.BatchSize)
	log.Printf("  sidecar_port    = %d", cfg.SidecarPort)
	log.Printf("  result_mode     = %s", cfg.ResultTransportMode)
	log.Printf("  trace_enabled   = %v", cfg.TraceEnabled)
	log.Printf("====================")

	queue := sidecar.NewLocalQueue(endpointID)

	// SLO-driven dynamic pull backpressure (default OFF). When disabled we never
	// construct the monitor and never set a cap provider, so RouterPullWorker
	// uses the static BatchSize + Prefetch cap (zero-change / zero-cost path).
	var sloMonitor *sidecar.SloBackpressureMonitor
	if cfg.SLODynamicPullEnabled && cfg.SidecarMode == "pull" {
		sloMonitor = sidecar.NewSloBackpressureMonitor(cfg, cfg.PullCap(), endpointID)
	}

	var puller *sidecar.RouterPullWorker
	if cfg.SidecarMode == "pull" {
		puller = sidecar.NewRouterPullWorker(cfg, queue, endpointID)
		if sloMonitor != nil {
			puller.SetCapProvider(sloMonitor.GetCap)
		}
		puller.Start()
	}
	if sloMonitor != nil {
		sloMonitor.Start()
	}

	var kvMon *sidecar.KvUsageMonitor
	if cfg.KVUsageReport {
		kvMon = sidecar.NewKvUsageMonitor(cfg)
		sidecar.BindKvUsageMonitor(kvMon)
		kvMon.Start()
	} else {
		sidecar.BindKvUsageMonitor(nil)
	}

	poster := sidecar.NewResultPoster(cfg)
	poster.Start()

	// Total workers = BATCH_SIZE + PREFETCH (parity with Python main.py).
	totalWorkers := cfg.PullCap()

	var busyCount atomic.Int64
	sidecar.WorkersTotal.WithLabelValues(endpointID).Set(float64(totalWorkers))

	for i := 0; i < totalWorkers; i++ {
		w := sidecar.NewInferenceWorker(i, cfg, queue, poster, puller, endpointID, &busyCount)
		w.Start()
	}

	kvSub := sidecar.NewKVSubscriber(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := kvSub.Start(ctx); err != nil {
		log.Printf("[main] kv subscriber start error: %v", err)
	}

	sidecar.StartGoroutineGauge()

	log.Printf("[sidecar] running in %s mode (BATCH_SIZE=%d, PREFETCH=%d, workers=%d, "+
		"FORCE_IGNORE_EOS=%v, STREAMING_MODE=%v, port=%d, endpoint_id=%s)",
		strings.ToUpper(cfg.SidecarMode), cfg.BatchSize, cfg.Prefetch, totalWorkers,
		cfg.ForceIgnoreEos, cfg.StreamingMode, cfg.SidecarPort, endpointID)

	r := chi.NewRouter()
	prober := sidecar.NewEngineProber(cfg)
	healthHandler := func(readiness bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			body, code := sidecar.HealthResponse(r.Context(), cfg, queue, puller, kvSub, prober, readiness)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			json.NewEncoder(w).Encode(body)
		}
	}
	r.Get("/health", healthHandler(false))
	r.Get("/ready", healthHandler(true))
	r.Handle("/metrics", promhttp.Handler())

	engineHealthyCached := func(r *http.Request) bool {
		if puller != nil {
			return puller.EngineHealthy()
		}
		return prober.Probe(r.Context(), true)
	}

	r.Post("/push", func(w http.ResponseWriter, r *http.Request) {
		var item struct {
			ReqID  string         `json:"req_id"`
			Prompt string         `json:"prompt"`
			Meta   map[string]any `json:"meta"`
		}
		if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}

		st := queue.State()
		logicalBefore := st.Pending + st.Inflight

		// Readiness + backpressure gate (central-push / push): give the router a
		// signal to requeue instead of overrunning a warming/full pod.
		if !engineHealthyCached(r) {
			reason := "engine_unhealthy"
			if cfg.InferenceEngine == "vllm" {
				reason = "vllm_unhealthy"
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{"status": "unavailable", "reason": reason})
			return
		}
		if cap := cfg.PullCap(); cap > 0 && logicalBefore >= cap {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{"status": "busy", "reason": "queue_full", "logical": logicalBefore})
			return
		}

		sidecar.ReceivedRequests.WithLabelValues(endpointID).Inc()

		meta := item.Meta
		if meta == nil {
			meta = map[string]any{}
		}

		if cfg.TraceEnabled {
			nowPush := float64(time.Now().UnixNano()) / 1e9
			tr, _ := meta["__trace__"].(map[string]any)
			out := map[string]any{}
			for k, v := range tr {
				out[k] = v
			}
			out["t_arrive_sidecar_push"] = nowPush
			out["rcpt_push_recv_wall"] = nowPush
			out["sidecar_queue_len_before"] = st.Pending
			out["sidecar_inflight_before"] = st.Inflight
			out["sidecar_logical_before"] = logicalBefore
			out["sidecar_queue_len_after"] = st.Pending + 1
			out["sidecar_logical_after"] = logicalBefore + 1
			meta["__trace__"] = out
		}

		queue.Put(sidecar.QueueItem{
			ReqID:  item.ReqID,
			Prompt: item.Prompt,
			Meta:   meta,
		})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	addr := fmt.Sprintf(":%d", cfg.SidecarPort)
	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		log.Printf("[main] HTTP server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[main] server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("[main] received signal %v, shutting down...", sig)

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("[main] server shutdown error: %v", err)
	}
	if sloMonitor != nil {
		sloMonitor.Stop()
	}
	if kvMon != nil {
		kvMon.Stop()
		sidecar.BindKvUsageMonitor(nil)
	}
	if puller != nil {
		puller.Stop()
	}
	kvSub.Stop()
	cancel()
	log.Println("[main] shutdown complete")
}
