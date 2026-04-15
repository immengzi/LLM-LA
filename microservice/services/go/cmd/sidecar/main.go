package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/saeid/kv-serving-go/internal/sidecar"
)

func main() {
	cfg := sidecar.LoadConfig()
	endpointID := cfg.ContainerName

	log.Printf("=== Go Sidecar ===")
	log.Printf("  mode            = %s", cfg.SidecarMode)
	log.Printf("  endpoint_id     = %s", endpointID)
	log.Printf("  router_url      = %s", cfg.RouterURL)
	log.Printf("  vllm_url        = %s", cfg.VLLMURL)
	log.Printf("  model_name      = %s", cfg.ModelName)
	log.Printf("  batch_size      = %d", cfg.BatchSize)
	log.Printf("  sidecar_port    = %d", cfg.SidecarPort)
	log.Printf("  result_mode     = %s", cfg.ResultTransportMode)
	log.Printf("  trace_enabled   = %v", cfg.TraceEnabled)
	log.Printf("====================")

	queue := sidecar.NewLocalQueue(endpointID)

	var puller *sidecar.RouterPullWorker
	if cfg.SidecarMode == "pull" {
		puller = sidecar.NewRouterPullWorker(cfg, queue, endpointID)
	}

	poster := sidecar.NewResultPoster(cfg)
	poster.Start()

	var busyCount atomic.Int64
	sidecar.WorkersTotal.WithLabelValues(endpointID).Set(float64(cfg.BatchSize))

	for i := 0; i < cfg.BatchSize; i++ {
		w := sidecar.NewVLLMWorker(i, cfg, queue, poster, puller, endpointID, &busyCount)
		w.Start()
	}

	kvSub := sidecar.NewKVSubscriber(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := kvSub.Start(ctx); err != nil {
		log.Printf("[main] kv subscriber start error: %v", err)
	}

	sidecar.StartGoroutineGauge()

	if puller != nil {
		puller.PullIfCapacity()
	}

	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		st := queue.State()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "ok",
			"queue_len": st.Pending,
			"inflight":  st.Inflight,
			"logical":   st.Pending + st.Inflight,
		})
	})
	r.Handle("/metrics", promhttp.Handler())

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
		queue.Put(sidecar.QueueItem{
			ReqID:  item.ReqID,
			Prompt: item.Prompt,
			Meta:   item.Meta,
		})
		sidecar.ReceivedRequests.WithLabelValues(endpointID).Inc()
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
	kvSub.Stop()
	cancel()
	log.Println("[main] shutdown complete")
}
