package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	sc "github.com/vllmkv/operator/pkg/sidecar"
)

func main() {
	cfg := sc.LoadConfig()

	log.Printf("[sidecar] mode=%s container=%s vllm=%s router=%s",
		cfg.SidecarMode, cfg.ContainerName, cfg.VllmURL, cfg.RouterURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := sc.NewLocalQueue()
	wake := make(chan struct{}, 1)

	poster := sc.NewResultPoster(cfg)

	// Workers
	numWorkers := cfg.BatchSize
	for i := 0; i < numWorkers; i++ {
		w := sc.NewVLLMWorker(i, queue, poster, cfg, wake)
		go w.Run(ctx)
	}

	// Pull loop (pull mode only)
	if cfg.IsPullMode() {
		puller := sc.NewRouterPuller(queue, cfg, wake)
		go puller.Run(ctx)
	}

	// Redis for KV subscriber
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
	})
	defer rdb.Close()

	kvSub := sc.NewKVSubscriber(cfg, rdb)
	go kvSub.Run(ctx)

	// Metrics updater
	go func() {
		for {
			sc.MetricQueuePending.Set(float64(queue.Pending()))
			sc.MetricQueueInflight.Set(float64(queue.Inflight()))
			sc.MetricQueueLogical.Set(float64(queue.Logical()))
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// HTTP server
	srv := sc.NewServer(queue, cfg, wake)
	addr := fmt.Sprintf("0.0.0.0:%d", cfg.SidecarPort)
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler()}

	go func() {
		log.Printf("[sidecar] listening on %s", addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[sidecar] http: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("[sidecar] shutting down")

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	httpSrv.Shutdown(shutdownCtx)
}
