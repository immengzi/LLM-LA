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

	"github.com/saeid/kv-serving-go/internal/gateway"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := gateway.LoadConfig()
	cfg.PrintBanner()

	gateway.RegisterMetrics()

	kvWatcher := gateway.NewKVWatcher(cfg)
	kvWatcher.Start()
	log.Println("[router] KVWatcher started.")

	queue := gateway.NewCentralQueue(cfg, kvWatcher)
	results := gateway.NewResultStore()
	results.StartCleanupLoop(5*time.Minute, 1*time.Second)

	var pushRouter *gateway.PushDispatcher
	if cfg.IsPushMode() {
		pushRouter = gateway.NewPushDispatcher(cfg)
		pushRouter.RefreshEndpoints()
		log.Printf("[router] PushRouter started in mode=%s", cfg.RouterMode)
	} else {
		log.Println("[router] running in PULL mode.")
	}

	srv := gateway.NewServer(cfg, queue, results, kvWatcher, pushRouter)
	router := srv.Router()

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  time.Duration(cfg.ResultTimeoutS+10) * time.Second,
		WriteTimeout: time.Duration(cfg.ResultTimeoutS+10) * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("[router] Listening on %s", addr)
		errCh <- httpServer.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("[router] Received signal %v, shutting down...", sig)
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("[router] Server error: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("[router] Shutdown error: %v", err)
	}

	kvWatcher.Stop()
	log.Println("[router] Shutdown complete.")
}
