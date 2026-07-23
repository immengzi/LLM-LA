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

	registry := gateway.MaybeLoadModelRegistry(cfg)
	kv := gateway.NewKVAware()

	hashClient := gateway.NewHashClient(cfg)

	// KVWatcher discovers k8s pods; external-push has none, so it is disabled
	// there (OwnerLookup below still provides prefix owners from the router-side
	// KV subscriber). Mirrors api.py skipping the watcher for external-push.
	kvWatcher := gateway.NewKVWatcher(cfg, kv, registry)
	if !cfg.IsExternalPush() {
		kvWatcher.Start()
		log.Println("[router] KVWatcher started.")
	} else {
		log.Println("[router] external-push mode: KVWatcher disabled (no k8s pods).")
	}

	queue := gateway.NewCentralQueue(cfg, kv)
	results := gateway.NewResultStore()
	results.StartCleanupLoop(
		time.Duration(cfg.PollResultTTLS*float64(time.Second)),
		time.Duration(cfg.PollCleanupIntervalS*float64(time.Second)),
	)

	// PushRouter is used for pod discovery + delivery by push-* AND central-push.
	var pushRouter *gateway.PushDispatcher
	if cfg.UsesPushDelivery() {
		pushRouter = gateway.NewPushDispatcher(cfg, kv)
		pushRouter.RefreshEndpoints()
		log.Printf("[router] PushRouter started in mode=%s", cfg.RouterMode)
	} else {
		log.Println("[router] running in PULL mode.")
	}

	srv := gateway.NewServer(cfg, queue, results, kv, hashClient, registry, kvWatcher, pushRouter)

	// Targeted per-request block-owner lookup (preferred routing source; also
	// used for exact, eviction-aware kv_hit measurement when only measuring).
	var ownerLookup *gateway.OwnerLookup
	if (cfg.KVAware || cfg.MeasurePrefix) && cfg.KVOwnerSource == "lookup" {
		ownerLookup = gateway.NewOwnerLookup(cfg)
		srv.SetOwnerLookup(ownerLookup)
		log.Printf("[router] KV owner lookup ready (targeted Redis, max_blocks=%d).", cfg.KVLookupMaxBlocks)
	}

	// Decoupled push dispatcher: buffers requests so the request handler never
	// blocks on the sidecar push (parity with router/api.py _PushDispatcher).
	var pushDispatch *gateway.PushDispatchQueue
	if cfg.IsPushMode() && cfg.PushDecoupleDispatch {
		pushDispatch = gateway.NewPushDispatchQueue(cfg, srv.DispatchPushJob, srv.StoreLocalResult)
		pushDispatch.Start()
		srv.SetPushDispatch(pushDispatch)
		defer pushDispatch.Stop()
		log.Printf("[router] push-dispatch decoupling enabled (workers=%d, queue_max=%d)", cfg.PushDispatchWorkers, cfg.PushDispatchQueueMax)
	}

	// Central-push: router-driven dispatch from the central queue. No legacy
	// push-dispatch workers (those are for queue-less push-*).
	if cfg.IsCentralPush() && pushRouter != nil {
		centralPush := gateway.NewCentralPushDispatcher(queue, pushRouter, cfg.CentralPushCap, cfg.CentralPushIntervalS)
		centralPush.Start()
		srv.SetCentralPush(centralPush)
		defer centralPush.Stop()
		log.Printf("[router] CentralPushDispatch started (cap=%d interval_s=%.3f)", cfg.CentralPushCap, cfg.CentralPushIntervalS)
	}

	// External-push: static external vLLM endpoints (no k8s pods, no sidecar).
	// Admits + schedules like central-push, but delivers directly to each
	// external vLLM and ingests inline. Prefix routing works via a router-side
	// KV-events subscriber writing owners keyed by the endpoint id.
	if cfg.IsExternalPush() {
		externalReg := gateway.NewExternalRegistry(cfg)
		externalReg.RefreshHealth(true)
		externalClient := gateway.NewExternalVLLMClient(cfg, externalReg)
		srv.SetExternalRegistry(externalReg)

		externalSubs := gateway.NewRouterKVSubscriberPool(cfg, externalReg)
		externalSubs.Start(context.Background())
		defer externalSubs.Stop()

		extPush := gateway.NewExternalPushDispatcher(queue, externalReg, externalClient, srv.IngestResult, cfg.ExternalPushCap, cfg.ExternalPushIntervalS)
		extPush.Start()
		srv.SetExternalPush(extPush)
		defer extPush.Stop()
		log.Printf("[router] ExternalPushDispatch started (endpoints=%d cap=%d interval_s=%.3f kv_events=%v)",
			len(cfg.StaticEndpoints), cfg.ExternalPushCap, cfg.ExternalPushIntervalS, cfg.ExternalKVEvents)
	}

	// SLO subsystem: the registry always exists (so requests carrying SLO
	// annotations are tracked even when SLO_AWARE is off), and the same engine
	// drives the queue's SLO-aware scheduling when SLO_AWARE is enabled.
	sloEngine := gateway.NewSLOEngine(cfg)
	queue.SetSLOEngine(sloEngine)
	srv.SetSLORegistry(sloEngine)

	// Optional async pubsub publisher.
	if cfg.TransportMode == "async_pubsub" {
		pub, err := gateway.NewResultPublisher(cfg)
		if err != nil {
			log.Printf("[router] WARNING: failed to start pubsub publisher: %v", err)
		} else if err := pub.Start(); err != nil {
			log.Printf("[router] WARNING: failed to start pubsub publisher: %v", err)
		} else {
			srv.SetPublisher(pub)
			defer pub.Stop()
			log.Printf("[router] PubSub enabled: bind=%s topic=%s hwm=%d", cfg.ResultsZMQBind, cfg.ResultsZMQTopic, cfg.ResultsZMQHWM)
		}
	}

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

	if !cfg.IsExternalPush() {
		kvWatcher.Stop()
	}
	if ownerLookup != nil {
		ownerLookup.Close()
	}
	log.Println("[router] Shutdown complete.")
}
