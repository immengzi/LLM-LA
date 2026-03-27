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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	router "github.com/vllmkv/operator/pkg/router"
)

func main() {
	cfg := router.LoadConfig()

	log.Printf("[router] mode=%s kv_aware=%v len_aware=%v len_policy=%s",
		cfg.RouterMode, cfg.KVAware, cfg.LenAware, cfg.LenPolicy)
	log.Printf("[router] transport=%s result_transport=%s", cfg.TransportMode, cfg.ResultTransportMode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	state := router.NewState()

	// Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
	})
	defer rdb.Close()

	// KV watcher
	if cfg.KVAware {
		watcher := router.NewKVWatcher(rdb, state, cfg)
		go watcher.Run(ctx)
	}

	// Hash client
	var hashClient *router.HashClient
	if cfg.KVAware {
		hashClient = router.NewHashClient(cfg)
	}

	// ZMQ publisher
	pub := router.NewResultPublisher(cfg)
	if err := pub.Start(ctx); err != nil {
		log.Fatalf("[router] zmq pub start: %v", err)
	}
	defer pub.Close()

	// K8s client + push router
	var pushRouter *router.PushRouter
	if cfg.IsPushMode() {
		cs := buildK8sClient()
		disc := router.NewPodDiscovery(cs, cfg)
		pushRouter = router.NewPushRouter(cfg)
		go router.DiscoveryLoop(ctx, disc, pushRouter)
		go router.PushDispatchLoop(ctx, state, pushRouter)
	}

	// Cleanup loop
	go func() {
		for {
			time.Sleep(30 * time.Second)
			state.CleanupExpired(5 * time.Minute)
		}
	}()

	// Metrics updater
	go func() {
		for {
			router.MetricQueueLen.Set(float64(state.QueueLen()))
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// HTTP server
	srv := router.NewServer(state, cfg, pushRouter, pub, hashClient)
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler()}

	go func() {
		log.Printf("[router] listening on %s", addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[router] http: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("[router] shutting down")

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	httpSrv.Shutdown(shutdownCtx)
}

func buildK8sClient() *kubernetes.Clientset {
	var config *rest.Config
	var err error

	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		config, err = rest.InClusterConfig()
	} else {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.ExpandEnv("$HOME/.kube/config")
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		log.Fatalf("[k8s] config: %v", err)
	}

	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("[k8s] clientset: %v", err)
	}
	return cs
}
