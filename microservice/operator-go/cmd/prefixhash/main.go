package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	ph "github.com/vllmkv/operator/pkg/prefixhash"
)

func main() {
	cfg := ph.LoadConfig()
	srv := ph.NewServer(cfg)

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Port)
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler()}

	go func() {
		log.Printf("[prefix-hash] listening on %s block_size=%d", addr, cfg.BlockSize)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[prefix-hash] http: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("[prefix-hash] shutting down")
}
