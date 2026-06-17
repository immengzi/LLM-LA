package gateway

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// KVWatcher periodically scans Redis for KV-cache block ownership keys of the
// form {MODEL_NAME}:kvblock:* and registers (block_hash -> owners) into the
// shared kvAware state. Mirrors src/services/router_service/router/kv_watcher.py.
//
// Owner identity is the pod name (the Python watcher uses pod_name as the
// endpoint key, which matches the endpoint a sidecar reports on /pull).
type KVWatcher struct {
	cfg *Config
	kv  *kvAware
	reg *ModelRegistry

	rdb *redis.Client

	mu          sync.Mutex
	lastScanTS  map[string]float64
	pods        map[string]string // pod_name -> pod_ip
	lastDiscov  float64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewKVWatcher(cfg *Config, kv *kvAware, reg *ModelRegistry) *KVWatcher {
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
	})
	return &KVWatcher{
		cfg:        cfg,
		kv:         kv,
		reg:        reg,
		rdb:        rdb,
		lastScanTS: make(map[string]float64),
		pods:       make(map[string]string),
		stopCh:     make(chan struct{}),
	}
}

func (w *KVWatcher) logKV(level, format string, args ...interface{}) {
	mode := strings.ToLower(w.cfg.KVLogKeys)
	msg := fmt.Sprintf(format, args...)
	switch level {
	case "always":
		log.Printf("[KVWatcher] %s", msg)
	case "summary":
		if mode == "summary" || mode == "full" {
			log.Printf("[KVWatcher] %s", msg)
		}
	case "full":
		if mode == "full" {
			log.Printf("[KVWatcher] %s", msg)
		}
	}
}

func (w *KVWatcher) Start() {
	w.wg.Add(1)
	go w.run()
	w.logKV("always", "Started (redis=%s:%d, model=%s, interval_s=%g, max_keys=%d, log_mode=%s)",
		w.cfg.RedisHost, w.cfg.RedisPort, w.cfg.ModelName,
		w.cfg.KVWatchIntervalS, w.cfg.KVWatchMaxKeys, w.cfg.KVLogKeys)
}

func (w *KVWatcher) Stop() {
	close(w.stopCh)
	w.wg.Wait()
	_ = w.rdb.Close()
	w.logKV("always", "Stopped")
}

// GetLastScanTS returns the timestamp of the last successful KV scan for an
// endpoint (used for staleness discounting), or 0 if never scanned.
func (w *KVWatcher) GetLastScanTS(endpoint string) float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastScanTS[endpoint]
}

func (w *KVWatcher) run() {
	defer w.wg.Done()
	interval := time.Duration(w.cfg.KVWatchIntervalS * float64(time.Second))
	if interval <= 0 {
		interval = time.Second
	}
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		now := nowS()
		if now-w.lastDiscov >= w.cfg.KVDiscoveryIntervalS {
			w.discover()
			w.lastDiscov = now
		}

		w.mu.Lock()
		havePods := len(w.pods) > 0
		w.mu.Unlock()

		if havePods {
			if w.reg != nil && w.reg.Enabled() {
				for _, mname := range w.reg.Names() {
					w.scanOnce(mname)
				}
			} else {
				w.scanOnce(w.cfg.ModelName)
			}
		}

		select {
		case <-w.stopCh:
			return
		case <-time.After(interval):
		}
	}
}

func (w *KVWatcher) discover() {
	var pods map[string]string
	if w.reg != nil && w.reg.Enabled() {
		pods = make(map[string]string)
		for _, e := range w.reg.Entries() {
			if e.LabelSelector != "" {
				mp := discoverPodsSelector(w.cfg, e.LabelSelector)
				for k, v := range mp {
					pods[k] = v
				}
				w.logKV("summary", "discovered %d pods for model=%s", len(mp), e.Name)
			}
		}
	} else {
		pods = discoverPods(w.cfg)
		w.logKV("summary", "discovered %d pods", len(pods))
	}
	w.mu.Lock()
	w.pods = pods
	w.mu.Unlock()
}

func (w *KVWatcher) scanOnce(model string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pattern := fmt.Sprintf("%s:kvblock:*", model)
	maxKeys := w.cfg.KVWatchMaxKeys
	seen := 0

	w.mu.Lock()
	pods := w.pods
	w.mu.Unlock()

	var cursor uint64
	for {
		keys, next, err := w.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			w.logKV("always", "scan failed: %v", err)
			return
		}
		for _, key := range keys {
			mapping, err := w.rdb.HGetAll(ctx, key).Result()
			if err != nil || len(mapping) == 0 {
				continue
			}
			idx := strings.LastIndex(key, ":")
			if idx < 0 || idx+1 >= len(key) {
				continue
			}
			blockHash := key[idx+1:]
			if !isIntegerLiteral(blockHash) {
				continue
			}

			epOwners := make([]string, 0, len(mapping))
			for podName := range mapping {
				if _, ok := pods[podName]; ok {
					epOwners = append(epOwners, podName)
				}
			}
			if len(epOwners) > 0 {
				w.kv.registerBlockOwners(blockHash, epOwners)
				scanNow := nowS()
				w.mu.Lock()
				for _, ep := range epOwners {
					w.lastScanTS[ep] = scanNow
				}
				w.mu.Unlock()
			}
			w.logKV("full", "key=%s eps=%v", key, epOwners)

			seen++
			if seen >= maxKeys {
				break
			}
		}
		cursor = next
		if cursor == 0 || seen >= maxKeys {
			break
		}
	}

	if seen > 0 {
		w.logKV("summary", "scan complete: scanned=%d keys", seen)
	} else {
		w.logKV("summary", "scan complete: no kvblock keys found")
	}
}
