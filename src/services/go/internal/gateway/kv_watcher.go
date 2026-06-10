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

// KVWatcher periodically scans Redis for KV-cache block ownership keys
// of the form {MODEL_NAME}:kvblock:* and builds a map of
// endpoint -> set of block hashes. This mirrors the Python KVWatcher.
type KVWatcher struct {
	cfg *Config

	rdb *redis.Client

	mu             sync.RWMutex
	endpointBlocks map[string]map[string]bool // endpoint -> set of block hash strings

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewKVWatcher(cfg *Config) *KVWatcher {
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
	})
	return &KVWatcher{
		cfg:            cfg,
		rdb:            rdb,
		endpointBlocks: make(map[string]map[string]bool),
		stopCh:         make(chan struct{}),
	}
}

// Start begins the background scan loop.
func (w *KVWatcher) Start() {
	w.wg.Add(1)
	go w.run()
	log.Printf("[KVWatcher] Started (redis=%s:%d, model=%s)", w.cfg.RedisHost, w.cfg.RedisPort, w.cfg.ModelName)
}

// Stop signals the background loop to exit and waits for it.
func (w *KVWatcher) Stop() {
	close(w.stopCh)
	w.wg.Wait()
	_ = w.rdb.Close()
	log.Println("[KVWatcher] Stopped")
}

// BlocksForEndpoint returns the list of block hash strings known for
// the given endpoint. Thread-safe.
func (w *KVWatcher) BlocksForEndpoint(endpoint string) []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	blocks, ok := w.endpointBlocks[endpoint]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(blocks))
	for b := range blocks {
		out = append(out, b)
	}
	return out
}

func (w *KVWatcher) run() {
	defer w.wg.Done()

	scanInterval := 1 * time.Second
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.scanOnce()
		}
	}
}

func (w *KVWatcher) scanOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pattern := fmt.Sprintf("%s:kvblock:*", w.cfg.ModelName)

	newMap := make(map[string]map[string]bool)

	var cursor uint64
	seen := 0
	maxKeys := 200

	for {
		keys, nextCursor, err := w.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			log.Printf("[KVWatcher] scan failed: %v", err)
			return
		}

		for _, key := range keys {
			mapping, err := w.rdb.HGetAll(ctx, key).Result()
			if err != nil || len(mapping) == 0 {
				continue
			}

			parts := strings.Split(key, ":")
			if len(parts) < 3 {
				continue
			}
			blockHash := parts[len(parts)-1]

			for podName := range mapping {
				endpoint := podName
				if _, ok := newMap[endpoint]; !ok {
					newMap[endpoint] = make(map[string]bool)
				}
				newMap[endpoint][blockHash] = true
			}

			seen++
			if seen >= maxKeys {
				break
			}
		}

		cursor = nextCursor
		if cursor == 0 || seen >= maxKeys {
			break
		}
	}

	w.mu.Lock()
	w.endpointBlocks = newMap
	w.mu.Unlock()
}
