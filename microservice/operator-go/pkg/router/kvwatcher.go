package router

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// KVWatcher scans Redis for KV block ownership and updates the router state.
type KVWatcher struct {
	rdb   *redis.Client
	state *State
	cfg   *Config
}

func NewKVWatcher(rdb *redis.Client, state *State, cfg *Config) *KVWatcher {
	return &KVWatcher{rdb: rdb, state: state, cfg: cfg}
}

// Run scans Redis for block ownership periodically.
func (w *KVWatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(w.cfg.KVWatchIntervalS * float64(time.Second)))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.scan(ctx)
		}
	}
}

func (w *KVWatcher) scan(ctx context.Context) {
	pattern := fmt.Sprintf("%s:kvblock:*", w.cfg.ModelName)
	var cursor uint64
	var scanned int64

	for {
		keys, next, err := w.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			log.Printf("[kv-watcher] scan error: %v", err)
			return
		}

		for _, key := range keys {
			if scanned >= w.cfg.KVWatchMaxKeys {
				return
			}
			scanned++

			vals, err := w.rdb.HGetAll(ctx, key).Result()
			if err != nil {
				continue
			}

			blockHash := parseBlockHashFromKey(key, w.cfg.ModelName)
			if blockHash == 0 {
				continue
			}

			var endpoints []string
			for ep := range vals {
				endpoints = append(endpoints, ep)
			}
			w.state.RegisterBlockOwners(blockHash, endpoints)
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}
}

func parseBlockHashFromKey(key, model string) int64 {
	prefix := fmt.Sprintf("%s:kvblock:", model)
	if len(key) <= len(prefix) {
		return 0
	}
	hashStr := key[len(prefix):]
	var h int64
	fmt.Sscanf(hashStr, "%d", &h)
	return h
}
