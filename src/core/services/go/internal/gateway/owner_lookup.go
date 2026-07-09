package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// OwnerLookup resolves KV-block ownership on demand by HGETALL-ing exactly a
// request's own block hashes, instead of blindly scanning the whole keyspace
// (see KVWatcher). The per-pod sidecar keeps {model}:kvblock:{hash} fresh (it
// applies BlockRemoved on eviction), so a direct lookup of a request's leading
// blocks yields exact, current owners without the sampling lag of the scan.
//
// Mirrors src/core/services/router_service/router/owner_lookup.py.
type OwnerLookup struct {
	cfg *Config
	rdb *redis.Client
}

// NewOwnerLookup constructs the targeted-lookup client (exported for main wiring).
func NewOwnerLookup(cfg *Config) *OwnerLookup {
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
	})
	return &OwnerLookup{cfg: cfg, rdb: rdb}
}

func (o *OwnerLookup) Close() {
	if o.rdb != nil {
		_ = o.rdb.Close()
	}
}

// FetchBlockOwners returns block_hash -> set(owner pod) for a request's leading
// blocks, capped at KVLookupMaxBlocks. Returns nil on error or when disabled;
// the caller then falls back to affinity / no KV credit, so a Redis hiccup can
// never break dispatch.
func (o *OwnerLookup) FetchBlockOwners(model string, blockHashes []string) map[string]map[string]bool {
	if len(blockHashes) == 0 || o.rdb == nil {
		return nil
	}

	limit := o.cfg.KVLookupMaxBlocks
	if limit > 0 && len(blockHashes) > limit {
		blockHashes = blockHashes[:limit]
	}

	prefix := ""
	if model != "" {
		prefix = model + ":"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pipe := o.rdb.Pipeline()
	cmds := make([]*redis.StringStringMapCmd, len(blockHashes))
	for i, h := range blockHashes {
		cmds[i] = pipe.HGetAll(ctx, prefix+"kvblock:"+h)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil
	}

	owners := make(map[string]map[string]bool)
	for i, h := range blockHashes {
		mapping, err := cmds[i].Result()
		if err != nil || len(mapping) == 0 {
			continue
		}
		set := make(map[string]bool, len(mapping))
		for pod := range mapping {
			set[pod] = true
		}
		owners[h] = set
	}
	return owners
}
