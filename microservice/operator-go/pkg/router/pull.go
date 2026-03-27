package router

import (
	"sort"

	"github.com/vllmkv/operator/pkg/models"
)

// PullForEndpoint dequeues a batch for the requesting sidecar.
// Applies KV-aware ordering + length-aware sorting when enabled.
func PullForEndpoint(state *State, cfg *Config, endpoint string, want int) []*models.Request {
	poolSize := want * cfg.PoolFactor
	pool := state.DequeueUpTo(poolSize)
	if len(pool) == 0 {
		return nil
	}

	if cfg.KVAware {
		sortByKVHits(state, pool, endpoint)
	}

	if cfg.LenAware {
		sortLenAwareWithinTiers(pool, cfg, state, endpoint)
	}

	take := want
	if take > len(pool) {
		take = len(pool)
	}
	batch := pool[:take]
	state.ReturnToFront(pool[take:])
	return batch
}

type scoredReq struct {
	req    *models.Request
	kvHits int
}

func sortByKVHits(state *State, pool []*models.Request, endpoint string) {
	scored := make([]scoredReq, len(pool))
	for i, req := range pool {
		scored[i] = scoredReq{req: req, kvHits: prefixLen(state, req.ReqID, endpoint)}
	}
	sort.SliceStable(scored, func(i, j int) bool {
		return scored[i].kvHits > scored[j].kvHits
	})
	for i, s := range scored {
		pool[i] = s.req
	}
}

// prefixLen counts leading block hashes owned by endpoint (stops at first miss).
func prefixLen(state *State, reqID, endpoint string) int {
	blocks := state.GetRequestBlocks(reqID)
	count := 0
	for _, bh := range blocks {
		if state.BlockOwnedBy(bh, endpoint) {
			count++
		} else {
			break
		}
	}
	return count
}

func sortLenAwareWithinTiers(pool []*models.Request, cfg *Config, state *State, endpoint string) {
	type entry struct {
		idx       int
		kvHits    int
		lenScore  int
	}

	entries := make([]entry, len(pool))
	for i, req := range pool {
		hits := 0
		if cfg.KVAware {
			hits = prefixLen(state, req.ReqID, endpoint)
		}
		entries[i] = entry{idx: i, kvHits: hits, lenScore: predictOutputTokens(req.Prompt)}
	}

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].kvHits != entries[j].kvHits {
			return entries[i].kvHits > entries[j].kvHits
		}
		if cfg.LenPolicy == "long_first" {
			return entries[i].lenScore > entries[j].lenScore
		}
		return entries[i].lenScore < entries[j].lenScore // short_first default
	})

	sorted := make([]*models.Request, len(pool))
	for i, e := range entries {
		sorted[i] = pool[e.idx]
	}
	copy(pool, sorted)
}

func predictOutputTokens(prompt string) int {
	n := len(prompt) / 2
	if n < 1 {
		n = 1
	}
	return n
}
