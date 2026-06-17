package gateway

import "sync"

// kvAware ports src/services/router_service/router/kv_aware.py.
//
//   - reqBlocks:    req_id -> ordered list of block hashes (prefix chain)
//   - blockOwners:  block_hash -> set of endpoints that own that block
//
// Block hashes are kept as canonical decimal strings (Python uses arbitrary
// precision ints; comparing decimal strings avoids any 64-bit overflow while
// preserving exact equality semantics).
//
// prefixLen walks a request's block-hash chain and counts how many leading
// blocks are owned by a given endpoint (longest-prefix match), exactly like
// the Python implementation.
type kvAware struct {
	mu          sync.RWMutex
	reqBlocks   map[string][]string
	blockOwners map[string]map[string]bool
}

// NewKVAware constructs the shared KV-aware state (exported for main wiring).
func NewKVAware() *kvAware { return newKVAware() }

func newKVAware() *kvAware {
	return &kvAware{
		reqBlocks:   make(map[string][]string),
		blockOwners: make(map[string]map[string]bool),
	}
}

func (k *kvAware) registerRequestBlocks(reqID string, blockHashes []string) {
	cp := make([]string, len(blockHashes))
	copy(cp, blockHashes)
	k.mu.Lock()
	k.reqBlocks[reqID] = cp
	k.mu.Unlock()
}

func (k *kvAware) getRequestBlocks(reqID string) []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	b := k.reqBlocks[reqID]
	out := make([]string, len(b))
	copy(out, b)
	return out
}

func (k *kvAware) forgetRequest(reqID string) {
	k.mu.Lock()
	delete(k.reqBlocks, reqID)
	k.mu.Unlock()
}

// registerBlockOwners merges the given owners into the owner set for a block.
// Mirrors register_block_owners in kv_aware.py (additive merge; the Python
// router never prunes owners).
func (k *kvAware) registerBlockOwners(blockHash string, owners []string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	entry := k.blockOwners[blockHash]
	if entry == nil {
		entry = make(map[string]bool)
		k.blockOwners[blockHash] = entry
	}
	for _, ep := range owners {
		entry[ep] = true
	}
}

// prefixLen returns how many leading blocks of req_id are owned by endpoint.
func (k *kvAware) prefixLen(endpoint, reqID string) int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	blocks := k.reqBlocks[reqID]
	if len(blocks) == 0 {
		return 0
	}
	count := 0
	for _, h := range blocks {
		owners := k.blockOwners[h]
		if owners == nil || !owners[endpoint] {
			break
		}
		count++
	}
	return count
}
