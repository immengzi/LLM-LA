package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"
)

// deriveAffinityKey mirrors router/affinity.py:derive_affinity_key. It derives a
// stable per-conversation key from the model and the conversation's opening
// (system prompt + first user message), which is identical across every turn
// because each turn resends the full message history. Returns "" when there is
// no user message to key on.
func deriveAffinityKey(model string, messages []interface{}) string {
	if len(messages) == 0 {
		return ""
	}

	parts := []string{"model:" + model}
	sawUser := false
	for _, mi := range messages {
		m, ok := mi.(map[string]interface{})
		if !ok {
			continue
		}
		role, _ := m["role"].(string)
		content := messageContentText(m["content"])
		switch role {
		case "system":
			parts = append(parts, "system:"+content)
		case "user":
			parts = append(parts, "user:"+content)
			sawUser = true
		}
		if sawUser {
			break // first user message only -> stable across turns
		}
	}

	if !sawUser {
		return ""
	}

	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])[:16]
}

// messageContentText flattens an OpenAI/Anthropic message content (string or
// content-block array) into its text.
func messageContentText(content interface{}) string {
	switch c := content.(type) {
	case string:
		return c
	case []interface{}:
		var b strings.Builder
		for _, blk := range c {
			bm, ok := blk.(map[string]interface{})
			if !ok {
				continue
			}
			if t, _ := bm["type"].(string); t == "text" {
				if txt, ok := bm["text"].(string); ok {
					b.WriteString(txt)
				}
			}
		}
		return b.String()
	default:
		return ""
	}
}

type affinityEntry struct {
	endpoint string
	lastSeen time.Time
}

// AffinityMap is a thread-safe conversation-key -> endpoint map with TTL expiry,
// mirroring router/affinity.py:AffinityMap.
//
// Optional durability: when a store is provided, Claim is write-through to the
// store (off the hot path via the store's background writer), Warm bulk-loads
// the store into memory at startup, and Prefetch does the single per-request
// Redis GET at admission on a memory miss. The pull hot path (Lookup) stays
// purely in-memory. With no store, behavior is identical to before.
type AffinityMap struct {
	mu       sync.Mutex
	m        map[string]affinityEntry
	ttl      time.Duration
	store    affinityStore
	cacheMax int
}

func NewAffinityMap(ttlS float64) *AffinityMap {
	return NewAffinityMapWithStore(ttlS, nil, 0)
}

// NewAffinityMapWithStore builds an affinity map optionally backed by a durable
// store and bounded by cacheMax in-memory entries (0 = unbounded).
func NewAffinityMapWithStore(ttlS float64, store affinityStore, cacheMax int) *AffinityMap {
	return &AffinityMap{
		m:        make(map[string]affinityEntry),
		ttl:      time.Duration(ttlS * float64(time.Second)),
		store:    store,
		cacheMax: cacheMax,
	}
}

// Persistent reports whether a durable store is attached.
func (a *AffinityMap) Persistent() bool { return a.store != nil }

// evictIfNeededLocked bounds the in-memory cache; evicted keys stay durable in
// the store. Evicts oldest-by-lastSeen down to the bound. Caller holds a.mu.
func (a *AffinityMap) evictIfNeededLocked() {
	if a.cacheMax <= 0 || len(a.m) <= a.cacheMax {
		return
	}
	overflow := len(a.m) - a.cacheMax
	type kt struct {
		k string
		t time.Time
	}
	arr := make([]kt, 0, len(a.m))
	for k, e := range a.m {
		arr = append(arr, kt{k, e.lastSeen})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].t.Before(arr[j].t) })
	for i := 0; i < overflow; i++ {
		delete(a.m, arr[i].k)
	}
}

// Lookup returns the mapped endpoint if present and not expired, else "".
func (a *AffinityMap) Lookup(key string) string {
	if key == "" {
		return ""
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.m[key]
	if !ok {
		return ""
	}
	if time.Since(e.lastSeen) > a.ttl {
		delete(a.m, key)
		return ""
	}
	return e.endpoint
}

// Claim records (or refreshes) that this conversation key is served by
// endpoint. Write-through to the durable store when one is configured (the
// store's writer is asynchronous, so this stays off the hot path).
func (a *AffinityMap) Claim(key, endpoint string) {
	if key == "" || endpoint == "" {
		return
	}
	a.mu.Lock()
	a.m[key] = affinityEntry{endpoint: endpoint, lastSeen: time.Now()}
	a.evictIfNeededLocked()
	a.mu.Unlock()
	if a.store != nil {
		a.store.Put(key, endpoint)
	}
}

// Prefetch warms one key from the durable store into memory on an in-memory
// miss. Called once per request at admission. No-op (falls back to Lookup) when
// there is no store. Returns the resolved endpoint or "".
func (a *AffinityMap) Prefetch(key string) string {
	if key == "" {
		return ""
	}
	if hit := a.Lookup(key); hit != "" || a.store == nil {
		return hit
	}
	ep, ok := a.store.Get(key)
	if ok && ep != "" {
		a.mu.Lock()
		a.m[key] = affinityEntry{endpoint: ep, lastSeen: time.Now()}
		a.evictIfNeededLocked()
		a.mu.Unlock()
		return ep
	}
	return ""
}

// Warm bulk-loads the durable store into the in-memory cache. Returns the
// resulting map size.
func (a *AffinityMap) Warm() int {
	if a.store == nil {
		return 0
	}
	loaded := a.store.Warm()
	if len(loaded) == 0 {
		return 0
	}
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	for k, ep := range loaded {
		if ep != "" {
			a.m[k] = affinityEntry{endpoint: ep, lastSeen: now}
		}
	}
	a.evictIfNeededLocked()
	return len(a.m)
}

// Close flushes and closes the durable store (shutdown). No-op with no store.
func (a *AffinityMap) Close() {
	if a.store != nil {
		a.store.Close()
	}
}

// Size returns the number of live mappings.
func (a *AffinityMap) Size() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.m)
}
