package gateway

import (
	"crypto/rand"
	"fmt"
	"math"
	"sort"
	"sync"
)

// CentralQueue is a thread-safe FIFO queue with KV-aware and length-aware
// pull scheduling. It mirrors the Python RouterState queue logic.
type CentralQueue struct {
	mu      sync.Mutex
	entries []QueueEntry
	cfg     *Config

	kvWatcher *KVWatcher
}

func NewCentralQueue(cfg *Config, kvw *KVWatcher) *CentralQueue {
	RouterCentralQueueLength.Set(0)
	return &CentralQueue{
		cfg:       cfg,
		kvWatcher: kvw,
	}
}

// NextReqID generates a new unique request ID.
func (q *CentralQueue) NextReqID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Enqueue adds a request to the queue and returns the assigned request ID.
func (q *CentralQueue) Enqueue(entry QueueEntry) string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if entry.ReqID == "" {
		entry.ReqID = q.NextReqID()
	}
	if entry.EnqueuedAt == 0 {
		entry.EnqueuedAt = nowS()
	}
	if entry.Meta == nil {
		entry.Meta = make(map[string]interface{})
	}

	q.entries = append(q.entries, entry)
	RouterCentralQueueLength.Set(float64(len(q.entries)))
	RouterEnqueueTotal.Inc()
	return entry.ReqID
}

// Pull removes up to `want` items from the queue for the given endpoint,
// applying KV-aware and length-aware ordering when enabled.
func (q *CentralQueue) Pull(endpoint string, want int) []JobItem {
	if want <= 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.entries) == 0 {
		RouterCentralQueueLength.Set(0)
		return nil
	}

	RouterPullTotal.Inc()

	poolFactor := int(math.Max(1, q.cfg.PoolFactor))
	maxScan := len(q.entries)
	if want*poolFactor < maxScan {
		maxScan = want * poolFactor
	}

	pool := make([]QueueEntry, maxScan)
	copy(pool, q.entries[:maxScan])
	q.entries = q.entries[maxScan:]

	ordered := q.sortPool(pool, endpoint)

	takeN := want
	if takeN > len(ordered) {
		takeN = len(ordered)
	}

	chosen := ordered[:takeN]
	leftovers := ordered[takeN:]

	if len(leftovers) > 0 {
		q.entries = append(leftovers, q.entries...)
	}

	RouterCentralQueueLength.Set(float64(len(q.entries)))

	items := make([]JobItem, len(chosen))
	for i, e := range chosen {
		meta := e.Meta
		if meta == nil {
			meta = make(map[string]interface{})
		}
		items[i] = JobItem{
			ReqID:      e.ReqID,
			Prompt:     e.Prompt,
			TEnqClient: e.EnqueuedAt,
			Meta:       meta,
		}
	}

	RouterPullItemsTotal.Add(float64(len(items)))
	return items
}

type scoredEntry struct {
	entry  QueueEntry
	kvHits int
}

// sortPool applies KV-aware then length-aware sorting on the pool.
func (q *CentralQueue) sortPool(pool []QueueEntry, endpoint string) []QueueEntry {
	kvEnabled := q.cfg.KVAware
	lenEnabled := q.cfg.LenAware
	lenPolicy := q.cfg.LenPolicy

	items := make([]scoredEntry, len(pool))
	for i, e := range pool {
		hits := 0
		if kvEnabled && q.kvWatcher != nil {
			blocks := q.kvWatcher.BlocksForEndpoint(endpoint)
			hits = q.countOverlap(e.ReqID, blocks)
		}
		items[i] = scoredEntry{entry: e, kvHits: hits}
	}

	if kvEnabled {
		sort.SliceStable(items, func(i, j int) bool {
			return items[i].kvHits > items[j].kvHits
		})
	}

	if lenEnabled {
		items = q.sortByLength(items, lenPolicy)
	}

	result := make([]QueueEntry, len(items))
	for i, s := range items {
		result[i] = s.entry
	}
	return result
}

// sortByLength sorts within KV tiers by prompt length.
func (q *CentralQueue) sortByLength(items []scoredEntry, policy string) []scoredEntry {
	if len(items) <= 1 {
		return items
	}

	tiers := make(map[int][]scoredEntry)
	var tierKeys []int
	for _, item := range items {
		k := item.kvHits
		if _, ok := tiers[k]; !ok {
			tierKeys = append(tierKeys, k)
		}
		tiers[k] = append(tiers[k], item)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(tierKeys)))

	var result []scoredEntry
	for _, k := range tierKeys {
		tier := tiers[k]
		if policy == "short_first" {
			sort.SliceStable(tier, func(i, j int) bool {
				return len(tier[i].entry.Prompt) < len(tier[j].entry.Prompt)
			})
		} else if policy == "long_first" {
			sort.SliceStable(tier, func(i, j int) bool {
				return len(tier[i].entry.Prompt) > len(tier[j].entry.Prompt)
			})
		}
		result = append(result, tier...)
	}
	return result
}

// countOverlap counts how many of the endpoint's known block hashes
// match blocks associated with this request. In a full implementation
// this would consult a request-to-blocks mapping (from the hash service).
// For now returns the number of endpoint blocks (placeholder for scoring).
func (q *CentralQueue) countOverlap(_ string, endpointBlocks []string) int {
	return len(endpointBlocks)
}

// Size returns the current queue length.
func (q *CentralQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}
