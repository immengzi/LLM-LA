package gateway

import (
	"crypto/rand"
	"fmt"
	"log"
	"sort"
	"sync"
)

// queueItem mirrors the Python queue tuple (req_id, prompt, t_enq, meta).
type queueItem struct {
	reqID  string
	prompt string
	tEnq   float64
	meta   map[string]interface{}
}

// sloEngine is the integration point for SLO-aware scheduling. It is nil unless
// SLO_AWARE is enabled. Implemented in slo.go (Phase 2).
type sloEngine interface {
	// sloAwareSort returns the ordered pool plus the kv_hits map, mirroring
	// RouterState._slo_aware_sort.
	sloAwareSort(pool []queueItem, endpoint string, want int, kv *kvAware) ([]queueItem, map[string]int)
	// applyAdmissionThrottle mirrors RouterState._apply_admission_throttle.
	applyAdmissionThrottle(currentWant int, endpoint string, allItems []queueItem) int
	// onDispatch updates the SLO registry/metrics for chosen items.
	onDispatch(endpoint string, chosen []queueItem, kvHits map[string]int)
	// incrementInflight mirrors batch_estimator.increment_inflight.
	incrementInflight(endpoint string, n int)
}

// CentralQueue is the Go analogue of Python's RouterState queue: per-model
// FIFO queues with KV-aware + length-aware (and optional SLO-aware) pull
// scheduling. Result waiters live in ResultStore.
type CentralQueue struct {
	mu     sync.Mutex
	cfg    *Config
	kv     *kvAware
	pred   OutputLengthPredictor
	slo    sloEngine // nil unless SLO_AWARE

	defaultModel string
	queues       map[string][]queueItem

	// req_id -> endpoint that pulled it (streaming identity / push notify).
	reqEndpoint map[string]string

	// req_id -> streaming chunk channel (SSE).
	chunkMu     sync.Mutex
	chunkQueues map[string]chan map[string]interface{}
}

func NewCentralQueue(cfg *Config, kv *kvAware) *CentralQueue {
	setCentralQueueLength(0)
	return &CentralQueue{
		cfg:          cfg,
		kv:           kv,
		pred:         getLengthPredictor(cfg),
		defaultModel: cfg.ModelName,
		queues:       make(map[string][]queueItem),
		reqEndpoint:  make(map[string]string),
		chunkQueues:  make(map[string]chan map[string]interface{}),
	}
}

// RegisterChunkQueue creates a streaming chunk channel for req_id.
func (q *CentralQueue) RegisterChunkQueue(reqID string) chan map[string]interface{} {
	ch := make(chan map[string]interface{}, 1024)
	q.chunkMu.Lock()
	q.chunkQueues[reqID] = ch
	q.chunkMu.Unlock()
	return ch
}

// PushChunk delivers a chunk to the req_id's chunk channel. Returns false if
// no chunk queue is registered (non-streaming request).
func (q *CentralQueue) PushChunk(reqID string, chunk map[string]interface{}) bool {
	q.chunkMu.Lock()
	ch, ok := q.chunkQueues[reqID]
	q.chunkMu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- chunk:
	default:
	}
	return true
}

// RemoveChunkQueue removes the chunk channel for req_id.
func (q *CentralQueue) RemoveChunkQueue(reqID string) {
	q.chunkMu.Lock()
	delete(q.chunkQueues, reqID)
	q.chunkMu.Unlock()
	q.ForgetReqEndpoint(reqID)
}

// HasChunkQueue reports whether a chunk queue is registered for req_id.
func (q *CentralQueue) HasChunkQueue(reqID string) bool {
	q.chunkMu.Lock()
	defer q.chunkMu.Unlock()
	_, ok := q.chunkQueues[reqID]
	return ok
}

// SetSLOEngine wires the SLO-aware scheduler (Phase 2).
func (q *CentralQueue) SetSLOEngine(e sloEngine) { q.slo = e }

// NextReqID generates a new unique request ID (uuid4 hex, like Python).
func (q *CentralQueue) NextReqID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x", b)
}

func (q *CentralQueue) getQueueLocked(model string) string {
	if model == "" {
		model = q.defaultModel
	}
	if _, ok := q.queues[model]; !ok {
		q.queues[model] = nil
	}
	return model
}

func (q *CentralQueue) totalSizeLocked() int {
	n := 0
	for _, ql := range q.queues {
		n += len(ql)
	}
	return n
}

// Enqueue appends a request to the per-model queue.
func (q *CentralQueue) Enqueue(prompt string, tEnqClient float64, meta map[string]interface{}, reqID, model string) string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if reqID == "" {
		reqID = q.NextReqID()
	}
	ts := tEnqClient
	if ts == 0 {
		ts = nowS()
	}
	if meta == nil {
		meta = make(map[string]interface{})
	}
	m := q.getQueueLocked(model)
	q.queues[m] = append(q.queues[m], queueItem{reqID: reqID, prompt: prompt, tEnq: ts, meta: meta})
	setCentralQueueLength(q.totalSizeLocked())
	return reqID
}

// UpdateMeta updates meta in place for a queued request (used to inject trace
// info after enqueue without changing order/timestamps).
func (q *CentralQueue) UpdateMeta(reqID string, meta map[string]interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for model, ql := range q.queues {
		for i := range ql {
			if ql[i].reqID == reqID {
				q.queues[model][i].meta = meta
				return
			}
		}
	}
}

// Pull removes up to `want` items for the endpoint+model, applying KV-aware,
// length-aware, and (optionally) SLO-aware ordering. Mirrors
// RouterState.pull_for_endpoint.
func (q *CentralQueue) Pull(endpoint string, want int, model string) []JobItem {
	if want <= 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	m := q.getQueueLocked(model)
	ql := q.queues[m]
	if len(ql) == 0 {
		setCentralQueueLength(q.totalSizeLocked())
		return nil
	}

	poolFactor := q.cfg.PoolFactor
	if poolFactor < 1 {
		poolFactor = 1
	}
	maxScan := len(ql)
	if want*int(poolFactor) < maxScan {
		maxScan = want * int(poolFactor)
	}

	pool := make([]queueItem, maxScan)
	copy(pool, ql[:maxScan])
	q.queues[m] = ql[maxScan:]
	setCentralQueueLength(q.totalSizeLocked())

	q.logReq("endpoint=%s want=%d pool_size=%d", endpoint, want, len(pool))

	sloAware := q.cfg.SLOAware && q.slo != nil

	var ordered []queueItem
	var kvHits map[string]int
	if sloAware {
		ordered, kvHits = q.slo.sloAwareSort(pool, endpoint, want, q.kv)
	} else {
		ordered, kvHits = q.legacySort(pool, endpoint)
	}

	effectiveWant := want
	if q.cfg.FixedBatchSize > 0 && effectiveWant > q.cfg.FixedBatchSize {
		effectiveWant = q.cfg.FixedBatchSize
	}
	if sloAware && q.cfg.AdmissionThrottle {
		allItems := q.allItemsLocked()
		effectiveWant = q.slo.applyAdmissionThrottle(effectiveWant, endpoint, allItems)
	}

	takeN := effectiveWant
	if takeN > len(ordered) {
		takeN = len(ordered)
	}
	chosenRaw := ordered[:takeN]
	leftovers := ordered[takeN:]

	kvEnabled := q.cfg.KVAware

	// Trace enrichment.
	dispatchTS := nowS()
	chosen := make([]queueItem, len(chosenRaw))
	if q.cfg.TraceEnabled {
		qlenAtDispatch := q.totalSizeLocked()
		for i, it := range chosenRaw {
			mt := cloneMeta(it.meta)
			tr := traceOf(mt)
			if _, ok := tr["t_enq_router_queue"]; !ok {
				tr["t_enq_router_queue"] = it.tEnq
			}
			if _, ok := tr["endpoint"]; !ok {
				tr["endpoint"] = endpoint
			}
			tr["t_dispatch_router"] = dispatchTS
			tr["router_queue_len_at_dispatch"] = qlenAtDispatch
			if kvEnabled {
				tr["kv_hits_len"] = kvHits[it.reqID]
			}
			mt["__trace__"] = tr
			chosen[i] = queueItem{reqID: it.reqID, prompt: it.prompt, tEnq: it.tEnq, meta: mt}
		}
	} else {
		copy(chosen, chosenRaw)
	}

	// SLO dispatch tracking + inflight increment.
	if sloAware {
		q.slo.onDispatch(endpoint, chosen, kvHits)
		q.slo.incrementInflight(endpoint, len(chosen))
	}

	// Dispatch metrics + endpoint tracking.
	for _, it := range chosen {
		incDispatch(endpoint)
		q.reqEndpoint[it.reqID] = endpoint
	}

	// Requeue leftovers at the front (preserve order).
	if len(leftovers) > 0 {
		q.queues[m] = append(append([]queueItem{}, leftovers...), q.queues[m]...)
	}
	setCentralQueueLength(q.totalSizeLocked())

	items := make([]JobItem, len(chosen))
	for i, it := range chosen {
		meta := it.meta
		if meta == nil {
			meta = make(map[string]interface{})
		}
		items[i] = JobItem{ReqID: it.reqID, Prompt: it.prompt, TEnqClient: it.tEnq, Meta: meta}
	}
	return items
}

// legacySort mirrors RouterState._legacy_sort exactly.
func (q *CentralQueue) legacySort(pool []queueItem, endpoint string) ([]queueItem, map[string]int) {
	kvEnabled := q.cfg.KVAware
	lenEnabled := q.cfg.LenAware
	lenPolicy := q.cfg.LenPolicy

	kvHitsMap := make(map[string]int, len(pool))

	type withKV struct {
		item   queueItem
		kvHits int
	}
	poolWithKV := make([]withKV, 0, len(pool))
	if kvEnabled {
		for _, it := range pool {
			hits := q.kv.prefixLen(endpoint, it.reqID)
			kvHitsMap[it.reqID] = hits
			poolWithKV = append(poolWithKV, withKV{it, hits})
		}
	} else {
		for _, it := range pool {
			poolWithKV = append(poolWithKV, withKV{it, 0})
		}
	}

	// Group by kv_hits.
	kvToItems := make(map[int][]queueItem)
	for _, w := range poolWithKV {
		kvToItems[w.kvHits] = append(kvToItems[w.kvHits], w.item)
	}

	levels := make([]int, 0, len(kvToItems))
	for k := range kvToItems {
		levels = append(levels, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(levels)))

	ordered := make([]queueItem, 0, len(pool))
	for _, lvl := range levels {
		tier := kvToItems[lvl]
		// tier.sort(key=lambda x: x[0]) -> by req_id ascending.
		sort.SliceStable(tier, func(i, j int) bool { return tier[i].reqID < tier[j].reqID })

		if lenEnabled && lenPolicy != "" {
			tier = q.selectLenAware(tier, lenPolicy)
		}
		ordered = append(ordered, tier...)
	}
	return ordered, kvHitsMap
}

// selectLenAware mirrors len_select.select_len_aware: stable sort by predicted
// output tokens (ascending for short_first, descending for long_first).
func (q *CentralQueue) selectLenAware(candidates []queueItem, policy string) []queueItem {
	type scored struct {
		item  queueItem
		score int
	}
	arr := make([]scored, len(candidates))
	for i, c := range candidates {
		s := q.pred.PredictOutTokens(c.prompt, c.reqID)
		if s <= 0 {
			s = q.cfg.DefaultMaxTokens
		}
		arr[i] = scored{c, s}
	}
	reverse := policy == "long_first"
	sort.SliceStable(arr, func(i, j int) bool {
		if reverse {
			return arr[i].score > arr[j].score
		}
		return arr[i].score < arr[j].score
	})
	out := make([]queueItem, len(arr))
	for i, s := range arr {
		out[i] = s.item
	}
	return out
}

func (q *CentralQueue) allItemsLocked() []queueItem {
	var all []queueItem
	for _, ql := range q.queues {
		n := len(ql)
		if n > 50 {
			n = 50
		}
		all = append(all, ql[:n]...)
	}
	return all
}

// GetReqEndpoint returns the endpoint that pulled this req_id, or "".
func (q *CentralQueue) GetReqEndpoint(reqID string) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.reqEndpoint[reqID]
}

func (q *CentralQueue) ForgetReqEndpoint(reqID string) {
	q.mu.Lock()
	delete(q.reqEndpoint, reqID)
	q.mu.Unlock()
}

// Size returns total queued items (optionally for a single model).
func (q *CentralQueue) Size(model ...string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(model) > 0 && model[0] != "" {
		return len(q.queues[model[0]])
	}
	return q.totalSizeLocked()
}

func (q *CentralQueue) logReq(format string, args ...interface{}) {
	if q.cfg.ReqLogMode == "off" {
		return
	}
	if q.cfg.ReqLogMode == "summary" {
		// only summary-level lines; pull pool detail is "full"
		return
	}
	log.Printf("[PullRouter] "+format, args...)
}

func cloneMeta(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}

func traceOf(m map[string]interface{}) map[string]interface{} {
	if tr, ok := m["__trace__"].(map[string]interface{}); ok {
		out := make(map[string]interface{}, len(tr)+4)
		for k, v := range tr {
			out[k] = v
		}
		return out
	}
	return make(map[string]interface{})
}
