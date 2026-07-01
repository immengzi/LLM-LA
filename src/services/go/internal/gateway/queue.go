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

	// Key-affinity conversation->endpoint map (nil unless AFFINITY_ENABLED).
	affinity *AffinityMap

	// req_id -> endpoint that pulled it (streaming identity / push notify).
	reqEndpoint map[string]string

	// req_id -> streaming chunk channel (SSE).
	chunkMu     sync.Mutex
	chunkQueues map[string]chan map[string]interface{}
}

func NewCentralQueue(cfg *Config, kv *kvAware) *CentralQueue {
	setCentralQueueLength(0)
	var aff *AffinityMap
	if cfg.AffinityEnabled {
		aff = NewAffinityMap(cfg.AffinityTTLS)
	}
	return &CentralQueue{
		cfg:          cfg,
		kv:           kv,
		pred:         getLengthPredictor(cfg),
		defaultModel: cfg.ModelName,
		queues:       make(map[string][]queueItem),
		affinity:     aff,
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
func (q *CentralQueue) SetSLOEngine(e sloEngine) {
	q.slo = e
	if impl, ok := e.(*sloEngineImpl); ok {
		impl.affinity = q.affinity
	}
}

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

// publishQueueMetricsLocked sets the legacy global gauge plus the additive
// per-model breakdown used by per-model autoscaling. Must hold q.mu.
func (q *CentralQueue) publishQueueMetricsLocked() {
	setCentralQueueLength(q.totalSizeLocked())
	for model, ql := range q.queues {
		setCentralQueueLengthByModel(model, len(ql))
	}
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
	q.publishQueueMetricsLocked()
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
		q.publishQueueMetricsLocked()
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
	q.publishQueueMetricsLocked()

	// Hard-mode affinity: withhold items pinned to a different endpoint (still
	// within their hold window) so they wait for their pod.
	var heldBack []queueItem
	if q.affinity != nil && q.cfg.AffinityMode == "hard" {
		pool, heldBack = q.affinityFilterHard(pool, endpoint)
	}

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

	// Dispatch metrics + endpoint tracking. Also capture the per-request routing
	// decision (independent of TRACE) so recordLatency can enrich /latency_log.
	// Mirrors record_routing in router_state.py: reuse the sort's kv_hits when KV
	// routing is on, else compute prefixLen directly (0 when measurement is off).
	logBlockHashes := q.cfg.LogBlockHashes
	for _, it := range chosen {
		incDispatch(endpoint)
		q.reqEndpoint[it.reqID] = endpoint

		hits := 0
		if kvEnabled {
			hits = kvHits[it.reqID]
		} else {
			hits = q.kv.prefixLen(endpoint, it.reqID)
		}
		blocks := q.kv.getRequestBlocks(it.reqID)
		info := routingInfo{endpoint: endpoint, kvHitsLen: hits, totalBlocks: len(blocks)}
		if it.meta != nil {
			if ak, ok := it.meta["__affinity_key__"].(string); ok && ak != "" {
				info.affinityKey, info.hasAffinity = ak, true
			}
		}
		if logBlockHashes {
			info.blockHashes, info.hasBlocks = blocks, true
		}
		q.kv.recordRouting(it.reqID, info)
	}

	// Affinity: record where each keyed conversation was dispatched so
	// subsequent turns follow the cache to this endpoint.
	if q.affinity != nil {
		q.affinityRecordDispatch(chosen, endpoint)
	}

	// Requeue held-back (hard-mode) items + leftovers at the front, order-preserving.
	requeueFront := append(append([]queueItem{}, heldBack...), leftovers...)
	if len(requeueFront) > 0 {
		q.queues[m] = append(requeueFront, q.queues[m]...)
	}
	q.publishQueueMetricsLocked()

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

		// Soft-mode affinity: prefer items pinned to this endpoint within the
		// tier (no-op when affinity is disabled or in hard mode).
		tier = q.affinitySoftPartition(tier, endpoint)

		ordered = append(ordered, tier...)
	}
	return ordered, kvHitsMap
}

// affinityKeyOf returns the conversation affinity key stamped in meta, or "".
func affinityKeyOf(meta map[string]interface{}) string {
	if meta == nil {
		return ""
	}
	k, _ := meta["__affinity_key__"].(string)
	return k
}

// affinityMatch reports whether this item's conversation maps to endpoint.
func (q *CentralQueue) affinityMatch(endpoint string, meta map[string]interface{}) bool {
	if q.affinity == nil {
		return false
	}
	key := affinityKeyOf(meta)
	if key == "" {
		return false
	}
	return q.affinity.Lookup(key) == endpoint
}

// affinityFilterHard partitions the pool into (available, heldBack). An item is
// held back when its conversation is pinned to a different endpoint and is still
// within the hold window (router-stamped __affinity_ts__ + AffinityHardTimeoutS).
func (q *CentralQueue) affinityFilterHard(pool []queueItem, endpoint string) ([]queueItem, []queueItem) {
	if q.affinity == nil {
		return pool, nil
	}
	now := nowS()
	timeout := q.cfg.AffinityHardTimeoutS
	available := make([]queueItem, 0, len(pool))
	var heldBack []queueItem
	holds, releases := 0, 0

	for _, it := range pool {
		key := affinityKeyOf(it.meta)
		if key == "" {
			available = append(available, it)
			continue
		}
		target := q.affinity.Lookup(key)
		if target == "" || target == endpoint {
			available = append(available, it)
			continue
		}
		affTS := it.tEnq
		if v, ok := it.meta["__affinity_ts__"].(float64); ok {
			affTS = v
		}
		if now-affTS >= timeout {
			available = append(available, it)
			releases++
		} else {
			heldBack = append(heldBack, it)
			holds++
		}
	}

	if holds > 0 {
		incAffinityHold(holds)
	}
	if releases > 0 {
		incAffinityRelease(releases)
	}
	return available, heldBack
}

// affinitySoftPartition stable-partitions a tier so items pinned to endpoint
// come first, preserving input order within each group.
func (q *CentralQueue) affinitySoftPartition(tier []queueItem, endpoint string) []queueItem {
	if q.affinity == nil || q.cfg.AffinityMode != "soft" {
		return tier
	}
	matched := make([]queueItem, 0, len(tier))
	unmatched := make([]queueItem, 0, len(tier))
	for _, it := range tier {
		if q.affinityMatch(endpoint, it.meta) {
			matched = append(matched, it)
		} else {
			unmatched = append(unmatched, it)
		}
	}
	if len(matched) == 0 {
		return tier
	}
	return append(matched, unmatched...)
}

// affinityRecordDispatch claims each dispatched conversation key for endpoint
// and counts affinity hits.
func (q *CentralQueue) affinityRecordDispatch(chosen []queueItem, endpoint string) {
	if q.affinity == nil {
		return
	}
	hits := 0
	for _, it := range chosen {
		key := affinityKeyOf(it.meta)
		if key == "" {
			continue
		}
		if q.affinity.Lookup(key) == endpoint {
			hits++
		}
		q.affinity.Claim(key, endpoint)
	}
	if hits > 0 {
		incAffinityHit(hits)
	}
	setAffinityMapSize(q.affinity.Size())
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
