package gateway

import (
	"crypto/rand"
	"fmt"
	"log"
	"math"
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

// kvUsageSample is a single GPU KV usage reading with its wall-clock timestamp.
type kvUsageSample struct {
	value float64
	ts    float64
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
	mu   sync.Mutex
	cfg  *Config
	kv   *kvAware
	pred OutputLengthPredictor
	slo  sloEngine // nil unless SLO_AWARE

	defaultModel string
	queues       map[string][]queueItem

	// Key-affinity conversation->endpoint map (nil unless AFFINITY_ENABLED).
	affinity *AffinityMap
	// True when the affinity map is backed by a durable Redis store. Gates the
	// seenEndpoints readiness check + prefetch/warm. Mirrors
	// RouterState._affinity_persist.
	affinityPersist bool

	// Endpoint (pod) liveness for persisted affinity: last time each endpoint
	// pulled. Used only when persistence is on, to treat mappings to
	// stale/absent (post-redeploy renamed) pods as misses. A sidecar /pull is
	// health-gated, so this doubles as the per-pod READY timestamp. Mirrors
	// RouterState._seen_endpoints.
	seenEndpoints map[string]float64

	// Soft KV divert: endpoint -> latest reported GPU KV usage sample. Updated
	// from /pull kv_usage and central-push /health polls. Stale samples ignored.
	// Mirrors RouterState._kv_usage_by_endpoint.
	kvUsageByEndpoint map[string]kvUsageSample
	// Hysteresis: endpoints currently considered under KV pressure. Mirrors
	// RouterState._kv_pressure_active.
	kvPressureActive map[string]bool

	// req_id -> endpoint that pulled it (streaming identity / push notify).
	reqEndpoint map[string]string

	// Always-on per-endpoint in-flight count for pull mode: items dispatched to
	// each endpoint that have not yet returned a /result. Maintained regardless
	// of SLO_AWARE (the SLO batchSizeEstimator only tracks this under SLO).
	// Incremented at pull dispatch, decremented on /result. Mirrors
	// RouterState._inflight_by_endpoint in router_state.py.
	inflightByEndpoint map[string]int

	// Sum of ISL tokens in flight per endpoint (P0 observability): the
	// token-weighted companion to inflightByEndpoint. Mirrors
	// RouterState._inflight_tokens_by_endpoint. reqISLTokens remembers the token
	// count charged for each in-flight req so release subtracts it exactly.
	inflightTokensByEndpoint map[string]int
	reqISLTokens             map[string]int

	// Per-endpoint last /pull wall-clock timestamp. Feeds the fairness liveness
	// signal + the fleet-average denominator. Mirrors RouterState._last_pull_ts.
	lastPullByEndpoint map[string]float64

	// Per-endpoint last-successful-result wall-clock timestamp. Under central-push
	// the dispatcher (not the sidecar) stamps lastPullByEndpoint every tick, so
	// this becomes the stuck-detection liveness signal instead. Mirrors
	// RouterState._last_result_ts.
	lastResultByEndpoint map[string]float64

	// req_id -> streaming chunk channel (SSE).
	chunkMu     sync.Mutex
	chunkQueues map[string]chan map[string]interface{}
}

func NewCentralQueue(cfg *Config, kv *kvAware) *CentralQueue {
	setCentralQueueLength(0)
	var aff *AffinityMap
	affinityPersist := false
	if cfg.AffinityEnabled {
		var store affinityStore
		if cfg.AffinityPersistEnabled {
			// build_affinity_store never fails hard: on any error persistence is
			// simply disabled (log-and-continue), matching the Python router.
			store = buildAffinityStore(cfg)
		}
		aff = NewAffinityMapWithStore(cfg.AffinityTTLS, store, cfg.AffinityCacheMax)
		affinityPersist = store != nil
	}
	return &CentralQueue{
		cfg:                      cfg,
		kv:                       kv,
		pred:                     getLengthPredictor(cfg),
		defaultModel:             cfg.ModelName,
		queues:                   make(map[string][]queueItem),
		affinity:                 aff,
		affinityPersist:          affinityPersist,
		seenEndpoints:            make(map[string]float64),
		kvUsageByEndpoint:        make(map[string]kvUsageSample),
		kvPressureActive:         make(map[string]bool),
		reqEndpoint:              make(map[string]string),
		inflightByEndpoint:       make(map[string]int),
		inflightTokensByEndpoint: make(map[string]int),
		reqISLTokens:             make(map[string]int),
		lastPullByEndpoint:       make(map[string]float64),
		lastResultByEndpoint:     make(map[string]float64),
		chunkQueues:              make(map[string]chan map[string]interface{}),
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
	q.publishLivenessLocked()
}

// livenessTSMapLocked returns which per-endpoint timestamp map backs stuck
// detection. In central-push the router (not the sidecar) drives dispatch, so
// lastPullByEndpoint is stamped every tick and no longer reflects sidecar
// liveness; use last-successful-result instead. Pull mode keeps last-/pull.
func (q *CentralQueue) livenessTSMapLocked() map[string]float64 {
	if q.cfg.IsCentralPush() {
		return q.lastResultByEndpoint
	}
	return q.lastPullByEndpoint
}

// publishLivenessLocked sets fairness liveness gauges (must hold q.mu). No-op
// unless StuckPullSeconds > 0. A pod is "stuck" when its liveness signal (last
// /pull, or last result under central-push) is older than the threshold while
// the central queue is backed up.
func (q *CentralQueue) publishLivenessLocked() {
	thr := q.cfg.StuckPullSeconds
	if thr <= 0 {
		return
	}
	now := nowS()
	backedUp := q.totalSizeLocked() > 0
	for ep, last := range q.livenessTSMapLocked() {
		age := now - last
		setEndpointLastPullSeconds(ep, age)
		if backedUp && age > float64(thr) {
			setEndpointStuck(ep, 1)
		} else {
			setEndpointStuck(ep, 0)
		}
	}
}

// isEndpointStuckLocked reports whether a pod's liveness signal is older than
// StuckPullSeconds while work is queued (must hold q.mu). Used for
// RELEASE_ON_STUCK affinity fallback. Liveness = last /pull (pull mode) or
// last result (central-push).
func (q *CentralQueue) isEndpointStuckLocked(endpoint string) bool {
	thr := q.cfg.StuckPullSeconds
	if thr <= 0 || endpoint == "" {
		return false
	}
	if q.totalSizeLocked() <= 0 {
		return false
	}
	last, ok := q.livenessTSMapLocked()[endpoint]
	if !ok {
		return true
	}
	return (nowS() - last) > float64(thr)
}

// applyFairThrottleLocked applies the pull-mode fairness throttle (must hold
// q.mu). Returns the possibly-reordered pool plus the new effective want.
// Mirrors RouterState._apply_fair_throttle.
func (q *CentralQueue) applyFairThrottleLocked(endpoint string, want, effectiveWant int, ordered []queueItem) ([]queueItem, int) {
	if !q.cfg.FairPull || endpoint == "" || effectiveWant <= 0 {
		return ordered, effectiveWant
	}

	// Denominator: every endpoint we know about (in-flight and/or has pulled),
	// including this one, so a fresh/cold pod counts as 0.
	active := make(map[string]struct{})
	for ep := range q.inflightByEndpoint {
		active[ep] = struct{}{}
	}
	for ep := range q.lastPullByEndpoint {
		active[ep] = struct{}{}
	}
	active[endpoint] = struct{}{}
	n := len(active)
	if n < 1 {
		n = 1
	}
	total := 0
	for ep := range active {
		total += q.inflightByEndpoint[ep]
	}
	avg := float64(total) / float64(n)
	me := q.inflightByEndpoint[endpoint]

	margin := q.cfg.FairMargin
	if margin < 1.0 {
		margin = 1.0
	}
	floor := q.cfg.FairFloor
	if floor < 0 {
		floor = 0
	}
	ceiling := margin * avg

	// Movable budget = how many more (unpinned) items this pod may take to fill
	// up to the ceiling. Truncate toward zero; clamp to floor below.
	movableRoom := int(ceiling - float64(me))
	if movableRoom >= effectiveWant {
		return ordered, effectiveWant
	}
	if movableRoom < floor {
		movableRoom = floor
	}

	// Split into self-pinned (always honored) vs movable, preserving order.
	pinned := make([]queueItem, 0, len(ordered))
	rest := make([]queueItem, 0, len(ordered))
	for _, it := range ordered {
		if q.affinityMatch(endpoint, it.meta) {
			pinned = append(pinned, it)
		} else {
			rest = append(rest, it)
		}
	}
	newOrdered := append(pinned, rest...)
	newEffective := len(pinned) + movableRoom
	if newEffective > effectiveWant {
		newEffective = effectiveWant
	}
	if newEffective < 0 {
		newEffective = 0
	}
	if newEffective != effectiveWant {
		q.logReq("fair throttle endpoint=%s me=%d avg=%.1f ceiling=%.1f pinned=%d movable_room=%d eff %d->%d",
			endpoint, me, avg, ceiling, len(pinned), movableRoom, effectiveWant, newEffective)
	}
	return newOrdered, newEffective
}

// ---------------------------------------------------------------------------
// Soft KV divert helpers (mirror router_state.py)
// ---------------------------------------------------------------------------

// RecordKVUsage stores a sidecar-reported GPU KV usage sample and updates the
// pressure hysteresis. Normalizes >1 values (percent) and rejects NaN/negative.
// Acquires q.mu itself (called outside the pull lock). Mirrors record_kv_usage.
func (q *CentralQueue) RecordKVUsage(endpoint string, kvUsage float64) {
	if endpoint == "" {
		return
	}
	v := kvUsage
	if math.IsNaN(v) || v < 0.0 {
		return
	}
	if v > 1.0 {
		if v > 100.0 {
			v = 1.0
		} else {
			v = v / 100.0
		}
	}
	now := nowS()
	q.mu.Lock()
	q.kvUsageByEndpoint[endpoint] = kvUsageSample{value: v, ts: now}
	// Hysteresis bookkeeping (also updated in kvPressureForLocked at pull time).
	if q.kvPressureActive[endpoint] {
		if v < q.cfg.KVPressureLow {
			q.kvPressureActive[endpoint] = false
		}
	} else if v >= q.cfg.KVPressureHigh {
		q.kvPressureActive[endpoint] = true
	}
	q.mu.Unlock()
	setEndpointKVUsage(endpoint, v)
}

// freshKVLocked returns the fresh kv_usage for endpoint, or ok=false when
// missing/stale. Caller holds q.mu. Mirrors _fresh_kv.
func (q *CentralQueue) freshKVLocked(endpoint string, now float64) (float64, bool) {
	rec, ok := q.kvUsageByEndpoint[endpoint]
	if !ok {
		return 0, false
	}
	staleS := q.cfg.KVUsageStaleS
	if staleS > 0 && (now-rec.ts) > staleS {
		return 0, false
	}
	return rec.value, true
}

// kvPressureForLocked applies HIGH/LOW hysteresis and records the new state.
// Caller holds q.mu. Mirrors _kv_pressure_for.
func (q *CentralQueue) kvPressureForLocked(endpoint string, kv float64) bool {
	var active bool
	if q.kvPressureActive[endpoint] {
		active = kv >= q.cfg.KVPressureLow
	} else {
		active = kv >= q.cfg.KVPressureHigh
	}
	q.kvPressureActive[endpoint] = active
	return active
}

// hasHealthyPeerLocked reports whether some OTHER endpoint has a fresh
// kv_usage < PEER_OK. Caller holds q.mu. Mirrors _has_healthy_peer.
func (q *CentralQueue) hasHealthyPeerLocked(endpoint string, now float64) bool {
	peerOK := q.cfg.KVPressurePeerOK
	peers := make(map[string]struct{})
	for ep := range q.kvUsageByEndpoint {
		peers[ep] = struct{}{}
	}
	for ep := range q.lastPullByEndpoint {
		peers[ep] = struct{}{}
	}
	for ep := range q.inflightByEndpoint {
		peers[ep] = struct{}{}
	}
	for ep := range peers {
		if ep == endpoint {
			continue
		}
		if kv, ok := q.freshKVLocked(ep, now); ok && kv < peerOK {
			return true
		}
	}
	return false
}

// applyKVSoftDivertLocked trims cold work from a high-KV pod when a healthier
// peer exists. Keeps affinity self-pins and items with prefixLen >=
// KVSoftMinHits. Default-off / missing KV / fleet-full => no-op. Caller holds
// q.mu. Mirrors _apply_kv_soft_divert.
func (q *CentralQueue) applyKVSoftDivertLocked(endpoint string, effectiveWant int, ordered []queueItem) ([]queueItem, int) {
	if !q.cfg.KVSoftDivert || endpoint == "" {
		return ordered, effectiveWant
	}
	if effectiveWant <= 0 || len(ordered) == 0 {
		setKVSoftDivertActive(endpoint, 0)
		return ordered, effectiveWant
	}

	now := nowS()
	kv, ok := q.freshKVLocked(endpoint, now)
	if !ok {
		setKVSoftDivertActive(endpoint, 0)
		return ordered, effectiveWant
	}
	if !q.kvPressureForLocked(endpoint, kv) {
		setKVSoftDivertActive(endpoint, 0)
		return ordered, effectiveWant
	}
	if !q.hasHealthyPeerLocked(endpoint, now) {
		setKVSoftDivertActive(endpoint, 0)
		return ordered, effectiveWant
	}

	minHits := q.cfg.KVSoftMinHits
	keep := make([]queueItem, 0, len(ordered))
	rest := make([]queueItem, 0, len(ordered))
	for _, it := range ordered {
		if q.affinityMatch(endpoint, it.meta) {
			keep = append(keep, it)
			continue
		}
		if q.kv.prefixLen(endpoint, it.reqID) >= minHits {
			keep = append(keep, it)
		} else {
			rest = append(rest, it)
		}
	}

	// trimmed = min(want, len(ordered)) - min(want, len(keep)); newEffective =
	// min(want, len(keep)).
	grantable := effectiveWant
	if grantable > len(ordered) {
		grantable = len(ordered)
	}
	kept := effectiveWant
	if kept > len(keep) {
		kept = len(keep)
	}
	trimmed := grantable - kept
	if trimmed < 0 {
		trimmed = 0
	}
	setKVSoftDivertActive(endpoint, 1)
	if trimmed > 0 {
		incKVSoftDivertTrimmed(endpoint, trimmed)
		q.logReq("kv soft divert endpoint=%s kv=%.3f kept=%d trimmed=%d eff %d->%d",
			endpoint, kv, len(keep), trimmed, effectiveWant, kept)
	}
	// Kept items first (relative order preserved), then the rest (requeued from
	// ordered[effectiveWant:] by the caller). Mirrors keep + rest in Python.
	return append(keep, rest...), kept
}

// ---------------------------------------------------------------------------
// Persistent-affinity helpers (mirror router_state.py)
// ---------------------------------------------------------------------------

// endpointAvailableLocked reports whether an affinity-target endpoint is a
// valid, READY routing target. Caller holds q.mu. Mirrors _endpoint_available:
//
//	release-on-stuck && stuck            -> false (release pin to LB)
//	persist off                          -> true  (legacy: honor in-memory pin)
//	seenEndpoints[target] missing        -> false (never-ready / still loading)
//	now - seenEndpoints[target] > STALE  -> false (gone / scaled down)
//	otherwise                            -> true  (ready & serving -> honor pin)
func (q *CentralQueue) endpointAvailableLocked(endpoint string) bool {
	if q.cfg.AffinityReleaseOnStuck && q.isEndpointStuckLocked(endpoint) {
		return false
	}
	if !q.affinityPersist {
		return true
	}
	if endpoint == "" {
		return false
	}
	last, ok := q.seenEndpoints[endpoint]
	if !ok {
		return false
	}
	return (nowS() - last) <= q.cfg.AffinityEndpointStaleS
}

// AffinityPrefetch warms one conversation key from the durable store into
// memory at admission (once per request). No-op when affinity is disabled or
// not persisted. Mirrors affinity_prefetch.
func (q *CentralQueue) AffinityPrefetch(key string) {
	if q.affinity == nil || !q.affinityPersist || key == "" {
		return
	}
	q.affinity.Prefetch(key)
}

// WarmAffinityFromStore reloads the affinity map from Redis at startup and
// returns the loaded count. No-op when not persisted. Mirrors
// warm_affinity_from_store.
func (q *CentralQueue) WarmAffinityFromStore() int {
	if q.affinity == nil || !q.affinityPersist {
		return 0
	}
	n := q.affinity.Warm()
	setAffinityMapSize(q.affinity.Size())
	log.Printf("[PullRouter] affinity map warmed from store: %d mappings", n)
	return n
}

// CloseAffinityStore flushes + closes the durable affinity store (shutdown).
func (q *CentralQueue) CloseAffinityStore() {
	if q.affinity != nil {
		q.affinity.Close()
	}
}

// LastPullSnapshot returns a copy of the per-endpoint last-/pull timestamp map.
func (q *CentralQueue) LastPullSnapshot() map[string]float64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]float64, len(q.lastPullByEndpoint))
	for k, v := range q.lastPullByEndpoint {
		out[k] = v
	}
	return out
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
// RouterState.pull_for_endpoint. The optional wantPrefillTokens (P2) is the
// per-pull uncached-prefill token budget; when omitted (or 0) the router falls
// back to its PrefillTokenBudget and, if that is also 0, to a pure count slice.
func (q *CentralQueue) Pull(endpoint string, want int, model string, wantPrefillTokens ...int) []JobItem {
	if want <= 0 {
		return nil
	}
	budgetTokens := 0
	if len(wantPrefillTokens) > 0 {
		budgetTokens = wantPrefillTokens[0]
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Track endpoint liveness for persisted-affinity availability. A sidecar
	// only issues /pull when vLLM /health == 200 within the last ~5s (the
	// health gate), so a pull ⟹ this pod was serviceable ≤5s ago; this table
	// therefore doubles as the per-pod READY timestamp. Stamp before the empty
	// -queue early return so a fresh pod becomes available on its first pull.
	// See docs/internal/persistent-affinity-map.md. Mirrors router_state.py.
	if q.affinityPersist && endpoint != "" {
		q.seenEndpoints[endpoint] = nowS()
	}

	// Always record last-pull time (fairness liveness + fleet-average source).
	if endpoint != "" {
		q.lastPullByEndpoint[endpoint] = nowS()
	}

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

	// Pull-mode fairness (load-aware grant throttle). No-op unless FairPull is
	// on; only trims the movable/unpinned tail (self-pinned items are kept), so
	// KV/affinity ordering is never overridden. Mirrors _apply_fair_throttle.
	ordered, effectiveWant = q.applyFairThrottleLocked(endpoint, want, effectiveWant, ordered)

	// Soft KV divert: on a GPU-KV-saturated pod with a healthier peer, keep
	// prefix hits + affinity pins and leave cold work for the peers. No-op when
	// disabled / KV samples missing / fleet all-high. Mirrors
	// _apply_kv_soft_divert.
	ordered, effectiveWant = q.applyKVSoftDivertLocked(endpoint, effectiveWant, ordered)

	takeN := effectiveWant
	if takeN > len(ordered) {
		takeN = len(ordered)
	}

	kvEnabled := q.cfg.KVAware

	// Prefill-token budget (P2): further shrink takeN so the grant fits an
	// uncached-prefill token budget instead of a pure count slice. No-op unless
	// PullBudgetEnabled + a positive budget. Mirrors _apply_prefill_token_budget.
	takeN = q.applyPrefillTokenBudgetLocked(endpoint, takeN, ordered, kvEnabled, budgetTokens)

	chosenRaw := ordered[:takeN]
	leftovers := ordered[takeN:]

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

	// Always-on per-endpoint in-flight bookkeeping (independent of SLO): count
	// these dispatched items as now being served by this endpoint. Held under
	// q.mu (Pull holds it), so update the map inline and publish the gauge.
	// Mirrors router_state.py.
	q.inflightByEndpoint[endpoint] += len(chosen)
	setEndpointInflight(endpoint, q.inflightByEndpoint[endpoint])

	// Dispatch metrics + endpoint tracking. Also capture the per-request routing
	// decision (independent of TRACE) so recordLatency can enrich /latency_log.
	// Mirrors record_routing in router_state.py: reuse the sort's kv_hits when KV
	// routing is on, else compute prefixLen directly (0 when measurement is off).
	logBlockHashes := q.cfg.LogBlockHashes
	addedTokens := 0
	for _, it := range chosen {
		incDispatch(endpoint)
		q.reqEndpoint[it.reqID] = endpoint

		// P0: charge this endpoint's in-flight token sum with the request's ISL.
		if tok := q.islTokensFor(it.reqID, it.meta); tok > 0 {
			q.reqISLTokens[it.reqID] = tok
			addedTokens += tok
		}

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
	if addedTokens > 0 {
		q.inflightTokensByEndpoint[endpoint] += addedTokens
		setEndpointInflightTokens(endpoint, q.inflightTokensByEndpoint[endpoint])
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

// islTokensFor returns the best-effort ISL token count for a request: the exact
// meta["__isl_tokens__"] when present, else a block-granular estimate from the
// registered block hashes (len(blocks) * KVBlockSize). Returns 0 when unknown.
// Mirrors RouterState._isl_tokens_for; the Go hash service only returns block
// hashes (no exact token length), so in practice the block estimate is used.
func (q *CentralQueue) islTokensFor(reqID string, meta map[string]interface{}) int {
	if meta != nil {
		switch n := meta["__isl_tokens__"].(type) {
		case int:
			if n > 0 {
				return n
			}
		case int64:
			if n > 0 {
				return int(n)
			}
		case float64:
			if n > 0 {
				return int(n)
			}
		}
	}
	blocks := q.kv.getRequestBlocks(reqID)
	if len(blocks) > 0 {
		blk := q.cfg.KVBlockSize
		if blk <= 0 {
			blk = 128
		}
		return len(blocks) * blk
	}
	return 0
}

// decEndpointTokens subtracts released ISL tokens from an endpoint's in-flight
// token sum (clamped at 0) and republishes the gauge. Acquires q.mu itself, so
// callers must NOT hold it. Mirrors RouterState._dec_endpoint_tokens.
func (q *CentralQueue) decEndpointTokens(endpoint string, tokens int) {
	if endpoint == "" || tokens <= 0 {
		return
	}
	q.mu.Lock()
	v := q.inflightTokensByEndpoint[endpoint] - tokens
	if v < 0 {
		v = 0
	}
	q.inflightTokensByEndpoint[endpoint] = v
	q.mu.Unlock()
	setEndpointInflightTokens(endpoint, v)
}

// applyPrefillTokenBudgetLocked shrinks takeN so the grant fits an uncached-
// prefill token budget (P2). Must hold q.mu. Returns the number of leading items
// from ordered to take. No-op unless PullBudgetEnabled + a positive budget is
// resolvable. Ordering is never changed; the first item is always admitted so a
// request larger than the whole budget still makes progress. Only ever reduces
// the count. Mirrors RouterState._apply_prefill_token_budget.
func (q *CentralQueue) applyPrefillTokenBudgetLocked(endpoint string, takeN int, ordered []queueItem, kvEnabled bool, wantPrefillTokens int) int {
	if !q.cfg.PullBudgetEnabled || takeN <= 0 {
		return takeN
	}
	budget := wantPrefillTokens
	if budget <= 0 {
		budget = q.cfg.PrefillTokenBudget
	}
	if budget <= 0 {
		return takeN
	}
	blk := q.cfg.KVBlockSize
	if blk <= 0 {
		blk = 128
	}
	spent, k := 0, 0
	for _, it := range ordered[:takeN] {
		isl := q.islTokensFor(it.reqID, it.meta)
		cached := 0
		if kvEnabled {
			cached = q.kv.prefixLen(endpoint, it.reqID) * blk
		}
		uncached := isl - cached
		if uncached < 0 {
			uncached = 0
		}
		// Always admit the first item; else stop before exceeding the budget.
		if k > 0 && spent+uncached > budget {
			break
		}
		spent += uncached
		k++
	}
	setPullGrantedPrefillTokens(endpoint, spent)
	if k < takeN {
		incPullBudgetBound(endpoint)
	}
	return k
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
		// Fall back to normal LB when there is no pin, the pin is us, or the
		// pinned pod is no longer an available/ready routing target (scaled
		// down / renamed by a redeploy, or stuck when RELEASE_ON_STUCK is on).
		// Matches router_state.py: this branch is NOT counted as a release.
		if target == "" || target == endpoint || !q.endpointAvailableLocked(target) {
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

// IncEndpointInflight increments the always-on per-endpoint in-flight count.
func (q *CentralQueue) IncEndpointInflight(endpoint string, n int) {
	if endpoint == "" || n <= 0 {
		return
	}
	q.mu.Lock()
	q.inflightByEndpoint[endpoint] += n
	v := q.inflightByEndpoint[endpoint]
	q.mu.Unlock()
	setEndpointInflight(endpoint, v)
}

// DecEndpointInflight decrements the always-on per-endpoint in-flight count
// (clamped at 0), called when a result arrives for the endpoint.
func (q *CentralQueue) DecEndpointInflight(endpoint string, n int) {
	if endpoint == "" || n <= 0 {
		return
	}
	q.mu.Lock()
	cur := q.inflightByEndpoint[endpoint]
	v := cur - n
	if v < 0 {
		v = 0
	}
	q.inflightByEndpoint[endpoint] = v
	// Liveness signal for central-push stuck detection: a result drained.
	q.lastResultByEndpoint[endpoint] = nowS()
	q.mu.Unlock()
	setEndpointInflight(endpoint, v)
}

// ReleaseInflight idempotently releases the in-flight slot for a request. Pops
// the req_id -> endpoint mapping (recorded at dispatch) and decrements that
// endpoint's count exactly once. Safe to call from both the /result path and
// the wait-timeout reconcile: whichever runs first performs the decrement.
// Mirrors RouterState.release_inflight. endpointHint is unused when the request
// was tracked here; kept for signature parity / push-* no-op safety.
func (q *CentralQueue) ReleaseInflight(reqID, endpointHint string) {
	q.mu.Lock()
	ep, ok := q.reqEndpoint[reqID]
	if ok {
		delete(q.reqEndpoint, reqID)
	}
	tok := q.reqISLTokens[reqID]
	delete(q.reqISLTokens, reqID)
	q.mu.Unlock()
	if ep != "" {
		q.DecEndpointInflight(ep, 1)
		if tok > 0 {
			q.decEndpointTokens(ep, tok)
		}
	}
}

// ActiveModels returns the model queue keys currently known (for the
// central-push dispatcher), always including the default model.
func (q *CentralQueue) ActiveModels() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, 0, len(q.queues)+1)
	seen := make(map[string]struct{})
	for m := range q.queues {
		out = append(out, m)
		seen[m] = struct{}{}
	}
	if q.defaultModel != "" {
		if _, ok := seen[q.defaultModel]; !ok {
			out = append(out, q.defaultModel)
		}
	}
	return out
}

// RequeueFront puts items back at the FRONT of a model queue, order-preserving.
// Used by the central-push dispatcher when a delivery fails. Caller is
// responsible for the matching in-flight release. Mirrors requeue_front.
func (q *CentralQueue) RequeueFront(model string, items []JobItem) {
	if len(items) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	m := q.getQueueLocked(model)
	qi := make([]queueItem, 0, len(items))
	for _, it := range items {
		qi = append(qi, queueItem{reqID: it.ReqID, prompt: it.Prompt, tEnq: it.TEnqClient, meta: it.Meta})
	}
	q.queues[m] = append(qi, q.queues[m]...)
	q.publishQueueMetricsLocked()
}

// GetEndpointInflight returns the current in-flight count for one endpoint.
func (q *CentralQueue) GetEndpointInflight(endpoint string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inflightByEndpoint[endpoint]
}

// EndpointInflightSnapshot returns a copy of the per-endpoint in-flight map.
func (q *CentralQueue) EndpointInflightSnapshot() map[string]int {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]int, len(q.inflightByEndpoint))
	for k, v := range q.inflightByEndpoint {
		out[k] = v
	}
	return out
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
