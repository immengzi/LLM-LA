package gateway

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func strp(s string) *string   { return &s }
func f64p(f float64) *float64 { return &f }
func intp(i int) *int         { return &i }

// TestPrefixLen verifies longest-prefix matching against block ownership.
func TestPrefixLen(t *testing.T) {
	kv := newKVAware()
	kv.registerRequestBlocks("r1", []string{"10", "20", "30", "40"})

	// epA owns the first three blocks; epB owns only the first.
	kv.registerBlockOwners("10", []string{"epA", "epB"})
	kv.registerBlockOwners("20", []string{"epA"})
	kv.registerBlockOwners("30", []string{"epA"})
	// block 40 owned by nobody.

	if got := kv.prefixLen("epA", "r1"); got != 3 {
		t.Fatalf("prefixLen(epA) = %d, want 3", got)
	}
	if got := kv.prefixLen("epB", "r1"); got != 1 {
		t.Fatalf("prefixLen(epB) = %d, want 1", got)
	}
	if got := kv.prefixLen("epC", "r1"); got != 0 {
		t.Fatalf("prefixLen(epC) = %d, want 0", got)
	}
	if got := kv.prefixLen("epA", "missing"); got != 0 {
		t.Fatalf("prefixLen(missing req) = %d, want 0", got)
	}
}

// TestMeasurePrefixEnabled verifies prefix blocks are computed whenever routing
// needs them (KVAware) or measurement/logging is requested, mirroring api.py.
func TestMeasurePrefixEnabled(t *testing.T) {
	cases := []struct {
		kvAware, measure, logHashes bool
		want                        bool
	}{
		{false, false, false, false},
		{true, false, false, true},
		{false, true, false, true},
		{false, false, true, true},
		{true, true, true, true},
	}
	for _, c := range cases {
		cfg := &Config{KVAware: c.kvAware, MeasurePrefix: c.measure, LogBlockHashes: c.logHashes}
		if got := cfg.MeasurePrefixEnabled(); got != c.want {
			t.Fatalf("MeasurePrefixEnabled(kv=%v,measure=%v,log=%v) = %v, want %v",
				c.kvAware, c.measure, c.logHashes, got, c.want)
		}
	}
}

// TestRecordPopRouting verifies the routing store round-trips, pops once, and
// evicts oldest entries past the cap. Mirrors record_routing/pop_routing.
func TestRecordPopRouting(t *testing.T) {
	kv := newKVAware()
	kv.recordRouting("r1", routingInfo{endpoint: "epA", kvHitsLen: 2, totalBlocks: 5})

	got, ok := kv.popRouting("r1")
	if !ok {
		t.Fatal("popRouting(r1) missing after record")
	}
	if got.endpoint != "epA" || got.kvHitsLen != 2 || got.totalBlocks != 5 {
		t.Fatalf("popRouting(r1) = %+v, want epA/2/5", got)
	}
	if _, ok := kv.popRouting("r1"); ok {
		t.Fatal("popRouting(r1) should be empty after pop")
	}

	// Overflow the cap: the oldest surviving entry must be evicted.
	for i := 0; i < routingMax+10; i++ {
		kv.recordRouting(fmt.Sprintf("k%d", i), routingInfo{endpoint: "ep"})
	}
	if _, ok := kv.popRouting("k0"); ok {
		t.Fatal("k0 should have been evicted past the cap")
	}
	if _, ok := kv.popRouting(fmt.Sprintf("k%d", routingMax+9)); !ok {
		t.Fatal("most recent entry should still be present")
	}
}

// TestEnrichRoutingFields verifies /latency_log enrichment mirrors _routing_fields:
// always kv_hits_len/total_blocks/matched_tokens/kv_hit, plus affinity_key and
// block_hashes when recorded.
func TestEnrichRoutingFields(t *testing.T) {
	s := &Server{cfg: &Config{KVBlockSize: 128}, kv: newKVAware()}
	s.kv.recordRouting("r1", routingInfo{
		endpoint:    "epA",
		kvHitsLen:   3,
		totalBlocks: 4,
		affinityKey: "conv-1",
		hasAffinity: true,
		blockHashes: []string{"10", "20", "30"},
		hasBlocks:   true,
	})

	entry := map[string]interface{}{"rid": "r1"}
	s.enrichRoutingFields(entry)

	if entry["kv_hits_len"] != 3 || entry["total_blocks"] != 4 {
		t.Fatalf("counts = %v/%v, want 3/4", entry["kv_hits_len"], entry["total_blocks"])
	}
	if entry["matched_tokens"] != 3*128 {
		t.Fatalf("matched_tokens = %v, want %d", entry["matched_tokens"], 3*128)
	}
	if entry["kv_hit"] != true {
		t.Fatalf("kv_hit = %v, want true", entry["kv_hit"])
	}
	if entry["affinity_key"] != "conv-1" {
		t.Fatalf("affinity_key = %v, want conv-1", entry["affinity_key"])
	}
	if bh, ok := entry["block_hashes"].([]interface{}); !ok || len(bh) != 3 {
		t.Fatalf("block_hashes = %v, want 3 elems", entry["block_hashes"])
	}

	// kv_hit is false at zero hits, and a missing record is a no-op.
	s.kv.recordRouting("r2", routingInfo{endpoint: "epB", kvHitsLen: 0, totalBlocks: 0})
	e2 := map[string]interface{}{"rid": "r2"}
	s.enrichRoutingFields(e2)
	if e2["kv_hit"] != false || e2["matched_tokens"] != 0 {
		t.Fatalf("zero-hit entry = %+v, want kv_hit=false matched_tokens=0", e2)
	}
	if _, ok := e2["affinity_key"]; ok {
		t.Fatal("affinity_key should be absent when not recorded")
	}

	e3 := map[string]interface{}{"rid": "missing"}
	s.enrichRoutingFields(e3)
	if len(e3) != 1 {
		t.Fatalf("missing record should be a no-op, got %+v", e3)
	}
}

func cloneFleet(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// mkFairItems builds bare queue items with optional affinity keys in meta.
func mkFairItems(ids ...string) []queueItem {
	out := make([]queueItem, len(ids))
	for i, id := range ids {
		out[i] = queueItem{reqID: id, meta: map[string]interface{}{}}
	}
	return out
}

// TestFairThrottle verifies the pull-mode fairness grant throttle mirrors
// _apply_fair_throttle: underloaded pods keep full want, near-ceiling pods fill
// exactly the gap, overloaded pods drop to the floor.
func TestFairThrottle(t *testing.T) {
	// Fleet A=40, B=20, C=0 -> avg 20, ceiling 1.25*20 = 25.
	newQ := func(fleet map[string]int) *CentralQueue {
		cfg := &Config{FairPull: true, FairMargin: 1.25, FairFloor: 1}
		q := NewCentralQueue(cfg, newKVAware())
		q.inflightByEndpoint = fleet
		return q
	}

	fleet := map[string]int{"A": 40, "B": 20, "C": 0}
	ord := mkFairItems("1", "2", "3", "4", "5", "6", "7", "8")

	// Underloaded C (me=0): unchanged.
	if _, eff := newQ(cloneFleet(fleet)).applyFairThrottleLocked("C", 8, 8, ord); eff != 8 {
		t.Fatalf("underloaded C eff=%d, want 8", eff)
	}
	// Overloaded A (me=40 > 25): floor.
	if _, eff := newQ(cloneFleet(fleet)).applyFairThrottleLocked("A", 8, 8, ord); eff != 1 {
		t.Fatalf("overloaded A eff=%d, want 1 (floor)", eff)
	}
	// Near-ceiling: fleet X=22,Y=18 -> avg 20, ceiling 25, me=22 -> room 3.
	nearQ := newQ(map[string]int{"X": 22, "Y": 18})
	if _, eff := nearQ.applyFairThrottleLocked("X", 8, 8, ord); eff != 3 {
		t.Fatalf("near-ceiling X eff=%d, want 3", eff)
	}
	// Disabled switch: no-op even when overloaded.
	offCfg := &Config{FairPull: false, FairMargin: 1.25, FairFloor: 1}
	offQ := NewCentralQueue(offCfg, newKVAware())
	offQ.inflightByEndpoint = cloneFleet(fleet)
	if _, eff := offQ.applyFairThrottleLocked("A", 8, 8, ord); eff != 8 {
		t.Fatalf("FairPull off eff=%d, want 8 (no-op)", eff)
	}
}

// TestFairThrottlePinnedExempt verifies self-pinned (affinity) items are always
// granted and moved to the front; only the movable tail is trimmed.
func TestFairThrottlePinnedExempt(t *testing.T) {
	cfg := &Config{FairPull: true, FairMargin: 1.25, FairFloor: 1, AffinityEnabled: true, AffinityMode: "soft", AffinityTTLS: 300}
	q := NewCentralQueue(cfg, newKVAware())
	// A is badly overloaded -> movable budget clamps to floor (1).
	q.inflightByEndpoint = map[string]int{"A": 40, "B": 0}
	q.affinity.Claim("k1", "A")
	q.affinity.Claim("k2", "A")

	pin := func(id, key string) queueItem {
		return queueItem{reqID: id, meta: map[string]interface{}{"__affinity_key__": key}}
	}
	// Two pinned-to-A items interleaved with movable ones (pinned at the tail).
	ord := []queueItem{
		{reqID: "u1", meta: map[string]interface{}{}},
		{reqID: "u2", meta: map[string]interface{}{}},
		{reqID: "u3", meta: map[string]interface{}{}},
		pin("p1", "k1"),
		pin("p2", "k2"),
	}
	got, eff := q.applyFairThrottleLocked("A", 5, 5, ord)
	// 2 pinned always honored + floor 1 movable = 3.
	if eff != 3 {
		t.Fatalf("pinned-exempt eff=%d, want 3", eff)
	}
	// Pinned items must be at the front so ordered[:eff] keeps them.
	if got[0].reqID != "p1" || got[1].reqID != "p2" {
		t.Fatalf("pinned not front-loaded: %s,%s", got[0].reqID, got[1].reqID)
	}
}

// TestSplitWordTokensLossless verifies the SSE tokenizer reconstructs the input
// exactly when its tokens are concatenated.
func TestSplitWordTokensLossless(t *testing.T) {
	cases := []string{
		"hello world",
		"  leading spaces",
		"trailing\nnewlines\n",
		"tabs\tand spaces",
		"single",
		"",
	}
	for _, c := range cases {
		toks := splitWordTokens(c)
		if joined := strings.Join(toks, ""); joined != c {
			t.Fatalf("splitWordTokens(%q) joined to %q", c, joined)
		}
	}
}

// TestComputeSlackTTFT checks slack sign and binding for a TTFT SLO.
func TestComputeSlackTTFT(t *testing.T) {
	lp := newLinearLatencyPredictor(defaultLatencyProfile(), false, 0)

	loose := &sloEntry{
		SLOType:      strp("ttft"),
		InputTokens:  100,
		DeadlineTTFT: f64p(nowS() + 10.0),
	}
	slack, binding := computeSlack(loose, lp, 8, 0, 0.0, 16)
	if binding != "ttft" {
		t.Fatalf("binding = %q, want ttft", binding)
	}
	if slack <= 0 {
		t.Fatalf("loose deadline should yield positive slack, got %g", slack)
	}

	tight := &sloEntry{
		SLOType:      strp("ttft"),
		InputTokens:  100,
		DeadlineTTFT: f64p(nowS() - 10.0),
	}
	slackTight, _ := computeSlack(tight, lp, 8, 0, 0.0, 16)
	if slackTight >= 0 {
		t.Fatalf("past deadline should yield negative slack, got %g", slackTight)
	}

	// No SLO type -> infinite slack.
	none := &sloEntry{InputTokens: 100}
	if s, _ := computeSlack(none, lp, 8, 0, 0.0, 16); s != sloInf {
		t.Fatalf("nil SLOType should yield sloInf, got %g", s)
	}
}

// TestLinearPredictorMonotonic verifies the analytical model is monotonic in
// its key inputs.
func TestLinearPredictorMonotonic(t *testing.T) {
	lp := newLinearLatencyPredictor(defaultLatencyProfile(), false, 0)

	if lp.predictTTFT(1000, 0, 8) <= lp.predictTTFT(100, 0, 8) {
		t.Fatal("TTFT should grow with input tokens")
	}
	if lp.predictTPOT(16, 500) <= lp.predictTPOT(1, 500) {
		t.Fatal("TPOT should grow with batch size")
	}
	// Cached prefix reduces cold-compute work, so TTFT should not increase.
	withCache := lp.predictTTFT(1000, 500, 8)
	noCache := lp.predictTTFT(1000, 0, 8)
	if withCache > noCache+1e-12 {
		t.Fatalf("cached prefix should not increase TTFT: %g > %g", withCache, noCache)
	}
}

// TestBayesianUpdate verifies the online correction factor moves toward
// observed latency.
func TestBayesianUpdate(t *testing.T) {
	base := newLinearLatencyPredictor(defaultLatencyProfile(), false, 0)
	bp := newBayesianLatencyPredictor(base)

	before := bp.predictTTFT(500, 0, 8)
	predictedBase := base.predictTTFT(500, 0, 8)

	// Observe an actual TTFT 3x larger than the base prediction.
	for i := 0; i < 50; i++ {
		bp.update(latencyObservation{
			inputTokens: 500,
			batchSize:   8,
			actualTTFTs: predictedBase * 3.0,
		})
	}
	after := bp.predictTTFT(500, 0, 8)
	if after <= before {
		t.Fatalf("bayesian TTFT should rise after high observations: before=%g after=%g", before, after)
	}
	if math.IsNaN(after) || math.IsInf(after, 0) {
		t.Fatalf("bayesian TTFT not finite: %g", after)
	}
}

// TestCentralPushModePredicates verifies the mode allowlist/aliases and the
// pull/push-delivery predicates for central-push mirror the Python config.
func TestCentralPushModePredicates(t *testing.T) {
	for _, alias := range []string{"central-push", "central_push", "centralpush", "CENTRAL-PUSH"} {
		// SidecarEnabled true = today's default sidecar-backed central-push.
		c := &Config{RouterMode: alias, SidecarEnabled: true}
		c.normalize()
		if c.RouterMode != "central-push" {
			t.Fatalf("alias %q normalized to %q, want central-push", alias, c.RouterMode)
		}
		if !c.IsCentralPush() {
			t.Fatalf("IsCentralPush false for %q", alias)
		}
		if !c.UsesCentralQueue() {
			t.Fatalf("central-push must use the central queue")
		}
		if !c.UsesPushDelivery() {
			t.Fatalf("central-push must use push delivery")
		}
		if c.IsCentralPushDirect() || c.UsesDirectDelivery() {
			t.Fatalf("sidecar-backed central-push must not use direct delivery")
		}
		if c.IsPushMode() {
			t.Fatalf("central-push must not report IsPushMode")
		}
	}

	// Sidecar-less central-push: same central queue, but direct delivery.
	direct := &Config{RouterMode: "central-push", SidecarEnabled: false}
	direct.normalize()
	if !direct.UsesCentralQueue() {
		t.Fatalf("sidecar-less central-push must still use the central queue")
	}
	if direct.UsesPushDelivery() {
		t.Fatalf("sidecar-less central-push must not use push delivery")
	}
	if !direct.IsCentralPushDirect() || !direct.UsesDirectDelivery() {
		t.Fatalf("sidecar-less central-push must use direct delivery")
	}

	// pull: central queue, no push delivery.
	pull := &Config{RouterMode: "pull"}
	pull.normalize()
	if !pull.UsesCentralQueue() || pull.UsesPushDelivery() {
		t.Fatalf("pull predicates wrong: queue=%v delivery=%v", pull.UsesCentralQueue(), pull.UsesPushDelivery())
	}
	// push-rr: push delivery, no central queue.
	push := &Config{RouterMode: "push-rr"}
	push.normalize()
	if push.UsesCentralQueue() || !push.UsesPushDelivery() {
		t.Fatalf("push-rr predicates wrong: queue=%v delivery=%v", push.UsesCentralQueue(), push.UsesPushDelivery())
	}

	// Cap/interval sanitation.
	c := &Config{RouterMode: "central-push", CentralPushCap: 0, CentralPushIntervalS: 0}
	c.normalize()
	if c.CentralPushCap != 1 {
		t.Fatalf("CentralPushCap floor = %d, want 1", c.CentralPushCap)
	}
	if c.CentralPushIntervalS != 0.05 {
		t.Fatalf("CentralPushIntervalS default = %v, want 0.05", c.CentralPushIntervalS)
	}
}

// TestActiveModelsIncludesDefault verifies ActiveModels always includes the
// default model queue key so a fresh dispatcher still has a target.
func TestActiveModelsIncludesDefault(t *testing.T) {
	cfg := &Config{RouterMode: "central-push", ModelName: "m0"}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())
	models := q.ActiveModels()
	found := false
	for _, m := range models {
		if m == "m0" {
			found = true
		}
	}
	if !found {
		t.Fatalf("ActiveModels %v missing default m0", models)
	}
	// After enqueue to a second model, both appear once.
	q.Enqueue("p", 0, nil, "r1", "m1")
	seen := map[string]int{}
	for _, m := range q.ActiveModels() {
		seen[m]++
	}
	if seen["m0"] != 1 || seen["m1"] != 1 {
		t.Fatalf("ActiveModels dedup wrong: %v", seen)
	}
}

// TestReleaseInflightIdempotent verifies ReleaseInflight decrements exactly once
// even when called twice (result path + timeout reconcile).
func TestReleaseInflightIdempotent(t *testing.T) {
	cfg := &Config{RouterMode: "central-push", ModelName: "m0"}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())

	// Simulate a dispatch: record mapping + in-flight.
	q.IncEndpointInflight("epA", 1)
	q.mu.Lock()
	q.reqEndpoint["r1"] = "epA"
	q.mu.Unlock()

	if got := q.GetEndpointInflight("epA"); got != 1 {
		t.Fatalf("inflight before = %d, want 1", got)
	}
	q.ReleaseInflight("r1", "epA")
	if got := q.GetEndpointInflight("epA"); got != 0 {
		t.Fatalf("inflight after first release = %d, want 0", got)
	}
	// Second release is a no-op (map entry already gone).
	q.ReleaseInflight("r1", "epA")
	if got := q.GetEndpointInflight("epA"); got != 0 {
		t.Fatalf("inflight after second release = %d, want 0 (idempotent)", got)
	}
}

// TestRequeueFrontOrder verifies RequeueFront re-admits items at the front,
// order-preserving, so a failed central-push delivery is retried first.
func TestRequeueFrontOrder(t *testing.T) {
	cfg := &Config{RouterMode: "central-push", ModelName: "m0"}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())

	q.Enqueue("p-existing", 0, nil, "existing", "m0")
	q.RequeueFront("m0", []JobItem{
		{ReqID: "a", Prompt: "pa", Meta: map[string]interface{}{}},
		{ReqID: "b", Prompt: "pb", Meta: map[string]interface{}{}},
	})

	// Pull everything (no fairness/affinity), verify a,b come before existing.
	items := q.Pull("epA", 10, "m0")
	if len(items) != 3 {
		t.Fatalf("pulled %d items, want 3", len(items))
	}
	if items[0].ReqID != "a" || items[1].ReqID != "b" || items[2].ReqID != "existing" {
		t.Fatalf("requeue order wrong: %s,%s,%s", items[0].ReqID, items[1].ReqID, items[2].ReqID)
	}
}

// TestCentralPushLivenessSignal verifies stuck detection uses last-result under
// central-push (not last-pull, which the dispatcher stamps every tick).
func TestCentralPushLivenessSignal(t *testing.T) {
	cfg := &Config{RouterMode: "central-push", ModelName: "m0", StuckPullSeconds: 1}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())

	// Back up the queue so stuck detection is active.
	q.Enqueue("p", 0, nil, "r1", "m0")

	// A recent /pull but stale result should still be flagged stuck under
	// central-push (the pull map is not the liveness source).
	q.mu.Lock()
	q.lastPullByEndpoint["epA"] = nowS()           // fresh pull
	q.lastResultByEndpoint["epA"] = nowS() - 100.0 // stale result
	stuck := q.isEndpointStuckLocked("epA")
	q.mu.Unlock()
	if !stuck {
		t.Fatal("central-push: endpoint with stale result should be stuck despite fresh pull")
	}

	// Fresh result -> not stuck.
	q.mu.Lock()
	q.lastResultByEndpoint["epA"] = nowS()
	stuck = q.isEndpointStuckLocked("epA")
	q.mu.Unlock()
	if stuck {
		t.Fatal("central-push: endpoint with fresh result should not be stuck")
	}
}
