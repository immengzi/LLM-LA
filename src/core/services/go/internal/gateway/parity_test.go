package gateway

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func strp(s string) *string    { return &s }
func f64p(f float64) *float64   { return &f }
func intp(i int) *int           { return &i }

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
