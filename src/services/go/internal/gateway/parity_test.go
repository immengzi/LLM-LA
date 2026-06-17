package gateway

import (
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
