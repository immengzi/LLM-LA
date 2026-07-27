package gateway

import "testing"

const promSampleText = `# HELP vllm:prompt_tokens_total prompt tokens
# TYPE vllm:prompt_tokens_total counter
vllm:prompt_tokens_total{model_name="m"} 100
vllm:generation_tokens_total{model_name="m"} 50
vllm:prompt_tokens_total{model_name="m2"} 25
vllm:num_requests_running 3
`

// TestPickMinScore mirrors the Python _pick_min_score.
func TestPickMinScore(t *testing.T) {
	if got := pickMinScore([]epScore{{"a", 0.9, true}, {"b", 0.2, true}, {"c", 0.5, true}}); got != "b" {
		t.Fatalf("smallest: got %q, want b", got)
	}
	if got := pickMinScore([]epScore{{"a", 0, false}, {"b", 0.7, true}}); got != "b" {
		t.Fatalf("skip-failed: got %q, want b", got)
	}
	if got := pickMinScore([]epScore{{"a", 0, false}, {"b", 0, false}}); got != "" {
		t.Fatalf("all-failed: got %q, want empty", got)
	}
	if got := pickMinScore(nil); got != "" {
		t.Fatalf("empty: got %q, want empty", got)
	}
}

// TestParsePromSums verifies base metrics are summed across label sets.
func TestParsePromSums(t *testing.T) {
	sums, seen := parsePromSums(promSampleText, []string{"vllm:prompt_tokens_total", "vllm:missing"})
	if sums["vllm:prompt_tokens_total"] != 125.0 {
		t.Fatalf("prompt sum = %v, want 125", sums["vllm:prompt_tokens_total"])
	}
	if seen["vllm:missing"] {
		t.Fatal("absent metric should have seen=false")
	}
}

// TestTotalTokensFromSums checks prompt+generation, single-counter, and absent.
func TestTotalTokensFromSums(t *testing.T) {
	sums, seen := parsePromSums(promSampleText, []string{"vllm:prompt_tokens_total", "vllm:generation_tokens_total"})
	if v, ok := totalTokensFromSums(sums, seen); !ok || v != 175.0 {
		t.Fatalf("total = %v ok=%v, want 175 true", v, ok)
	}
	// Single counter present -> other treated as 0.
	s2, se2 := parsePromSums("vllm:prompt_tokens_total 42\n", []string{"vllm:prompt_tokens_total", "vllm:generation_tokens_total"})
	if v, ok := totalTokensFromSums(s2, se2); !ok || v != 42.0 {
		t.Fatalf("single = %v ok=%v, want 42 true", v, ok)
	}
	// Neither present -> ok=false.
	s3, se3 := parsePromSums("vllm:num_requests_running 3\n", []string{"vllm:prompt_tokens_total", "vllm:generation_tokens_total"})
	if _, ok := totalTokensFromSums(s3, se3); ok {
		t.Fatal("absent counters should be ok=false")
	}
}

// TestChooseLowerLoad verifies the power-of-two-choices comparator mirrors
// the Python _pick_lower_load: smaller load wins, ties keep the first sampled
// endpoint, and a failed probe (ok=false) is treated as worst.
func TestChooseLowerLoad(t *testing.T) {
	cases := []struct {
		name string
		a    string
		sa   int
		aOK  bool
		b    string
		sb   int
		bOK  bool
		want string
	}{
		{"a-smaller", "a", 1, true, "b", 2, true, "a"},
		{"b-smaller", "a", 3, true, "b", 2, true, "b"},
		{"tie-first", "a", 2, true, "b", 2, true, "a"},
		{"a-failed", "a", 0, false, "b", 5, true, "b"},
		{"b-failed", "a", 5, true, "b", 0, false, "a"},
		{"both-failed", "a", 0, false, "b", 0, false, "a"},
	}
	for _, c := range cases {
		if got := chooseLowerLoad(c.a, c.sa, c.aOK, c.b, c.sb, c.bOK); got != c.want {
			t.Fatalf("%s: chooseLowerLoad = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestKVCost mirrors the Python _kv_cost: cost = scale*max(prefill-credit*hits,0)+load.
func TestKVCost(t *testing.T) {
	if got := kvCost(10, 5, 5.0, 1.0, 1.0); got != 10.0 {
		t.Fatalf("kvCost(10,5,5) = %v, want 10", got)
	}
	if got := kvCost(10, 8, 9.0, 1.0, 1.0); got != 11.0 {
		t.Fatalf("kvCost(10,8,9) = %v, want 11", got)
	}
	// Overlap credit over-subtracts -> adjusted prefill floors at 0.
	if got := kvCost(4, 10, 3.0, 1.0, 1.0); got != 3.0 {
		t.Fatalf("kvCost clamp = %v, want 3", got)
	}
	// scale weights prefill: 2*(10-2)+1 = 17.
	if got := kvCost(10, 2, 1.0, 1.0, 2.0); got != 17.0 {
		t.Fatalf("kvCost scale = %v, want 17", got)
	}
}

// TestSelectByCostArgmin verifies deterministic argmin (temperature 0), stable
// tie-break, and empty handling.
func TestSelectByCostArgmin(t *testing.T) {
	eps := []string{"a", "b", "c"}
	costs := map[string]float64{"a": 18, "b": 10, "c": 11}
	if got := selectByCost(eps, costs, 0); got != "b" {
		t.Fatalf("argmin = %q, want b", got)
	}
	// Tie -> first in eps order.
	if got := selectByCost([]string{"a", "b"}, map[string]float64{"a": 5, "b": 5}, 0); got != "a" {
		t.Fatalf("tie = %q, want a", got)
	}
	if got := selectByCost(nil, nil, 0); got != "" {
		t.Fatalf("empty = %q, want empty", got)
	}
}

// TestSelectByCostSoftmax verifies low-cost workers dominate sampling at low
// temperature.
func TestSelectByCostSoftmax(t *testing.T) {
	eps := []string{"a", "b", "c"}
	costs := map[string]float64{"a": 100, "b": 0, "c": 100}
	b := 0
	for i := 0; i < 400; i++ {
		if selectByCost(eps, costs, 0.5) == "b" {
			b++
		}
	}
	if b < 360 {
		t.Fatalf("softmax picked cheapest %d/400, want >360", b)
	}
}
