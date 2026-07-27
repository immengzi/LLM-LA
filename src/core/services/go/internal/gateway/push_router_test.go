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
