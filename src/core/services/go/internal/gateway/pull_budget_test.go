package gateway

import "testing"

// TestISLTokensFor verifies the exact-meta-then-block-estimate fallback (P0).
func TestISLTokensFor(t *testing.T) {
	cfg := &Config{ModelName: "m0", KVBlockSize: 128}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())

	// Block-granular estimate when no meta is present.
	q.kv.registerRequestBlocks("r1", []string{"1", "2", "3"})
	if got := q.islTokensFor("r1", nil); got != 3*128 {
		t.Fatalf("block estimate = %d, want %d", got, 3*128)
	}
	// Exact meta wins over the estimate.
	if got := q.islTokensFor("r1", map[string]interface{}{"__isl_tokens__": 999}); got != 999 {
		t.Fatalf("meta exact = %d, want 999", got)
	}
	// float64 (JSON-decoded) also accepted.
	if got := q.islTokensFor("r1", map[string]interface{}{"__isl_tokens__": float64(1234)}); got != 1234 {
		t.Fatalf("meta float = %d, want 1234", got)
	}
	// Unknown request with no meta -> 0.
	if got := q.islTokensFor("missing", nil); got != 0 {
		t.Fatalf("unknown = %d, want 0", got)
	}
}

// TestInflightTokensTrackedAndReleased verifies dispatch charges the endpoint's
// in-flight token sum and release subtracts it exactly (P0).
func TestInflightTokensTrackedAndReleased(t *testing.T) {
	cfg := &Config{ModelName: "m0", KVBlockSize: 128}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())

	q.Enqueue("a", 0, map[string]interface{}{"__isl_tokens__": 500}, "r1", "m0")
	q.Enqueue("b", 0, map[string]interface{}{"__isl_tokens__": 300}, "r2", "m0")

	if items := q.Pull("epA", 2, "m0"); len(items) != 2 {
		t.Fatalf("pulled %d, want 2", len(items))
	}
	q.mu.Lock()
	got := q.inflightTokensByEndpoint["epA"]
	q.mu.Unlock()
	if got != 800 {
		t.Fatalf("inflight tokens = %d, want 800", got)
	}

	q.ReleaseInflight("r1", "epA")
	q.mu.Lock()
	got = q.inflightTokensByEndpoint["epA"]
	q.mu.Unlock()
	if got != 300 {
		t.Fatalf("inflight tokens after release = %d, want 300", got)
	}

	// Idempotent release: second call must not double-subtract.
	q.ReleaseInflight("r1", "epA")
	q.mu.Lock()
	got = q.inflightTokensByEndpoint["epA"]
	q.mu.Unlock()
	if got != 300 {
		t.Fatalf("inflight tokens after double release = %d, want 300", got)
	}
}

// TestPrefillBudgetDisabled verifies a pure count slice when the feature is off.
func TestPrefillBudgetDisabled(t *testing.T) {
	cfg := &Config{ModelName: "m0", KVBlockSize: 128, PullBudgetEnabled: false}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())
	q.Enqueue("a", 0, map[string]interface{}{"__isl_tokens__": 100000}, "r1", "m0")
	q.Enqueue("b", 0, map[string]interface{}{"__isl_tokens__": 100000}, "r2", "m0")
	if items := q.Pull("epA", 2, "m0"); len(items) != 2 {
		t.Fatalf("disabled pulled %d, want 2 (count slice)", len(items))
	}
}

// TestPrefillBudgetZeroIsCountOnly verifies enabled-but-zero-budget is a no-op.
func TestPrefillBudgetZeroIsCountOnly(t *testing.T) {
	cfg := &Config{ModelName: "m0", KVBlockSize: 128, PullBudgetEnabled: true, PrefillTokenBudget: 0}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())
	q.Enqueue("a", 0, map[string]interface{}{"__isl_tokens__": 9999}, "r1", "m0")
	q.Enqueue("b", 0, map[string]interface{}{"__isl_tokens__": 9999}, "r2", "m0")
	if items := q.Pull("epA", 2, "m0"); len(items) != 2 {
		t.Fatalf("zero-budget pulled %d, want 2 (count-only)", len(items))
	}
}

// TestPrefillBudgetCapsByTokens verifies the budget (not the count cap) binds.
func TestPrefillBudgetCapsByTokens(t *testing.T) {
	cfg := &Config{ModelName: "m0", KVBlockSize: 128, PullBudgetEnabled: true, PrefillTokenBudget: 1000}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())
	q.Enqueue("a", 0, map[string]interface{}{"__isl_tokens__": 400}, "r1", "m0")
	q.Enqueue("b", 0, map[string]interface{}{"__isl_tokens__": 400}, "r2", "m0")
	q.Enqueue("c", 0, map[string]interface{}{"__isl_tokens__": 400}, "r3", "m0")
	items := q.Pull("epA", 3, "m0")
	if len(items) != 2 || items[0].ReqID != "r1" || items[1].ReqID != "r2" {
		t.Fatalf("budget cap got %d items %v, want [r1 r2]", len(items), ids(items))
	}
	if q.Size("m0") != 1 {
		t.Fatalf("remaining = %d, want 1", q.Size("m0"))
	}
}

// TestPrefillBudgetAlwaysAdmitsFirst verifies a request larger than the whole
// budget still makes progress (no deadlock).
func TestPrefillBudgetAlwaysAdmitsFirst(t *testing.T) {
	cfg := &Config{ModelName: "m0", KVBlockSize: 128, PullBudgetEnabled: true, PrefillTokenBudget: 100}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())
	q.Enqueue("a", 0, map[string]interface{}{"__isl_tokens__": 50000}, "big", "m0")
	q.Enqueue("b", 0, map[string]interface{}{"__isl_tokens__": 10}, "small", "m0")
	items := q.Pull("epA", 2, "m0")
	if len(items) != 1 || items[0].ReqID != "big" {
		t.Fatalf("always-first got %d %v, want [big]", len(items), ids(items))
	}
}

// TestPrefillBudgetSidecarOverridesConfig verifies the per-pull budget wins.
func TestPrefillBudgetSidecarOverridesConfig(t *testing.T) {
	cfg := &Config{ModelName: "m0", KVBlockSize: 128, PullBudgetEnabled: true, PrefillTokenBudget: 100000}
	cfg.normalize()
	q := NewCentralQueue(cfg, newKVAware())
	q.Enqueue("a", 0, map[string]interface{}{"__isl_tokens__": 400}, "r1", "m0")
	q.Enqueue("b", 0, map[string]interface{}{"__isl_tokens__": 400}, "r2", "m0")
	// Per-pull budget 500: 400 ok, +400 = 800 > 500 -> stop at 1.
	items := q.Pull("epA", 2, "m0", 500)
	if len(items) != 1 || items[0].ReqID != "r1" {
		t.Fatalf("sidecar-budget got %d %v, want [r1]", len(items), ids(items))
	}
}

func ids(items []JobItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.ReqID
	}
	return out
}
