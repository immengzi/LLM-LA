package gateway

import (
	"encoding/json"
	"testing"
)

// newSoftDivertQueue builds a CentralQueue with a clean, deterministic baseline
// for soft-divert tests, mirroring the `cfg` fixture in
// tests/test_router_state_pull.py. Soft divert is ON; KV/len/affinity/fair are
// off unless a test flips them.
func newSoftDivertQueue() *CentralQueue {
	cfg := LoadConfig()
	cfg.KVAware = false
	cfg.LenAware = false
	cfg.LenPolicy = "short_first"
	cfg.PoolFactor = 4
	cfg.AffinityEnabled = false
	cfg.AffinityMode = "soft"
	cfg.SLOAware = false
	cfg.FixedBatchSize = 0
	cfg.FairPull = false
	cfg.TraceEnabled = false
	cfg.LogBlockHashes = false
	cfg.KVSoftDivert = true
	cfg.KVPressureHigh = 0.85
	cfg.KVPressureLow = 0.75
	cfg.KVPressurePeerOK = 0.70
	cfg.KVSoftMinHits = 1
	cfg.KVUsageStaleS = 30.0
	return NewCentralQueue(cfg, newKVAware())
}

func enqueueN(q *CentralQueue, n int, model string) {
	for i := 0; i < n; i++ {
		q.Enqueue("p", nowS(), nil, "", model)
	}
}

// seedKV mirrors _seed_kv: record self usage + an optional peer, and stamp both
// as having pulled (so the fleet/peer denominators see them).
func seedKV(q *CentralQueue, ep string, kv float64, peer string, peerKV float64) {
	q.RecordKVUsage(ep, kv)
	if peer != "" {
		q.RecordKVUsage(peer, peerKV)
		q.mu.Lock()
		q.lastPullByEndpoint[peer] = 1.0
		q.mu.Unlock()
	}
	q.mu.Lock()
	q.lastPullByEndpoint[ep] = 1.0
	q.mu.Unlock()
}

// TestKVSoftDivertKeepsPrefixAndAffinity mirrors
// test_kv_soft_divert_keeps_prefix_and_affinity: on a saturated pod with a
// healthy peer, cold work is trimmed but prefix hits + affinity pins are kept.
func TestKVSoftDivertKeepsPrefixAndAffinity(t *testing.T) {
	q := newSoftDivertQueue()
	q.cfg.KVAware = true
	q.cfg.AffinityEnabled = true
	q.affinity = NewAffinityMap(300.0)
	q.affinity.Claim("conv-hot", "epHot")
	seedKV(q, "epHot", 0.95, "epCool", 0.20)

	q.kv.registerRequestBlocks("hit", []string{"1", "2"})
	q.kv.setRequestOwners("hit", map[string]map[string]bool{
		"1": {"epHot": true}, "2": {"epHot": true},
	})
	q.kv.registerRequestBlocks("cold", []string{"9"})
	q.kv.setRequestOwners("cold", map[string]map[string]bool{})

	q.Enqueue("c", nowS(), nil, "cold", "m")
	q.Enqueue("h", nowS(), nil, "hit", "m")
	q.Enqueue("p", nowS(), map[string]interface{}{"__affinity_key__": "conv-hot"}, "pin", "m")

	items := q.Pull("epHot", 3, "m")
	ids := idsOf(items)
	if contains(ids, "cold") {
		t.Fatalf("cold work should be diverted, got %v", ids)
	}
	if len(ids) != 2 || !contains(ids, "hit") || !contains(ids, "pin") {
		t.Fatalf("expected {hit,pin}, got %v", ids)
	}
	if q.Size() != 1 {
		t.Fatalf("expected cold requeued (size 1), got %d", q.Size())
	}
}

// TestKVSoftDivertNoOpWhenAllPeersHigh mirrors
// test_kv_soft_divert_no_op_when_all_peers_high.
func TestKVSoftDivertNoOpWhenAllPeersHigh(t *testing.T) {
	q := newSoftDivertQueue()
	seedKV(q, "epA", 0.95, "epB", 0.90)
	enqueueN(q, 4, "m")
	if got := len(q.Pull("epA", 4, "m")); got != 4 {
		t.Fatalf("no healthy peer -> no divert; want 4, got %d", got)
	}
}

// TestKVSoftDivertNoOpWhenDisabled mirrors test_kv_soft_divert_no_op_when_disabled.
func TestKVSoftDivertNoOpWhenDisabled(t *testing.T) {
	q := newSoftDivertQueue()
	q.cfg.KVSoftDivert = false
	seedKV(q, "epA", 0.99, "epB", 0.1)
	enqueueN(q, 3, "m")
	if got := len(q.Pull("epA", 3, "m")); got != 3 {
		t.Fatalf("disabled -> no divert; want 3, got %d", got)
	}
}

// TestKVSoftDivertNoOpWhenSelfKVMissing mirrors
// test_kv_soft_divert_no_op_when_self_kv_missing.
func TestKVSoftDivertNoOpWhenSelfKVMissing(t *testing.T) {
	q := newSoftDivertQueue()
	q.RecordKVUsage("epB", 0.1)
	q.mu.Lock()
	q.lastPullByEndpoint["epA"] = 1.0
	q.lastPullByEndpoint["epB"] = 1.0
	q.mu.Unlock()
	enqueueN(q, 3, "m")
	if got := len(q.Pull("epA", 3, "m")); got != 3 {
		t.Fatalf("self KV missing -> no divert; want 3, got %d", got)
	}
}

// TestKVSoftDivertStalePeerNoDivert mirrors test_kv_soft_divert_stale_peer_no_divert.
func TestKVSoftDivertStalePeerNoDivert(t *testing.T) {
	q := newSoftDivertQueue()
	q.cfg.KVUsageStaleS = 1.0
	q.RecordKVUsage("epA", 0.95)
	q.mu.Lock()
	q.kvUsageByEndpoint["epB"] = kvUsageSample{value: 0.1, ts: nowS() - 60.0} // stale
	q.lastPullByEndpoint["epA"] = 1.0
	q.lastPullByEndpoint["epB"] = 1.0
	q.mu.Unlock()
	enqueueN(q, 3, "m")
	if got := len(q.Pull("epA", 3, "m")); got != 3 {
		t.Fatalf("stale peer -> no healthy peer -> no divert; want 3, got %d", got)
	}
}

// TestKVSoftDivertHysteresis mirrors test_kv_soft_divert_hysteresis_low: once
// pressured, a pod keeps trimming until usage drops below LOW.
func TestKVSoftDivertHysteresis(t *testing.T) {
	q := newSoftDivertQueue()
	seedKV(q, "epA", 0.95, "epB", 0.1)
	q.mu.Lock()
	entered := q.kvPressureActive["epA"]
	q.mu.Unlock()
	if !entered {
		t.Fatal("epA should have entered pressure at 0.95")
	}

	// Still above LOW -> stays pressured; no pins/hits -> trims all.
	q.RecordKVUsage("epA", 0.80)
	enqueueN(q, 3, "m")
	if got := len(q.Pull("epA", 3, "m")); got != 0 {
		t.Fatalf("still pressured -> trim to 0, got %d", got)
	}

	// Drop below LOW -> clears pressure -> no-op.
	q.RecordKVUsage("epA", 0.50)
	enqueueN(q, 3, "m")
	if got := len(q.Pull("epA", 3, "m")); got != 3 {
		t.Fatalf("pressure cleared -> no divert; want 3, got %d", got)
	}
}

// TestKVSoftDivertComposesWithFairPull mirrors
// test_kv_soft_divert_composes_with_fair_pull: fair throttle caps to floor=1,
// then soft divert trims that last cold item to 0.
func TestKVSoftDivertComposesWithFairPull(t *testing.T) {
	q := newSoftDivertQueue()
	q.cfg.FairPull = true
	q.cfg.FairMargin = 1.0
	q.cfg.FairFloor = 1
	q.mu.Lock()
	q.inflightByEndpoint["epA"] = 8
	q.inflightByEndpoint["epB"] = 0
	q.mu.Unlock()
	seedKV(q, "epA", 0.95, "epB", 0.1)
	enqueueN(q, 5, "m")
	if got := len(q.Pull("epA", 5, "m")); got != 0 {
		t.Fatalf("fair floor then soft divert -> 0, got %d", got)
	}
}

// TestPullRequestDecodesKVUsage verifies the /pull body carries the optional
// kv_usage fraction (present and absent), so the handler can feed soft divert.
func TestPullRequestDecodesKVUsage(t *testing.T) {
	var withKV PullRequest
	if err := json.Unmarshal([]byte(`{"endpoint":"ep","want":4,"model":"m","kv_usage":0.83}`), &withKV); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if withKV.KvUsage == nil || *withKV.KvUsage != 0.83 {
		t.Fatalf("kv_usage not decoded: %v", withKV.KvUsage)
	}

	var noKV PullRequest
	if err := json.Unmarshal([]byte(`{"endpoint":"ep","want":4,"model":"m"}`), &noKV); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if noKV.KvUsage != nil {
		t.Fatalf("kv_usage should be nil when absent, got %v", *noKV.KvUsage)
	}
}

// TestRecordKVUsageNormalizesAndRejects verifies percent normalization and
// NaN/negative rejection, mirroring record_kv_usage.
func TestRecordKVUsageNormalizesAndRejects(t *testing.T) {
	q := newSoftDivertQueue()

	q.RecordKVUsage("ep", 85.0) // percent -> 0.85
	q.mu.Lock()
	got := q.kvUsageByEndpoint["ep"].value
	q.mu.Unlock()
	if got != 0.85 {
		t.Fatalf("percent normalize: want 0.85, got %v", got)
	}

	q.RecordKVUsage("ep", 500.0) // >100 clamps to 1.0
	q.mu.Lock()
	got = q.kvUsageByEndpoint["ep"].value
	q.mu.Unlock()
	if got != 1.0 {
		t.Fatalf(">100 clamp: want 1.0, got %v", got)
	}

	q.RecordKVUsage("ep", -0.5) // rejected -> unchanged
	q.mu.Lock()
	got = q.kvUsageByEndpoint["ep"].value
	q.mu.Unlock()
	if got != 1.0 {
		t.Fatalf("negative should be rejected, value changed to %v", got)
	}
}
