package sidecar

import "testing"

// setCachedKV binds a monitor with a fixed cached value (ok=true) for tests.
func setCachedKV(v float64) {
	m := &KvUsageMonitor{value: v, ok: true}
	BindKvUsageMonitor(m)
}

// TestApplyKvPullGate covers the KV-memory pull gate decision logic (P1):
// disabled -> passthrough, no sample -> fail open, and stop/taper/passthrough
// across the [LOW, HIGH] window.
func TestApplyKvPullGate(t *testing.T) {
	defer BindKvUsageMonitor(nil)

	cfg := &Config{KVPullGateEnabled: true, KVPullGateHigh: 0.90, KVPullGateLow: 0.70}
	w := &RouterPullWorker{cfg: cfg, endpointID: "ep"}

	// Disabled -> passthrough even at high kv.
	setCachedKV(0.99)
	w.cfg.KVPullGateEnabled = false
	if got := w.applyKvPullGate(8); got != 8 {
		t.Fatalf("disabled = %d, want 8", got)
	}
	w.cfg.KVPullGateEnabled = true

	// No sample -> fail open.
	BindKvUsageMonitor(&KvUsageMonitor{ok: false})
	if got := w.applyKvPullGate(8); got != 8 {
		t.Fatalf("no-sample = %d, want 8", got)
	}

	// Below LOW -> passthrough.
	setCachedKV(0.50)
	if got := w.applyKvPullGate(8); got != 8 {
		t.Fatalf("below-low = %d, want 8", got)
	}

	// At / above HIGH -> blocked.
	setCachedKV(0.90)
	if got := w.applyKvPullGate(8); got != 0 {
		t.Fatalf("at-high = %d, want 0", got)
	}
	setCachedKV(0.97)
	if got := w.applyKvPullGate(8); got != 0 {
		t.Fatalf("above-high = %d, want 0", got)
	}

	// Midpoint taper: kv=0.80 -> scale ~0.5 -> ~half of 8 (int truncation: 3 or 4).
	setCachedKV(0.80)
	if got := w.applyKvPullGate(8); got < 3 || got > 4 {
		t.Fatalf("midpoint = %d, want 3 or 4", got)
	}

	// Taper is monotonic non-increasing across the window, ending at 0.
	last := 100
	for _, kv := range []float64{0.70, 0.75, 0.80, 0.85, 0.89, 0.90} {
		setCachedKV(kv)
		got := w.applyKvPullGate(16)
		if got > last {
			t.Fatalf("taper not monotonic at kv=%.2f: %d > %d", kv, got, last)
		}
		last = got
	}
	if last != 0 {
		t.Fatalf("taper end = %d, want 0", last)
	}
}

// TestKvPullGateWindowNormalize verifies the config clamps the gate window to
// 0 <= LOW <= HIGH <= 1.
func TestKvPullGateWindowNormalize(t *testing.T) {
	c := &Config{KVPullGateHigh: 1.5, KVPullGateLow: -0.2}
	c.normalize()
	if c.KVPullGateHigh != 1.0 {
		t.Fatalf("HIGH = %v, want 1.0", c.KVPullGateHigh)
	}
	if c.KVPullGateLow != 0.0 {
		t.Fatalf("LOW = %v, want 0.0", c.KVPullGateLow)
	}

	c2 := &Config{KVPullGateHigh: 0.5, KVPullGateLow: 0.8}
	c2.normalize()
	if c2.KVPullGateLow > c2.KVPullGateHigh {
		t.Fatalf("LOW %v > HIGH %v after normalize", c2.KVPullGateLow, c2.KVPullGateHigh)
	}
}
