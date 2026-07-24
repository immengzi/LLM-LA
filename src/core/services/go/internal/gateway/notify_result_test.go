package gateway

import "testing"

// Go mirror of the Python router tests for issue #12. Verifies (1) the
// PushDispatcher in-flight accounting used by push-leastq/local and (2) that
// ingestResultPayload recovers the completing pod's identity from the result
// payload so NotifyResult actually fires — the decrement that stops
// push-leastq from degenerating into round-robin.

func newLeastQDispatcher(endpoints ...string) *PushDispatcher {
	pd := NewPushDispatcher(&Config{}, newKVAware())
	pd.endpoints = append([]string{}, endpoints...)
	pd.urls = make(map[string]string, len(endpoints))
	for _, ep := range endpoints {
		pd.urls[ep] = "http://" + ep + ":8000"
	}
	return pd
}

// ---------------------------------------------------------------------------
// PushDispatcher accounting
// ---------------------------------------------------------------------------

func TestNotifyResultDecrements(t *testing.T) {
	pd := newLeastQDispatcher("A", "B")
	pd.logicalInflight["A"] = 3
	pd.NotifyResult("A")
	if got := pd.logicalInflight["A"]; got != 2 {
		t.Fatalf("logicalInflight[A] = %d, want 2", got)
	}
}

func TestNotifyResultFloorsAtZero(t *testing.T) {
	pd := newLeastQDispatcher("A")
	pd.NotifyResult("A") // already 0
	if got := pd.logicalInflight["A"]; got != 0 {
		t.Fatalf("logicalInflight[A] = %d, want 0 (no negative)", got)
	}
}

func TestNotifyResultIgnoresUnknownAndEmpty(t *testing.T) {
	pd := newLeastQDispatcher("A")
	pd.logicalInflight["A"] = 1
	pd.NotifyResult("ghost")
	pd.NotifyResult("")
	if got := pd.logicalInflight["A"]; got != 1 {
		t.Fatalf("logicalInflight[A] = %d, want 1 (untouched)", got)
	}
}

func TestPickLeastQLocalPicksLeastLoaded(t *testing.T) {
	pd := newLeastQDispatcher("A", "B", "C")
	pd.logicalInflight["A"] = 5
	pd.logicalInflight["B"] = 1
	pd.logicalInflight["C"] = 9
	if got := pd.pickLeastQLocal(); got != "B" {
		t.Fatalf("pickLeastQLocal() = %q, want B", got)
	}
}

func TestLeastQSteersTowardFastWorker(t *testing.T) {
	pd := newLeastQDispatcher("fast", "slow1", "slow2")
	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		ep := pd.pickLeastQLocal()
		pd.logicalInflight[ep]++ // dispatch increment
		counts[ep]++
		if ep == "fast" {
			pd.NotifyResult("fast") // fast pod completes immediately
		}
	}
	if counts["fast"] <= counts["slow1"]+counts["slow2"] {
		t.Fatalf("fast=%d slow1=%d slow2=%d: least-queue did not steer to fast",
			counts["fast"], counts["slow1"], counts["slow2"])
	}
}

func TestWithoutNotifyResultDegeneratesToRoundRobin(t *testing.T) {
	pd := newLeastQDispatcher("fast", "slow1", "slow2")
	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		ep := pd.pickLeastQLocal()
		pd.logicalInflight[ep]++ // dispatch increment, but never NotifyResult
		counts[ep]++
	}
	// The regression from issue #12: uniform 1/N split regardless of speed.
	if counts["fast"] != 100 || counts["slow1"] != 100 || counts["slow2"] != 100 {
		t.Fatalf("counts = %v, want uniform 100 each (round-robin)", counts)
	}
}

// ---------------------------------------------------------------------------
// Gateway ingestion contract: endpoint resolution -> NotifyResult
// ---------------------------------------------------------------------------

func newIngestServer(pd *PushDispatcher) *Server {
	cfg := &Config{}
	return &Server{
		cfg:        cfg,
		queue:      NewCentralQueue(cfg, newKVAware()),
		results:    NewResultStore(),
		pushRouter: pd,
	}
}

func TestIngestTopLevelEndpointTriggersCompletion(t *testing.T) {
	pd := newLeastQDispatcher("pod-a")
	pd.logicalInflight["pod-a"] = 2
	s := newIngestServer(pd)

	s.ingestResultPayload(map[string]interface{}{
		"req_id":   "r1",
		"endpoint": "pod-a",
		"result":   map[string]interface{}{"output": "ok"},
	})
	if got := pd.logicalInflight["pod-a"]; got != 1 {
		t.Fatalf("logicalInflight[pod-a] = %d, want 1", got)
	}
}

func TestIngestErrorPayloadWithEndpointTriggersCompletion(t *testing.T) {
	pd := newLeastQDispatcher("pod-a")
	pd.logicalInflight["pod-a"] = 1
	s := newIngestServer(pd)

	// Mirrors the sidecar error path after the fix (top-level endpoint present).
	s.ingestResultPayload(map[string]interface{}{
		"req_id":   "r-err",
		"endpoint": "pod-a",
		"result": map[string]interface{}{
			"output":        "[sidecar error: boom]",
			"finish_reason": "error",
		},
	})
	if got := pd.logicalInflight["pod-a"]; got != 0 {
		t.Fatalf("logicalInflight[pod-a] = %d, want 0 (error released slot)", got)
	}
}

func TestIngestEndpointIDFallback(t *testing.T) {
	pd := newLeastQDispatcher("pod-b")
	pd.logicalInflight["pod-b"] = 2
	s := newIngestServer(pd)

	s.ingestResultPayload(map[string]interface{}{
		"req_id": "r2",
		"result": map[string]interface{}{"output": "ok", "endpoint_id": "pod-b"},
	})
	if got := pd.logicalInflight["pod-b"]; got != 1 {
		t.Fatalf("logicalInflight[pod-b] = %d, want 1 (endpoint_id fallback)", got)
	}
}

func TestIngestTopLevelEndpointTakesPrecedence(t *testing.T) {
	pd := newLeastQDispatcher("pod-top", "pod-nested")
	pd.logicalInflight["pod-top"] = 2
	pd.logicalInflight["pod-nested"] = 2
	s := newIngestServer(pd)

	s.ingestResultPayload(map[string]interface{}{
		"req_id":   "r3",
		"endpoint": "pod-top",
		"result":   map[string]interface{}{"output": "ok", "endpoint_id": "pod-nested"},
	})
	if pd.logicalInflight["pod-top"] != 1 || pd.logicalInflight["pod-nested"] != 2 {
		t.Fatalf("top=%d nested=%d, want top=1 nested=2 (top-level wins)",
			pd.logicalInflight["pod-top"], pd.logicalInflight["pod-nested"])
	}
}

func TestIngestMissingEndpointSignalsNoCompletion(t *testing.T) {
	pd := newLeastQDispatcher("pod-a")
	pd.logicalInflight["pod-a"] = 2
	s := newIngestServer(pd)

	// Pre-fix error path: no top-level endpoint, no endpoint_id.
	s.ingestResultPayload(map[string]interface{}{
		"req_id": "r4",
		"result": map[string]interface{}{"output": "[sidecar error: boom]"},
	})
	if got := pd.logicalInflight["pod-a"]; got != 2 {
		t.Fatalf("logicalInflight[pod-a] = %d, want 2 (no completion signalled)", got)
	}
}

func TestIngestMissingReqIDIsIgnored(t *testing.T) {
	pd := newLeastQDispatcher("pod-a")
	pd.logicalInflight["pod-a"] = 2
	s := newIngestServer(pd)

	s.ingestResultPayload(map[string]interface{}{
		"endpoint": "pod-a",
		"result":   map[string]interface{}{"output": "ok"},
	})
	if got := pd.logicalInflight["pod-a"]; got != 2 {
		t.Fatalf("logicalInflight[pod-a] = %d, want 2 (missing req_id ignored)", got)
	}
}
