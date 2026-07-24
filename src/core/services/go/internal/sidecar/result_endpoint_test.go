package sidecar

import (
	"errors"
	"testing"
)

// Guards the issue #12 fix at the point of emission: the sidecar must stamp the
// top-level "endpoint" (its container name) onto result payloads so the gateway
// can decrement its per-endpoint in-flight counter. The error path is the one
// still unguarded upstream, so we assert it here directly.
//
// NewResultPoster is used without Start(), so Submit() just buffers the payload
// on the internal channel and we can read it back without any HTTP round-trip.

func TestSubmitErrorIncludesEndpoint(t *testing.T) {
	cfg := &Config{ContainerName: "pod-xyz"}
	poster := NewResultPoster(cfg)
	w := &VLLMWorker{cfg: cfg, poster: poster}

	w.submitError("req-1", errors.New("boom"))

	select {
	case payload := <-poster.ch:
		if payload["req_id"] != "req-1" {
			t.Fatalf("req_id = %v, want req-1", payload["req_id"])
		}
		if payload["endpoint"] != "pod-xyz" {
			t.Fatalf("endpoint = %v, want pod-xyz (container name)", payload["endpoint"])
		}
		if _, ok := payload["result"].(map[string]any); !ok {
			t.Fatalf("result payload missing or wrong type: %v", payload["result"])
		}
	default:
		t.Fatal("submitError did not enqueue a result payload")
	}
}
