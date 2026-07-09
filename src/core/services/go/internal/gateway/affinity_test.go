package gateway

import (
	"testing"
	"time"
)

// TestDeriveAffinityKeyStableAcrossTurns verifies the key derived from a
// conversation's opening prefix is identical on later turns (which resend the
// full history) and that content-block arrays match plain-string content.
func TestDeriveAffinityKeyStableAcrossTurns(t *testing.T) {
	turn1 := []interface{}{
		map[string]interface{}{"role": "system", "content": "You are helpful"},
		map[string]interface{}{"role": "user", "content": "hi"},
		map[string]interface{}{"role": "assistant", "content": "hello"},
	}
	turn2 := append(append([]interface{}{}, turn1...),
		map[string]interface{}{"role": "user", "content": "second turn"},
	)

	k1 := deriveAffinityKey("m", turn1)
	k2 := deriveAffinityKey("m", turn2)
	if k1 == "" || k1 != k2 {
		t.Fatalf("key must be stable across turns: %q vs %q", k1, k2)
	}

	// content-block array form must hash identically to the string form.
	blockForm := []interface{}{
		map[string]interface{}{"role": "user", "content": []interface{}{
			map[string]interface{}{"type": "text", "text": "hi"},
		}},
	}
	stringForm := []interface{}{
		map[string]interface{}{"role": "user", "content": "hi"},
	}
	if deriveAffinityKey("m", blockForm) != deriveAffinityKey("m", stringForm) {
		t.Fatal("content-block and string content must derive the same key")
	}

	// No user message -> no key.
	noUser := []interface{}{map[string]interface{}{"role": "system", "content": "x"}}
	if deriveAffinityKey("m", noUser) != "" {
		t.Fatal("expected empty key when no user message present")
	}

	// Different model -> different key.
	if deriveAffinityKey("m1", turn1) == deriveAffinityKey("m2", turn1) {
		t.Fatal("model should be part of the key")
	}
}

// TestAffinityMapTTLAndClaim verifies claim/lookup and TTL expiry.
func TestAffinityMapTTLAndClaim(t *testing.T) {
	am := NewAffinityMap(0.05)
	if am.Lookup("k") != "" {
		t.Fatal("empty map should miss")
	}
	am.Claim("k", "ep-a")
	if am.Lookup("k") != "ep-a" {
		t.Fatalf("expected ep-a, got %q", am.Lookup("k"))
	}
	if am.Size() != 1 {
		t.Fatalf("expected size 1, got %d", am.Size())
	}
	time.Sleep(70 * time.Millisecond)
	if am.Lookup("k") != "" {
		t.Fatal("entry should have expired")
	}
}

func newTestQueue(mode string) *CentralQueue {
	cfg := LoadConfig()
	cfg.KVAware = false
	cfg.LenAware = false
	cfg.AffinityEnabled = true
	cfg.AffinityMode = mode
	cfg.AffinityHardTimeoutS = 100
	return NewCentralQueue(cfg, newKVAware())
}

// TestHardAffinityHoldsForOtherEndpoint verifies hard mode withholds a pinned
// conversation from a non-matching endpoint and serves it on the right one.
func TestHardAffinityHoldsForOtherEndpoint(t *testing.T) {
	q := newTestQueue("hard")

	metaA := map[string]interface{}{"__affinity_key__": "A", "__affinity_ts__": nowS()}
	q.Enqueue("pa", nowS(), cloneMeta(metaA), "", "m")

	// First pull on ep-a claims A -> ep-a.
	got := q.Pull("ep-a", 5, "m")
	if len(got) != 1 {
		t.Fatalf("first pull expected 1 item, got %d", len(got))
	}
	if q.affinity.Lookup("A") != "ep-a" {
		t.Fatalf("A should be pinned to ep-a, got %q", q.affinity.Lookup("A"))
	}

	// New A turn + a fresh B turn.
	raID := q.Enqueue("pa2", nowS(), cloneMeta(metaA), "", "m")
	metaB := map[string]interface{}{"__affinity_key__": "B", "__affinity_ts__": nowS()}
	rbID := q.Enqueue("pb", nowS(), cloneMeta(metaB), "", "m")

	// ep-b pulls: must get B, must NOT get the A turn (held).
	gotB := q.Pull("ep-b", 5, "m")
	ids := idsOf(gotB)
	if !contains(ids, rbID) {
		t.Fatalf("ep-b should serve B turn %s, got %v", rbID, ids)
	}
	if contains(ids, raID) {
		t.Fatalf("ep-b must not serve held A turn %s, got %v", raID, ids)
	}

	// ep-a pulls: gets the held A turn.
	gotA := q.Pull("ep-a", 5, "m")
	if !contains(idsOf(gotA), raID) {
		t.Fatalf("ep-a should serve held A turn %s, got %v", raID, idsOf(gotA))
	}
}

// TestSoftAffinityPrefersMatchingEndpoint verifies soft mode reorders a matched
// conversation ahead of others but still allows any endpoint to serve.
func TestSoftAffinityPrefersMatchingEndpoint(t *testing.T) {
	q := newTestQueue("soft")
	q.affinity.Claim("A", "ep-a")

	metaA := map[string]interface{}{"__affinity_key__": "A"}
	metaO := map[string]interface{}{"__affinity_key__": "Z"}
	// Use req IDs where the matched item ("z9") sorts AFTER the non-matched one
	// ("a0") by req_id, so only affinity reordering can put it first.
	q.Enqueue("po", nowS(), cloneMeta(metaO), "a0", "m")
	q.Enqueue("pa", nowS(), cloneMeta(metaA), "z9", "m")

	got := q.Pull("ep-a", 1, "m")
	if len(got) != 1 || got[0].ReqID != "z9" {
		t.Fatalf("soft mode should serve matched item z9 first, got %v", idsOf(got))
	}
}

func idsOf(items []JobItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.ReqID
	}
	return out
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
