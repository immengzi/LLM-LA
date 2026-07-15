package gateway

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// In-memory fake Redis backend (duck-typed subset used by RedisAffinityStore),
// mirroring the FakeRedis in tests/test_affinity_persist.py.
// ---------------------------------------------------------------------------
type fakeVal struct {
	v   string
	exp time.Time // zero => no expiry
}

type fakeAffinityRedis struct {
	mu    sync.Mutex
	store map[string]fakeVal
}

func newFakeAffinityRedis() *fakeAffinityRedis {
	return &fakeAffinityRedis{store: make(map[string]fakeVal)}
}

func (f *fakeAffinityRedis) aliveLocked(k string) (string, bool) {
	item, ok := f.store[k]
	if !ok {
		return "", false
	}
	if !item.exp.IsZero() && time.Now().After(item.exp) {
		delete(f.store, k)
		return "", false
	}
	return item.v, true
}

func (f *fakeAffinityRedis) get(k string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.aliveLocked(k)
}

func (f *fakeAffinityRedis) setBatch(kvs map[string]string, ttl time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	for k, v := range kvs {
		f.store[k] = fakeVal{v: v, exp: exp}
	}
}

func (f *fakeAffinityRedis) scanAll(prefix string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for k := range f.store {
		if v, ok := f.aliveLocked(k); ok && len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out[k] = v
		}
	}
	return out
}

func (f *fakeAffinityRedis) has(k string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.store[k]
	return ok
}

func (f *fakeAffinityRedis) close() {}

func waitUntil(pred func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return pred()
}

func makeStore(backend *fakeAffinityRedis, ttlSeconds int) *RedisAffinityStore {
	return NewRedisAffinityStore(backend, "affinity:test:model-x", ttlSeconds, 100000, 256)
}

// ---------------------------------------------------------------------------
// Store + AffinityMap unit tests
// ---------------------------------------------------------------------------

func TestAffinityWriteThroughPersistsToRedis(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	am := NewAffinityMapWithStore(300.0, store, 0)
	if !am.Persistent() {
		t.Fatal("map should report persistent")
	}
	am.Claim("conv1", "pod-a")
	if am.Lookup("conv1") != "pod-a" { // in-memory is immediate
		t.Fatal("in-memory claim should be immediate")
	}
	if !waitUntil(func() bool { v, ok := store.Get("conv1"); return ok && v == "pod-a" }, 2*time.Second) {
		t.Fatal("write-through to Redis did not land")
	}
	if !backend.has("affinity:test:model-x:conv1") {
		t.Fatal("namespaced raw key missing in backing")
	}
}

func TestAffinityLookupInMemoryOnly(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	am := NewAffinityMapWithStore(300.0, store, 0)
	am.Claim("c", "pod-a")
	// Corrupt the store; a hot-path lookup must NOT consult Redis.
	backend.mu.Lock()
	backend.store = map[string]fakeVal{}
	backend.mu.Unlock()
	if am.Lookup("c") != "pod-a" {
		t.Fatal("hot-path lookup must be in-memory only")
	}
}

func TestAffinityPrefetchLoadsFromRedisOnMiss(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	// Seed Redis directly, as if written by a previous router instance.
	backend.setBatch(map[string]string{"affinity:test:model-x:c": "pod-z"}, 0)
	am := NewAffinityMapWithStore(300.0, store, 0)
	if am.Lookup("c") != "" {
		t.Fatal("should not be in memory yet")
	}
	if am.Prefetch("c") != "pod-z" {
		t.Fatal("prefetch should GET from Redis")
	}
	if am.Lookup("c") != "pod-z" {
		t.Fatal("prefetch should populate memory")
	}
}

func TestAffinityWarmReloadsMapAtStartup(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	backend.setBatch(map[string]string{
		"affinity:test:model-x:a": "pod-1",
		"affinity:test:model-x:b": "pod-2",
		"other:namespace:c":       "pod-3", // different ns -> ignored
	}, 0)
	am := NewAffinityMapWithStore(300.0, store, 0)
	if n := am.Warm(); n != 2 {
		t.Fatalf("warm loaded %d, want 2", n)
	}
	if am.Lookup("a") != "pod-1" || am.Lookup("b") != "pod-2" || am.Lookup("c") != "" {
		t.Fatal("warm loaded wrong mappings")
	}
}

func TestAffinityRedeployReloadFromSameBacking(t *testing.T) {
	backend := newFakeAffinityRedis()
	store1 := makeStore(backend, 0)
	am1 := NewAffinityMapWithStore(300.0, store1, 0)
	am1.Claim("conv", "pod-old")
	if !waitUntil(func() bool { v, ok := store1.Get("conv"); return ok && v == "pod-old" }, 2*time.Second) {
		t.Fatal("first store did not persist")
	}
	store1.Close()

	// Fresh store over the SAME durable backing (redeploy).
	store2 := makeStore(backend, 0)
	defer store2.Close()
	am2 := NewAffinityMapWithStore(300.0, store2, 0)
	if n := am2.Warm(); n != 1 {
		t.Fatalf("redeploy warm loaded %d, want 1", n)
	}
	if am2.Lookup("conv") != "pod-old" {
		t.Fatal("redeploy lost the mapping")
	}
}

func TestAffinityRedisTTLExpiryAndNoExpiry(t *testing.T) {
	// ttl>0: key expires from Redis after the window.
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 1)
	am := NewAffinityMapWithStore(300.0, store, 0)
	am.Claim("k", "pod-a")
	if !waitUntil(func() bool { v, ok := store.Get("k"); return ok && v == "pod-a" }, 2*time.Second) {
		t.Fatal("ttl key not written")
	}
	if !waitUntil(func() bool { _, ok := store.Get("k"); return !ok }, 3*time.Second) {
		t.Fatal("ttl key should have expired")
	}
	store.Close()

	// ttl==0: key persists (no expiry).
	backend0 := newFakeAffinityRedis()
	store0 := makeStore(backend0, 0)
	defer store0.Close()
	am0 := NewAffinityMapWithStore(300.0, store0, 0)
	am0.Claim("k", "pod-a")
	if !waitUntil(func() bool { v, ok := store0.Get("k"); return ok && v == "pod-a" }, 2*time.Second) {
		t.Fatal("no-ttl key not written")
	}
	time.Sleep(300 * time.Millisecond)
	if v, ok := store0.Get("k"); !ok || v != "pod-a" {
		t.Fatal("no-ttl key should persist")
	}
}

func TestAffinityCacheBoundEvictionKeepsDurableCopy(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	am := NewAffinityMapWithStore(300.0, store, 2)
	am.Claim("a", "pod-a")
	time.Sleep(5 * time.Millisecond)
	am.Claim("b", "pod-b")
	time.Sleep(5 * time.Millisecond)
	am.Claim("c", "pod-c") // exceeds bound -> oldest ("a") evicted from memory
	if am.Size() != 2 {
		t.Fatalf("cache bound: size = %d, want 2", am.Size())
	}
	if am.Lookup("a") != "" {
		t.Fatal("oldest key should be evicted from memory")
	}
	if !waitUntil(func() bool { v, ok := store.Get("a"); return ok && v == "pod-a" }, 2*time.Second) {
		t.Fatal("evicted key must remain durable in Redis")
	}
	if am.Prefetch("a") != "pod-a" {
		t.Fatal("evicted key should be recoverable via prefetch")
	}
}

func TestAffinityConcurrentWriteThroughSameKey(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	am := NewAffinityMapWithStore(300.0, store, 0)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ep := fmt.Sprintf("pod-%d", id)
			for j := 0; j < 50; j++ {
				am.Claim("hot", ep)
			}
		}(i)
	}
	wg.Wait()
	final := am.Lookup("hot")
	valid := map[string]bool{"pod-0": true, "pod-1": true, "pod-2": true, "pod-3": true}
	if !valid[final] {
		t.Fatalf("final winner %q not one of the writers", final)
	}
	if !waitUntil(func() bool { v, ok := store.Get("hot"); return ok && v == final }, 2*time.Second) {
		t.Fatal("Redis should converge to the in-memory winner")
	}
}

func TestAffinityNoStoreIsLegacy(t *testing.T) {
	am := NewAffinityMap(300.0) // no store
	if am.Persistent() {
		t.Fatal("map without store must not be persistent")
	}
	am.Claim("c", "pod-a")
	if am.Lookup("c") != "pod-a" {
		t.Fatal("legacy claim/lookup broken")
	}
	if am.Prefetch("c") != "pod-a" { // prefetch mirrors lookup when no store
		t.Fatal("prefetch should mirror lookup with no store")
	}
	if am.Prefetch("missing") != "" {
		t.Fatal("prefetch of missing key should be empty")
	}
	if am.Warm() != 0 {
		t.Fatal("warm with no store should be 0")
	}
}

// ---------------------------------------------------------------------------
// CentralQueue dispatch-flow integration (mirrors the RouterState tests)
// ---------------------------------------------------------------------------

func newPersistQueue(store affinityStore) *CentralQueue {
	cfg := LoadConfig()
	cfg.KVAware = false
	cfg.LenAware = false
	cfg.AffinityEnabled = true
	cfg.AffinityMode = "hard"
	cfg.AffinityHardTimeoutS = 100
	q := NewCentralQueue(cfg, newKVAware())
	q.affinity = NewAffinityMapWithStore(300.0, store, 0)
	q.affinityPersist = true
	q.seenEndpoints = map[string]float64{}
	return q
}

func affItem(rid, key string) queueItem {
	ts := nowS()
	return queueItem{
		reqID:  rid,
		prompt: "prompt-" + rid,
		tEnq:   ts,
		meta:   map[string]interface{}{"__affinity_key__": key, "__affinity_ts__": ts},
	}
}

func TestHardFilterHoldsWhenPinnedToAvailableOtherPod(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	q := newPersistQueue(store)
	q.affinity.Claim("c", "pod-b")
	q.seenEndpoints["pod-b"] = nowS() // pod-b is alive
	avail, held := q.affinityFilterHard([]queueItem{affItem("r1", "c")}, "pod-a")
	if len(avail) != 0 {
		t.Fatalf("item should be withheld for pod-b, got available=%v", idsOfQ(avail))
	}
	if len(held) != 1 || held[0].reqID != "r1" {
		t.Fatalf("expected r1 held, got %v", idsOfQ(held))
	}
}

func TestHardFilterFallsBackWhenPinnedPodUnavailable(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	q := newPersistQueue(store)
	q.affinity.Claim("c", "pod-old") // renamed/scaled-down pod
	q.seenEndpoints["pod-new"] = nowS()
	avail, held := q.affinityFilterHard([]queueItem{affItem("r1", "c")}, "pod-new")
	if len(avail) != 1 || avail[0].reqID != "r1" {
		t.Fatalf("should fall back to LB, got available=%v", idsOfQ(avail))
	}
	if len(held) != 0 {
		t.Fatalf("nothing should be held, got %v", idsOfQ(held))
	}
}

func TestDispatchWritebackPersistsNewPod(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	q := newPersistQueue(store)
	q.affinityRecordDispatch([]queueItem{affItem("r1", "c")}, "pod-new")
	if q.affinity.Lookup("c") != "pod-new" {
		t.Fatal("dispatch should claim the new pod")
	}
	if !waitUntil(func() bool { v, ok := store.Get("c"); return ok && v == "pod-new" }, 2*time.Second) {
		t.Fatal("dispatch write-back should persist")
	}
}

func TestEndpointAvailableGatingOffWhenNotPersisted(t *testing.T) {
	cfg := LoadConfig()
	q := NewCentralQueue(cfg, newKVAware())
	q.affinityPersist = false
	if !q.endpointAvailableLocked("anything") {
		t.Fatal("with persistence off, availability must always be true")
	}
}

func TestEndpointAvailableRespectsStaleWindow(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	q := newPersistQueue(store)
	q.seenEndpoints["pod-fresh"] = nowS()
	q.seenEndpoints["pod-stale"] = nowS() - 10000.0
	if !q.endpointAvailableLocked("pod-fresh") {
		t.Fatal("fresh pod should be available")
	}
	if q.endpointAvailableLocked("pod-stale") {
		t.Fatal("stale pod should be unavailable")
	}
	if q.endpointAvailableLocked("pod-never") {
		t.Fatal("never-seen pod should be unavailable")
	}
}

// TestColdStartContinuityHonoredAfterFirstPull mirrors
// test_cold_start_continuity_honored_after_first_pull: a warmed cross-pod
// mapping is honored only after the target pod's first (health-gated) pull.
func TestColdStartContinuityHonoredAfterFirstPull(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	q := newPersistQueue(store)
	q.affinity.Claim("c", "podB") // warmed mapping

	avail, held := q.affinityFilterHard([]queueItem{affItem("r1", "c")}, "podA")
	if len(avail) != 1 || len(held) != 0 {
		t.Fatal("before podB pulls, pin must fall back (not held)")
	}
	if q.endpointAvailableLocked("podB") {
		t.Fatal("podB unavailable until it pulls")
	}

	// podB becomes ready and issues its first (health-gated) pull; the pull
	// stamps seenEndpoints even though the queue is empty.
	q.Pull("podB", 1, "m")
	if !q.endpointAvailableLocked("podB") {
		t.Fatal("podB should be available after its first pull")
	}

	avail2, held2 := q.affinityFilterHard([]queueItem{affItem("r2", "c")}, "podA")
	if len(avail2) != 0 || len(held2) != 1 || held2[0].reqID != "r2" {
		t.Fatal("after podB pulls, pin must be honored (held)")
	}
}

func TestNeverReadyPodIsNeverHonored(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	q := newPersistQueue(store)
	q.affinity.Claim("c", "ghost") // ghost never pulls
	q.Pull("podA", 1, "m")         // a different, live pod pulls
	if q.endpointAvailableLocked("ghost") {
		t.Fatal("ghost pod should never be available")
	}
	avail, held := q.affinityFilterHard([]queueItem{affItem("r1", "c")}, "podA")
	if len(avail) != 1 || len(held) != 0 {
		t.Fatal("mapping to ghost must fall back to LB")
	}
}

func TestScaleDownAgesOutPastStale(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	q := newPersistQueue(store)
	stale := q.cfg.AffinityEndpointStaleS
	q.affinity.Claim("c", "podB")

	q.seenEndpoints["podB"] = nowS() - (stale + 10.0)
	if q.endpointAvailableLocked("podB") {
		t.Fatal("aged-out pod should be unavailable")
	}
	avail, held := q.affinityFilterHard([]queueItem{affItem("r1", "c")}, "podA")
	if len(avail) != 1 || len(held) != 0 {
		t.Fatal("aged-out pin must fall back")
	}

	q.seenEndpoints["podB"] = nowS() // fresh pull re-validates
	if !q.endpointAvailableLocked("podB") {
		t.Fatal("fresh pull should re-validate availability")
	}
}

func TestFeatureOffHardFilterIsLegacy(t *testing.T) {
	backend := newFakeAffinityRedis()
	store := makeStore(backend, 0)
	defer store.Close()
	q := newPersistQueue(store)
	q.affinity.Claim("c", "podB") // podB never seen pulling
	q.affinityPersist = false     // persist OFF -> legacy path
	if !q.endpointAvailableLocked("anything") {
		t.Fatal("persist off -> always available")
	}
	avail, held := q.affinityFilterHard([]queueItem{affItem("r1", "c")}, "podA")
	if len(avail) != 0 || len(held) != 1 || held[0].reqID != "r1" {
		t.Fatal("legacy hard filter should hold the pin for podB")
	}
}

func idsOfQ(items []queueItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.reqID
	}
	return out
}
