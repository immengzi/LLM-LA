package gateway

import (
	"sync"
	"time"
)

// ResultStore provides a channel-based result delivery mechanism for
// correlating async request/response flows. It mirrors the Python
// RouterState result waiter logic with asyncio Futures.
type ResultStore struct {
	mu       sync.Mutex
	waiters  map[string]chan map[string]interface{}
	results  map[string]map[string]interface{}
	storeTSs map[string]time.Time
}

func NewResultStore() *ResultStore {
	return &ResultStore{
		waiters:  make(map[string]chan map[string]interface{}),
		results:  make(map[string]map[string]interface{}),
		storeTSs: make(map[string]time.Time),
	}
}

// Register creates a buffered channel for the given request ID and
// returns it. If a result already arrived before registration, the
// channel is pre-filled.
func (rs *ResultStore) Register(reqID string) <-chan map[string]interface{} {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	ch := make(chan map[string]interface{}, 1)
	rs.waiters[reqID] = ch

	if result, ok := rs.results[reqID]; ok {
		ch <- result
		delete(rs.results, reqID)
		delete(rs.storeTSs, reqID)
	}
	return ch
}

// Deliver sends a result to the waiter for the given request ID.
// If no waiter exists yet, the result is buffered for later retrieval.
func (rs *ResultStore) Deliver(reqID string, result map[string]interface{}) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if ch, ok := rs.waiters[reqID]; ok {
		select {
		case ch <- result:
		default:
		}
		return
	}

	rs.results[reqID] = result
	rs.storeTSs[reqID] = time.Now()
}

// WaitFor blocks until a result arrives for reqID or until the timeout
// elapses. Returns nil on timeout.
func (rs *ResultStore) WaitFor(reqID string, timeout time.Duration) map[string]interface{} {
	rs.mu.Lock()
	ch, ok := rs.waiters[reqID]
	rs.mu.Unlock()

	if !ok {
		return nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result := <-ch:
		rs.cleanup(reqID)
		return result
	case <-timer.C:
		rs.cleanup(reqID)
		return nil
	}
}

// cleanup removes the waiter and any buffered result for reqID.
func (rs *ResultStore) cleanup(reqID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	delete(rs.waiters, reqID)
	delete(rs.results, reqID)
	delete(rs.storeTSs, reqID)
}

// StartCleanupLoop periodically removes stale results that were never
// collected. Prevents unbounded memory growth from abandoned requests.
func (rs *ResultStore) StartCleanupLoop(ttl time.Duration, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			rs.mu.Lock()
			now := time.Now()
			for rid, ts := range rs.storeTSs {
				if now.Sub(ts) >= ttl {
					delete(rs.results, rid)
					delete(rs.storeTSs, rid)
					if ch, ok := rs.waiters[rid]; ok {
						close(ch)
						delete(rs.waiters, rid)
					}
				}
			}
			rs.mu.Unlock()
		}
	}()
}
