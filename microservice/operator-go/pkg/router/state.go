package router

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/vllmkv/operator/pkg/models"
)

// State is the router's central queue and result store.
type State struct {
	mu             sync.Mutex
	queue          []*models.Request
	pendingChans   map[string]chan *models.Result // /enqueue sync waiters
	resultStore    map[string]*models.Result      // polled results
	resultStoredAt map[string]time.Time
	requestBlocks  map[string][]int64            // req_id -> block hashes (KV)
	blockOwners    map[int64]map[string]bool     // block_hash -> set{endpoint}
}

func NewState() *State {
	return &State{
		pendingChans:   make(map[string]chan *models.Result),
		resultStore:    make(map[string]*models.Result),
		resultStoredAt: make(map[string]time.Time),
		requestBlocks:  make(map[string][]int64),
		blockOwners:    make(map[int64]map[string]bool),
	}
}

func (s *State) QueueLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

func (s *State) Enqueue(req *models.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, req)
}

// DequeueUpTo pops up to n items from the front of the queue.
func (s *State) DequeueUpTo(n int) []*models.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n > len(s.queue) {
		n = len(s.queue)
	}
	if n == 0 {
		return nil
	}
	batch := make([]*models.Request, n)
	copy(batch, s.queue[:n])
	s.queue = s.queue[n:]
	return batch
}

// ReturnToFront pushes items back to the front of the queue (unused pull pool).
func (s *State) ReturnToFront(items []*models.Request) {
	if len(items) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(items, s.queue...)
}

// WaitForResult blocks until a result arrives or timeout.
func (s *State) WaitForResult(reqID string, timeout time.Duration) (*models.Result, bool) {
	ch := make(chan *models.Result, 1)
	s.mu.Lock()
	s.pendingChans[reqID] = ch
	s.mu.Unlock()

	select {
	case r := <-ch:
		return r, true
	case <-time.After(timeout):
		s.mu.Lock()
		delete(s.pendingChans, reqID)
		s.mu.Unlock()
		return nil, false
	}
}

// SetResult delivers a result to a sync waiter or stores it.
func (s *State) SetResult(reqID string, result *models.Result) {
	s.mu.Lock()
	ch, waiting := s.pendingChans[reqID]
	if waiting {
		delete(s.pendingChans, reqID)
	}
	s.resultStore[reqID] = result
	s.resultStoredAt[reqID] = time.Now()
	s.mu.Unlock()

	if waiting {
		ch <- result
	}
}

// RegisterRequestBlocks stores prefix block hashes for a request (KV-aware).
func (s *State) RegisterRequestBlocks(reqID string, hashes []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestBlocks[reqID] = hashes
}

// GetRequestBlocks returns the block hashes for a request.
func (s *State) GetRequestBlocks(reqID string) []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestBlocks[reqID]
}

// RegisterBlockOwners records which endpoints own a block hash.
func (s *State) RegisterBlockOwners(blockHash int64, endpoints []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	owners := make(map[string]bool, len(endpoints))
	for _, ep := range endpoints {
		owners[ep] = true
	}
	s.blockOwners[blockHash] = owners
}

// BlockOwnedBy checks if an endpoint owns a specific block.
func (s *State) BlockOwnedBy(blockHash int64, endpoint string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	owners, ok := s.blockOwners[blockHash]
	if !ok {
		return false
	}
	return owners[endpoint]
}

// CleanupExpired removes old results from the store.
func (s *State) CleanupExpired(ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().Add(-ttl)
	for id, ts := range s.resultStoredAt {
		if ts.Before(cutoff) {
			delete(s.resultStore, id)
			delete(s.resultStoredAt, id)
			delete(s.requestBlocks, id)
		}
	}
}

func GenReqID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
