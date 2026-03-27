package sidecar

import "sync"

// Item is a request queued in the sidecar's local queue.
type Item struct {
	ReqID  string
	Prompt string
	Meta   map[string]interface{}
}

// LocalQueue is a thread-safe bounded queue with inflight tracking.
type LocalQueue struct {
	mu       sync.Mutex
	items    []Item
	inflight int
}

func NewLocalQueue() *LocalQueue {
	return &LocalQueue{}
}

func (q *LocalQueue) Push(item Item) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, item)
}

// Pop removes and returns the first item, incrementing inflight. Returns false if empty.
func (q *LocalQueue) Pop() (Item, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return Item{}, false
	}
	item := q.items[0]
	q.items = q.items[1:]
	q.inflight++
	return item, true
}

// TaskDone decrements the inflight counter.
func (q *LocalQueue) TaskDone() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.inflight > 0 {
		q.inflight--
	}
}

func (q *LocalQueue) Pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *LocalQueue) Inflight() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inflight
}

// Logical returns pending + inflight (total load).
func (q *LocalQueue) Logical() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) + q.inflight
}
