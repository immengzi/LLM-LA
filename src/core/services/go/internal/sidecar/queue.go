package sidecar

import (
	"sync"
)

type QueueItem struct {
	ReqID  string
	Prompt string
	Meta   map[string]any
}

type QueueState struct {
	Pending  int
	Inflight int
}

type LocalQueue struct {
	mu         sync.Mutex
	items      []QueueItem
	inflight   int
	endpointID string
}

func NewLocalQueue(endpointID string) *LocalQueue {
	return &LocalQueue{endpointID: endpointID}
}

func (q *LocalQueue) Put(item QueueItem) {
	q.mu.Lock()
	q.items = append(q.items, item)
	q.updateGauge()
	q.mu.Unlock()
}

func (q *LocalQueue) GetNoWait() (QueueItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return QueueItem{}, false
	}
	item := q.items[0]
	q.items = q.items[1:]
	q.inflight++
	q.updateGauge()
	return item, true
}

func (q *LocalQueue) TaskDone() {
	q.mu.Lock()
	if q.inflight > 0 {
		q.inflight--
	}
	q.updateGauge()
	q.mu.Unlock()
}

func (q *LocalQueue) State() QueueState {
	q.mu.Lock()
	s := QueueState{Pending: len(q.items), Inflight: q.inflight}
	q.mu.Unlock()
	return s
}

// must be called with mu held
func (q *LocalQueue) updateGauge() {
	QueueLength.WithLabelValues(q.endpointID).Set(float64(len(q.items) + q.inflight))
}
