package sidecar

import "testing"

// TestLocalQueueFIFO verifies basic FIFO ordering and pending/inflight state
// transitions, mirroring the Python sidecar LocalQueue tests.
func TestLocalQueueFIFO(t *testing.T) {
	q := NewLocalQueue("pod-a")
	for i, id := range []string{"r0", "r1", "r2"} {
		q.Put(QueueItem{ReqID: id, Prompt: "p", Meta: map[string]any{"i": i}})
	}
	if s := q.State(); s.Pending != 3 || s.Inflight != 0 {
		t.Fatalf("after 3 puts state = %+v, want {3 0}", s)
	}
	for _, want := range []string{"r0", "r1", "r2"} {
		item, ok := q.GetNoWait()
		if !ok || item.ReqID != want {
			t.Fatalf("GetNoWait = (%v, %v), want %s", item.ReqID, ok, want)
		}
	}
}

func TestLocalQueueEmptyGet(t *testing.T) {
	q := NewLocalQueue("pod-a")
	if _, ok := q.GetNoWait(); ok {
		t.Fatal("GetNoWait on empty queue returned ok=true")
	}
}

func TestLocalQueueInflightAccounting(t *testing.T) {
	q := NewLocalQueue("pod-a")
	q.Put(QueueItem{ReqID: "r1"})
	q.Put(QueueItem{ReqID: "r2"})
	q.GetNoWait()
	if s := q.State(); s.Pending != 1 || s.Inflight != 1 {
		t.Fatalf("after one get state = %+v, want {1 1}", s)
	}
	q.TaskDone()
	if s := q.State(); s.Pending != 1 || s.Inflight != 0 {
		t.Fatalf("after task done state = %+v, want {1 0}", s)
	}
}

func TestLocalQueueTaskDoneNeverNegative(t *testing.T) {
	q := NewLocalQueue("pod-a")
	q.TaskDone()
	q.TaskDone()
	if s := q.State(); s.Inflight != 0 {
		t.Fatalf("inflight went negative: %+v", s)
	}
}
