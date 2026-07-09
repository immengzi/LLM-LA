package gateway

import (
	"sync"
	"sync/atomic"
)

// push_dispatch.go ports the _PushDispatcher background dispatcher in
// router/api.py: it decouples request-handler latency from the sidecar push by
// buffering jobs in a bounded queue drained by a worker pool.

type pushJob struct {
	reqID   string
	prompt  string
	meta    map[string]interface{}
	tSubmit float64
}

// PushDispatchQueue is the decoupled dispatcher. handle performs the actual
// KV registration + push and returns an error on failure; storeErr records a
// synthetic error result so /enqueue waiters are unblocked.
type PushDispatchQueue struct {
	cfg       *Config
	ch        chan pushJob
	workers   int
	maxDelayS float64
	stopped   atomic.Bool
	wg        sync.WaitGroup

	handle   func(reqID, prompt string, meta map[string]interface{}) error
	storeErr func(reqID string, result map[string]interface{})
}

func NewPushDispatchQueue(
	cfg *Config,
	handle func(reqID, prompt string, meta map[string]interface{}) error,
	storeErr func(reqID string, result map[string]interface{}),
) *PushDispatchQueue {
	qmax := cfg.PushDispatchQueueMax
	if qmax < 1 {
		qmax = 1
	}
	workers := cfg.PushDispatchWorkers
	if workers < 1 {
		workers = 1
	}
	maxDelay := cfg.PushDispatchMaxDelayS
	if maxDelay < 0 {
		maxDelay = 0
	}
	setPushDispatchQueueLength(0)
	return &PushDispatchQueue{
		cfg:       cfg,
		ch:        make(chan pushJob, qmax),
		workers:   workers,
		maxDelayS: maxDelay,
		handle:    handle,
		storeErr:  storeErr,
	}
}

func (d *PushDispatchQueue) qsize() int { return len(d.ch) }

// Start launches the worker pool.
func (d *PushDispatchQueue) Start() {
	for i := 0; i < d.workers; i++ {
		d.wg.Add(1)
		go d.worker()
	}
}

// Stop closes the queue and waits for workers to drain.
func (d *PushDispatchQueue) Stop() {
	if d.stopped.Swap(true) {
		return
	}
	close(d.ch)
	d.wg.Wait()
	setPushDispatchQueueLength(d.qsize())
}

// TrySubmit is a non-blocking enqueue. Returns false if the queue is full or
// the dispatcher is stopped.
func (d *PushDispatchQueue) TrySubmit(reqID, prompt string, meta map[string]interface{}) bool {
	if d.stopped.Load() {
		return false
	}
	job := pushJob{reqID: reqID, prompt: prompt, meta: cloneMeta(meta), tSubmit: nowS()}
	select {
	case d.ch <- job:
		incPushDispatchEnqueued()
		setPushDispatchQueueLength(d.qsize())
		return true
	default:
		incPushDispatchDropped()
		setPushDispatchQueueLength(d.qsize())
		return false
	}
}

func (d *PushDispatchQueue) worker() {
	defer d.wg.Done()
	for job := range d.ch {
		setPushDispatchQueueLength(d.qsize())
		incPushDispatchStarted()

		if d.maxDelayS > 0 {
			age := nowS() - job.tSubmit
			if age > d.maxDelayS {
				incPushDispatchFailed()
				d.storeErr(job.reqID, map[string]interface{}{
					"error": "push_dispatch_stale",
				})
				setPushDispatchQueueLength(d.qsize())
				continue
			}
		}

		if err := d.handle(job.reqID, job.prompt, job.meta); err != nil {
			incPushDispatchFailed()
			d.storeErr(job.reqID, map[string]interface{}{
				"error": "push_dispatch_failed: " + err.Error(),
			})
		}
		setPushDispatchQueueLength(d.qsize())
	}
}
