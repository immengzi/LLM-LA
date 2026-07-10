package gateway

import (
	"log"
	"sync"
	"time"
)

// CentralPushDispatcher implements central-push: admit into the central queue
// like pull (so KV-affinity, fairness and SLO scheduling all apply), but let
// the router -- not the sidecar -- decide when and how much to dispatch. Each
// pass computes a per-endpoint capacity of CAP - in-flight and asks the existing
// scheduler (Pull) for that many items, then delivers via POST /push.
//
// Mirrors router/central_push.py CentralPushDispatcher.
type CentralPushDispatcher struct {
	queue      *CentralQueue
	pushRouter *PushDispatcher
	cap        int
	interval   time.Duration

	kickCh chan struct{}
	stopCh chan struct{}
	doneCh chan struct{}

	mu      sync.Mutex // single-flight: passes never overlap
	started bool
}

// NewCentralPushDispatcher builds a dispatcher. cap is the per-pod concurrency
// ceiling (CAP - in-flight items are pushed); intervalS is the periodic tick.
func NewCentralPushDispatcher(queue *CentralQueue, pushRouter *PushDispatcher, cap int, intervalS float64) *CentralPushDispatcher {
	if cap < 1 {
		cap = 1
	}
	if intervalS <= 0 {
		intervalS = 0.05
	}
	return &CentralPushDispatcher{
		queue:      queue,
		pushRouter: pushRouter,
		cap:        cap,
		interval:   time.Duration(intervalS * float64(time.Second)),
		kickCh:     make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// Start launches the dispatch loop goroutine.
func (d *CentralPushDispatcher) Start() {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	d.started = true
	d.mu.Unlock()
	go d.run()
	log.Printf("[CentralPush] started (cap=%d interval=%s)", d.cap, d.interval)
}

// Stop signals the loop to exit and waits for it.
func (d *CentralPushDispatcher) Stop() {
	d.mu.Lock()
	if !d.started {
		d.mu.Unlock()
		return
	}
	d.mu.Unlock()
	close(d.stopCh)
	<-d.doneCh
	log.Printf("[CentralPush] stopped")
}

// Kick requests a dispatch pass now (coalesced via a size-1 channel).
func (d *CentralPushDispatcher) Kick() {
	select {
	case d.kickCh <- struct{}{}:
	default:
	}
}

func (d *CentralPushDispatcher) run() {
	defer close(d.doneCh)
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.dispatchPass()
		case <-d.kickCh:
			d.dispatchPass()
		}
	}
}

func (d *CentralPushDispatcher) dispatchPass() {
	endpoints := d.pushRouter.EndpointsSnapshot()
	if len(endpoints) == 0 {
		return
	}
	incCentralPushPass()

	models := d.queue.ActiveModels()
	// Local view of in-flight, kept current within the pass so grants across
	// models/endpoints respect the per-pod cap cumulatively.
	inflight := d.queue.EndpointInflightSnapshot()
	failed := make(map[string]struct{})

	for _, model := range models {
		for _, ep := range endpoints {
			if _, bad := failed[ep]; bad {
				continue
			}
			cur := inflight[ep]
			want := d.cap - cur
			if want < 0 {
				want = 0
			}
			setCentralPushWant(ep, want)
			if want <= 0 {
				continue
			}

			items := d.queue.Pull(ep, want, model)
			if len(items) == 0 {
				continue
			}
			inflight[ep] = cur + len(items)

			for idx, it := range items {
				if err := d.pushRouter.PushToEndpoint(ep, it.ReqID, it.Prompt, it.Meta); err != nil {
					// Delivery failed. Every item from here to the end of this
					// batch was already dequeued and counted in-flight by Pull but
					// not delivered, so roll all of them back: release in-flight +
					// re-admit to the front for a later pass, and skip this
					// endpoint for the rest of this pass.
					remaining := items[idx:]
					for _, rem := range remaining {
						d.queue.ReleaseInflight(rem.ReqID, ep)
					}
					d.queue.RequeueFront(model, remaining)
					if inflight[ep] >= len(remaining) {
						inflight[ep] -= len(remaining)
					} else {
						inflight[ep] = 0
					}
					incCentralPushFailed(ep, 1)
					incCentralPushRequeued(ep, len(remaining))
					failed[ep] = struct{}{}
					log.Printf("[CentralPush] delivery failed ep=%s req_id=%s: %v; requeued %d item(s)", ep, it.ReqID, err, len(remaining))
					break
				}
				incCentralPushPushed(ep, 1)
			}
		}
	}
}
