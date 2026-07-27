package gateway

import (
	"log"
	"sync"
	"time"
)

// ExternalPushDispatcher implements external-push: like central-push it admits
// into the central queue (so KV-affinity, fairness and SLO scheduling via Pull
// all apply) and the router -- not a sidecar -- decides when/how much to
// dispatch. The one difference: endpoints are STATIC external vLLM servers (no
// sidecar), so delivery goes DIRECTLY to the external vLLM OpenAI endpoint and
// the response is ingested inline via the ingest callback (ingestResultPayload).
//
// Concurrency model: each pass grants CAP - in-flight per healthy endpoint via
// Pull (which increments in-flight), then delivers each item in its own
// goroutine, because a single external vLLM call is the FULL inference and can
// be slow. In-flight is held from selection until the goroutine ingests a
// result (success OR error), which releases it -- so the per-endpoint CAP is
// respected without blocking the dispatch loop.
//
// Mirrors router/external_push.py ExternalPushDispatcher.
type ExternalPushDispatcher struct {
	queue    *CentralQueue
	registry VLLMRegistry
	client   *ExternalVLLMClient
	ingest   func(map[string]interface{})
	cap      int
	interval time.Duration

	kickCh chan struct{}
	stopCh chan struct{}
	doneCh chan struct{}

	mu      sync.Mutex // single-flight: passes never overlap
	started bool
	wg      sync.WaitGroup // outstanding deliveries (drained on Stop)
}

func NewExternalPushDispatcher(queue *CentralQueue, registry VLLMRegistry, client *ExternalVLLMClient, ingest func(map[string]interface{}), cap int, intervalS float64) *ExternalPushDispatcher {
	if cap < 1 {
		cap = 1
	}
	if intervalS <= 0 {
		intervalS = 0.05
	}
	return &ExternalPushDispatcher{
		queue:    queue,
		registry: registry,
		client:   client,
		ingest:   ingest,
		cap:      cap,
		interval: time.Duration(intervalS * float64(time.Second)),
		kickCh:   make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (d *ExternalPushDispatcher) Start() {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	d.started = true
	d.mu.Unlock()
	go d.run()
	log.Printf("[ExternalPush] started (cap=%d interval=%s)", d.cap, d.interval)
}

func (d *ExternalPushDispatcher) Stop() {
	d.mu.Lock()
	if !d.started {
		d.mu.Unlock()
		return
	}
	d.mu.Unlock()
	close(d.stopCh)
	<-d.doneCh
	// Let outstanding deliveries finish ingesting (best-effort, bounded).
	done := make(chan struct{})
	go func() { d.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	log.Printf("[ExternalPush] stopped")
}

// Kick requests a dispatch pass now (coalesced via a size-1 channel).
func (d *ExternalPushDispatcher) Kick() {
	select {
	case d.kickCh <- struct{}{}:
	default:
	}
}

func (d *ExternalPushDispatcher) run() {
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

func (d *ExternalPushDispatcher) dispatchPass() {
	d.registry.RefreshHealth(false)
	endpoints := d.registry.HealthyIDs()
	if len(endpoints) == 0 {
		return
	}
	incCentralPushPass()

	models := d.queue.ActiveModels()
	inflight := d.queue.EndpointInflightSnapshot()

	for _, model := range models {
		for _, ep := range endpoints {
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

			for _, it := range items {
				d.spawnDelivery(ep, it)
			}
		}
	}
}

// spawnDelivery delivers one request directly to the external vLLM in its own
// goroutine, then ingests the result (releasing in-flight). Never panics out.
func (d *ExternalPushDispatcher) spawnDelivery(ep string, item JobItem) {
	incDispatch(ep)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		result := d.client.Deliver(ep, item.ReqID, item.Prompt, item.Meta)
		payload := map[string]interface{}{"req_id": item.ReqID, "result": result, "endpoint": ep}
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[ExternalPush] ingest panic ep=%s req_id=%s: %v", ep, item.ReqID, r)
					d.queue.ReleaseInflight(item.ReqID, ep)
				}
			}()
			d.ingest(payload)
		}()
		if fr, ok := result["finish_reason"].(string); ok && fr == "error" {
			incCentralPushFailed(ep, 1)
		} else {
			incCentralPushPushed(ep, 1)
		}
	}()
}
