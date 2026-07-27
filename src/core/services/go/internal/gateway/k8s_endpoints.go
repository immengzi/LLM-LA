package gateway

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// k8s_endpoints.go implements the registry for sidecar-less push-*/central-push
// (ROUTER_SIDECAR_ENABLED=false with ROUTER_MODE push-* or central-push). The
// router keeps k8s pod discovery (and for central-push the central queue —
// KV-affinity/fairness/SLO still apply via Pull), but delivers each request
// DIRECTLY to the pod's vLLM OpenAI endpoint instead of the per-pod sidecar /push.
//
// To maximize reuse it presents the same VLLMRegistry surface as
// ExternalRegistry, so it drives the existing ExternalPushDispatcher +
// ExternalVLLMClient unchanged. The only difference vs the static external
// registry: endpoints come from live k8s discovery (discoverPods) and can
// churn, so this registry also owns a per-pod RouterKVSubscriber it starts/stops
// as pods appear/vanish. Endpoint identity IS the pod name -- identical to the
// sidecar push path (PushDispatcher) -- so affinity, in-flight, metrics and
// Redis KV owners are keyed the same way with or without the sidecar.
//
// Faithful port of router/k8s_endpoints.py:K8sVLLMRegistry.
type K8sVLLMRegistry struct {
	cfg    *Config
	client *http.Client
	ctx    context.Context

	kvEnabled bool
	model     string

	mu            sync.Mutex
	byID          map[string]ExternalEndpointConfig
	healthy       map[string]bool
	subs          map[string]*RouterKVSubscriber
	lastDiscovery time.Time
	lastProbe     time.Time
}

// NewK8sVLLMRegistry builds the registry and does an initial (synchronous)
// discovery so the first dispatch pass already has pods.
func NewK8sVLLMRegistry(ctx context.Context, cfg *Config) *K8sVLLMRegistry {
	model := cfg.ModelName
	t := time.Duration(cfg.PushHTTPTimeoutS * float64(time.Second))
	r := &K8sVLLMRegistry{
		cfg:       cfg,
		client:    &http.Client{Timeout: t},
		ctx:       ctx,
		kvEnabled: cfg.KVAware, // KV-events subscriber only matters for prefix routing
		model:     model,
		byID:      make(map[string]ExternalEndpointConfig),
		healthy:   make(map[string]bool),
		subs:      make(map[string]*RouterKVSubscriber),
	}
	r.discover(true)
	return r
}

// ---- VLLMRegistry surface --------------------------------------------------

func (r *K8sVLLMRegistry) AllIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.byID))
	for id := range r.byID {
		out = append(out, id)
	}
	return out
}

func (r *K8sVLLMRegistry) Get(id string) (ExternalEndpointConfig, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.byID[id]
	return e, ok
}

func (r *K8sVLLMRegistry) HealthyIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.byID))
	for id := range r.byID {
		if r.healthy[id] {
			out = append(out, id)
		}
	}
	return out
}

// RefreshHealth re-discovers pods (throttled) so the endpoint set tracks scale
// up/down + restarts, then probes each pod's vLLM /health (throttled) to gate
// dispatch. Matches ExternalRegistry.RefreshHealth semantics.
func (r *K8sVLLMRegistry) RefreshHealth(force bool) {
	r.discover(force)

	interval := time.Duration(r.cfg.ExternalHealthIntervalS * float64(time.Second))
	r.mu.Lock()
	if !force && interval > 0 && time.Since(r.lastProbe) < interval {
		r.mu.Unlock()
		return
	}
	r.lastProbe = time.Now()
	eps := make([]ExternalEndpointConfig, 0, len(r.byID))
	for _, e := range r.byID {
		eps = append(eps, e)
	}
	r.mu.Unlock()

	type res struct {
		id string
		ok bool
	}
	ch := make(chan res, len(eps))
	var wg sync.WaitGroup
	for _, e := range eps {
		wg.Add(1)
		go func(e ExternalEndpointConfig) {
			defer wg.Done()
			resp, err := r.client.Get(e.URL + "/health")
			if err != nil {
				ch <- res{e.ID, false}
				return
			}
			resp.Body.Close()
			ch <- res{e.ID, resp.StatusCode == http.StatusOK}
		}(e)
	}
	wg.Wait()
	close(ch)

	r.mu.Lock()
	for x := range ch {
		r.healthy[x.id] = x.ok
	}
	r.mu.Unlock()
}

// Stop tears down all KV subscribers.
func (r *K8sVLLMRegistry) Stop() {
	r.mu.Lock()
	subs := r.subs
	r.subs = make(map[string]*RouterKVSubscriber)
	r.mu.Unlock()
	for _, s := range subs {
		s.Stop()
	}
}

// ---- discovery internals ---------------------------------------------------

func (r *K8sVLLMRegistry) vllmURL(ip string) string {
	return fmt.Sprintf("http://%s:%d", ip, r.cfg.VLLMPort)
}

func (r *K8sVLLMRegistry) makeEndpoint(pod, ip string) ExternalEndpointConfig {
	var kvEps []string
	if r.kvEnabled {
		kvEps = []string{fmt.Sprintf("tcp://%s:%d", ip, r.cfg.VLLMKvEventsPort)}
	}
	return ExternalEndpointConfig{
		ID:                pod,
		URL:               r.vllmURL(ip),
		Model:             r.model,
		KVEventsEndpoints: kvEps,
		KVEventsTopic:     r.cfg.VLLMKvEventsTopic,
	}
}

// discover re-lists pods (throttled by KVDiscoveryIntervalS unless force) and
// reconciles the endpoint set + KV subscribers. Caller must NOT hold r.mu.
func (r *K8sVLLMRegistry) discover(force bool) {
	interval := time.Duration(r.cfg.KVDiscoveryIntervalS * float64(time.Second))
	r.mu.Lock()
	if !force && len(r.byID) > 0 && time.Since(r.lastDiscovery) < interval {
		r.mu.Unlock()
		return
	}
	r.lastDiscovery = time.Now()
	r.mu.Unlock()

	pods := discoverPods(r.cfg)

	r.mu.Lock()
	if len(pods) == 0 && len(r.byID) > 0 {
		// Transient discovery failure: keep the last known set (matches
		// PushDispatcher's sticky behavior) rather than dropping everything.
		r.mu.Unlock()
		return
	}

	// Added / changed pods (start subscribers under lock is fine; Start only
	// spawns a goroutine).
	changed := false
	newIDs := make(map[string]bool, len(pods))
	for pod, ip := range pods {
		newIDs[pod] = true
		existing, ok := r.byID[pod]
		url := r.vllmURL(ip)
		if ok && existing.URL == url {
			continue
		}
		ep := r.makeEndpoint(pod, ip)
		r.byID[pod] = ep
		if _, seen := r.healthy[pod]; !seen {
			r.healthy[pod] = true
		}
		// (Re)start the subscriber if new or IP changed.
		if old := r.subs[pod]; old != nil {
			old.Stop()
			delete(r.subs, pod)
		}
		if r.kvEnabled && len(ep.KVEventsEndpoints) > 0 {
			sub := NewRouterKVSubscriber(r.cfg, ep)
			sub.Start(r.ctx)
			r.subs[pod] = sub
		}
		changed = true
	}

	// Removed pods.
	for pod := range r.byID {
		if newIDs[pod] {
			continue
		}
		delete(r.byID, pod)
		delete(r.healthy, pod)
		if sub := r.subs[pod]; sub != nil {
			sub.Stop()
			delete(r.subs, pod)
		}
		changed = true
	}
	ids := make([]string, 0, len(r.byID))
	for id := range r.byID {
		ids = append(ids, id)
	}
	r.mu.Unlock()

	if changed {
		sort.Strings(ids)
		log.Printf("[k8s-registry] discovered %d vLLM pods: %v", len(ids), ids)
	}
}
