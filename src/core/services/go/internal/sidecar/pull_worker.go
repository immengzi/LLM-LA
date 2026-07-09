package sidecar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const vllmHealthProbeInterval = 5 * time.Second

type RouterPullWorker struct {
	cfg        *Config
	queue      *LocalQueue
	endpointID string
	client     *http.Client

	pulling      atomic.Bool
	firstSuccess atomic.Bool
	stopCh       chan struct{}

	// vLLM health gate
	healthMu           sync.Mutex
	vllmHealthy        atomic.Bool
	vllmLastProbe      time.Time
	vllmUnhealthyLogged bool
}

func NewRouterPullWorker(cfg *Config, queue *LocalQueue, endpointID string) *RouterPullWorker {
	transport := &http.Transport{
		MaxIdleConns:        cfg.RouterPoolMaxsize,
		MaxIdleConnsPerHost: cfg.RouterPoolMaxsize,
	}
	return &RouterPullWorker{
		cfg:        cfg,
		queue:      queue,
		endpointID: endpointID,
		client: &http.Client{
			Timeout:   time.Duration(cfg.RouterPullTimeoutS * float64(time.Second)),
			Transport: transport,
		},
		stopCh: make(chan struct{}),
	}
}

// Start launches the background poll loop that keeps the local queue warm,
// mirroring RouterPullWorker._poll_loop.
func (w *RouterPullWorker) Start() {
	pullCap := w.cfg.PullCap()
	log.Printf("[sidecar] RouterPullWorker ready (endpoint_id=%s, BATCH_SIZE=%d, PREFETCH=%d, pull_cap=%d)",
		w.endpointID, w.cfg.BatchSize, w.cfg.Prefetch, pullCap)
	go w.pollLoop()
}

// probeVLLM issues a GET to vLLM /health with a short timeout.
func (w *RouterPullWorker) probeVLLM() bool {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(w.cfg.VLLMURL + "/health")
	if err != nil {
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// CheckVLLMHealth returns the cached health status, re-probing at most every
// vllmHealthProbeInterval.
func (w *RouterPullWorker) CheckVLLMHealth() bool {
	w.healthMu.Lock()
	defer w.healthMu.Unlock()

	now := time.Now()
	if now.Sub(w.vllmLastProbe) < vllmHealthProbeInterval {
		return w.vllmHealthy.Load()
	}

	healthy := w.probeVLLM()
	w.vllmLastProbe = now

	wasHealthy := w.vllmHealthy.Load()
	w.vllmHealthy.Store(healthy)

	if healthy && !wasHealthy {
		log.Println("[sidecar] vLLM is healthy again — resuming pulls")
		w.vllmUnhealthyLogged = false
	} else if !healthy && !w.vllmUnhealthyLogged {
		log.Println("[sidecar] vLLM health check FAILED — pausing pulls until recovery")
		w.vllmUnhealthyLogged = true
	}
	return healthy
}

// VLLMHealthy returns the last known vLLM health status (lock-free).
func (w *RouterPullWorker) VLLMHealthy() bool {
	return w.vllmHealthy.Load()
}

func (w *RouterPullWorker) Stop() {
	close(w.stopCh)
}

func (w *RouterPullWorker) pollLoop() {
	interval := w.cfg.PullIntervalS
	if interval < 0.01 {
		interval = 0.01
	}
	d := time.Duration(interval * float64(time.Second))
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}
		// LOAD-BEARING health gate: pulls happen only when vLLM /health is 200.
		// The router's persistent-affinity readiness anchor relies on
		// "a pull ⟹ this pod was ready ≤5s ago". Do NOT pull before vLLM is
		// ready (no warmup pull / relaxed gate) without re-evaluating the
		// affinity readiness predicate. See
		// docs/internal/persistent-affinity-map.md (invariant).
		if w.CheckVLLMHealth() {
			w.PullIfCapacity()
		}
		select {
		case <-w.stopCh:
			return
		case <-time.After(d):
		}
	}
}

type pullRequest struct {
	Endpoint string `json:"endpoint"`
	Want     int    `json:"want"`
	Model    string `json:"model"`
}

type pullResponseItem struct {
	ReqID  string         `json:"req_id"`
	Prompt string         `json:"prompt"`
	Meta   map[string]any `json:"meta"`
}

type pullResponse struct {
	Items []pullResponseItem `json:"items"`
}

func (w *RouterPullWorker) PullIfCapacity() {
	// LOAD-BEARING health gate (see pollLoop + the affinity readiness invariant
	// in docs/internal/persistent-affinity-map.md): never pull before vLLM is
	// ready, or the router's per-pod READY anchor breaks.
	if !w.vllmHealthy.Load() {
		return
	}
	if !w.pulling.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer w.pulling.Store(false)
		w.doPull()
	}()
}

func (w *RouterPullWorker) doPull() {
	st := w.queue.State()
	totalReserved := st.Pending + st.Inflight
	pullCap := w.cfg.PullCap()
	if totalReserved >= pullCap {
		return
	}
	want := pullCap - totalReserved
	if want <= 0 {
		return
	}

	body, err := json.Marshal(pullRequest{Endpoint: w.endpointID, Want: want, Model: w.cfg.ModelName})
	if err != nil {
		log.Printf("[pull] marshal error: %v", err)
		return
	}

	url := fmt.Sprintf("%s/pull", w.cfg.RouterURL)
	resp, err := w.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		if w.firstSuccess.Load() {
			log.Printf("[sidecar] /pull error: %v", err)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		if w.firstSuccess.Load() {
			log.Printf("[sidecar] /pull failed: %d", resp.StatusCode)
		}
		return
	}

	var pr pullResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		log.Printf("[pull] decode error: %v", err)
		return
	}

	if !w.firstSuccess.Load() {
		log.Println("[sidecar] first successful /pull; normal logging enabled.")
		w.firstSuccess.Store(true)
	}

	if len(pr.Items) == 0 {
		return
	}

	nowPull := float64(time.Now().UnixNano()) / 1e9
	stBefore := w.queue.State()
	logicalBefore := stBefore.Pending + stBefore.Inflight
	queueLenAfter := stBefore.Pending + len(pr.Items)
	logicalAfter := logicalBefore + len(pr.Items)

	for _, item := range pr.Items {
		meta := item.Meta
		if meta == nil {
			meta = make(map[string]any)
		}
		ReceivedRequests.WithLabelValues(w.endpointID).Inc()

		if w.cfg.TraceEnabled {
			tr := traceMap(meta)
			tr["t_arrive_sidecar_pull"] = nowPull
			tr["sidecar_queue_len_before_pull"] = stBefore.Pending
			tr["sidecar_inflight_before_pull"] = stBefore.Inflight
			tr["sidecar_logical_before_pull"] = logicalBefore
			tr["sidecar_queue_len_after_pull"] = queueLenAfter
			tr["sidecar_logical_after_pull"] = logicalAfter
			meta["__trace__"] = tr
		}

		w.queue.Put(QueueItem{ReqID: item.ReqID, Prompt: item.Prompt, Meta: meta})
	}
}

// traceMap returns a copy of meta["__trace__"] (or a fresh map).
func traceMap(meta map[string]any) map[string]any {
	if tr, ok := meta["__trace__"].(map[string]any); ok {
		out := make(map[string]any, len(tr)+6)
		for k, v := range tr {
			out[k] = v
		}
		return out
	}
	return make(map[string]any)
}
