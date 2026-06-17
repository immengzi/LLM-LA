package sidecar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type RouterPullWorker struct {
	cfg        *Config
	queue      *LocalQueue
	endpointID string
	client     *http.Client

	pulling      atomic.Bool
	firstSuccess atomic.Bool
	stopCh       chan struct{}
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
		w.PullIfCapacity()
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
