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

	pulling     atomic.Bool
	firstPulled atomic.Bool
}

func NewRouterPullWorker(cfg *Config, queue *LocalQueue, endpointID string) *RouterPullWorker {
	return &RouterPullWorker{
		cfg:        cfg,
		queue:      queue,
		endpointID: endpointID,
		client: &http.Client{
			Timeout: time.Duration(cfg.RouterPullTimeoutS * float64(time.Second)),
		},
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
	want := w.cfg.BatchSize - (st.Pending + st.Inflight)
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
		if w.firstPulled.Load() {
			log.Printf("[pull] request error: %v", err)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		if w.firstPulled.Load() {
			log.Printf("[pull] non-200 response: %d", resp.StatusCode)
		}
		return
	}

	var pr pullResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		log.Printf("[pull] decode error: %v", err)
		return
	}

	if len(pr.Items) > 0 {
		w.firstPulled.Store(true)
	}

	for _, item := range pr.Items {
		if w.cfg.TraceEnabled {
			if item.Meta == nil {
				item.Meta = make(map[string]any)
			}
			item.Meta["_trace_pull_ts"] = float64(time.Now().UnixNano()) / 1e9
		}
		w.queue.Put(QueueItem{
			ReqID:  item.ReqID,
			Prompt: item.Prompt,
			Meta:   item.Meta,
		})
		ReceivedRequests.WithLabelValues(w.endpointID).Inc()
	}
}
