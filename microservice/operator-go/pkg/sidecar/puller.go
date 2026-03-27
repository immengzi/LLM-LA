package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// RouterPuller pulls work from the router when the sidecar has capacity.
type RouterPuller struct {
	queue  *LocalQueue
	cfg    *Config
	client *http.Client
	wake   chan struct{} // poke workers when items arrive
}

func NewRouterPuller(queue *LocalQueue, cfg *Config, wake chan struct{}) *RouterPuller {
	return &RouterPuller{
		queue: queue,
		cfg:   cfg,
		client: &http.Client{
			Timeout: time.Duration(cfg.RouterPullTimeoutS * float64(time.Second)),
		},
		wake: wake,
	}
}

func (p *RouterPuller) Run(ctx context.Context) {
	interval := time.Duration(p.cfg.PullIntervalS * float64(time.Second))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		want := p.cfg.BatchSize - p.queue.Logical()
		if want <= 0 {
			time.Sleep(interval)
			continue
		}

		items, err := p.pull(ctx, want)
		if err != nil {
			log.Printf("[puller] error: %v", err)
			time.Sleep(interval)
			continue
		}

		for _, item := range items {
			p.queue.Push(item)
		}

		if len(items) > 0 {
			select {
			case p.wake <- struct{}{}:
			default:
			}
		}

		if len(items) == 0 {
			time.Sleep(interval)
		}
	}
}

type pullRequest struct {
	Endpoint string `json:"endpoint"`
	Want     int    `json:"want"`
}

type pullResponse struct {
	Items []struct {
		ReqID           string                 `json:"req_id"`
		Prompt          string                 `json:"prompt"`
		ClientEnqueueTS float64                `json:"t_enq_client,omitempty"`
		Meta            map[string]interface{} `json:"meta,omitempty"`
	} `json:"items"`
}

func (p *RouterPuller) pull(ctx context.Context, want int) ([]Item, error) {
	body, _ := json.Marshal(pullRequest{
		Endpoint: p.cfg.ContainerName,
		Want:     want,
	})

	url := fmt.Sprintf("%s/pull", p.cfg.RouterURL)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pull: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("pull: status %d", resp.StatusCode)
	}

	var pr pullResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, fmt.Errorf("pull decode: %w", err)
	}

	items := make([]Item, len(pr.Items))
	for i, it := range pr.Items {
		meta := it.Meta
		if meta == nil {
			meta = make(map[string]interface{})
		}
		items[i] = Item{ReqID: it.ReqID, Prompt: it.Prompt, Meta: meta}
	}
	return items, nil
}
