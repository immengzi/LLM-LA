package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/vllmkv/operator/pkg/models"
)

// PushRouter dispatches requests to sidecars in push mode.
type PushRouter struct {
	mu        sync.Mutex
	endpoints []string // sidecar URLs (http://<ip>:<port>)
	rrIndex   int
	inflight  map[string]int // endpoint -> count (leastq-local)
	client    *http.Client
	cfg       *Config
}

func NewPushRouter(cfg *Config) *PushRouter {
	return &PushRouter{
		inflight: make(map[string]int),
		client: &http.Client{
			Timeout: time.Duration(cfg.PushHTTPTimeoutS * float64(time.Second)),
		},
		cfg: cfg,
	}
}

func (p *PushRouter) SetEndpoints(eps []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.endpoints = eps
}

func (p *PushRouter) Endpoints() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	dst := make([]string, len(p.endpoints))
	copy(dst, p.endpoints)
	return dst
}

// RouteAndPush selects an endpoint and pushes the request to its sidecar.
func (p *PushRouter) RouteAndPush(ctx context.Context, req *models.Request) error {
	ep, err := p.selectEndpoint(ctx)
	if err != nil {
		return err
	}

	body, _ := json.Marshal(map[string]interface{}{
		"req_id": req.ReqID,
		"prompt": req.Prompt,
		"meta":   req.Meta,
	})

	url := fmt.Sprintf("%s/push", ep)
	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")

	p.mu.Lock()
	p.inflight[ep]++
	p.mu.Unlock()

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("push to %s: %w", ep, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 300 {
		return fmt.Errorf("push to %s: status %d", ep, resp.StatusCode)
	}
	return nil
}

// NotifyResult decrements inflight for leastq-local.
func (p *PushRouter) NotifyResult(endpoint string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inflight[endpoint] > 0 {
		p.inflight[endpoint]--
	}
}

func (p *PushRouter) selectEndpoint(ctx context.Context) (string, error) {
	p.mu.Lock()
	eps := make([]string, len(p.endpoints))
	copy(eps, p.endpoints)
	p.mu.Unlock()

	if len(eps) == 0 {
		return "", fmt.Errorf("no endpoints available")
	}

	switch p.cfg.RouterMode {
	case "push-random":
		return eps[rand.Intn(len(eps))], nil
	case "push-leastq":
		return p.selectLeastQueue(ctx, eps)
	default: // push-rr
		return p.selectRoundRobin(eps), nil
	}
}

func (p *PushRouter) selectRoundRobin(eps []string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	ep := eps[p.rrIndex%len(eps)]
	p.rrIndex++
	return ep
}

func (p *PushRouter) selectLeastQueue(ctx context.Context, eps []string) (string, error) {
	if p.cfg.PushLeastqMode == "health" {
		return p.selectLeastQueueHealth(ctx, eps)
	}
	return p.selectLeastQueueLocal(eps), nil
}

func (p *PushRouter) selectLeastQueueLocal(eps []string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	best := eps[0]
	bestN := p.inflight[best]
	for _, ep := range eps[1:] {
		n := p.inflight[ep]
		if n < bestN {
			best, bestN = ep, n
		}
	}
	return best
}

func (p *PushRouter) selectLeastQueueHealth(ctx context.Context, eps []string) (string, error) {
	type result struct {
		ep  string
		len int
	}

	ch := make(chan result, len(eps))
	for _, ep := range eps {
		go func(ep string) {
			url := fmt.Sprintf("%s/health", ep)
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			resp, err := p.client.Do(req)
			if err != nil {
				ch <- result{ep: ep, len: 999999}
				return
			}
			defer resp.Body.Close()
			var h struct {
				Logical  *int `json:"logical"`
				QueueLen *int `json:"queue_len"`
			}
			json.NewDecoder(resp.Body).Decode(&h)
			n := 999999
			if h.Logical != nil {
				n = *h.Logical
			} else if h.QueueLen != nil {
				n = *h.QueueLen
			}
			ch <- result{ep: ep, len: n}
		}(ep)
	}

	best := result{len: 999999}
	for range eps {
		r := <-ch
		if r.len < best.len {
			best = r
		}
	}
	if best.ep == "" {
		return "", fmt.Errorf("all health checks failed")
	}
	return best.ep, nil
}

// PushDispatchLoop dequeues from the central queue and pushes to sidecars.
func PushDispatchLoop(ctx context.Context, state *State, push *PushRouter) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reqs := state.DequeueUpTo(1)
		if len(reqs) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		req := reqs[0]
		if err := push.RouteAndPush(ctx, req); err != nil {
			log.Printf("[push] failed req_id=%s: %v, re-queueing", req.ReqID, err)
			state.ReturnToFront(reqs)
			time.Sleep(100 * time.Millisecond)
		}
	}
}
