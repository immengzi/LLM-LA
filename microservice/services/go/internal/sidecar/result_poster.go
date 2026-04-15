package sidecar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"time"
)

type ResultPoster struct {
	cfg    *Config
	ch     chan map[string]any
	client *http.Client
	url    string
}

func NewResultPoster(cfg *Config) *ResultPoster {
	url := fmt.Sprintf("%s/result", cfg.RouterURL)
	if cfg.ResultTransportMode == "submit_ack" {
		url = fmt.Sprintf("%s%s", cfg.RouterURL, cfg.ResultSubmitPath)
	}

	return &ResultPoster{
		cfg: cfg,
		ch:  make(chan map[string]any, 100000),
		client: &http.Client{
			Timeout: time.Duration(cfg.RouterResultTimeoutS * float64(time.Second)),
		},
		url: url,
	}
}

func (p *ResultPoster) Start() {
	go p.drain()
}

func (p *ResultPoster) Submit(result map[string]any) {
	select {
	case p.ch <- result:
	default:
		log.Printf("[result_poster] channel full, dropping result for req_id=%v", result["req_id"])
	}
}

func (p *ResultPoster) drain() {
	for result := range p.ch {
		p.post(result)
	}
}

func (p *ResultPoster) post(result map[string]any) {
	body, err := json.Marshal(result)
	if err != nil {
		log.Printf("[result_poster] marshal error: %v", err)
		return
	}

	maxAttempts := 1
	if p.cfg.ResultPostRetry {
		maxAttempts = p.cfg.ResultPostMaxRetries + 1
		if maxAttempts < 1 {
			maxAttempts = 1
		}
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			backoff := p.cfg.ResultPostBackoffBaseS * math.Pow(2, float64(attempt-1))
			jitter := backoff * 0.1 * rand.Float64()
			backoff += jitter
			if backoff > p.cfg.ResultPostBackoffCapS {
				backoff = p.cfg.ResultPostBackoffCapS
			}
			time.Sleep(time.Duration(backoff * float64(time.Second)))
		}

		resp, err := p.client.Post(p.url, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("[result_poster] attempt %d/%d error: %v", attempt+1, maxAttempts, err)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return
		}
		log.Printf("[result_poster] attempt %d/%d status: %d", attempt+1, maxAttempts, resp.StatusCode)
	}
}
