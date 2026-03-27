package sidecar

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// ResultPoster posts completed results back to the router.
type ResultPoster struct {
	cfg    *Config
	client *http.Client
}

func NewResultPoster(cfg *Config) *ResultPoster {
	return &ResultPoster{
		cfg: cfg,
		client: &http.Client{
			Timeout: time.Duration(cfg.RouterResultTimeoutS * float64(time.Second)),
		},
	}
}

// Submit sends a result to the router's /result endpoint.
func (p *ResultPoster) Submit(reqID string, result map[string]interface{}) {
	payload := map[string]interface{}{
		"req_id":   reqID,
		"result":   result,
		"endpoint": p.cfg.ContainerName,
	}

	body, _ := json.Marshal(payload)
	url := p.cfg.ResultURL()

	resp, err := p.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[poster] error req_id=%s: %v", reqID, err)
		return
	}
	resp.Body.Close()

	if resp.StatusCode >= 300 {
		log.Printf("[poster] unexpected status %d for req_id=%s", resp.StatusCode, reqID)
	}
}
