package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HashClient calls the prefix-hash service to compute block hashes.
type HashClient struct {
	url    string
	client *http.Client
}

func NewHashClient(cfg *Config) *HashClient {
	return &HashClient{
		url: cfg.HashServiceURL,
		client: &http.Client{
			Timeout: time.Duration(cfg.HashTimeoutS * float64(time.Second)),
		},
	}
}

type hashRequest struct {
	Prompt string `json:"prompt"`
}

type hashResponse struct {
	BlockHashes []int64 `json:"block_hashes"`
}

// ComputeHashes returns prefix block hashes for a prompt.
func (h *HashClient) ComputeHashes(ctx context.Context, prompt string) ([]int64, error) {
	body, _ := json.Marshal(hashRequest{Prompt: prompt})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.url+"/compute_hashes", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hash service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("hash service: status %d", resp.StatusCode)
	}

	var result hashResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("hash decode: %w", err)
	}
	return result.BlockHashes, nil
}
