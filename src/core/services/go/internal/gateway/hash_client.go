package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HashClient calls a /compute_hashes endpoint to obtain vLLM-compatible KV
// block hashes for a request. By default (KV_HASH_SOURCE=inline) this targets
// the in-container Python hasher (127.0.0.1:9095) that runs the router's shared
// prefix_hash.py, so the Go gateway and Python router produce identical hashes.
// With KV_HASH_SOURCE=external it targets the legacy standalone vllm-cpu-hash
// service instead.
type HashClient struct {
	cfg    *Config
	client *http.Client
}

func NewHashClient(cfg *Config) *HashClient {
	return &HashClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: time.Duration(cfg.HashTimeoutS * float64(time.Second)),
			Transport: &http.Transport{
				MaxIdleConns:        cfg.HashMaxKeepalive,
				MaxIdleConnsPerHost: cfg.HashMaxKeepalive,
				IdleConnTimeout:     time.Duration(cfg.HashKeepaliveExpiryS * float64(time.Second)),
			},
		},
	}
}

type hashRequest struct {
	Prompt   string        `json:"prompt,omitempty"`
	Messages []interface{} `json:"messages,omitempty"`
	Tools    []interface{} `json:"tools,omitempty"`
	Backend  string        `json:"backend,omitempty"`
}

type hashResponse struct {
	BlockHashes []json.Number `json:"block_hashes"`
}

// ComputeHashes returns the ordered block-hash chain (as canonical decimal
// strings), or an error. Callers treat any error as "no hashes" (best-effort).
//
// Behavior depends on KV_HASH_SOURCE:
//   - inline:   send structured messages+tools (else the flat prompt) to the
//     in-container hasher, matching the Python router's prefix_hash.py input.
//   - external: send only the flat prompt to the legacy vllm-cpu-hash service,
//     preserving the former behavior exactly.
func (h *HashClient) ComputeHashes(ctx context.Context, prompt string, messages, tools []interface{}) ([]string, error) {
	var reqBody hashRequest
	if strings.ToLower(h.cfg.KVHashSource) == "external" {
		reqBody = hashRequest{Prompt: prompt}
	} else if len(messages) > 0 {
		reqBody = hashRequest{Messages: messages, Tools: tools, Backend: h.cfg.KVHashBackend}
	} else {
		reqBody = hashRequest{Prompt: prompt, Tools: tools, Backend: h.cfg.KVHashBackend}
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/compute_hashes", h.cfg.HashServiceURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("hash service status %d", resp.StatusCode)
	}

	var hr hashResponse
	if err := json.NewDecoder(resp.Body).Decode(&hr); err != nil {
		return nil, err
	}
	return safeIntList(hr.BlockHashes), nil
}

// safeIntList mirrors _safe_int_list: best-effort int conversion, dropping
// anything that does not parse. Returns canonical decimal strings so block
// hashes that exceed 64-bit range are preserved exactly.
func safeIntList(xs []json.Number) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		s := x.String()
		if isIntegerLiteral(s) {
			out = append(out, s)
			continue
		}
		if f, err := x.Float64(); err == nil {
			out = append(out, strconv.FormatInt(int64(f), 10))
		}
	}
	return out
}

// isIntegerLiteral reports whether s is a canonical base-10 integer literal
// (optional leading '-'), i.e. has no decimal point or exponent.
func isIntegerLiteral(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	if s[0] == '-' || s[0] == '+' {
		i = 1
	}
	if i >= len(s) {
		return false
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
