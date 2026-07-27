package gateway

import "time"

// EnqueueRequest matches the Python EnqueueRequest pydantic model.
type EnqueueRequest struct {
	Prompt        string                 `json:"prompt"`
	ReqID         string                 `json:"req_id,omitempty"`
	TEnqClient    *float64               `json:"t_enq_client,omitempty"`
	Meta          map[string]interface{} `json:"meta,omitempty"`
	Model         string                 `json:"model,omitempty"`
	SLOType       *string                `json:"slo_type,omitempty"`
	SLOTtftMs     *float64               `json:"slo_ttft_ms,omitempty"`
	SLOTpotMs     *float64               `json:"slo_tpot_ms,omitempty"`
	SLOE2eMs      *float64               `json:"slo_e2e_ms,omitempty"`
	TaskType      *string                `json:"task_type,omitempty"`
	OutputLenHint *int                   `json:"output_len_hint,omitempty"`
}

// PullRequest matches the Python PullRequest pydantic model.
type PullRequest struct {
	Endpoint string `json:"endpoint"`
	Want     int    `json:"want"`
	Model    string `json:"model,omitempty"`
	// KvUsage is the sidecar's optional GPU KV cache usage fraction [0,1],
	// piggybacked on /pull to feed soft KV divert. Nil when the sidecar does
	// not report it (KV_USAGE_REPORT off). Mirrors PullRequest.kv_usage.
	KvUsage *float64 `json:"kv_usage,omitempty"`
	// Optional per-pull uncached-prefill token budget (P2). 0 => router uses its
	// own PREFILL_TOKEN_BUDGET; budgeting only runs when PULL_BUDGET_ENABLED.
	WantPrefillTokens int `json:"want_prefill_tokens,omitempty"`
}

// PullResponse matches the Python PullResponse pydantic model.
type PullResponse struct {
	Items []JobItem `json:"items"`
}

// JobItem matches the Python JobItem pydantic model.
type JobItem struct {
	ReqID      string                 `json:"req_id"`
	Prompt     string                 `json:"prompt"`
	TEnqClient float64                `json:"t_enq_client"`
	Meta       map[string]interface{} `json:"meta"`
}

// ChatCompletionRequest matches the OpenAI chat completions request format.
type ChatCompletionRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	MaxTokens   *int          `json:"max_tokens,omitempty"`
	Temperature *float64      `json:"temperature,omitempty"`
	Stream      *bool         `json:"stream,omitempty"`
}

// ChatMessage represents a single message in the chat format.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ResultPayload matches the Python ResultRequest model and legacy format.
type ResultPayload struct {
	ReqID    string                 `json:"req_id"`
	Result   map[string]interface{} `json:"result,omitempty"`
	Endpoint string                 `json:"endpoint,omitempty"`
	Output   interface{}            `json:"output,omitempty"`
	Trace    map[string]interface{} `json:"trace,omitempty"`
}

// QueueEntry is the internal representation of a queued request.
type QueueEntry struct {
	ReqID      string
	Prompt     string
	Meta       map[string]interface{}
	EnqueuedAt float64
	SLOType    *string
	SLOTtftMs  *float64
	SLOTpotMs  *float64
	SLOE2eMs   *float64
	TaskType   *string
	OutputLen  *int
}

func nowS() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}
