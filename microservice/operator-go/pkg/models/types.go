package models

// Request is a prompt queued in the router's central queue.
type Request struct {
	ReqID           string                 `json:"req_id"`
	Prompt          string                 `json:"prompt"`
	Meta            map[string]interface{} `json:"meta,omitempty"`
	ClientEnqueueTS float64                `json:"t_enq_client,omitempty"`
	RouterEnqueueTS float64                `json:"-"`
}

// Result holds the completed output from vLLM, returned to clients.
type Result struct {
	ReqID    string                 `json:"req_id"`
	Result   map[string]interface{} `json:"result,omitempty"`
	Endpoint string                 `json:"endpoint,omitempty"`
	Trace    map[string]interface{} `json:"trace,omitempty"`
}

// --- HTTP request / response bodies (matching Python models) ---

type EnqueueRequest struct {
	Prompt          string                 `json:"prompt"`
	ReqID           string                 `json:"req_id,omitempty"`
	ClientEnqueueTS float64                `json:"t_enq_client,omitempty"`
	Meta            map[string]interface{} `json:"meta,omitempty"`
}

type SubmitResponse struct {
	ReqID string `json:"req_id"`
}

type EnqueueResponse struct {
	ReqID  string                 `json:"req_id"`
	Result map[string]interface{} `json:"result"`
}

type PullRequest struct {
	Endpoint string `json:"endpoint"`
	Want     int    `json:"want"`
}

type JobItem struct {
	ReqID           string                 `json:"req_id"`
	Prompt          string                 `json:"prompt"`
	ClientEnqueueTS float64                `json:"t_enq_client,omitempty"`
	Meta            map[string]interface{} `json:"meta,omitempty"`
}

type PullResponse struct {
	Items []JobItem `json:"items"`
}

type ResultPayload struct {
	ReqID    string                 `json:"req_id"`
	Result   map[string]interface{} `json:"result,omitempty"`
	Output   string                 `json:"output,omitempty"`
	Endpoint string                 `json:"endpoint,omitempty"`
	Trace    map[string]interface{} `json:"trace,omitempty"`
}

type HealthResponse struct {
	Status   string `json:"status"`
	QueueLen int    `json:"queue_len"`
}

type StatusResponse struct {
	Status string `json:"status"`
}
