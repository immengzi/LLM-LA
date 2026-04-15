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

type VLLMWorker struct {
	id         int
	cfg        *Config
	queue      *LocalQueue
	poster     *ResultPoster
	puller     *RouterPullWorker // nil in push mode
	endpointID string
	client     *http.Client
	busyCount  *atomic.Int64
}

func NewVLLMWorker(
	id int,
	cfg *Config,
	queue *LocalQueue,
	poster *ResultPoster,
	puller *RouterPullWorker,
	endpointID string,
	busyCount *atomic.Int64,
) *VLLMWorker {
	return &VLLMWorker{
		id:         id,
		cfg:        cfg,
		queue:      queue,
		poster:     poster,
		puller:     puller,
		endpointID: endpointID,
		busyCount:  busyCount,
		client: &http.Client{
			Timeout: time.Duration(cfg.VLLMTimeoutS * float64(time.Second)),
		},
	}
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model              string        `json:"model"`
	Messages           []chatMessage `json:"messages"`
	MaxTokens          int           `json:"max_tokens"`
	Temperature        float64       `json:"temperature"`
	ChatTemplateKwargs map[string]any `json:"chat_template_kwargs,omitempty"`
}

func (w *VLLMWorker) Start() {
	go w.loop()
}

func (w *VLLMWorker) loop() {
	idleTicks := 0
	for {
		item, ok := w.queue.GetNoWait()
		if !ok {
			idleTicks++
			if idleTicks%100 == 0 && w.puller != nil {
				w.puller.PullIfCapacity()
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		idleTicks = 0

		w.busyCount.Add(1)
		WorkersBusy.WithLabelValues(w.endpointID).Set(float64(w.busyCount.Load()))

		w.process(item)

		w.busyCount.Add(-1)
		WorkersBusy.WithLabelValues(w.endpointID).Set(float64(w.busyCount.Load()))
		w.queue.TaskDone()

		CompletedRequests.WithLabelValues(w.endpointID).Inc()

		if w.puller != nil {
			w.puller.PullIfCapacity()
		}
	}
}

func (w *VLLMWorker) process(item QueueItem) {
	start := time.Now()

	maxTokens := 128
	if v, ok := item.Meta["max_tokens"]; ok {
		switch t := v.(type) {
		case float64:
			maxTokens = int(t)
		case int:
			maxTokens = t
		case json.Number:
			if n, err := t.Int64(); err == nil {
				maxTokens = int(n)
			}
		}
	}

	temperature := 0.0
	if v, ok := item.Meta["temperature"]; ok {
		switch t := v.(type) {
		case float64:
			temperature = t
		case json.Number:
			if f, err := t.Float64(); err == nil {
				temperature = f
			}
		}
	}

	enableThinking := false
	if v, ok := item.Meta["enable_thinking"]; ok {
		if b, ok2 := v.(bool); ok2 {
			enableThinking = b
		}
	}

	req := chatRequest{
		Model:       w.cfg.ModelName,
		Messages:    []chatMessage{{Role: "user", Content: item.Prompt}},
		MaxTokens:   maxTokens,
		Temperature: temperature,
		ChatTemplateKwargs: map[string]any{
			"enable_thinking": enableThinking,
		},
	}

	body, err := json.Marshal(req)
	if err != nil {
		log.Printf("[worker-%d] marshal error: %v", w.id, err)
		w.submitError(item.ReqID, err, time.Since(start), item.Meta)
		return
	}

	url := fmt.Sprintf("%s/v1/chat/completions", w.cfg.VLLMURL)
	resp, err := w.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[worker-%d] vllm request error: %v", w.id, err)
		w.submitError(item.ReqID, err, time.Since(start), item.Meta)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[worker-%d] read response error: %v", w.id, err)
		w.submitError(item.ReqID, err, time.Since(start), item.Meta)
		return
	}

	latency := time.Since(start).Seconds()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[worker-%d] vllm non-200: %d body=%s", w.id, resp.StatusCode, string(respBody))
		w.submitError(item.ReqID, fmt.Errorf("vllm status %d", resp.StatusCode), time.Since(start), item.Meta)
		return
	}

	var raw map[string]any
	if err := json.Unmarshal(respBody, &raw); err != nil {
		log.Printf("[worker-%d] decode error: %v", w.id, err)
		w.submitError(item.ReqID, err, time.Since(start), item.Meta)
		return
	}

	output, finishReason, usage := extractChatResult(raw)

	result := map[string]any{
		"req_id": item.ReqID,
		"result": map[string]any{
			"output":        output,
			"finish_reason": finishReason,
			"latency_s":     latency,
			"raw":           raw,
			"usage":         usage,
		},
	}

	if w.cfg.TraceEnabled {
		trace := map[string]any{
			"vllm_start_ts":  float64(start.UnixNano()) / 1e9,
			"vllm_end_ts":    float64(time.Now().UnixNano()) / 1e9,
			"vllm_latency_s": latency,
		}
		if pullTS, ok := item.Meta["_trace_pull_ts"]; ok {
			trace["pull_ts"] = pullTS
		}
		result["result"].(map[string]any)["trace"] = trace
	}

	w.poster.Submit(result)
}

func extractChatResult(raw map[string]any) (string, string, map[string]any) {
	choices, ok := raw["choices"].([]any)
	if !ok || len(choices) == 0 {
		return "", "", nil
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		return "", "", nil
	}

	finishReason, _ := choice["finish_reason"].(string)

	msg, ok := choice["message"].(map[string]any)
	if !ok {
		return "", finishReason, nil
	}
	content, _ := msg["content"].(string)

	usage, _ := raw["usage"].(map[string]any)

	return content, finishReason, usage
}

func (w *VLLMWorker) submitError(reqID string, err error, elapsed time.Duration, meta map[string]any) {
	result := map[string]any{
		"req_id": reqID,
		"result": map[string]any{
			"output":        "",
			"finish_reason": "error",
			"latency_s":     elapsed.Seconds(),
			"error":         err.Error(),
		},
	}
	if w.cfg.TraceEnabled {
		if pullTS, ok := meta["_trace_pull_ts"]; ok {
			result["result"].(map[string]any)["trace"] = map[string]any{"pull_ts": pullTS}
		}
	}
	w.poster.Submit(result)
}
