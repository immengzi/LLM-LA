package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// VLLMWorker dequeues items and sends them to the local vLLM server.
type VLLMWorker struct {
	id     int
	queue  *LocalQueue
	poster *ResultPoster
	cfg    *Config
	client *http.Client
	wake   chan struct{} // signal when new items arrive
}

func NewVLLMWorker(id int, queue *LocalQueue, poster *ResultPoster, cfg *Config, wake chan struct{}) *VLLMWorker {
	return &VLLMWorker{
		id:    id,
		queue: queue,
		poster: poster,
		cfg:   cfg,
		client: &http.Client{
			Timeout: time.Duration(cfg.VllmTimeoutS * float64(time.Second)),
		},
		wake: wake,
	}
}

func (w *VLLMWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		item, ok := w.queue.Pop()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-w.wake:
				continue
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		w.process(ctx, item)
		w.queue.TaskDone()
	}
}

func (w *VLLMWorker) process(ctx context.Context, item Item) {
	maxTokens := 128
	if mt, ok := item.Meta["max_tokens"]; ok {
		if n, ok := mt.(float64); ok {
			maxTokens = int(n)
		}
	}

	temperature := 0.0
	if t, ok := item.Meta["temperature"]; ok {
		if f, ok := t.(float64); ok {
			temperature = f
		}
	}

	enableThinking := false
	if et, ok := item.Meta["enable_thinking"]; ok {
		if b, ok := et.(bool); ok {
			enableThinking = b
		}
	}

	reqBody := map[string]interface{}{
		"model": w.cfg.ModelName,
		"messages": []map[string]string{
			{"role": "user", "content": item.Prompt},
		},
		"max_tokens":  maxTokens,
		"temperature": temperature,
	}
	if enableThinking {
		reqBody["chat_template_kwargs"] = map[string]interface{}{
			"enable_thinking": true,
		}
	}

	body, _ := json.Marshal(reqBody)
	url := fmt.Sprintf("%s/v1/chat/completions", w.cfg.VllmURL)
	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")

	tStart := time.Now()
	resp, err := w.client.Do(httpReq)
	elapsed := time.Since(tStart)

	if err != nil {
		log.Printf("[worker-%d] vllm error req_id=%s: %v", w.id, item.ReqID, err)
		w.poster.Submit(item.ReqID, map[string]interface{}{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	var result map[string]interface{}
	if err := json.Unmarshal(respBytes, &result); err != nil {
		log.Printf("[worker-%d] vllm decode error req_id=%s: %v", w.id, item.ReqID, err)
		w.poster.Submit(item.ReqID, map[string]interface{}{"error": "decode_error", "raw": string(respBytes)})
		return
	}

	result["vllm_elapsed_s"] = elapsed.Seconds()
	w.poster.Submit(item.ReqID, result)
}
