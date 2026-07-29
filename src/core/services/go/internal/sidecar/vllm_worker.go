package sidecar

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type InferenceWorker struct {
	id          int
	cfg         *Config
	queue       *LocalQueue
	poster      *ResultPoster
	puller      *RouterPullWorker // nil in push mode
	endpointID  string
	client      *http.Client
	chunkClient *http.Client
	busyCount   *atomic.Int64
}

func NewInferenceWorker(
	id int,
	cfg *Config,
	queue *LocalQueue,
	poster *ResultPoster,
	puller *RouterPullWorker,
	endpointID string,
	busyCount *atomic.Int64,
) *InferenceWorker {
	return &InferenceWorker{
		id:         id,
		cfg:        cfg,
		queue:      queue,
		poster:     poster,
		puller:     puller,
		endpointID: endpointID,
		busyCount:  busyCount,
		client: &http.Client{
			Timeout: time.Duration(cfg.InferenceTimeoutS * float64(time.Second)),
		},
		chunkClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// VLLMWorker is retained as a source-compatible alias.
type VLLMWorker = InferenceWorker

func NewVLLMWorker(id int, cfg *Config, queue *LocalQueue, poster *ResultPoster, puller *RouterPullWorker, endpointID string, busyCount *atomic.Int64) *VLLMWorker {
	return NewInferenceWorker(id, cfg, queue, poster, puller, endpointID, busyCount)
}

func (w *InferenceWorker) Start() {
	go w.loop()
}

func (w *InferenceWorker) loop() {
	const idleSleep = 5 * time.Millisecond
	for {
		item, ok := w.queue.GetNoWait()
		if !ok {
			time.Sleep(idleSleep)
			continue
		}

		busyNow := w.busyCount.Add(1)
		WorkersBusy.WithLabelValues(w.endpointID).Set(float64(busyNow))

		w.process(item)

		busyNow = w.busyCount.Add(-1)
		if busyNow < 0 {
			busyNow = 0
		}
		WorkersBusy.WithLabelValues(w.endpointID).Set(float64(busyNow))

		w.queue.TaskDone()

		if w.puller != nil {
			w.puller.PullIfCapacity()
		}
	}
}

// vllmResult bundles the outcome of a single vLLM call.
type vllmResult struct {
	outputText   string
	finishReason string
	hasFinish    bool
	usage        map[string]any
	rawVllm      map[string]any
	latencyS     float64
	hasLatency   bool
	ttftS        float64
	hasTtft      bool
}

func (w *InferenceWorker) process(item QueueItem) {
	reqID := item.ReqID
	prompt := item.Prompt
	meta := item.Meta
	if meta == nil {
		meta = map[string]any{}
	}

	if w.cfg.TraceEnabled {
		st := w.queue.State()
		tr := traceMap(meta)
		tr["t_dequeue_sidecar"] = nowF()
		tr["sidecar_queue_len_at_dequeue"] = st.Pending
		tr["sidecar_inflight_at_dequeue"] = st.Inflight
		tr["sidecar_logical_at_dequeue"] = st.Pending + st.Inflight
		meta["__trace__"] = tr
	}

	payload, forwardStream := w.buildPayload(prompt, meta)

	if w.cfg.TraceEnabled {
		tr := traceMap(meta)
		tr["t_vllm_send"] = nowF()
		meta["__trace__"] = tr
	}

	useStream := w.cfg.StreamingMode

	var res vllmResult
	var err error
	if useStream {
		payload["stream"] = true
		payload["stream_options"] = map[string]any{"include_usage": true}
		res, err = w.callStreaming(reqID, payload, forwardStream)
	} else {
		res, err = w.callNonStreaming(reqID, payload)
	}

	if err != nil {
		log.Printf("[sidecar] inference request failed for req_id=%s: %v", reqID, err)
		w.submitError(reqID, err)
		return
	}

	if w.cfg.TraceEnabled {
		tr := traceMap(meta)
		tr["t_vllm_recv"] = nowF()
		if res.hasTtft {
			tr["ttft_sidecar_s"] = res.ttftS
		}
		st := w.queue.State()
		tr["sidecar_queue_len_at_result"] = st.Pending
		tr["sidecar_inflight_at_result"] = st.Inflight
		tr["sidecar_logical_at_result"] = st.Pending + st.Inflight
		tr["t_post_result_sidecar"] = nowF()
		meta["__trace__"] = tr
	}

	resultObj := map[string]any{
		"output":      res.outputText,
		"endpoint_id": w.cfg.ContainerName,
	}
	if res.hasFinish {
		resultObj["finish_reason"] = res.finishReason
	}
	if res.rawVllm != nil {
		if tc := extractToolCalls(res.rawVllm); len(tc) > 0 {
			resultObj["tool_calls"] = tc
		}
	}
	if res.hasLatency {
		resultObj["latency_s"] = res.latencyS
	}
	if res.hasTtft {
		resultObj["ttft_sidecar_s"] = res.ttftS
	}
	if res.rawVllm != nil {
		resultObj["raw"] = res.rawVllm
	}
	if res.usage != nil {
		resultObj["usage"] = res.usage
	}
	if w.cfg.TraceEnabled {
		resultObj["trace"] = traceMap(meta)
	}

	CompletedRequests.WithLabelValues(w.cfg.ContainerName).Inc()

	// Top-level "endpoint" is the completion signal the gateway uses to
	// decrement its per-endpoint in-flight counters (push-leastq NotifyResult
	// and pull-fairness release). Sent explicitly and symmetrically with the
	// error path in submitError.
	w.poster.Submit(map[string]any{
		"req_id":   reqID,
		"endpoint": w.cfg.ContainerName,
		"result":   resultObj,
	})
}

// buildPayload constructs the vLLM request body and reports whether the
// originating client requested a streamed response (forward_stream).
func (w *InferenceWorker) buildPayload(prompt string, meta map[string]any) (map[string]any, bool) {
	payload := map[string]any{}
	forwardStream := false

	if cr, ok := meta["__chat_request__"].(map[string]any); ok {
		for k, v := range cr {
			payload[k] = v
		}
		payload["model"] = w.cfg.ModelName
		payload["stream"] = false
		if s, ok := cr["stream"].(bool); ok {
			forwardStream = s
		}
	} else {
		payload["model"] = w.cfg.ModelName
		payload["messages"] = []map[string]any{{"role": "user", "content": prompt}}
		payload["max_tokens"] = metaInt(meta, "max_tokens", 128)
		payload["temperature"] = metaFloat(meta, "temperature", 0.0)
		// Preserve the historical vLLM payload exactly. SGLang must use its
		// tokenizer default unless the caller explicitly supplies an override;
		// forcing false can make inference tokenization diverge from KV hashing.
		if w.cfg.InferenceEngine != "sglang" {
			payload["chat_template_kwargs"] = map[string]any{
				"enable_thinking": metaBool(meta, "enable_thinking", false),
			}
		} else if _, ok := meta["enable_thinking"]; ok {
			payload["chat_template_kwargs"] = map[string]any{
				"enable_thinking": metaBool(meta, "enable_thinking", false),
			}
		}
		if _, ok := meta["min_tokens"]; ok {
			payload["min_tokens"] = metaInt(meta, "min_tokens", 0)
		}
	}

	if v, ok := meta["ignore_eos"]; ok && toBool(v) {
		payload["ignore_eos"] = true
	}
	if w.cfg.ForceIgnoreEos {
		payload["ignore_eos"] = true
	}

	return payload, forwardStream
}

func (w *InferenceWorker) callNonStreaming(reqID string, payload map[string]any) (vllmResult, error) {
	var res vllmResult

	body, err := json.Marshal(payload)
	if err != nil {
		return res, err
	}

	url := fmt.Sprintf("%s/v1/chat/completions", w.cfg.InferenceURL)
	tSend := time.Now()
	resp, err := w.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return res, err
	}
	defer resp.Body.Close()

	res.latencyS = time.Since(tSend).Seconds()
	res.hasLatency = true

	dec := json.NewDecoder(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		res.outputText = fmt.Sprintf("[%s error %d]", w.engineLabel(), resp.StatusCode)
		return res, nil
	}

	var data map[string]any
	if err := dec.Decode(&data); err != nil {
		log.Printf("[sidecar] parse error for req_id=%s: %v", reqID, err)
		res.outputText = fmt.Sprintf("[parse error in %s response]", w.engineLabel())
		return res, nil
	}
	res.rawVllm = data

	if choices, ok := data["choices"].([]any); ok && len(choices) > 0 {
		if first, ok := choices[0].(map[string]any); ok {
			if msg, ok := first["message"].(map[string]any); ok {
				if c, ok := msg["content"].(string); ok && c != "" {
					res.outputText = c
				} else {
					res.outputText = fmt.Sprintf("%v", first)
				}
			} else {
				res.outputText = fmt.Sprintf("%v", first)
			}
			if fr, ok := first["finish_reason"].(string); ok {
				res.finishReason = fr
				res.hasFinish = true
			} else if fr, ok := data["finish_reason"].(string); ok {
				res.finishReason = fr
				res.hasFinish = true
			}
		}
	} else {
		res.outputText = fmt.Sprintf("%v", data)
	}

	if u, ok := data["usage"].(map[string]any); ok {
		res.usage = u
	}
	return res, nil
}

func (w *InferenceWorker) callStreaming(reqID string, payload map[string]any, forwardStream bool) (vllmResult, error) {
	var res vllmResult

	body, err := json.Marshal(payload)
	if err != nil {
		return res, err
	}

	url := fmt.Sprintf("%s/v1/chat/completions", w.cfg.InferenceURL)
	routerChunkURL := fmt.Sprintf("%s/result_chunk", w.cfg.RouterURL)

	tSend := time.Now()
	resp, err := w.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return res, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("[sidecar] inference stream error: %d", resp.StatusCode)
		res.outputText = fmt.Sprintf("[%s error %d]", w.engineLabel(), resp.StatusCode)
		return res, nil
	}

	var parts []string
	toolAcc := map[int]map[string]any{}
	var streamID, streamModel string
	var streamCreated int64
	var hasCreated bool
	var tFirstToken time.Time
	var hasFirstToken bool
	chunkIdx := 0
	var pendingFinal map[string]any

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r\n")
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		dataStr := line[6:]
		if strings.TrimSpace(dataStr) == "[DONE]" {
			break
		}
		var chunk map[string]any
		if err := json.Unmarshal([]byte(dataStr), &chunk); err != nil {
			continue
		}

		if streamID == "" {
			if s, ok := chunk["id"].(string); ok {
				streamID = s
			}
		}
		if !hasCreated {
			if c, ok := chunk["created"].(float64); ok {
				streamCreated = int64(c)
				hasCreated = true
			}
		}
		if streamModel == "" {
			if s, ok := chunk["model"].(string); ok {
				streamModel = s
			}
		}

		var deltaContent string
		var deltaToolCalls []any
		var chunkFR string
		var hasChunkFR bool

		if choices, ok := chunk["choices"].([]any); ok && len(choices) > 0 {
			if c0, ok := choices[0].(map[string]any); ok {
				if delta, ok := c0["delta"].(map[string]any); ok {
					if content, ok := delta["content"].(string); ok && content != "" {
						if !hasFirstToken {
							tFirstToken = time.Now()
							hasFirstToken = true
						}
						parts = append(parts, content)
						deltaContent = content
					}
					if tcs, ok := delta["tool_calls"].([]any); ok && len(tcs) > 0 {
						if !hasFirstToken {
							tFirstToken = time.Now()
							hasFirstToken = true
						}
						deltaToolCalls = tcs
						mergeToolCallDelta(toolAcc, tcs)
					}
				}
				if fr, ok := c0["finish_reason"].(string); ok {
					res.finishReason = fr
					res.hasFinish = true
					chunkFR = fr
					hasChunkFR = true
				}
			}
		}

		if u, ok := chunk["usage"].(map[string]any); ok && len(u) > 0 {
			res.usage = u
		}

		// If a buffered is_final is waiting and usage just arrived, flush it
		if forwardStream && pendingFinal != nil && res.usage != nil {
			pendingFinal["usage"] = res.usage
			w.forwardChunk(routerChunkURL, pendingFinal)
			chunkIdx++
			pendingFinal = nil
		}

		if forwardStream && (deltaContent != "" || len(deltaToolCalls) > 0 || hasChunkFR) {
			isFinal := hasChunkFR
			chunkPayload := map[string]any{
				"req_id":      reqID,
				"chunk_idx":   chunkIdx,
				"delta":       deltaContent,
				"is_final":    isFinal,
				"endpoint_id": w.cfg.ContainerName,
			}
			if len(deltaToolCalls) > 0 {
				chunkPayload["tool_calls"] = deltaToolCalls
			}
			if hasChunkFR {
				chunkPayload["finish_reason"] = chunkFR
			}
			if isFinal && res.usage != nil {
				chunkPayload["usage"] = res.usage
				w.forwardChunk(routerChunkURL, chunkPayload)
				chunkIdx++
			} else if isFinal {
				// Buffer: wait for the trailing usage chunk from vLLM
				pendingFinal = chunkPayload
			} else {
				w.forwardChunk(routerChunkURL, chunkPayload)
				chunkIdx++
			}
		}
	}

	// Flush any buffered is_final that never got a usage chunk
	if forwardStream && pendingFinal != nil {
		if res.usage != nil {
			pendingFinal["usage"] = res.usage
		}
		w.forwardChunk(routerChunkURL, pendingFinal)
	}

	res.outputText = strings.Join(parts, "")
	res.latencyS = time.Since(tSend).Seconds()
	res.hasLatency = true

	completedToolCalls := completeToolCalls(toolAcc)
	if streamID == "" {
		streamID = fmt.Sprintf("chatcmpl-%s", reqID)
	}
	if !hasCreated {
		streamCreated = tSend.Unix()
	}
	if streamModel == "" {
		streamModel = w.cfg.ModelName
	}
	usageOut := res.usage
	if usageOut == nil {
		usageOut = map[string]any{}
	}
	var finishOut any
	if res.hasFinish {
		finishOut = res.finishReason
	}
	res.rawVllm = map[string]any{
		"id":      streamID,
		"object":  "chat.completion",
		"created": streamCreated,
		"model":   streamModel,
		"choices": []any{
			map[string]any{
				"index": 0,
				"message": map[string]any{
					"role":       "assistant",
					"content":    res.outputText,
					"tool_calls": completedToolCalls,
				},
				"finish_reason": finishOut,
			},
		},
		"usage": usageOut,
	}

	if hasFirstToken {
		res.ttftS = tFirstToken.Sub(tSend).Seconds()
		res.hasTtft = true
	}

	return res, nil
}

func (w *InferenceWorker) forwardChunk(url string, payload map[string]any) {
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	resp, err := w.chunkClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		if w.cfg.DebugEnabled() {
			log.Printf("[sidecar] chunk forward failed: %v", err)
		}
		return
	}
	resp.Body.Close()
}

func (w *InferenceWorker) submitError(reqID string, err error) {
	// An errored request still consumed a slot, so carry the top-level
	// "endpoint" here too or the gateway's in-flight counter leaks on failure.
	w.poster.Submit(map[string]any{
		"req_id":   reqID,
		"endpoint": w.cfg.ContainerName,
		"result": map[string]any{
			"output":        fmt.Sprintf("[sidecar error: %v]", err),
			"finish_reason": "error",
			"error":         err.Error(),
		},
	})
}

func (w *InferenceWorker) engineLabel() string {
	if w.cfg.InferenceEngine == "vllm" || w.cfg.InferenceEngine == "" {
		return "vLLM"
	}
	return "inference"
}

// extractToolCalls pulls choices[0].message.tool_calls from a raw vLLM response.
func extractToolCalls(raw map[string]any) []any {
	choices, ok := raw["choices"].([]any)
	if !ok || len(choices) == 0 {
		return nil
	}
	c0, ok := choices[0].(map[string]any)
	if !ok {
		return nil
	}
	msg, ok := c0["message"].(map[string]any)
	if !ok {
		return nil
	}
	tc, ok := msg["tool_calls"].([]any)
	if !ok || len(tc) == 0 {
		return nil
	}
	return tc
}

// mergeToolCallDelta accumulates OpenAI streaming tool_call deltas by index,
// mirroring _merge_tool_call_delta in vllm_client.py.
func mergeToolCallDelta(acc map[int]map[string]any, toolCalls []any) {
	for i, raw := range toolCalls {
		tc, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		idx := i
		if v, ok := tc["index"]; ok {
			idx = toInt(v, i)
		}
		cur, exists := acc[idx]
		if !exists {
			id, _ := tc["id"].(string)
			if id == "" {
				id = fmt.Sprintf("call_%d", idx)
			}
			typ, _ := tc["type"].(string)
			if typ == "" {
				typ = "function"
			}
			cur = map[string]any{
				"id":       id,
				"type":     typ,
				"function": map[string]any{"name": "", "arguments": ""},
			}
			acc[idx] = cur
		}
		if id, ok := tc["id"].(string); ok && id != "" {
			cur["id"] = id
		}
		if typ, ok := tc["type"].(string); ok && typ != "" {
			cur["type"] = typ
		}
		if fn, ok := tc["function"].(map[string]any); ok {
			curFn, ok := cur["function"].(map[string]any)
			if !ok {
				curFn = map[string]any{"name": "", "arguments": ""}
				cur["function"] = curFn
			}
			if name, ok := fn["name"].(string); ok && name != "" {
				curFn["name"] = name
			}
			if args, ok := fn["arguments"].(string); ok {
				prev, _ := curFn["arguments"].(string)
				curFn["arguments"] = prev + args
			}
		}
	}
}

// completeToolCalls returns finished tool_calls ordered by streaming index,
// mirroring _complete_tool_calls in vllm_client.py.
func completeToolCalls(acc map[int]map[string]any) []any {
	idxs := make([]int, 0, len(acc))
	for k := range acc {
		idxs = append(idxs, k)
	}
	sort.Ints(idxs)

	out := make([]any, 0, len(idxs))
	for _, idx := range idxs {
		tc := acc[idx]
		fn, ok := tc["function"].(map[string]any)
		if !ok {
			continue
		}
		name, _ := fn["name"].(string)
		if name == "" {
			continue
		}
		args, _ := fn["arguments"].(string)
		id, _ := tc["id"].(string)
		if id == "" {
			id = fmt.Sprintf("call_%d", idx)
		}
		typ, _ := tc["type"].(string)
		if typ == "" {
			typ = "function"
		}
		out = append(out, map[string]any{
			"id":   id,
			"type": typ,
			"function": map[string]any{
				"name":      name,
				"arguments": args,
			},
		})
	}
	return out
}

func nowF() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func metaInt(meta map[string]any, key string, def int) int {
	v, ok := meta[key]
	if !ok {
		return def
	}
	return toInt(v, def)
}

func metaFloat(meta map[string]any, key string, def float64) float64 {
	v, ok := meta[key]
	if !ok {
		return def
	}
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f
		}
	}
	return def
}

func metaBool(meta map[string]any, key string, def bool) bool {
	v, ok := meta[key]
	if !ok {
		return def
	}
	return toBool(v)
}

func toInt(v any, def int) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return int(n)
		}
		if f, err := t.Float64(); err == nil {
			return int(f)
		}
	}
	return def
}

func toBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case float64:
		return t != 0
	case int:
		return t != 0
	case string:
		return t != "" && t != "false" && t != "0"
	}
	return false
}
