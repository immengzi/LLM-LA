package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// chatMessage is a parsed OpenAI chat message (content normalized to a string).
type chatMessage struct {
	role    string
	content string
}

// parseChatMessages extracts messages from the raw request body, normalizing
// content blocks (string or array of {type:text,text}) like the Python
// _ChatMessage validator.
func parseChatMessages(raw map[string]interface{}) []chatMessage {
	msgsRaw, _ := raw["messages"].([]interface{})
	out := make([]chatMessage, 0, len(msgsRaw))
	for _, mi := range msgsRaw {
		m, ok := mi.(map[string]interface{})
		if !ok {
			continue
		}
		role, _ := m["role"].(string)
		out = append(out, chatMessage{role: role, content: normalizeContent(m["content"])})
	}
	return out
}

func normalizeContent(v interface{}) string {
	switch c := v.(type) {
	case nil:
		return ""
	case string:
		return c
	case []interface{}:
		var parts []string
		for _, block := range c {
			switch b := block.(type) {
			case string:
				parts = append(parts, b)
			case map[string]interface{}:
				if t, _ := b["type"].(string); t == "text" {
					if txt, ok := b["text"].(string); ok {
						parts = append(parts, txt)
					} else {
						parts = append(parts, "")
					}
				}
			}
		}
		return strings.Join(parts, "\n")
	default:
		return fmt.Sprintf("%v", c)
	}
}

// messagesToPrompt mirrors _messages_to_prompt.
func messagesToPrompt(messages []chatMessage) string {
	parts := make([]string, 0, len(messages))
	for _, msg := range messages {
		role := strings.TrimSpace(strings.ToLower(msg.role))
		content := strings.TrimSpace(msg.content)
		switch role {
		case "system":
			parts = append(parts, "System: "+content)
		case "user":
			parts = append(parts, "User: "+content)
		case "assistant":
			parts = append(parts, "Assistant: "+content)
		case "tool":
			parts = append(parts, "Tool: "+content)
		default:
			parts = append(parts, content)
		}
	}
	return strings.Join(parts, "\n")
}

// checkAPIKey mirrors _check_api_key.
func (s *Server) checkAPIKey(r *http.Request) bool {
	key := s.cfg.APIKey
	if key == "" {
		return true
	}
	auth := r.Header.Get("authorization")
	if strings.HasPrefix(auth, "Bearer ") && auth[7:] == key {
		return true
	}
	if r.Header.Get("api-key") == key {
		return true
	}
	if r.Header.Get("x-api-key") == key {
		return true
	}
	return false
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if !s.checkAPIKey(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"detail": "Invalid or missing API key"})
		return
	}

	var raw map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	model, _ := raw["model"].(string)
	if model == "" {
		model = "served-model"
		raw["model"] = "served-model"
	}
	messages := parseChatMessages(raw)
	prompt := messagesToPrompt(messages)

	stream, _ := raw["stream"].(bool)

	hasToolMessages := false
	for _, m := range messages {
		if strings.ToLower(strings.TrimSpace(m.role)) == "tool" {
			hasToolMessages = true
			break
		}
	}
	_ = hasToolMessages

	// Reconstruct forwarded body (drop stream_options).
	chatBody := cloneMeta(raw)
	delete(chatBody, "stream_options")

	resolvedModel, ok := s.resolveModel(model)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("Unknown model '%s'. Available: %v", model, s.registry.Names())})
		return
	}

	if stream {
		s.chatStream(w, r, prompt, model, resolvedModel, chatBody)
		return
	}

	rid, tStart, result := s.enqueueAndWait(w, prompt, resolvedModel, chatBody)
	if result == nil {
		return // error already written
	}

	tDone := nowS()
	e2eS := tDone - tStart

	outputText, _ := result["output"].(string)
	finishReason := "stop"
	if fr, ok := result["finish_reason"].(string); ok && fr != "" {
		finishReason = fr
	}
	usage := mapOf(result["usage"])
	endpointID := result["endpoint_id"]
	toolCalls := extractToolCalls(result)

	var ttftS *float64
	if v, ok := result["ttft_sidecar_s"].(float64); ok {
		ttftS = &v
	}
	completionTokens := toInt(usage["completion_tokens"])
	var tpotAvg *float64
	if ttftS != nil && completionTokens > 1 {
		decode := e2eS - *ttftS
		if decode > 0 {
			t := decode / float64(completionTokens-1)
			tpotAvg = &t
		}
	}

	observeRequestE2E(e2eS, model)
	if ttftS != nil {
		observeRequestTTFT(*ttftS, model)
	}
	if tpotAvg != nil {
		observeRequestTPOTAvg(*tpotAvg, model)
	}

	xlat := map[string]interface{}{"e2e_ms": round2(e2eS * 1000)}
	if ttftS != nil {
		xlat["ttft_ms"] = round2(*ttftS * 1000)
	}
	if tpotAvg != nil {
		xlat["tpot_avg_ms"] = round2(*tpotAvg * 1000)
	}
	if endpointID != nil {
		xlat["endpoint"] = fmt.Sprintf("%v", endpointID)
	}
	lat := map[string]interface{}{
		"ts": nowS(), "t_start": tStart, "rid": rid, "model": model, "stream": false,
		"finish_reason":     finishReason,
		"prompt_tokens":     toInt(usage["prompt_tokens"]),
		"completion_tokens": completionTokens,
	}
	for k, v := range xlat {
		lat[k] = v
	}
	s.attachRequestBody(lat, chatBody)
	s.recordLatency(lat)

	message := map[string]interface{}{"role": "assistant"}
	if len(toolCalls) > 0 {
		message["content"] = nil
		message["tool_calls"] = toolCalls
	} else {
		message["content"] = outputText
	}

	usageOut := passthroughUsage(usage)
	usageOut["completion_tokens"] = completionTokens

	resp := map[string]interface{}{
		"id":      "chatcmpl-" + rid,
		"object":  "chat.completion",
		"created": int64(tStart),
		"model":   model,
		"choices": []map[string]interface{}{
			{"index": 0, "message": message, "finish_reason": finishReason},
		},
		"usage": usageOut,
	}
	if endpointID != nil {
		resp["system_fingerprint"] = fmt.Sprintf("%v", endpointID)
	}
	writeJSON(w, http.StatusOK, resp)
}

// enqueueAndWait mirrors _enqueue_and_wait (chat path). Writes the error
// response itself on timeout and returns a nil result in that case.
func (s *Server) enqueueAndWait(w http.ResponseWriter, prompt, resolvedModel string, chatBody map[string]interface{}) (string, float64, map[string]interface{}) {
	tStart := nowS()
	incAdmission()

	meta := map[string]interface{}{"__source__": "litellm", "__chat_request__": chatBody}
	s.injectAffinity(meta, resolvedModel, chatBody)

	var rid string
	isPull := !s.isPushMode()
	if s.isPushMode() {
		rid = s.queue.NextReqID()
	} else {
		rid = s.queue.Enqueue(prompt, tStart, meta, "", resolvedModel)
	}

	s.logReq("chat_completions rid=%s prompt_len=%d", rid, len(prompt))
	s.results.Register(rid)
	s.dispatch(rid, prompt, meta, isPull)

	timeout := time.Duration(s.cfg.ResultTimeoutS * float64(time.Second))
	result := s.results.WaitFor(rid, timeout)
	if result == nil {
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timeout waiting for vLLM result"})
		return rid, tStart, nil
	}
	return rid, tStart, result
}

// truncateBodyForLog returns body as-is when it serializes within the
// configured byte cap, else a bounded marker. Cap of 0 means unlimited.
// Mirrors the Python/client truncation so logs.json bodies share one shape.
func (s *Server) truncateBodyForLog(body map[string]interface{}) interface{} {
	maxBytes := s.cfg.LogRequestBodyMaxBytes
	b, err := json.Marshal(body)
	if err != nil {
		return body
	}
	if maxBytes > 0 && len(b) > maxBytes {
		preview := string(b)
		if len(preview) > maxBytes {
			preview = preview[:maxBytes]
		}
		return map[string]interface{}{
			"_truncated": true,
			"bytes":      len(b),
			"preview":    preview,
		}
	}
	return body
}

// attachRequestBody adds a truncated "request_body" to a latency ring entry
// when ROUTER_LOG_REQUEST_BODY is enabled. No-op otherwise (zero cost).
func (s *Server) attachRequestBody(lat map[string]interface{}, body map[string]interface{}) {
	if !s.cfg.LogRequestBody || body == nil {
		return
	}
	lat["request_body"] = s.truncateBodyForLog(body)
}

func (s *Server) chatStream(w http.ResponseWriter, r *http.Request, prompt, model, resolvedModel string, chatBody map[string]interface{}) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}

	tStart := nowS()
	incAdmission()

	meta := map[string]interface{}{"__source__": "litellm", "__chat_request__": chatBody}
	s.injectAffinity(meta, resolvedModel, chatBody)
	var rid string
	isPull := !s.isPushMode()
	if s.isPushMode() {
		rid = s.queue.NextReqID()
	} else {
		rid = s.queue.Enqueue(prompt, tStart, meta, "", resolvedModel)
	}

	chunkQ := s.queue.RegisterChunkQueue(rid)
	defer s.queue.RemoveChunkQueue(rid)

	s.logReq("chat_completions_stream rid=%s prompt_len=%d", rid, len(prompt))
	s.results.Register(rid)
	s.dispatch(rid, prompt, meta, isPull)

	// Fallback: if the full result arrives (non-streaming sidecar), push it.
	go func() {
		timeout := time.Duration(s.cfg.ResultTimeoutS * float64(time.Second))
		result := s.results.WaitFor(rid, timeout)
		if result != nil {
			s.queue.PushChunk(rid, map[string]interface{}{"__full_result__": result})
		}
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	chunkID := "chatcmpl-" + rid
	created := int64(tStart)
	timeout := time.Duration(s.cfg.ResultTimeoutS * float64(time.Second))

	emit := func(s string) {
		fmt.Fprint(w, s)
		flusher.Flush()
	}
	sse := func(obj map[string]interface{}) {
		b, _ := json.Marshal(obj)
		emit("data: " + string(b) + "\n\n")
	}

	var streamEndpointID interface{}
	var tFirstChunk float64

	emitLatency := func(u map[string]interface{}, finReason string) {
		tEnd := nowS()
		e2eS := tEnd - tStart
		xlat := map[string]interface{}{"e2e_ms": round2(e2eS * 1000)}
		observeRequestE2E(e2eS, model)
		if tFirstChunk > 0 {
			ttftS := tFirstChunk - tStart
			xlat["ttft_ms"] = round2(ttftS * 1000)
			observeRequestTTFT(ttftS, model)
			ct := toInt(u["completion_tokens"])
			if ct > 1 {
				decode := e2eS - ttftS
				if decode > 0 {
					tpot := decode / float64(ct-1)
					xlat["tpot_avg_ms"] = round2(tpot * 1000)
					observeRequestTPOTAvg(tpot, model)
				}
			}
		}
		if streamEndpointID != nil {
			xlat["endpoint"] = fmt.Sprintf("%v", streamEndpointID)
		}
		lat := map[string]interface{}{
			"ts": tEnd, "t_start": tStart, "rid": rid, "model": model, "stream": true,
			"finish_reason":     finReason,
			"prompt_tokens":     toInt(u["prompt_tokens"]),
			"completion_tokens": toInt(u["completion_tokens"]),
		}
		for k, v := range xlat {
			lat[k] = v
		}
		s.attachRequestBody(lat, chatBody)
		s.recordLatency(lat)
	}

	// First chunk (or full result fallback).
	first, ok := recvChunk(chunkQ, timeout)
	if !ok {
		emit("data: [DONE]\n\n")
		return
	}

	if full, ok := first["__full_result__"].(map[string]interface{}); ok {
		outputText, _ := full["output"].(string)
		finishReason := "stop"
		if fr, ok := full["finish_reason"].(string); ok && fr != "" {
			finishReason = fr
		}
		u := mapOf(full["usage"])
		eid := full["endpoint_id"]
		streamEndpointID = eid
		tFirstChunk = nowS()
		emitLatency(u, finishReason)
		if tc := extractToolCalls(full); len(tc) > 0 {
			emit(buildSSEChunksWithToolCalls(rid, model, created, tc, finishReason, u, eid))
		} else {
			emit(buildSSEChunks(rid, model, created, outputText, finishReason, u, eid))
		}
		return
	}

	tFirstChunk = nowS()
	streamEndpointID = first["endpoint_id"]

	preamble := map[string]interface{}{
		"id": chunkID, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []map[string]interface{}{{"index": 0, "delta": map[string]interface{}{"role": "assistant", "content": ""}, "finish_reason": nil}},
	}
	if streamEndpointID != nil {
		preamble["system_fingerprint"] = fmt.Sprintf("%v", streamEndpointID)
	}
	sse(preamble)

	emitDelta := func(chunk map[string]interface{}) {
		delta := map[string]interface{}{}
		if dc, ok := chunk["delta"].(string); ok && dc != "" {
			delta["content"] = dc
		}
		if tc, ok := chunk["tool_calls"].([]interface{}); ok && len(tc) > 0 {
			delta["tool_calls"] = tc
		}
		if len(delta) > 0 {
			c := map[string]interface{}{
				"id": chunkID, "object": "chat.completion.chunk", "created": created, "model": model,
				"choices": []map[string]interface{}{{"index": 0, "delta": delta, "finish_reason": nil}},
			}
			if streamEndpointID != nil {
				c["system_fingerprint"] = fmt.Sprintf("%v", streamEndpointID)
			}
			sse(c)
		}
	}

	emitFinal := func(chunk map[string]interface{}) {
		fr := "stop"
		if v, ok := chunk["finish_reason"].(string); ok && v != "" {
			fr = v
		}
		u := mapOf(chunk["usage"])
		emitLatency(u, fr)
		final := map[string]interface{}{
			"id": chunkID, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []map[string]interface{}{{"index": 0, "delta": map[string]interface{}{}, "finish_reason": fr}},
			"usage":   u,
		}
		if streamEndpointID != nil {
			final["system_fingerprint"] = fmt.Sprintf("%v", streamEndpointID)
		}
		sse(final)
		emit("data: [DONE]\n\n")
	}

	emitDelta(first)
	if isFinal(first) {
		emitFinal(first)
		return
	}

	for {
		chunk, ok := recvChunk(chunkQ, timeout)
		if !ok {
			emit("data: [DONE]\n\n")
			return
		}
		if _, ok := chunk["__full_result__"]; ok {
			emit("data: [DONE]\n\n")
			return
		}
		if eid := chunk["endpoint_id"]; eid != nil && streamEndpointID == nil {
			streamEndpointID = eid
		}
		emitDelta(chunk)
		if isFinal(chunk) {
			emitFinal(chunk)
			return
		}
	}
}

func recvChunk(ch chan map[string]interface{}, timeout time.Duration) (map[string]interface{}, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case c := <-ch:
		return c, true
	case <-timer.C:
		return nil, false
	}
}

func isFinal(chunk map[string]interface{}) bool {
	v, ok := chunk["is_final"].(bool)
	return ok && v
}

// ---------------- SSE builders ----------------

func buildSSEChunks(rid, model string, created int64, outputText, finishReason string, usage map[string]interface{}, endpointID interface{}) string {
	chunkID := "chatcmpl-" + rid
	var sb strings.Builder

	preamble := map[string]interface{}{
		"id": chunkID, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []map[string]interface{}{{"index": 0, "delta": map[string]interface{}{"role": "assistant", "content": ""}, "finish_reason": nil}},
	}
	if endpointID != nil {
		preamble["system_fingerprint"] = fmt.Sprintf("%v", endpointID)
	}
	writeSSE(&sb, preamble)

	tokens := splitWordTokens(outputText)
	for _, tok := range tokens {
		chunk := map[string]interface{}{
			"id": chunkID, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []map[string]interface{}{{"index": 0, "delta": map[string]interface{}{"content": tok}, "finish_reason": nil}},
		}
		if endpointID != nil {
			chunk["system_fingerprint"] = fmt.Sprintf("%v", endpointID)
		}
		writeSSE(&sb, chunk)
	}

	final := map[string]interface{}{
		"id": chunkID, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []map[string]interface{}{{"index": 0, "delta": map[string]interface{}{}, "finish_reason": finishReason}},
		"usage":   passthroughUsage(usage),
	}
	if endpointID != nil {
		final["system_fingerprint"] = fmt.Sprintf("%v", endpointID)
	}
	writeSSE(&sb, final)
	sb.WriteString("data: [DONE]\n\n")
	return sb.String()
}

func buildSSEChunksWithToolCalls(rid, model string, created int64, toolCalls []interface{}, finishReason string, usage map[string]interface{}, endpointID interface{}) string {
	chunkID := "chatcmpl-" + rid
	var sb strings.Builder

	preamble := map[string]interface{}{
		"id": chunkID, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []map[string]interface{}{{"index": 0, "delta": map[string]interface{}{"role": "assistant", "content": nil, "tool_calls": []interface{}{}}, "finish_reason": nil}},
	}
	if endpointID != nil {
		preamble["system_fingerprint"] = fmt.Sprintf("%v", endpointID)
	}
	writeSSE(&sb, preamble)

	for i, tc := range toolCalls {
		entry := map[string]interface{}{"index": i}
		if m, ok := tc.(map[string]interface{}); ok {
			for k, v := range m {
				entry[k] = v
			}
		}
		chunk := map[string]interface{}{
			"id": chunkID, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []map[string]interface{}{{"index": 0, "delta": map[string]interface{}{"tool_calls": []interface{}{entry}}, "finish_reason": nil}},
		}
		if endpointID != nil {
			chunk["system_fingerprint"] = fmt.Sprintf("%v", endpointID)
		}
		writeSSE(&sb, chunk)
	}

	final := map[string]interface{}{
		"id": chunkID, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []map[string]interface{}{{"index": 0, "delta": map[string]interface{}{}, "finish_reason": finishReason}},
		"usage":   passthroughUsage(usage),
	}
	if endpointID != nil {
		final["system_fingerprint"] = fmt.Sprintf("%v", endpointID)
	}
	writeSSE(&sb, final)
	sb.WriteString("data: [DONE]\n\n")
	return sb.String()
}

func writeSSE(sb *strings.Builder, obj map[string]interface{}) {
	b, _ := json.Marshal(obj)
	sb.WriteString("data: ")
	sb.Write(b)
	sb.WriteString("\n\n")
}

// splitWordTokens mirrors the whitespace-boundary tokenizer in _build_sse_chunks:
// each whitespace char starts a new token (so concatenation is lossless).
func splitWordTokens(text string) []string {
	var tokens []string
	var buf strings.Builder
	for _, ch := range text {
		if (ch == ' ' || ch == '\n' || ch == '\t') && buf.Len() > 0 {
			tokens = append(tokens, buf.String())
			buf.Reset()
			buf.WriteRune(ch)
		} else {
			buf.WriteRune(ch)
		}
	}
	if buf.Len() > 0 {
		tokens = append(tokens, buf.String())
	}
	if len(tokens) == 0 {
		tokens = []string{""}
	}
	return tokens
}

func extractToolCalls(result map[string]interface{}) []interface{} {
	if direct, ok := result["tool_calls"].([]interface{}); ok && len(direct) > 0 {
		return direct
	}
	raw, ok := result["raw"].(map[string]interface{})
	if !ok {
		return nil
	}
	choices, ok := raw["choices"].([]interface{})
	if !ok || len(choices) == 0 {
		return nil
	}
	choice, ok := choices[0].(map[string]interface{})
	if !ok {
		return nil
	}
	msg, ok := choice["message"].(map[string]interface{})
	if !ok {
		return nil
	}
	tc, ok := msg["tool_calls"].([]interface{})
	if ok && len(tc) > 0 {
		return tc
	}
	return nil
}

func passthroughUsage(usage map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(usage)+3)
	for k, v := range usage {
		out[k] = v
	}
	if _, ok := out["prompt_tokens"]; !ok {
		out["prompt_tokens"] = 0
	}
	if _, ok := out["completion_tokens"]; !ok {
		out["completion_tokens"] = 0
	}
	if _, ok := out["total_tokens"]; !ok {
		out["total_tokens"] = 0
	}
	return out
}

func mapOf(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return map[string]interface{}{}
}

func round2(x float64) float64 {
	return float64(int64(x*100+0.5)) / 100.0
}
