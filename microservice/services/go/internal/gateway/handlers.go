package gateway

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server holds all dependencies for the HTTP handler layer.
type Server struct {
	cfg         *Config
	queue       *CentralQueue
	results     *ResultStore
	kvWatcher   *KVWatcher
	pushRouter  *PushDispatcher // nil in pull mode
}

func NewServer(cfg *Config, q *CentralQueue, rs *ResultStore, kvw *KVWatcher, pd *PushDispatcher) *Server {
	return &Server{
		cfg:        cfg,
		queue:      q,
		results:    rs,
		kvWatcher:  kvw,
		pushRouter: pd,
	}
}

// Router builds and returns the chi router with all endpoints registered.
func (s *Server) Router() chi.Router {
	r := chi.NewRouter()

	r.Post("/enqueue", s.handleEnqueue)
	r.Post("/submit", s.handleSubmit)
	r.Post("/pull", s.handlePull)
	r.Post("/result", s.handleResult)
	r.Post("/v1/chat/completions", s.handleChatCompletions)

	r.Get("/health", s.handleHealth)
	r.Get("/metrics", promhttp.Handler().ServeHTTP)
	r.Get("/debug/slo", s.handleDebugSLO)

	if s.cfg.ResultSubmitPath != "" && s.cfg.ResultSubmitPath != "/result" {
		r.Post(s.cfg.ResultSubmitPath, s.handleResultSubmitAck)
		log.Printf("[router] Result submit-ack route installed at %s", s.cfg.ResultSubmitPath)
	}

	if s.cfg.SubmitPath != "" && s.cfg.SubmitPath != "/submit" {
		r.Post(s.cfg.SubmitPath, s.handleSubmit)
		log.Printf("[router] Submit route installed at %s", s.cfg.SubmitPath)
	}

	return r
}

// --- POST /enqueue ---

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	tStart := nowS()
	RouterAdmissionTotal.Inc()

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	rid := req.ReqID
	if rid == "" {
		rid = s.queue.NextReqID()
	}

	tEnq := tStart
	if req.TEnqClient != nil {
		tEnq = *req.TEnqClient
	}
	meta := req.Meta
	if meta == nil {
		meta = make(map[string]interface{})
	}

	entry := QueueEntry{
		ReqID:      rid,
		Prompt:     req.Prompt,
		Meta:       meta,
		EnqueuedAt: tEnq,
		SLOType:    req.SLOType,
		SLOTtftMs:  req.SLOTtftMs,
		SLOTpotMs:  req.SLOTpotMs,
		SLOE2eMs:   req.SLOE2eMs,
		TaskType:   req.TaskType,
		OutputLen:  req.OutputLenHint,
	}

	s.results.Register(rid)

	if s.cfg.IsPushMode() && s.pushRouter != nil {
		if err := s.pushRouter.RouteAndPush(rid, req.Prompt, meta); err != nil {
			log.Printf("[router] push failed for req_id=%s: %v", rid, err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": fmt.Sprintf("push failed: %v", err),
			})
			return
		}
	} else {
		s.queue.Enqueue(entry)
	}

	s.logReq("enqueue rid=%s len=%d kv_aware=%v len_aware=%v", rid, len(req.Prompt), s.cfg.KVAware, s.cfg.LenAware)

	timeout := time.Duration(s.cfg.ResultTimeoutS * float64(time.Second))
	result := s.results.WaitFor(rid, timeout)

	if result == nil {
		s.logReq("timeout rid=%s after %.3fs", rid, nowS()-tStart)
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{
			"error": "timeout waiting for vLLM result",
		})
		return
	}

	s.logReq("complete rid=%s latency=%.3fs", rid, nowS()-tStart)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"req_id": rid,
		"result": result,
	})
}

// --- POST /submit ---

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	tStart := nowS()
	RouterAdmissionTotal.Inc()

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	rid := req.ReqID
	if rid == "" {
		rid = s.queue.NextReqID()
	}

	tEnq := tStart
	if req.TEnqClient != nil {
		tEnq = *req.TEnqClient
	}
	meta := req.Meta
	if meta == nil {
		meta = make(map[string]interface{})
	}

	entry := QueueEntry{
		ReqID:      rid,
		Prompt:     req.Prompt,
		Meta:       meta,
		EnqueuedAt: tEnq,
		SLOType:    req.SLOType,
		SLOTtftMs:  req.SLOTtftMs,
		SLOTpotMs:  req.SLOTpotMs,
		SLOE2eMs:   req.SLOE2eMs,
		TaskType:   req.TaskType,
		OutputLen:  req.OutputLenHint,
	}

	s.results.Register(rid)

	if s.cfg.IsPushMode() && s.pushRouter != nil {
		go func() {
			if err := s.pushRouter.RouteAndPush(rid, req.Prompt, meta); err != nil {
				log.Printf("[router] async push failed for req_id=%s: %v", rid, err)
				s.results.Deliver(rid, map[string]interface{}{
					"error": fmt.Sprintf("push failed: %v", err),
				})
			}
		}()
	} else {
		s.queue.Enqueue(entry)
	}

	s.logReq("submit rid=%s len=%d", rid, len(req.Prompt))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"req_id": rid})
}

// --- POST /pull ---

func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	var req PullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	items := s.queue.Pull(req.Endpoint, req.Want)
	if items == nil {
		items = []JobItem{}
	}

	if len(items) > 0 {
		ids := make([]string, len(items))
		for i, it := range items {
			ids[i] = it.ReqID
		}
		s.logReq("/pull ASSIGN endpoint=%s want=%d -> %d items %v",
			req.Endpoint, req.Want, len(items), ids)
	}

	writeJSON(w, http.StatusOK, PullResponse{Items: items})
}

// --- POST /result ---

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	var payload ResultPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	s.ingestResult(&payload)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- POST {RESULT_SUBMIT_PATH} ---

func (s *Server) handleResultSubmitAck(w http.ResponseWriter, r *http.Request) {
	var payload ResultPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	go s.ingestResult(&payload)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

// --- POST /v1/chat/completions ---

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	tStart := nowS()
	RouterAdmissionTotal.Inc()
	RouterV1ChatTotal.Inc()

	var req ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	if req.Model == "" {
		req.Model = "served-model"
	}

	prompt := messagesToPrompt(req.Messages)
	rid := s.queue.NextReqID()

	meta := map[string]interface{}{"__source__": "litellm"}

	entry := QueueEntry{
		ReqID:      rid,
		Prompt:     prompt,
		Meta:       meta,
		EnqueuedAt: tStart,
	}

	s.results.Register(rid)

	if s.cfg.IsPushMode() && s.pushRouter != nil {
		if err := s.pushRouter.RouteAndPush(rid, prompt, meta); err != nil {
			log.Printf("[router] push failed for chat rid=%s: %v", rid, err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": fmt.Sprintf("push failed: %v", err),
			})
			return
		}
	} else {
		s.queue.Enqueue(entry)
	}

	s.logReq("chat_completions rid=%s model=%s messages=%d prompt_len=%d",
		rid, req.Model, len(req.Messages), len(prompt))

	timeout := time.Duration(s.cfg.ResultTimeoutS * float64(time.Second))
	result := s.results.WaitFor(rid, timeout)

	routerLatency := nowS() - tStart

	if result == nil {
		s.logReq("chat_completions timeout rid=%s after %.3fs", rid, routerLatency)
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{
			"error": "timeout waiting for vLLM result",
		})
		return
	}

	outputText := ""
	if v, ok := result["output"]; ok {
		if s, ok := v.(string); ok {
			outputText = s
		}
	}

	finishReason := "stop"
	if v, ok := result["finish_reason"]; ok {
		if s, ok := v.(string); ok && s != "" {
			finishReason = s
		}
	}

	var promptTokens, completionTokens, totalTokens int
	if usage, ok := result["usage"].(map[string]interface{}); ok {
		if v, ok := usage["prompt_tokens"]; ok {
			promptTokens = toInt(v)
		}
		if v, ok := usage["completion_tokens"]; ok {
			completionTokens = toInt(v)
		}
		if v, ok := usage["total_tokens"]; ok {
			totalTokens = toInt(v)
		}
	}

	s.logReq("chat_completions complete rid=%s latency=%.3fs", rid, routerLatency)

	resp := map[string]interface{}{
		"id":      "chatcmpl-" + rid,
		"object":  "chat.completion",
		"created": int64(tStart),
		"model":   req.Model,
		"choices": []map[string]interface{}{
			{
				"index": 0,
				"message": map[string]string{
					"role":    "assistant",
					"content": outputText,
				},
				"finish_reason": finishReason,
			},
		},
		"usage": map[string]int{
			"prompt_tokens":     promptTokens,
			"completion_tokens": completionTokens,
			"total_tokens":      totalTokens,
		},
	}

	writeJSON(w, http.StatusOK, resp)
}

// --- GET /health ---

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "ok",
		"queue_len": s.queue.Size(),
	})
}

// --- GET /debug/slo ---

func (s *Server) handleDebugSLO(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{})
}

// --- Result ingestion ---

func (s *Server) ingestResult(payload *ResultPayload) {
	if payload.ReqID == "" {
		return
	}

	result := payload.Result

	// Backward compatibility: sidecar might send {output, trace} instead of {result}
	if result == nil && payload.Output != nil {
		result = map[string]interface{}{"output": payload.Output}
		if payload.Trace != nil {
			result["trace"] = payload.Trace
		}
	}

	if result == nil {
		return
	}

	if payload.Endpoint != "" && s.pushRouter != nil {
		s.pushRouter.NotifyResult(payload.Endpoint)
	}

	s.results.Deliver(payload.ReqID, result)
}

// --- Helpers ---

func messagesToPrompt(messages []ChatMessage) string {
	parts := make([]string, 0, len(messages))
	for _, msg := range messages {
		role := strings.TrimSpace(strings.ToLower(msg.Role))
		content := strings.TrimSpace(msg.Content)
		switch role {
		case "system":
			parts = append(parts, "System: "+content)
		case "user":
			parts = append(parts, "User: "+content)
		case "assistant":
			parts = append(parts, "Assistant: "+content)
		default:
			parts = append(parts, content)
		}
	}
	return strings.Join(parts, "\n")
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) logReq(format string, args ...interface{}) {
	if s.cfg.ReqLogMode == "off" {
		return
	}
	msg := fmt.Sprintf(format, args...)
	log.Printf("[API] %s (queue_len=%d)", msg, s.queue.Size())
}

func toInt(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return 0
	}
}
