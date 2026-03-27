package router

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vllmkv/operator/pkg/models"
)

// Server is the router HTTP server with all endpoints.
type Server struct {
	state  *State
	cfg    *Config
	push   *PushRouter
	pub    *ResultPublisher
	hash   *HashClient
	runIDs map[string]string // req_id -> run_id for pubsub
}

func NewServer(state *State, cfg *Config, push *PushRouter, pub *ResultPublisher, hash *HashClient) *Server {
	return &Server{
		state:  state,
		cfg:    cfg,
		push:   push,
		pub:    pub,
		hash:   hash,
		runIDs: make(map[string]string),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", promhttp.Handler().ServeHTTP)
	mux.HandleFunc("POST /enqueue", s.handleEnqueue)
	mux.HandleFunc("POST /submit", s.handleSubmit)
	mux.HandleFunc("POST /pull", s.handlePull)
	mux.HandleFunc("POST /result", s.handleResult)

	if s.cfg.SubmitPath != "/submit" && s.cfg.SubmitPath != "" {
		mux.HandleFunc("POST "+s.cfg.SubmitPath, s.handleSubmit)
	}
	if s.cfg.ResultTransportMode == "submit_ack" && s.cfg.ResultSubmitPath != "/result" {
		mux.HandleFunc("POST "+s.cfg.ResultSubmitPath, s.handleResultSubmitAck)
	}

	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := models.HealthResponse{Status: "ok", QueueLen: s.state.QueueLen()}
	writeJSON(w, 200, resp)
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var body models.EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", 400)
		return
	}

	req := s.buildRequest(&body)
	MetricEnqueueTotal.Inc()

	if s.cfg.KVAware {
		s.registerKVBlocks(r.Context(), req)
	}

	s.state.Enqueue(req)
	MetricQueueLen.Set(float64(s.state.QueueLen()))

	timeout := time.Duration(s.cfg.ResultTimeoutS * float64(time.Second))
	result, ok := s.state.WaitForResult(req.ReqID, timeout)
	if !ok {
		writeJSON(w, 504, map[string]string{"error": "timeout"})
		return
	}

	writeJSON(w, 200, models.EnqueueResponse{
		ReqID:  req.ReqID,
		Result: result.Result,
	})
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var body models.EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", 400)
		return
	}

	req := s.buildRequest(&body)
	MetricEnqueueTotal.Inc()

	if meta := req.Meta; meta != nil {
		if rid, ok := meta["__run_id"].(string); ok && rid != "" {
			s.runIDs[req.ReqID] = rid
		}
	}

	if s.cfg.KVAware {
		s.registerKVBlocks(r.Context(), req)
	}

	s.state.Enqueue(req)
	MetricQueueLen.Set(float64(s.state.QueueLen()))

	writeJSON(w, 202, models.SubmitResponse{ReqID: req.ReqID})
}

func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	var body models.PullRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", 400)
		return
	}

	MetricPullTotal.Inc()
	batch := PullForEndpoint(s.state, s.cfg, body.Endpoint, body.Want)
	MetricQueueLen.Set(float64(s.state.QueueLen()))

	items := make([]models.JobItem, len(batch))
	for i, req := range batch {
		items[i] = models.JobItem{
			ReqID:           req.ReqID,
			Prompt:          req.Prompt,
			ClientEnqueueTS: req.ClientEnqueueTS,
			Meta:            req.Meta,
		}
	}
	MetricPullItemsTotal.Add(float64(len(items)))

	writeJSON(w, 200, models.PullResponse{Items: items})
}

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	var body models.ResultPayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	s.ingestResult(&body)
	writeJSON(w, 200, models.StatusResponse{Status: "ok"})
}

func (s *Server) handleResultSubmitAck(w http.ResponseWriter, r *http.Request) {
	var body models.ResultPayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	go s.ingestResult(&body)
	writeJSON(w, 202, models.StatusResponse{Status: "accepted"})
}

func (s *Server) ingestResult(body *models.ResultPayload) {
	MetricResultTotal.Inc()

	resultMap := body.Result
	if resultMap == nil && body.Output != "" {
		resultMap = map[string]interface{}{"output": body.Output}
	}
	if body.Trace != nil && resultMap != nil {
		resultMap["trace"] = body.Trace
	}

	result := &models.Result{
		ReqID:    body.ReqID,
		Result:   resultMap,
		Endpoint: body.Endpoint,
	}

	s.state.SetResult(body.ReqID, result)

	if s.push != nil && body.Endpoint != "" {
		s.push.NotifyResult(body.Endpoint)
	}

	if s.cfg.IsAsyncPub() && s.pub != nil {
		runID := s.runIDs[body.ReqID]
		s.pub.Publish(result, runID)
		delete(s.runIDs, body.ReqID)
	}
}

func (s *Server) buildRequest(body *models.EnqueueRequest) *models.Request {
	reqID := body.ReqID
	if reqID == "" {
		reqID = GenReqID()
	}
	meta := body.Meta
	if meta == nil {
		meta = make(map[string]interface{})
	}
	return &models.Request{
		ReqID:           reqID,
		Prompt:          body.Prompt,
		Meta:            meta,
		ClientEnqueueTS: body.ClientEnqueueTS,
		RouterEnqueueTS: float64(time.Now().UnixNano()) / 1e9,
	}
}

func (s *Server) registerKVBlocks(ctx context.Context, req *models.Request) {
	if s.hash == nil {
		return
	}
	hashes, err := s.hash.ComputeHashes(ctx, req.Prompt)
	if err != nil {
		log.Printf("[kv] hash error req_id=%s: %v", req.ReqID, err)
		return
	}
	s.state.RegisterRequestBlocks(req.ReqID, hashes)
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
