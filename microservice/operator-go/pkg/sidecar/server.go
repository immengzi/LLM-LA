package sidecar

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server provides HTTP endpoints for the sidecar, matching the Python sidecar API.
type Server struct {
	queue *LocalQueue
	cfg   *Config
	wake  chan struct{}
}

func NewServer(queue *LocalQueue, cfg *Config, wake chan struct{}) *Server {
	return &Server{queue: queue, cfg: cfg, wake: wake}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", promhttp.Handler().ServeHTTP)
	mux.HandleFunc("POST /push", s.handlePush)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"status":    "ok",
		"pending":   s.queue.Pending(),
		"inflight":  s.queue.Inflight(),
		"logical":   s.queue.Logical(),
		"queue_len": s.queue.Logical(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handlePush accepts a pushed request from the router (push mode).
func (s *Server) handlePush(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ReqID  string                 `json:"req_id"`
		Prompt string                 `json:"prompt"`
		Meta   map[string]interface{} `json:"meta,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", 400)
		return
	}

	item := Item{ReqID: body.ReqID, Prompt: body.Prompt, Meta: body.Meta}
	s.queue.Push(item)

	select {
	case s.wake <- struct{}{}:
	default:
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
