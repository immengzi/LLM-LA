package sidecar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type statusSubscriber struct{ status SubscriberStatus }

func (s *statusSubscriber) Start(context.Context) error { return nil }
func (s *statusSubscriber) Stop()                       {}
func (s *statusSubscriber) Status() SubscriberStatus    { return s.status }
func (s *statusSubscriber) Ready() bool                 { return s.status.Ready }

func TestHealthGenerateIsReadinessOnly(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer engine.Close()
	cfg := &Config{
		InferenceEngine: "sglang", InferenceURL: engine.URL,
		InferenceHealthPath: "/health_generate", InferenceReadinessPath: "/health_generate",
		InferenceHealthTimeoutS: 1,
	}
	prober := NewEngineProber(cfg)
	if !prober.Probe(context.Background(), false) || !prober.Probe(context.Background(), true) {
		t.Fatal("expected successful probes")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 2 || paths[0] != "/health" || paths[1] != "/health_generate" {
		t.Fatalf("probe paths = %v", paths)
	}
}

func TestSGLangReadinessRequiresKVButLivenessDoesNot(t *testing.T) {
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer engine.Close()
	cfg := &Config{InferenceEngine: "sglang", InferenceURL: engine.URL,
		InferenceHealthPath: "/health", InferenceHealthTimeoutS: 1}
	queue := NewLocalQueue("pod")
	kv := &statusSubscriber{status: SubscriberStatus{
		FailClosed: true, Phase: "discovering", Detail: "engine loading", CacheVisibility: "none",
	}}
	prober := NewEngineProber(cfg)
	readyBody, readyCode := HealthResponse(context.Background(), cfg, queue, nil, kv, prober, true)
	if readyCode != http.StatusServiceUnavailable || readyBody["status"] != "kv_unready" {
		t.Fatalf("readiness = (%d, %v)", readyCode, readyBody)
	}
	liveBody, liveCode := HealthResponse(context.Background(), cfg, queue, nil, kv, prober, false)
	if liveCode != http.StatusOK || liveBody["status"] != "ok" {
		t.Fatalf("liveness = (%d, %v)", liveCode, liveBody)
	}
}

func TestVLLMHealthCompatibilityFields(t *testing.T) {
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer engine.Close()
	cfg := &Config{InferenceEngine: "vllm", InferenceURL: engine.URL,
		InferenceHealthPath: "/health", InferenceHealthTimeoutS: 1}
	body, code := HealthResponse(context.Background(), cfg, NewLocalQueue("pod"), nil, nil, NewEngineProber(cfg), false)
	if code != http.StatusServiceUnavailable || body["status"] != "vllm_unhealthy" ||
		body["vllm_healthy"] != false || body["engine_healthy"] != false {
		t.Fatalf("compatibility response = (%d, %v)", code, body)
	}
}
