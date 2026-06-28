package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// sloRegistry is the SLO bookkeeping interface (implemented in slo.go, Phase 2).
// It is nil unless SLO annotations / SLO_AWARE are in play. All call sites are
// nil-guarded so the router behaves identically to Python when SLO is off.
type sloRegistry interface {
	register(rid string, req *EnqueueRequest, arrivalTS float64)
	ingestResult(rid string, result map[string]interface{})
	onResult(rid, endpoint string)
	size() int
	debugEntry(rid string) (interface{}, bool)
	debugSummary() interface{}
}

// resultPublisher is the async pubsub publisher (implemented in pubsub.go,
// Phase 3). Nil unless TRANSPORT_MODE=async_pubsub.
type resultPublisher interface {
	Publish(payload map[string]interface{})
	Start() error
	Stop()
}

const latencyLogMax = 2000

// Server holds all dependencies for the HTTP handler layer.
type Server struct {
	cfg        *Config
	queue      *CentralQueue
	results    *ResultStore
	kv         *kvAware
	hashClient *HashClient
	registry   *ModelRegistry
	kvWatcher  *KVWatcher
	pushRouter   *PushDispatcher    // nil in pull mode
	pushDispatch *PushDispatchQueue // nil unless push + decouple dispatch
	publisher    resultPublisher    // nil unless async_pubsub
	slo          sloRegistry        // nil unless SLO

	ridRunIDMu sync.Mutex
	ridToRunID map[string]string

	latMu   sync.Mutex
	latRing []map[string]interface{}
}

func NewServer(cfg *Config, q *CentralQueue, rs *ResultStore, kv *kvAware, hc *HashClient, reg *ModelRegistry, kvw *KVWatcher, pd *PushDispatcher) *Server {
	return &Server{
		cfg:        cfg,
		queue:      q,
		results:    rs,
		kv:         kv,
		hashClient: hc,
		registry:   reg,
		kvWatcher:  kvw,
		pushRouter: pd,
		ridToRunID: make(map[string]string),
		latRing:    make([]map[string]interface{}, 0, latencyLogMax),
	}
}

// SetPublisher / SetSLORegistry / SetPushDispatch wire optional subsystems
// before serving.
func (s *Server) SetPublisher(p resultPublisher)      { s.publisher = p }
func (s *Server) SetSLORegistry(r sloRegistry)        { s.slo = r }
func (s *Server) SetPushDispatch(d *PushDispatchQueue) { s.pushDispatch = d }

// DispatchPushJob performs KV registration then pushes to a sidecar. Used as
// the worker callback for the decoupled push dispatcher.
func (s *Server) DispatchPushJob(rid, prompt string, meta map[string]interface{}) error {
	meta2 := s.registerKVBlocks(rid, prompt, meta, false)
	if s.pushRouter == nil {
		return fmt.Errorf("PushRouter not initialized")
	}
	return s.pushRouter.RouteAndPush(rid, prompt, meta2)
}

// StoreLocalResult stores a synthetic local result (used for push-dispatch
// errors) and publishes it, mirroring _store_and_maybe_publish_local_result.
func (s *Server) StoreLocalResult(rid string, result map[string]interface{}) {
	s.results.Deliver(rid, result)
	if s.publisher != nil {
		runID := s.popRunID(rid)
		pub := map[string]interface{}{"req_id": rid, "result": result}
		if runID != "" {
			pub["run_id"] = runID
		}
		s.publisher.Publish(pub)
	} else {
		s.popRunID(rid)
	}
}

// Router builds and returns the chi router with all endpoints registered.
func (s *Server) Router() chi.Router {
	r := chi.NewRouter()

	r.Post("/enqueue", s.handleEnqueue)
	r.Post("/submit", s.handleSubmit)
	r.Post("/pull", s.handlePull)
	r.Post("/result", s.handleResult)
	r.Post("/result_chunk", s.handleResultChunk)
	r.Post("/v1/chat/completions", s.handleChatCompletions)

	r.Get("/health", s.handleHealthAggregated)
	r.Get("/health/router", s.handleHealthRouter)
	r.Get("/health/backends", s.handleHealthBackends)
	r.Get("/metrics", promhttp.Handler().ServeHTTP)
	r.Get("/latency_log", s.handleLatencyLog)
	r.Get("/debug/slo", s.handleDebugSLOSummary)
	r.Get("/debug/slo/{req_id}", s.handleDebugSLO)

	// RESULT_SUBMIT_PATH only when RESULT_TRANSPORT_MODE=submit_ack (parity
	// with _install_result_submit_route).
	if s.cfg.ResultTransportMode == "submit_ack" {
		path := s.cfg.ResultSubmitPath
		if path == "" {
			path = "/result_submit"
		}
		if path == "/result" {
			log.Printf("[router] WARNING: RESULT_SUBMIT_PATH=/result is not allowed; ignoring.")
		} else {
			r.Post(path, s.handleResultSubmitAck)
			log.Printf("[router] Result submit-ack route installed at %s", path)
		}
	}

	// Custom SUBMIT_PATH alias (parity with _install_submit_route).
	if s.cfg.SubmitPath != "" && s.cfg.SubmitPath != "/submit" {
		r.Post(s.cfg.SubmitPath, s.handleSubmit)
		log.Printf("[router] Submit route installed at %s", s.cfg.SubmitPath)
	}

	return r
}

// ---------------- helpers ----------------

func (s *Server) isPushMode() bool { return s.cfg.IsPushMode() }

// resolveModel mirrors api._resolve_model; ok=false => 404.
func (s *Server) resolveModel(model string) (string, bool) {
	if !s.registry.Enabled() {
		return s.cfg.ModelName, true
	}
	return s.registry.Resolve(strings.TrimSpace(model), s.cfg.ModelName)
}

func (s *Server) rememberRunID(rid string, meta map[string]interface{}) {
	if meta == nil {
		return
	}
	v, ok := meta["__run_id"]
	if !ok {
		return
	}
	if rs, ok := v.(string); ok && rs != "" {
		s.ridRunIDMu.Lock()
		s.ridToRunID[rid] = rs
		s.ridRunIDMu.Unlock()
	}
}

func (s *Server) popRunID(rid string) string {
	s.ridRunIDMu.Lock()
	defer s.ridRunIDMu.Unlock()
	v := s.ridToRunID[rid]
	delete(s.ridToRunID, rid)
	return v
}

func (s *Server) recordLatency(entry map[string]interface{}) {
	s.latMu.Lock()
	if len(s.latRing) >= latencyLogMax {
		s.latRing = s.latRing[1:]
	}
	s.latRing = append(s.latRing, entry)
	s.latMu.Unlock()
	if b, err := json.Marshal(entry); err == nil {
		log.Printf("router.latency %s", string(b))
	}
}

// registerKVBlocks mirrors _maybe_register_kv_blocks: best-effort hash compute
// + register request blocks + trace enrichment. Returns the (possibly updated)
// meta.
func (s *Server) registerKVBlocks(rid, prompt string, meta map[string]interface{}, isPullMode bool) map[string]interface{} {
	m := cloneMeta(meta)
	if !s.cfg.KVAware {
		return m
	}

	if s.cfg.TraceEnabled {
		tr := traceOf(m)
		if _, ok := tr["router_block_hashes"]; !ok {
			tr["router_block_hashes"] = nil
		}
		m["__trace__"] = tr
		if isPullMode {
			s.queue.UpdateMeta(rid, m)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.cfg.HashTimeoutS*float64(time.Second)))
	defer cancel()
	hashes, err := s.hashClient.ComputeHashes(ctx, prompt)
	if err != nil {
		log.Printf("[router] WARNING: KV hash compute failed for req_id=%s: %v", rid, err)
		if s.cfg.TraceEnabled {
			tr := traceOf(m)
			if _, ok := tr["router_block_hashes"]; !ok {
				tr["router_block_hashes"] = nil
			}
			tr["router_kv_hash_error"] = err.Error()
			m["__trace__"] = tr
			if isPullMode {
				s.queue.UpdateMeta(rid, m)
			}
		}
		return m
	}

	s.kv.registerRequestBlocks(rid, hashes)

	if s.cfg.TraceEnabled {
		tr := traceOf(m)
		ifaces := make([]interface{}, len(hashes))
		for i, h := range hashes {
			ifaces[i] = h
		}
		tr["router_block_hashes"] = ifaces
		delete(tr, "router_kv_hash_error")
		m["__trace__"] = tr
		if isPullMode {
			s.queue.UpdateMeta(rid, m)
		}
	}
	return m
}

// --- POST /enqueue ---

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	tStart := nowS()
	incAdmission()

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	model, ok := s.resolveModel(req.Model)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("Unknown model '%s'. Available: %v", req.Model, s.registry.Names())})
		return
	}

	rid, meta, isPull := s.admit(&req, model, tStart)

	s.rememberRunID(rid, meta)
	if s.slo != nil {
		s.slo.register(rid, &req, tStart)
	}

	s.logReq("enqueue rid=%s len=%d kv_aware=%v len_aware=%v", rid, len(req.Prompt), s.cfg.KVAware, s.cfg.LenAware)

	s.results.Register(rid)

	s.dispatch(rid, req.Prompt, meta, isPull)

	timeout := time.Duration(s.cfg.ResultTimeoutS * float64(time.Second))
	result := s.results.WaitFor(rid, timeout)

	if result == nil {
		s.logReq("timeout rid=%s after %.3fs", rid, nowS()-tStart)
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timeout waiting for vLLM result"})
		return
	}

	if s.cfg.TraceEnabled {
		if tr, ok := result["trace"].(map[string]interface{}); ok {
			tr["t_enqueue_about_to_return"] = nowS()
			tr["t_enqueue_response"] = nowS()
			result["trace"] = tr
		}
		delete(result, "__trace__")
	}

	// rename endpoint -> pod
	if ep, ok := result["endpoint"]; ok {
		if _, hasPod := result["pod"]; !hasPod {
			result["pod"] = ep
			delete(result, "endpoint")
		}
	}
	if tr, ok := result["trace"].(map[string]interface{}); ok {
		if ep, ok := tr["endpoint"]; ok {
			if _, hasPod := tr["pod"]; !hasPod {
				tr["pod"] = ep
				delete(tr, "endpoint")
				result["trace"] = tr
			}
		}
	}

	s.logReq("complete rid=%s latency=%.3fs", rid, nowS()-tStart)
	writeJSON(w, http.StatusOK, map[string]interface{}{"req_id": rid, "result": result})
}

// --- POST /submit ---

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	tStart := nowS()
	incAdmission()

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	model, ok := s.resolveModel(req.Model)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("Unknown model '%s'. Available: %v", req.Model, s.registry.Names())})
		return
	}

	rid, meta, isPull := s.admit(&req, model, tStart)
	s.rememberRunID(rid, meta)
	if s.slo != nil {
		s.slo.register(rid, &req, tStart)
	}

	s.logReq("submit rid=%s len=%d", rid, len(req.Prompt))
	s.results.Register(rid)
	go s.dispatch(rid, req.Prompt, meta, isPull)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"req_id": rid})
}

// injectAffinity stamps the conversation affinity key + router-side timestamp
// into meta. No-op when affinity is disabled. Mirrors api.py:_inject_affinity:
// an explicit meta["affinity_key"] wins, otherwise the key is auto-derived from
// the conversation's stable prefix (model + system + first user message).
func (s *Server) injectAffinity(meta map[string]interface{}, model string, chatBody map[string]interface{}) {
	if !s.cfg.AffinityEnabled || meta == nil {
		return
	}
	key := ""
	if explicit, ok := meta["affinity_key"].(string); ok && explicit != "" {
		key = explicit
	} else if chatBody != nil {
		if msgs, ok := chatBody["messages"].([]interface{}); ok {
			key = deriveAffinityKey(model, msgs)
		}
	}
	if key != "" {
		meta["__affinity_key__"] = key
		meta["__affinity_ts__"] = nowS()
	}
}

// admit enqueues (pull) or allocates a req_id (push) and injects the arrival
// trace. Returns (rid, meta, isPullMode).
func (s *Server) admit(req *EnqueueRequest, model string, tStart float64) (string, map[string]interface{}, bool) {
	meta := req.Meta
	if meta == nil {
		meta = make(map[string]interface{})
	}
	s.injectAffinity(meta, model, nil)

	if s.cfg.TraceEnabled {
		qlen := s.queue.Size()
		tEnqClient := tStart
		if req.TEnqClient != nil {
			tEnqClient = *req.TEnqClient
		}
		trace := map[string]interface{}{
			"t_enq_client":               tEnqClient,
			"t_arrive_router":            tStart,
			"router_queue_len_at_arrive": qlen,
		}
		if _, ok := meta["__trace__"]; !ok {
			m2 := cloneMeta(meta)
			m2["__trace__"] = trace
			meta = m2
		}
	}

	if s.isPushMode() {
		return s.queue.NextReqID(), meta, false
	}
	tEnq := tStart
	if req.TEnqClient != nil {
		tEnq = *req.TEnqClient
	}
	rid := s.queue.Enqueue(req.Prompt, tEnq, meta, req.ReqID, model)
	if s.cfg.TraceEnabled {
		s.queue.UpdateMeta(rid, meta)
	}
	return rid, meta, true
}

// dispatch performs KV registration and (push mode) routes to a sidecar.
//
// When push-dispatch decoupling is enabled, the request is buffered in the
// dispatch queue and KV registration is deferred to the worker (matching
// router/api.py); otherwise the work is done synchronously here.
func (s *Server) dispatch(rid, prompt string, meta map[string]interface{}, isPull bool) {
	if s.isPushMode() && s.pushDispatch != nil {
		if ok := s.pushDispatch.TrySubmit(rid, prompt, meta); !ok {
			s.StoreLocalResult(rid, map[string]interface{}{"error": "push_dispatch_queue_full"})
		}
		return
	}

	meta2 := s.registerKVBlocks(rid, prompt, meta, isPull)
	if s.isPushMode() && s.pushRouter != nil {
		if err := s.pushRouter.RouteAndPush(rid, prompt, meta2); err != nil {
			log.Printf("[router] push failed for req_id=%s: %v", rid, err)
			s.results.Deliver(rid, map[string]interface{}{"error": fmt.Sprintf("push failed: %v", err)})
		}
	}
}

// --- POST /pull ---

func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	var req PullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	model, ok := s.resolveModel(req.Model)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("Unknown model '%s'. Available: %v", req.Model, s.registry.Names())})
		return
	}

	items := s.queue.Pull(req.Endpoint, req.Want, model)
	if items == nil {
		items = []JobItem{}
	}

	if len(items) > 0 {
		ids := make([]string, len(items))
		for i, it := range items {
			ids[i] = it.ReqID
		}
		s.logReq("/pull ASSIGN endpoint=%s want=%d model=%s -> %d items %v", req.Endpoint, req.Want, model, len(items), ids)
	}

	writeJSON(w, http.StatusOK, PullResponse{Items: items})
}

// --- POST /result ---

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	s.ingestResultPayload(payload)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- POST /result_chunk ---

func (s *Server) handleResultChunk(w http.ResponseWriter, r *http.Request) {
	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	rid, _ := payload["req_id"].(string)
	if rid == "" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "missing req_id"})
		return
	}
	s.queue.PushChunk(rid, payload)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- POST {RESULT_SUBMIT_PATH} ---

func (s *Server) handleResultSubmitAck(w http.ResponseWriter, r *http.Request) {
	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if _, ok := payload["req_id"]; !ok {
		writeJSON(w, http.StatusOK, map[string]string{"status": "missing req_id"})
		return
	}
	go s.ingestResultPayload(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

// ingestResultPayload mirrors _ingest_result_payload.
func (s *Server) ingestResultPayload(payload map[string]interface{}) {
	ridRaw, ok := payload["req_id"]
	if !ok || ridRaw == nil {
		return
	}
	rid := fmt.Sprintf("%v", ridRaw)

	var result map[string]interface{}
	if rv, ok := payload["result"].(map[string]interface{}); ok {
		result = rv
	}
	if result == nil {
		if out, ok := payload["output"]; ok {
			result = map[string]interface{}{"output": out}
			if tr, ok := payload["trace"].(map[string]interface{}); ok {
				result["trace"] = tr
			}
		}
	}
	if result == nil {
		return
	}

	endpoint, _ := payload["endpoint"].(string)
	if endpoint != "" && s.pushRouter != nil {
		s.pushRouter.NotifyResult(endpoint)
	}

	if s.cfg.TraceEnabled {
		tr := traceOf(result)
		tr["t_router_result_recv"] = nowS()
		tr["t_router_result_store"] = nowS()
		result["trace"] = tr
		delete(result, "__trace__")
	}

	s.results.Deliver(rid, result)

	if s.slo != nil {
		s.slo.ingestResult(rid, result)
		s.slo.onResult(rid, endpoint)
	}

	if s.publisher != nil {
		runID := s.popRunID(rid)
		pub := map[string]interface{}{"req_id": rid, "result": result}
		if endpoint != "" {
			pub["endpoint"] = endpoint
		}
		if runID != "" {
			pub["run_id"] = runID
		}
		s.publisher.Publish(pub)
	} else {
		s.popRunID(rid)
	}

	// Streaming fallback: if a chunk queue is waiting, deliver the full result.
	if s.queue.HasChunkQueue(rid) {
		s.queue.PushChunk(rid, map[string]interface{}{"__full_result__": result})
	}
}

// ---------------- health ----------------

func (s *Server) handleHealthRouter(w http.ResponseWriter, r *http.Request) {
	body := map[string]interface{}{"status": "ok", "queue_len": s.queue.Size()}
	if s.registry.Enabled() {
		body["models"] = s.registry.Names()
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) handleHealthBackends(w http.ResponseWriter, r *http.Request) {
	pods := discoverVLLMLeaders(s.cfg)
	if pods == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"status": "error", "detail": "pod discovery failed"})
		return
	}
	if len(pods) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"status": "unhealthy", "healthy": 0, "total": 0, "pods": map[string]interface{}{}})
		return
	}
	podStatus, healthy := probeBackends(s.cfg, pods)
	body := map[string]interface{}{
		"status":  ternaryStatus(healthy > 0),
		"healthy": healthy,
		"total":   len(podStatus),
		"pods":    podStatus,
	}
	code := http.StatusOK
	if healthy == 0 {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, body)
}

func (s *Server) handleHealthAggregated(w http.ResponseWriter, r *http.Request) {
	routerInfo := map[string]interface{}{"status": "ok", "queue_len": s.queue.Size()}
	if s.registry.Enabled() {
		routerInfo["models"] = s.registry.Names()
	}

	pods := discoverVLLMLeaders(s.cfg)
	healthy := 0
	podStatus := map[string]string{}
	if len(pods) > 0 {
		podStatus, healthy = probeBackends(s.cfg, pods)
	}

	code := http.StatusOK
	if healthy == 0 {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]interface{}{
		"status": ternaryStatus(healthy > 0),
		"router": routerInfo,
		"backends": map[string]interface{}{
			"healthy": healthy,
			"total":   len(pods),
			"pods":    podStatus,
		},
	})
}

func ternaryStatus(healthy bool) string {
	if healthy {
		return "healthy"
	}
	return "unhealthy"
}

// ---------------- latency log / debug ----------------

func (s *Server) handleLatencyLog(w http.ResponseWriter, r *http.Request) {
	last := 100
	if v := r.URL.Query().Get("last"); v != "" {
		if n := atoiSafe(v); n > 0 {
			last = n
		}
	}
	if last > latencyLogMax {
		last = latencyLogMax
	}
	if last < 1 {
		last = 1
	}
	s.latMu.Lock()
	n := len(s.latRing)
	start := n - last
	if start < 0 {
		start = 0
	}
	out := make([]map[string]interface{}, n-start)
	copy(out, s.latRing[start:])
	s.latMu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleDebugSLO(w http.ResponseWriter, r *http.Request) {
	rid := chi.URLParam(r, "req_id")
	if s.slo == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": fmt.Sprintf("No SLO entry for req_id=%s", rid)})
		return
	}
	entry, ok := s.slo.debugEntry(rid)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": fmt.Sprintf("No SLO entry for req_id=%s", rid)})
		return
	}
	writeJSON(w, http.StatusOK, entry)
}

func (s *Server) handleDebugSLOSummary(w http.ResponseWriter, r *http.Request) {
	if s.slo == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{})
		return
	}
	writeJSON(w, http.StatusOK, s.slo.debugSummary())
}

// ---------------- helpers ----------------

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

func atoiSafe(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
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
