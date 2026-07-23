package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

// external_endpoints.go implements the pieces the external-push router mode
// needs. Unlike every other mode, external-push does NOT discover Kubernetes
// pods and does NOT talk to a per-pod sidecar: the operator registers vLLM
// servers that live outside the cluster (by URL) via ROUTER_STATIC_ENDPOINTS.
//
// Faithful port of
// src/core/services/router_service/router/external_endpoints.py:
//   * ExternalRegistry    — static endpoint set + async /health readiness
//                           gating (replaces k8s pod discovery). Endpoint
//                           identity is the configured id and is used
//                           everywhere the pod name would be (in-flight,
//                           metrics, KV owners).
//   * ExternalVLLMClient  — delivers a single request DIRECTLY to an external
//                           vLLM OpenAI /v1/chat/completions endpoint and shapes
//                           the response into the same result object the sidecar
//                           posts to /result (non-streaming).
//   * RouterKVSubscriber  — optional per-endpoint ZMQ subscriber mirroring the
//                           sidecar KV-events -> Redis writer so prefix routing
//                           keeps working for external endpoints (owners keyed
//                           by the endpoint id).

// ExternalEndpointConfig is one parsed entry of ROUTER_STATIC_ENDPOINTS.
type ExternalEndpointConfig struct {
	ID                string   `json:"id"`
	URL               string   `json:"url"`
	Model             string   `json:"model"`
	KVEventsEndpoints []string `json:"kv_events_endpoints"`
	KVEventsTopic     string   `json:"kv_events_topic"`
}

// parseStaticEndpoints parses the ROUTER_STATIC_ENDPOINTS JSON array, filling
// defaults and skipping malformed entries. Mirrors _parse_static_endpoints in
// router/config.py: URL is required (trailing slash trimmed), id defaults to
// ext-N, topic defaults to "kv@".
func parseStaticEndpoints(raw string) []ExternalEndpointConfig {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var items []ExternalEndpointConfig
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		log.Printf("[external] failed to parse ROUTER_STATIC_ENDPOINTS: %v", err)
		return nil
	}
	out := make([]ExternalEndpointConfig, 0, len(items))
	for i, it := range items {
		url := strings.TrimRight(strings.TrimSpace(it.URL), "/")
		if url == "" {
			log.Printf("[external] skipping static endpoint %d: missing url", i)
			continue
		}
		id := strings.TrimSpace(it.ID)
		if id == "" {
			id = fmt.Sprintf("ext-%d", i+1)
		}
		topic := strings.TrimSpace(it.KVEventsTopic)
		if topic == "" {
			topic = "kv@"
		}
		out = append(out, ExternalEndpointConfig{
			ID:                id,
			URL:               url,
			Model:             strings.TrimSpace(it.Model),
			KVEventsEndpoints: it.KVEventsEndpoints,
			KVEventsTopic:     topic,
		})
	}
	return out
}

// ============================================================
// Registry (static endpoints + /health readiness gating)
// ============================================================

// ExternalRegistry holds the static external endpoint set and probes each
// vLLM's /health to gate dispatch. Endpoints with an unknown probe state are
// treated as healthy until proven otherwise, so a probe outage never strands a
// live backend.
type ExternalRegistry struct {
	cfg    *Config
	eps    []ExternalEndpointConfig
	byID   map[string]ExternalEndpointConfig
	client *http.Client

	mu        sync.Mutex
	healthy   map[string]bool
	lastProbe time.Time
}

// NewExternalRegistry builds the registry from the parsed config endpoints and
// resolves each endpoint's served model (defaulting to MODEL_NAME) so the KV
// subscriber and delivery payloads use a consistent name.
func NewExternalRegistry(cfg *Config) *ExternalRegistry {
	eps := make([]ExternalEndpointConfig, 0, len(cfg.StaticEndpoints))
	byID := make(map[string]ExternalEndpointConfig)
	healthy := make(map[string]bool)
	for _, e := range cfg.StaticEndpoints {
		model := e.Model
		if model == "" {
			model = cfg.ModelName
		}
		ep := ExternalEndpointConfig{
			ID:                e.ID,
			URL:               e.URL,
			Model:             model,
			KVEventsEndpoints: e.KVEventsEndpoints,
			KVEventsTopic:     e.KVEventsTopic,
		}
		eps = append(eps, ep)
		byID[ep.ID] = ep
		healthy[ep.ID] = true
	}
	t := time.Duration(cfg.PushHTTPTimeoutS * float64(time.Second))
	return &ExternalRegistry{
		cfg:     cfg,
		eps:     eps,
		byID:    byID,
		healthy: healthy,
		client:  &http.Client{Timeout: t},
	}
}

// AllIDs returns every configured endpoint id.
func (r *ExternalRegistry) AllIDs() []string {
	out := make([]string, 0, len(r.eps))
	for _, e := range r.eps {
		out = append(out, e.ID)
	}
	return out
}

// Get returns the endpoint config for an id (ok=false if unknown).
func (r *ExternalRegistry) Get(id string) (ExternalEndpointConfig, bool) {
	e, ok := r.byID[id]
	return e, ok
}

// HealthyIDs returns the ids currently considered healthy.
func (r *ExternalRegistry) HealthyIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.eps))
	for _, e := range r.eps {
		if r.healthy[e.ID] {
			out = append(out, e.ID)
		}
	}
	return out
}

// RefreshHealth probes each endpoint's /health, throttled to
// ExternalHealthIntervalS unless force is set.
func (r *ExternalRegistry) RefreshHealth(force bool) {
	interval := time.Duration(r.cfg.ExternalHealthIntervalS * float64(time.Second))
	r.mu.Lock()
	if !force && interval > 0 && time.Since(r.lastProbe) < interval {
		r.mu.Unlock()
		return
	}
	r.lastProbe = time.Now()
	eps := append([]ExternalEndpointConfig{}, r.eps...)
	r.mu.Unlock()

	type res struct {
		id string
		ok bool
	}
	ch := make(chan res, len(eps))
	var wg sync.WaitGroup
	for _, e := range eps {
		wg.Add(1)
		go func(e ExternalEndpointConfig) {
			defer wg.Done()
			resp, err := r.client.Get(e.URL + "/health")
			if err != nil {
				ch <- res{e.ID, false}
				return
			}
			resp.Body.Close()
			ch <- res{e.ID, resp.StatusCode == http.StatusOK}
		}(e)
	}
	wg.Wait()
	close(ch)

	r.mu.Lock()
	for x := range ch {
		r.healthy[x.id] = x.ok
	}
	r.mu.Unlock()
}

// ============================================================
// Direct vLLM delivery client (non-streaming)
// ============================================================

// ExternalVLLMClient delivers one request directly to an external vLLM OpenAI
// endpoint and shapes the response into the same result object the sidecar
// posts to /result, so ingestResultPayload handles it unchanged. Mirrors
// router/external_endpoints.py:ExternalVLLMClient.
type ExternalVLLMClient struct {
	cfg      *Config
	registry *ExternalRegistry
	client   *http.Client
}

func NewExternalVLLMClient(cfg *Config, registry *ExternalRegistry) *ExternalVLLMClient {
	t := time.Duration(cfg.ExternalVLLMTimeoutS * float64(time.Second))
	return &ExternalVLLMClient{
		cfg:      cfg,
		registry: registry,
		client:   &http.Client{Timeout: t},
	}
}

func (c *ExternalVLLMClient) buildPayload(ep ExternalEndpointConfig, prompt string, meta map[string]interface{}) map[string]interface{} {
	if cr, ok := meta["__chat_request__"].(map[string]interface{}); ok && len(cr) > 0 {
		payload := cloneMeta(cr)
		payload["model"] = ep.Model
		payload["stream"] = false
		return payload
	}
	maxTokens := c.cfg.DefaultMaxTokens
	if v, ok := meta["max_tokens"]; ok {
		if n := toInt(v); n > 0 {
			maxTokens = n
		}
	}
	temperature := 0.0
	if v, ok := meta["temperature"]; ok {
		temperature = toFloat(v)
	}
	enableThinking := false
	if v, ok := meta["enable_thinking"].(bool); ok {
		enableThinking = v
	}
	payload := map[string]interface{}{
		"model":       ep.Model,
		"messages":    []interface{}{map[string]interface{}{"role": "user", "content": prompt}},
		"max_tokens":  maxTokens,
		"temperature": temperature,
		"chat_template_kwargs": map[string]interface{}{
			"enable_thinking": enableThinking,
		},
		"stream": false,
	}
	if v, ok := meta["min_tokens"]; ok {
		if n := toInt(v); n > 0 {
			payload["min_tokens"] = n
		}
	}
	if v, ok := meta["ignore_eos"].(bool); ok && v {
		payload["ignore_eos"] = true
	}
	return payload
}

// Deliver calls the external vLLM and returns a router result object. It never
// returns an error: failures come back as an error result so the caller can
// ingest it and release in-flight.
func (c *ExternalVLLMClient) Deliver(epID, reqID, prompt string, meta map[string]interface{}) map[string]interface{} {
	ep, ok := c.registry.Get(epID)
	if !ok {
		return map[string]interface{}{
			"output":        fmt.Sprintf("[external error: unknown endpoint %s]", epID),
			"finish_reason": "error",
			"error":         "unknown_endpoint",
			"endpoint_id":   epID,
		}
	}

	payload := c.buildPayload(ep, prompt, meta)
	body, err := json.Marshal(payload)
	if err != nil {
		return map[string]interface{}{
			"output": fmt.Sprintf("[external error: %v]", err), "finish_reason": "error",
			"error": err.Error(), "endpoint_id": epID,
		}
	}

	url := ep.URL + "/v1/chat/completions"
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.cfg.ExternalVLLMTimeoutS*float64(time.Second)))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return map[string]interface{}{
			"output": fmt.Sprintf("[external error: %v]", err), "finish_reason": "error",
			"error": err.Error(), "endpoint_id": epID,
		}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return map[string]interface{}{
			"output": fmt.Sprintf("[external error: %v]", err), "finish_reason": "error",
			"error": err.Error(), "endpoint_id": epID,
		}
	}
	defer resp.Body.Close()

	latencyS := time.Since(t0).Seconds()
	result := map[string]interface{}{"endpoint_id": epID, "latency_s": latencyS}

	var raw bytes.Buffer
	_, _ = raw.ReadFrom(resp.Body)

	if resp.StatusCode != http.StatusOK {
		snippet := raw.String()
		if len(snippet) > 300 {
			snippet = snippet[:300]
		}
		result["output"] = fmt.Sprintf("[vLLM error %d]", resp.StatusCode)
		result["finish_reason"] = "error"
		result["error"] = fmt.Sprintf("http_%d: %s", resp.StatusCode, snippet)
		return result
	}

	var data map[string]interface{}
	if err := json.Unmarshal(raw.Bytes(), &data); err != nil {
		result["output"] = "[parse error in vLLM response]"
		result["finish_reason"] = "error"
		result["error"] = fmt.Sprintf("parse: %v", err)
		return result
	}

	result["raw"] = data
	if choices, ok := data["choices"].([]interface{}); ok && len(choices) > 0 {
		if first, ok := choices[0].(map[string]interface{}); ok {
			if msg, ok := first["message"].(map[string]interface{}); ok {
				if content, ok := msg["content"].(string); ok && content != "" {
					result["output"] = content
				} else {
					result["output"] = fmt.Sprintf("%v", first)
				}
				if tc, ok := msg["tool_calls"].([]interface{}); ok && len(tc) > 0 {
					result["tool_calls"] = tc
				}
			}
			if fr, ok := first["finish_reason"]; ok && fr != nil {
				result["finish_reason"] = fr
			} else if fr, ok := data["finish_reason"]; ok && fr != nil {
				result["finish_reason"] = fr
			}
		}
	} else {
		result["output"] = fmt.Sprintf("%v", data)
	}
	if usage, ok := data["usage"].(map[string]interface{}); ok {
		result["usage"] = usage
	}
	return result
}

// ============================================================
// Router-side KV-events subscriber (mirrors sidecar/kv_subscriber.go)
// ============================================================

// RouterKVSubscriber subscribes to one external vLLM's KV-cache-events ZMQ and
// writes Redis block-owner data keyed by the endpoint id, so prefix routing
// (OwnerLookup) finds owners for external endpoints just like it does for
// sidecar pods. Redis schema matches sidecar/zmq_subscriber.py (owner keyed by
// the endpoint id instead of the pod container name).
type RouterKVSubscriber struct {
	ep     ExternalEndpointConfig
	rdb    *redis.Client
	cancel context.CancelFunc
	done   chan struct{}
}

func NewRouterKVSubscriber(cfg *Config, ep ExternalEndpointConfig) *RouterKVSubscriber {
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
	})
	return &RouterKVSubscriber{ep: ep, rdb: rdb, done: make(chan struct{})}
}

func (s *RouterKVSubscriber) Start(ctx context.Context) {
	if len(s.ep.KVEventsEndpoints) == 0 {
		log.Printf("[external] KV subscriber skipped for %s (no kv_events_endpoints)", s.ep.ID)
		close(s.done)
		return
	}
	cctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	sub := zmq4.NewSub(cctx)
	connected := 0
	for _, addr := range s.ep.KVEventsEndpoints {
		if err := sub.Dial(addr); err != nil {
			log.Printf("[external] [%s] KV-SUB connect failed %s: %v", s.ep.ID, addr, err)
			continue
		}
		connected++
		log.Printf("[external] [%s] KV-SUB connected to %s", s.ep.ID, addr)
	}
	topic := s.ep.KVEventsTopic
	if topic == "" {
		topic = "kv@"
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, topic); err != nil {
		log.Printf("[external] [%s] KV-SUB subscribe failed: %v", s.ep.ID, err)
	}
	if connected == 0 {
		cancel()
		close(s.done)
		return
	}
	log.Printf("[external] KV subscriber started for %s: %v (topic %q, model %q)",
		s.ep.ID, s.ep.KVEventsEndpoints, topic, s.ep.Model)
	go s.loop(cctx, sub)
}

func (s *RouterKVSubscriber) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
	}
	_ = s.rdb.Close()
}

func (s *RouterKVSubscriber) loop(ctx context.Context, sub zmq4.Socket) {
	defer close(s.done)
	defer sub.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := sub.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			time.Sleep(time.Second)
			continue
		}
		if len(msg.Frames) != 3 {
			continue
		}
		if err := s.handlePayload(ctx, msg.Frames[2]); err != nil {
			log.Printf("[external] [%s] KV-SUB batch error: %v", s.ep.ID, err)
		}
	}
}

func (s *RouterKVSubscriber) handlePayload(ctx context.Context, payload []byte) error {
	var top []msgpack.RawMessage
	if err := msgpack.Unmarshal(payload, &top); err != nil {
		return fmt.Errorf("decode error (msgpack KVEventBatch): %w", err)
	}
	if len(top) < 2 {
		return nil
	}
	var events []msgpack.RawMessage
	if err := msgpack.Unmarshal(top[1], &events); err != nil {
		return fmt.Errorf("decode events: %w", err)
	}

	prefix := ""
	if s.ep.Model != "" {
		prefix = s.ep.Model + ":"
	}
	kvblocksKey := prefix + "kvblocks"
	podblocksKey := fmt.Sprintf("%spodblocks:%s", prefix, s.ep.ID)
	owner := s.ep.ID
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	pipe := s.rdb.Pipeline()
	for _, evRaw := range events {
		var ev []msgpack.RawMessage
		if err := msgpack.Unmarshal(evRaw, &ev); err != nil || len(ev) == 0 {
			continue
		}
		var tag string
		if err := msgpack.Unmarshal(ev[0], &tag); err != nil {
			continue
		}
		switch tag {
		case "BlockStored":
			if len(ev) < 2 {
				continue
			}
			for _, bh := range decodeExtHashList(ev[1]) {
				kvblockKey := prefix + "kvblock:" + bh
				pipe.HSet(ctx, kvblockKey, owner, ts)
				pipe.SAdd(ctx, podblocksKey, bh)
				pipe.HSet(ctx, kvblocksKey, bh, kvblockKey)
			}
		case "BlockRemoved":
			if len(ev) < 2 {
				continue
			}
			for _, bh := range decodeExtHashList(ev[1]) {
				kvblockKey := prefix + "kvblock:" + bh
				pipe.HDel(ctx, kvblockKey, owner)
				pipe.SRem(ctx, podblocksKey, bh)
			}
		case "AllBlocksCleared":
			if members, err := s.rdb.SMembers(ctx, podblocksKey).Result(); err == nil {
				for _, bh := range members {
					pipe.HDel(ctx, prefix+"kvblock:"+bh, owner)
				}
			}
			pipe.Del(ctx, podblocksKey)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("redis error: %w", err)
	}
	return nil
}

func decodeExtHashList(raw msgpack.RawMessage) []string {
	var nums []msgpack.RawMessage
	if err := msgpack.Unmarshal(raw, &nums); err != nil {
		return nil
	}
	out := make([]string, 0, len(nums))
	for _, n := range nums {
		var u uint64
		if err := msgpack.Unmarshal(n, &u); err == nil {
			out = append(out, strconv.FormatUint(u, 10))
			continue
		}
		var i int64
		if err := msgpack.Unmarshal(n, &i); err == nil {
			out = append(out, strconv.FormatInt(i, 10))
		}
	}
	return out
}

// RouterKVSubscriberPool manages one RouterKVSubscriber per external endpoint
// that declares kv_events_endpoints. No-op when ExternalKVEvents is off.
type RouterKVSubscriberPool struct {
	subs []*RouterKVSubscriber
}

func NewRouterKVSubscriberPool(cfg *Config, registry *ExternalRegistry) *RouterKVSubscriberPool {
	p := &RouterKVSubscriberPool{}
	if !cfg.ExternalKVEvents {
		return p
	}
	for _, id := range registry.AllIDs() {
		if ep, ok := registry.Get(id); ok && len(ep.KVEventsEndpoints) > 0 {
			p.subs = append(p.subs, NewRouterKVSubscriber(cfg, ep))
		}
	}
	return p
}

func (p *RouterKVSubscriberPool) Start(ctx context.Context) {
	for _, s := range p.subs {
		s.Start(ctx)
	}
}

func (p *RouterKVSubscriberPool) Stop() {
	for _, s := range p.subs {
		s.Stop()
	}
	p.subs = nil
}
