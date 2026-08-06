package sidecar

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

type EngineProber struct {
	cfg    *Config
	client *http.Client
	mu     sync.Mutex
	cache  map[bool]probeCache
}

type probeCache struct {
	at time.Time
	ok bool
}

func NewEngineProber(cfg *Config) *EngineProber {
	return &EngineProber{
		cfg:    cfg,
		client: &http.Client{Timeout: time.Duration(cfg.InferenceHealthTimeoutS * float64(time.Second))},
		cache:  make(map[bool]probeCache),
	}
}

func (p *EngineProber) Probe(ctx context.Context, readiness bool) bool {
	p.mu.Lock()
	cached := p.cache[readiness]
	if time.Since(cached.at) < time.Second {
		p.mu.Unlock()
		return cached.ok
	}
	p.cache[readiness] = probeCache{at: time.Now(), ok: cached.ok}
	p.mu.Unlock()

	profile := NewEngineHealthProfile(p.cfg)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.InferenceURL+profile.Path(readiness), nil)
	ok := false
	if err == nil {
		if resp, requestErr := p.client.Do(req); requestErr == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			ok = resp.StatusCode == http.StatusOK
		}
	}
	p.mu.Lock()
	p.cache[readiness] = probeCache{at: time.Now(), ok: ok}
	p.mu.Unlock()
	return ok
}

func HealthResponse(ctx context.Context, cfg *Config, queue *LocalQueue, puller *RouterPullWorker, kv KVSubscriber, prober *EngineProber, readiness bool) (map[string]any, int) {
	profile := NewEngineHealthProfile(cfg)
	engineOK := false
	if readiness && puller != nil {
		engineOK = puller.EngineHealthy()
	} else {
		engineOK = prober.Probe(ctx, readiness)
	}
	kvStatus := SubscriberStatus{FailClosed: true, Phase: "stopped", CacheVisibility: "none"}
	kvPresent := kv != nil
	if kvPresent {
		kvStatus = kv.Status()
	}
	kvRequired := readiness && profile.EngineType == "sglang"
	kvReady := kvPresent && kvStatus.Ready
	ok := engineOK && (!kvRequired || kvReady)
	status := "ok"
	if !engineOK {
		if profile.EngineType == "vllm" {
			status = "vllm_unhealthy"
		} else {
			status = "engine_unhealthy"
		}
	} else if kvRequired && !kvReady {
		status = "kv_unready"
	}
	state := queue.State()
	var kvBody any
	if kvPresent {
		kvBody = map[string]any{
			"healthy": kvStatus.Healthy, "fail_closed": kvStatus.FailClosed,
			"phase": kvStatus.Phase, "detail": kvStatus.Detail,
			"cache_visibility": kvStatus.CacheVisibility,
		}
	}
	body := map[string]any{
		"status": status, "engine": profile.EngineType, "engine_ready": engineOK,
		"probe":          map[bool]string{true: "readiness", false: "liveness"}[readiness],
		"engine_healthy": engineOK, "vllm_healthy": engineOK,
		"kv_ready": kvReady, "kv_status": kvBody,
		"queue_len": state.Pending, "inflight": state.Inflight,
		"logical": state.Pending + state.Inflight,
	}
	if kvUsage, ok := GetCachedKvUsage(); ok {
		body["kv_usage"] = kvUsage
	}
	if !ok {
		return body, http.StatusServiceUnavailable
	}
	return body, http.StatusOK
}
