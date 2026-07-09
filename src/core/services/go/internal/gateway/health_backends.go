package gateway

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// discoverVLLMLeaders mirrors api._discover_vllm_leaders: prefer leader-only
// pods (role=leader); fall back to all matching pods. Returns nil only when
// discovery is impossible (e.g. not running in-cluster), [] when no pods.
func discoverVLLMLeaders(cfg *Config) map[string]string {
	leaderSel := cfg.LabelSelector + ",role=leader"
	if pods := discoverPodsSelector(cfg, leaderSel); len(pods) > 0 {
		return pods
	}
	return discoverPodsSelector(cfg, cfg.LabelSelector)
}

// probeBackends concurrently GETs http://<ip>:<VLLM_PORT>/health for each pod
// and returns (podName -> "healthy"|"unhealthy", healthyCount).
func probeBackends(cfg *Config, pods map[string]string) (map[string]string, int) {
	type res struct {
		name string
		ok   bool
	}
	out := make(chan res, len(pods))
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 3 * time.Second}
	for name, ip := range pods {
		wg.Add(1)
		go func(name, ip string) {
			defer wg.Done()
			url := fmt.Sprintf("http://%s:%d/health", ip, cfg.VLLMPort)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				out <- res{name, false}
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				out <- res{name, false}
				return
			}
			resp.Body.Close()
			out <- res{name, resp.StatusCode == 200}
		}(name, ip)
	}
	wg.Wait()
	close(out)

	status := make(map[string]string, len(pods))
	healthy := 0
	for r := range out {
		if r.ok {
			status[r.name] = "healthy"
			healthy++
		} else {
			status[r.name] = "unhealthy"
		}
	}
	return status, healthy
}
