package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// promLineRE matches one Prometheus exposition line: name{labels} value.
var promLineRE = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE.+-]+)\s*$`)

// promSample holds one pod's scraped metric sums plus a presence map.
type promSample struct {
	sums map[string]float64
	seen map[string]bool
}

// parsePromSums sums each requested base metric across all its label sets.
// seen is false for a base that never appears. Mirrors _parse_prom_sums.
func parsePromSums(text string, bases []string) (map[string]float64, map[string]bool) {
	wanted := make(map[string]bool, len(bases))
	sums := make(map[string]float64, len(bases))
	seen := make(map[string]bool, len(bases))
	for _, b := range bases {
		wanted[b] = true
		sums[b] = 0
		seen[b] = false
	}
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || line[0] == '#' {
			continue
		}
		m := promLineRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if wanted[m[1]] {
			if v, err := strconv.ParseFloat(m[3], 64); err == nil {
				sums[m[1]] += v
				seen[m[1]] = true
			}
		}
	}
	return sums, seen
}

// totalTokensFromSums returns prompt+generation tokens processed. ok=false only
// when neither counter is present; a single missing counter is treated as 0.
func totalTokensFromSums(sums map[string]float64, seen map[string]bool) (float64, bool) {
	if !seen["vllm:prompt_tokens_total"] && !seen["vllm:generation_tokens_total"] {
		return 0, false
	}
	return sums["vllm:prompt_tokens_total"] + sums["vllm:generation_tokens_total"], true
}

// PushDispatcher discovers vLLM pods via the Kubernetes API and
// dispatches requests to their sidecars. Supports round-robin,
// random, and least-queue selection modes.
type PushDispatcher struct {
	cfg *Config
	kv  *kvAware

	mu              sync.Mutex
	endpoints       []string          // pod names
	urls            map[string]string // pod name -> sidecar base URL
	rrIdx           int
	lastDiscovery   time.Time
	logicalInflight map[string]int

	// TTL-cached vLLM /metrics scrape for metric-based push strategies.
	metricCache    map[string]promSample
	metricCacheTS  time.Time
	metricCacheKey string

	client *http.Client
}

func NewPushDispatcher(cfg *Config, kv *kvAware) *PushDispatcher {
	transport := &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     30 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		DialContext: (&net.Dialer{
			Timeout:   time.Duration(cfg.PushHTTPTimeoutS * float64(time.Second)),
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(cfg.PushHTTPTimeoutS * float64(time.Second)),
	}

	return &PushDispatcher{
		cfg:             cfg,
		kv:              kv,
		urls:            make(map[string]string),
		logicalInflight: make(map[string]int),
		client:          client,
	}
}

// RefreshEndpoints discovers pods from the Kubernetes API.
func (pd *PushDispatcher) RefreshEndpoints() {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.refreshLocked(false)
}

func (pd *PushDispatcher) refreshLocked(force bool) {
	interval := time.Duration(pd.cfg.KVDiscoveryIntervalS * float64(time.Second))
	if !force && len(pd.endpoints) > 0 && time.Since(pd.lastDiscovery) < interval {
		return
	}

	pods := discoverPods(pd.cfg)
	eps := make([]string, 0, len(pods))
	urls := make(map[string]string, len(pods))
	for name, ip := range pods {
		eps = append(eps, name)
		urls[name] = fmt.Sprintf("http://%s:%d", ip, pd.cfg.SidecarPort)
	}

	pd.endpoints = eps
	pd.urls = urls
	pd.lastDiscovery = time.Now()

	if len(pd.endpoints) > 0 {
		pd.rrIdx = pd.rrIdx % len(pd.endpoints)
	} else {
		pd.rrIdx = 0
	}

	log.Printf("[PushRouter] discovered %d pods: %v", len(pd.endpoints), pd.endpoints)
}

// pickEndpoint selects an endpoint per RouterMode. For push-leastq it honors
// PushLeastQMode ("health" queries each sidecar /health; "local" uses logical
// inflight counters).
func (pd *PushDispatcher) pickEndpoint(reqID string) string {
	switch pd.cfg.RouterMode {
	case "push-random":
		pd.mu.Lock()
		defer pd.mu.Unlock()
		if len(pd.endpoints) == 0 {
			return ""
		}
		return pd.endpoints[rand.Intn(len(pd.endpoints))]
	case "push-leastq":
		if pd.cfg.PushLeastQMode == "local" {
			return pd.pickLeastQLocal()
		}
		return pd.pickLeastQHealth()
	case "push-throughput":
		return pd.pickThroughput()
	case "push-p2c":
		return pd.pickP2C()
	case "push-kv-cost":
		return pd.pickKVCost(reqID)
	case "push-least-kv":
		return pd.pickLeastKV()
	default:
		return pd.pickRR()
	}
}

// kvCost is the KV-aware routing cost for one worker:
// cost = prefillLoadScale * max(prefillBlocks - overlapCredit*hits, 0) + load.
// Lower is better. Mirrors _kv_cost in Python.
func kvCost(prefillBlocks, hits int, load, overlapCredit, prefillLoadScale float64) float64 {
	adjusted := float64(prefillBlocks) - overlapCredit*float64(hits)
	if adjusted < 0 {
		adjusted = 0
	}
	return prefillLoadScale*adjusted + load
}

// selectByCost picks a worker from a cost map. temperature <= 0 -> deterministic
// argmin (ties keep the first in eps order); temperature > 0 -> softmax sampling
// over negated, min-shifted costs. Mirrors _select_by_cost in Python. eps gives
// a stable iteration order (Go maps are unordered).
func selectByCost(eps []string, costs map[string]float64, temperature float64) string {
	if len(eps) == 0 {
		return ""
	}
	if temperature <= 0 {
		best := eps[0]
		bestC := costs[best]
		for _, ep := range eps[1:] {
			if costs[ep] < bestC {
				bestC = costs[ep]
				best = ep
			}
		}
		return best
	}
	lo := costs[eps[0]]
	for _, ep := range eps[1:] {
		if costs[ep] < lo {
			lo = costs[ep]
		}
	}
	weights := make([]float64, len(eps))
	total := 0.0
	for i, ep := range eps {
		weights[i] = math.Exp(-(costs[ep] - lo) / temperature)
		total += weights[i]
	}
	if total <= 0 {
		return eps[0]
	}
	r := rand.Float64() * total
	acc := 0.0
	for i, ep := range eps {
		acc += weights[i]
		if r <= acc {
			return ep
		}
	}
	return eps[len(eps)-1]
}

// fetchHealthLoads queries each sidecar /health in parallel for its decode-load
// proxy (logical/queue_len). Missing/failed probes map to 0.0.
func (pd *PushDispatcher) fetchHealthLoads(eps []string, urls map[string]string) map[string]float64 {
	out := make(map[string]float64, len(eps))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ep := range eps {
		wg.Add(1)
		go func(ep string) {
			defer wg.Done()
			load := 0.0
			if url := urls[ep]; url != "" {
				if resp, err := pd.client.Get(url + "/health"); err == nil {
					if resp.StatusCode == http.StatusOK {
						var data map[string]interface{}
						if json.NewDecoder(resp.Body).Decode(&data) == nil {
							if v, ok := data["logical"]; ok {
								load = toFloat(v)
							} else if v, ok := data["queue_len"]; ok {
								load = toFloat(v)
							}
						}
					} else {
						io.Copy(io.Discard, resp.Body)
					}
					resp.Body.Close()
				}
			}
			mu.Lock()
			out[ep] = load
			mu.Unlock()
		}(ep)
	}
	wg.Wait()
	return out
}

// pickKVCost implements KV-aware cost routing (push-kv-cost): it
// scores each pod by cost = prefill_load_scale * max(prefill_blocks -
// overlap_credit*cached_prefix, 0) + decode_load and picks the min-cost pod (or
// softmax-samples when RouterTemperature > 0). Mirrors _pick_endpoint_kv_cost.
func (pd *PushDispatcher) pickKVCost(reqID string) string {
	pd.mu.Lock()
	eps := append([]string{}, pd.endpoints...)
	urls := make(map[string]string, len(pd.urls))
	for k, v := range pd.urls {
		urls[k] = v
	}
	inflight := make(map[string]int, len(pd.logicalInflight))
	for k, v := range pd.logicalInflight {
		inflight[k] = v
	}
	pd.mu.Unlock()

	if len(eps) == 0 {
		return ""
	}
	if len(eps) == 1 {
		return eps[0]
	}

	var loads map[string]float64
	if pd.cfg.PushLeastQMode == "local" {
		loads = make(map[string]float64, len(eps))
		for _, ep := range eps {
			loads[ep] = float64(inflight[ep])
		}
	} else {
		loads = pd.fetchHealthLoads(eps, urls)
	}

	prefillBlocks := 0
	if pd.kv != nil && reqID != "" {
		prefillBlocks = len(pd.kv.getRequestBlocks(reqID))
	}
	costs := make(map[string]float64, len(eps))
	for _, ep := range eps {
		hits := 0
		if pd.kv != nil && reqID != "" {
			hits = pd.kv.prefixLen(ep, reqID)
		}
		costs[ep] = kvCost(prefillBlocks, hits, loads[ep], pd.cfg.RouterKVOverlapCredit, pd.cfg.RouterPrefillLoadScale)
	}
	return selectByCost(eps, costs, pd.cfg.RouterTemperature)
}

// epScore pairs an endpoint with a numeric score and whether the probe
// produced a usable value.
// fetchHealthField queries every sidecar's /health in parallel and reads a
// numeric field (e.g. "kv_usage") from each. Missing/failed probes are ok=false.
func (pd *PushDispatcher) fetchHealthField(field string) []epScore {
	pd.mu.Lock()
	eps := append([]string{}, pd.endpoints...)
	urls := make(map[string]string, len(pd.urls))
	for k, v := range pd.urls {
		urls[k] = v
	}
	pd.mu.Unlock()

	scores := make([]epScore, len(eps))
	var wg sync.WaitGroup
	for i, ep := range eps {
		wg.Add(1)
		go func(i int, ep string) {
			defer wg.Done()
			scores[i] = epScore{ep: ep, ok: false}
			url := urls[ep]
			if url == "" {
				return
			}
			resp, err := pd.client.Get(url + "/health")
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				io.Copy(io.Discard, resp.Body)
				return
			}
			var data map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
				return
			}
			if v, ok := data[field]; ok && v != nil {
				scores[i] = epScore{ep: ep, score: toFloat(v), ok: true}
			}
		}(i, ep)
	}
	wg.Wait()
	return scores
}

// pickLeastKV routes to the pod with the lowest KV-cache occupancy, read from
// the sidecar-reported kv_usage on /health (requires sidecar.kvUsageReport).
// Covers the least-kv-cache and least-gpu-cache names. Falls back to round-robin
// if no pod reports kv_usage. Mirrors _pick_endpoint_least_kv.
func (pd *PushDispatcher) pickLeastKV() string {
	best := pickMinScore(pd.fetchHealthField("kv_usage"))
	if best == "" {
		return pd.pickRR()
	}
	return best
}

// chooseLowerLoad is the power-of-two-choices comparator: return the
// less-loaded of two endpoints. A false ok flag means the load probe failed
// for that endpoint (treated as +inf so a reachable peer wins); if both fail
// the first sampled endpoint is returned. Mirrors _pick_lower_load in Python.
func chooseLowerLoad(a string, sa int, aOK bool, b string, sb int, bOK bool) string {
	if !aOK && !bOK {
		return a
	}
	if !aOK {
		return b
	}
	if !bOK {
		return a
	}
	if sa <= sb {
		return a
	}
	return b
}

// pickP2C samples two distinct pods and routes to the less loaded one. Load
// comes from local logical inflight when PushLeastQMode=local, else each
// candidate's /health. Mirrors PushRouter._pick_endpoint_p2c.
func (pd *PushDispatcher) pickP2C() string {
	pd.mu.Lock()
	eps := append([]string{}, pd.endpoints...)
	urls := make(map[string]string, len(pd.urls))
	for k, v := range pd.urls {
		urls[k] = v
	}
	inflight := make(map[string]int, len(pd.logicalInflight))
	for k, v := range pd.logicalInflight {
		inflight[k] = v
	}
	pd.mu.Unlock()

	if len(eps) == 0 {
		return ""
	}
	if len(eps) == 1 {
		return eps[0]
	}

	i := rand.Intn(len(eps))
	j := rand.Intn(len(eps) - 1)
	if j >= i {
		j++
	}
	a, b := eps[i], eps[j]

	if pd.cfg.PushLeastQMode == "local" {
		return chooseLowerLoad(a, inflight[a], true, b, inflight[b], true)
	}
	sa, aOK := pd.fetchHealthLoad(urls[a])
	sb, bOK := pd.fetchHealthLoad(urls[b])
	return chooseLowerLoad(a, sa, aOK, b, sb, bOK)
}

// fetchHealthLoad returns a single sidecar's load score (logical/queue_len)
// from /health, and whether the probe succeeded.
func (pd *PushDispatcher) fetchHealthLoad(url string) (int, bool) {
	if url == "" {
		return 0, false
	}
	resp, err := pd.client.Get(url + "/health")
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return 0, false
	}
	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0, false
	}
	if v, ok := data["logical"]; ok {
		return int(toFloat(v)), true
	}
	if v, ok := data["queue_len"]; ok {
		return int(toFloat(v)), true
	}
	return 0, true
}

// epScore pairs an endpoint with a numeric score and whether the probe
// produced a usable value.
type epScore struct {
	ep    string
	score float64
	ok    bool
}

// pickMinScore returns the endpoint with the smallest score, skipping entries
// whose probe failed. Returns "" when none are usable. Mirrors _pick_min_score.
func pickMinScore(scores []epScore) string {
	best := ""
	var bestS float64
	for _, s := range scores {
		if !s.ok {
			continue
		}
		if best == "" || s.score < bestS {
			bestS = s.score
			best = s.ep
		}
	}
	return best
}

// scrapeMetricSums scrapes vLLM /metrics for every pod (deriving the metrics
// URL from the sidecar URL by swapping to VLLM_METRICS_PORT, default 8200) and
// sums the requested metric bases per pod, cached for PUSH_METRIC_TTL_S
// (default 1s). Mirrors _scrape_metric_sums.
func (pd *PushDispatcher) scrapeMetricSums(bases []string) map[string]promSample {
	now := time.Now()
	ttlS := 1.0
	if v := os.Getenv("PUSH_METRIC_TTL_S"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			ttlS = f
		}
	}
	key := strings.Join(bases, ",")

	pd.mu.Lock()
	if pd.metricCache != nil && pd.metricCacheKey == key &&
		now.Sub(pd.metricCacheTS) < time.Duration(ttlS*float64(time.Second)) {
		cached := pd.metricCache
		pd.mu.Unlock()
		return cached
	}
	eps := append([]string{}, pd.endpoints...)
	urls := make(map[string]string, len(pd.urls))
	for k, v := range pd.urls {
		urls[k] = v
	}
	pd.mu.Unlock()

	port := os.Getenv("VLLM_METRICS_PORT")
	if port == "" {
		port = "8200"
	}

	out := make(map[string]promSample, len(eps))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ep := range eps {
		wg.Add(1)
		go func(ep string) {
			defer wg.Done()
			empty := promSample{sums: map[string]float64{}, seen: map[string]bool{}}
			url := urls[ep]
			if url == "" {
				mu.Lock()
				out[ep] = empty
				mu.Unlock()
				return
			}
			base := url
			if idx := strings.LastIndex(url, ":"); idx >= 0 {
				base = url[:idx]
			}
			resp, err := pd.client.Get(base + ":" + port + "/metrics")
			if err != nil {
				mu.Lock()
				out[ep] = empty
				mu.Unlock()
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				io.Copy(io.Discard, resp.Body)
				mu.Lock()
				out[ep] = empty
				mu.Unlock()
				return
			}
			body, _ := io.ReadAll(resp.Body)
			sums, seen := parsePromSums(string(body), bases)
			mu.Lock()
			out[ep] = promSample{sums: sums, seen: seen}
			mu.Unlock()
		}(ep)
	}
	wg.Wait()

	pd.mu.Lock()
	pd.metricCache = out
	pd.metricCacheTS = now
	pd.metricCacheKey = key
	pd.mu.Unlock()
	return out
}

// pickThroughput routes to the pod that has processed the fewest total tokens
// (vllm:prompt_tokens_total + vllm:generation_tokens_total), favoring
// underloaded pods. Falls back to round-robin when the counters are absent.
func (pd *PushDispatcher) pickThroughput() string {
	samples := pd.scrapeMetricSums([]string{
		"vllm:prompt_tokens_total",
		"vllm:generation_tokens_total",
	})
	scores := make([]epScore, 0, len(samples))
	for ep, s := range samples {
		v, ok := totalTokensFromSums(s.sums, s.seen)
		scores = append(scores, epScore{ep: ep, score: v, ok: ok})
	}
	if best := pickMinScore(scores); best != "" {
		return best
	}
	return pd.pickRR()
}

func (pd *PushDispatcher) pickRR() string {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	if len(pd.endpoints) == 0 {
		return ""
	}
	ep := pd.endpoints[pd.rrIdx%len(pd.endpoints)]
	pd.rrIdx = (pd.rrIdx + 1) % len(pd.endpoints)
	return ep
}

func (pd *PushDispatcher) pickLeastQLocal() string {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	best := ""
	bestScore := 0
	for _, ep := range pd.endpoints {
		score := pd.logicalInflight[ep]
		if best == "" || score < bestScore {
			bestScore = score
			best = ep
		}
	}
	return best
}

// pickLeastQHealth queries every sidecar's /health endpoint in parallel and
// selects the one with the lowest logical (or queue_len) score. Mirrors
// PushRouter._pick_endpoint_leastq_health.
func (pd *PushDispatcher) pickLeastQHealth() string {
	pd.mu.Lock()
	eps := append([]string{}, pd.endpoints...)
	urls := make(map[string]string, len(pd.urls))
	for k, v := range pd.urls {
		urls[k] = v
	}
	pd.mu.Unlock()

	if len(eps) == 0 {
		return ""
	}

	type scoreRes struct {
		ep    string
		score int
		ok    bool
	}
	results := make([]scoreRes, len(eps))
	var wg sync.WaitGroup
	for i, ep := range eps {
		wg.Add(1)
		go func(i int, ep string) {
			defer wg.Done()
			url := urls[ep]
			if url == "" {
				results[i] = scoreRes{ep, 0, false}
				return
			}
			resp, err := pd.client.Get(url + "/health")
			if err != nil {
				results[i] = scoreRes{ep, 0, false}
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				io.Copy(io.Discard, resp.Body)
				results[i] = scoreRes{ep, 0, false}
				return
			}
			var data map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
				results[i] = scoreRes{ep, 0, false}
				return
			}
			score := 0
			if v, ok := data["logical"]; ok {
				score = int(toFloat(v))
			} else if v, ok := data["queue_len"]; ok {
				score = int(toFloat(v))
			}
			results[i] = scoreRes{ep, score, true}
		}(i, ep)
	}
	wg.Wait()

	best := ""
	bestScore := 0
	for _, r := range results {
		if !r.ok {
			continue
		}
		if best == "" || r.score < bestScore {
			bestScore = r.score
			best = r.ep
		}
	}
	return best
}

// RouteAndPush dispatches a request to a sidecar, with one retry after a forced
// endpoint refresh. Mirrors PushRouter.route_and_push including trace
// enrichment, dispatch metrics, and leastq-local inflight bookkeeping.
func (pd *PushDispatcher) RouteAndPush(reqID, prompt string, meta map[string]interface{}) error {
	pd.mu.Lock()
	pd.refreshLocked(false)
	pd.mu.Unlock()

	localMode := pd.cfg.RouterMode == "push-leastq" && pd.cfg.PushLeastQMode == "local"

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt == 1 {
			pd.mu.Lock()
			pd.refreshLocked(true)
			pd.mu.Unlock()
		}

		pd.mu.Lock()
		n := len(pd.endpoints)
		pd.mu.Unlock()
		if n == 0 {
			return fmt.Errorf("No endpoints available for push routing")
		}

		ep := pd.pickEndpoint(reqID)
		if ep == "" {
			return fmt.Errorf("Failed to pick endpoint")
		}

		pd.mu.Lock()
		url := pd.urls[ep]
		pd.mu.Unlock()
		if url == "" {
			lastErr = fmt.Errorf("No sidecar URL for endpoint %s", ep)
			continue
		}

		// Prom: outgoing dispatch (router -> sidecar).
		incDispatch(ep)

		var logicalBefore int
		hasLogical := false
		if localMode {
			pd.mu.Lock()
			logicalBefore = pd.logicalInflight[ep]
			pd.logicalInflight[ep] = logicalBefore + 1
			pd.mu.Unlock()
			hasLogical = true
		}

		sendMeta := meta
		if pd.cfg.TraceEnabled {
			sendMeta = cloneMeta(meta)
			tr := traceOf(sendMeta)
			if _, ok := tr["endpoint"]; !ok {
				tr["endpoint"] = ep
			}
			if _, ok := tr["router_mode"]; !ok {
				tr["router_mode"] = pd.cfg.RouterMode
			}
			tr["t_dispatch_router"] = nowS()
			if pd.cfg.KVAware && pd.kv != nil {
				blocks := pd.kv.getRequestBlocks(reqID)
				ifaces := make([]interface{}, len(blocks))
				for i, b := range blocks {
					ifaces[i] = b
				}
				tr["kv_block_hashes"] = ifaces
			}
			if hasLogical {
				tr["router_logical_inflight_before"] = logicalBefore
				tr["router_logical_inflight_after"] = logicalBefore + 1
			}
			sendMeta["__trace__"] = tr
		}

		// Capture the per-request routing decision (independent of TRACE) so
		// recordLatency can enrich /latency_log. prefixLen returns 0 when no
		// blocks were registered (measurement off). Mirrors push_router.py.
		if pd.kv != nil {
			blocks := pd.kv.getRequestBlocks(reqID)
			info := routingInfo{endpoint: ep, kvHitsLen: pd.kv.prefixLen(ep, reqID), totalBlocks: len(blocks)}
			if meta != nil {
				if ak, ok := meta["__affinity_key__"].(string); ok && ak != "" {
					info.affinityKey, info.hasAffinity = ak, true
				}
			}
			if pd.cfg.LogBlockHashes {
				info.blockHashes, info.hasBlocks = blocks, true
			}
			pd.kv.recordRouting(reqID, info)
		}

		payload := map[string]interface{}{
			"req_id": reqID,
			"prompt": prompt,
			"meta":   sendMeta,
		}
		if pd.cfg.PushLeastQMode == "local" {
			payload["endpoint"] = ep
		}

		body, err := json.Marshal(payload)
		if err != nil {
			if localMode {
				pd.decrementInflight(ep)
			}
			lastErr = err
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pd.cfg.PushHTTPTimeoutS*float64(time.Second)))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/push", bytes.NewReader(body))
		if err != nil {
			cancel()
			if localMode {
				pd.decrementInflight(ep)
			}
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := pd.client.Do(req)
		cancel()
		if err != nil {
			if localMode {
				pd.decrementInflight(ep)
			}
			lastErr = err
			log.Printf("[PushRouter] push failed for %s: %v", ep, err)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			if localMode {
				pd.decrementInflight(ep)
			}
			lastErr = fmt.Errorf("push to %s failed: status %d", ep, resp.StatusCode)
			log.Printf("[PushRouter] push to %s failed: status %d", ep, resp.StatusCode)
			continue
		}

		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("push failed")
}

// EndpointsSnapshot returns the current discovered pod names (refreshing if
// stale). Used by the central-push dispatcher to iterate delivery targets.
func (pd *PushDispatcher) EndpointsSnapshot() []string {
	pd.mu.Lock()
	pd.refreshLocked(false)
	out := append([]string{}, pd.endpoints...)
	pd.mu.Unlock()
	return out
}

// PushToEndpoint delivers a single pre-selected request to a specific sidecar
// via POST {url}/push. Unlike RouteAndPush, the target endpoint is chosen by
// the central scheduler (Pull), so there is no pickEndpoint. Returns an error
// on failure so the caller can requeue + release in-flight. Mirrors
// PushRouter.push_to_endpoint.
func (pd *PushDispatcher) PushToEndpoint(endpoint, reqID, prompt string, meta map[string]interface{}) error {
	pd.mu.Lock()
	url := pd.urls[endpoint]
	if url == "" {
		pd.refreshLocked(true)
		url = pd.urls[endpoint]
	}
	pd.mu.Unlock()
	if url == "" {
		return fmt.Errorf("No sidecar URL for endpoint %s", endpoint)
	}

	incDispatch(endpoint)

	sendMeta := meta
	if pd.cfg.TraceEnabled {
		sendMeta = cloneMeta(meta)
		tr := traceOf(sendMeta)
		if _, ok := tr["endpoint"]; !ok {
			tr["endpoint"] = endpoint
		}
		if _, ok := tr["router_mode"]; !ok {
			tr["router_mode"] = pd.cfg.RouterMode
		}
		tr["t_dispatch_router"] = nowS()
		if pd.cfg.KVAware && pd.kv != nil {
			blocks := pd.kv.getRequestBlocks(reqID)
			ifaces := make([]interface{}, len(blocks))
			for i, b := range blocks {
				ifaces[i] = b
			}
			tr["kv_block_hashes"] = ifaces
		}
		sendMeta["__trace__"] = tr
	}

	// Per-request routing decision (independent of TRACE) for /latency_log.
	if pd.kv != nil {
		blocks := pd.kv.getRequestBlocks(reqID)
		info := routingInfo{endpoint: endpoint, kvHitsLen: pd.kv.prefixLen(endpoint, reqID), totalBlocks: len(blocks)}
		if meta != nil {
			if ak, ok := meta["__affinity_key__"].(string); ok && ak != "" {
				info.affinityKey, info.hasAffinity = ak, true
			}
		}
		if pd.cfg.LogBlockHashes {
			info.blockHashes, info.hasBlocks = blocks, true
		}
		pd.kv.recordRouting(reqID, info)
	}

	payload := map[string]interface{}{
		"req_id":   reqID,
		"prompt":   prompt,
		"meta":     sendMeta,
		"endpoint": endpoint,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pd.cfg.PushHTTPTimeoutS*float64(time.Second)))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/push", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := pd.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("push to %s failed: status %d", endpoint, resp.StatusCode)
	}
	return nil
}

// NotifyResult decrements the logical inflight counter for an endpoint when a
// result arrives (only meaningful in leastq-local mode).
func (pd *PushDispatcher) NotifyResult(endpoint string) {
	if endpoint == "" {
		return
	}
	pd.mu.Lock()
	defer pd.mu.Unlock()
	found := false
	for _, ep := range pd.endpoints {
		if ep == endpoint {
			found = true
			break
		}
	}
	if !found {
		return
	}
	if pd.logicalInflight[endpoint] > 0 {
		pd.logicalInflight[endpoint]--
	}
}

func (pd *PushDispatcher) decrementInflight(ep string) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	if pd.logicalInflight[ep] > 0 {
		pd.logicalInflight[ep]--
	}
}

// discoverPods queries the Kubernetes API to find running pods
// matching the configured namespace and label selector.
// Uses raw HTTP to avoid importing the full client-go library.
func discoverPods(cfg *Config) map[string]string {
	return discoverPodsSelector(cfg, cfg.LabelSelector)
}

// discoverPodsSelector is discoverPods with an explicit label selector (used
// by multi-model discovery).
func discoverPodsSelector(cfg *Config, labelSelector string) map[string]string {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")

	if host == "" {
		return discoverPodsExternal(cfg)
	}

	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		log.Printf("[K8s] failed to read service account token: %v", err)
		return nil
	}

	caCert, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		log.Printf("[K8s] failed to read CA cert: %v", err)
		return nil
	}

	_ = caCert // In production, would configure TLS with this CA
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}

	url := fmt.Sprintf("https://%s:%s/api/v1/namespaces/%s/pods?labelSelector=%s",
		host, port, cfg.Namespace, labelSelector)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Printf("[K8s] failed to build request: %v", err)
		return nil
	}
	req.Header.Set("Authorization", "Bearer "+string(token))

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[K8s] list pods failed: %v", err)
		return nil
	}
	defer resp.Body.Close()

	return parsePodList(resp.Body)
}

// discoverPodsExternal tries to use kubeconfig for out-of-cluster discovery.
// Falls back to empty if unable.
func discoverPodsExternal(cfg *Config) map[string]string {
	log.Println("[K8s] not running in cluster; pod discovery unavailable")
	return nil
}

func parsePodList(body io.Reader) map[string]string {
	var podList struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Status struct {
				Phase string `json:"phase"`
				PodIP string `json:"podIP"`
			} `json:"status"`
		} `json:"items"`
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return nil
	}
	if err := json.Unmarshal(data, &podList); err != nil {
		return nil
	}

	result := make(map[string]string)
	for _, pod := range podList.Items {
		if pod.Status.Phase == "Running" && pod.Status.PodIP != "" {
			result[pod.Metadata.Name] = pod.Status.PodIP
		}
	}
	return result
}
