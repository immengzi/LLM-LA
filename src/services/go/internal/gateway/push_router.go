package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

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
func (pd *PushDispatcher) pickEndpoint() string {
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
	default:
		return pd.pickRR()
	}
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

		ep := pd.pickEndpoint()
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
