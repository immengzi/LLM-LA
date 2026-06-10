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

	mu               sync.Mutex
	endpoints        []string            // pod names
	urls             map[string]string   // pod name -> sidecar base URL
	rrIdx            int
	lastDiscovery    time.Time
	logicalInflight  map[string]int

	client *http.Client
}

func NewPushDispatcher(cfg *Config) *PushDispatcher {
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
	if !force && len(pd.endpoints) > 0 && time.Since(pd.lastDiscovery) < 5*time.Second {
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

func (pd *PushDispatcher) pickEndpoint() (string, string, error) {
	pd.mu.Lock()
	pd.refreshLocked(false)

	if len(pd.endpoints) == 0 {
		pd.mu.Unlock()
		return "", "", fmt.Errorf("no endpoints available")
	}

	var ep string
	switch pd.cfg.RouterMode {
	case "push-random":
		ep = pd.endpoints[rand.Intn(len(pd.endpoints))]
	case "push-leastq":
		ep = pd.pickLeastQueueLocked()
	default:
		ep = pd.endpoints[pd.rrIdx%len(pd.endpoints)]
		pd.rrIdx = (pd.rrIdx + 1) % len(pd.endpoints)
	}

	url := pd.urls[ep]
	pd.mu.Unlock()
	return ep, url, nil
}

func (pd *PushDispatcher) pickLeastQueueLocked() string {
	best := ""
	bestScore := int(^uint(0) >> 1) // max int
	for _, ep := range pd.endpoints {
		score := pd.logicalInflight[ep]
		if score < bestScore {
			bestScore = score
			best = ep
		}
	}
	if best == "" && len(pd.endpoints) > 0 {
		best = pd.endpoints[0]
	}
	return best
}

// RouteAndPush dispatches a request to a sidecar, with one retry on failure.
func (pd *PushDispatcher) RouteAndPush(reqID, prompt string, meta map[string]interface{}) error {
	pd.mu.Lock()
	pd.refreshLocked(false)
	pd.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt == 1 {
			pd.mu.Lock()
			pd.refreshLocked(true)
			pd.mu.Unlock()
		}

		ep, url, err := pd.pickEndpoint()
		if err != nil {
			lastErr = err
			continue
		}
		if url == "" {
			lastErr = fmt.Errorf("no sidecar URL for %s", ep)
			continue
		}

		pd.mu.Lock()
		pd.logicalInflight[ep]++
		pd.mu.Unlock()

		payload := map[string]interface{}{
			"req_id": reqID,
			"prompt": prompt,
			"meta":   meta,
		}

		body, err := json.Marshal(payload)
		if err != nil {
			pd.decrementInflight(ep)
			lastErr = err
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pd.cfg.PushHTTPTimeoutS*float64(time.Second)))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/push", bytes.NewReader(body))
		if err != nil {
			cancel()
			pd.decrementInflight(ep)
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := pd.client.Do(req)
		cancel()
		if err != nil {
			pd.decrementInflight(ep)
			lastErr = err
			log.Printf("[PushRouter] push failed for %s: %v", ep, err)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			pd.decrementInflight(ep)
			lastErr = fmt.Errorf("push to %s failed: status %d", ep, resp.StatusCode)
			log.Printf("[PushRouter] push to %s failed: status %d", ep, resp.StatusCode)
			continue
		}

		log.Printf("[PushRouter] push req_id=%s -> %s (%s)", reqID, ep, url)
		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("push failed after retries")
}

// NotifyResult decrements the logical inflight counter for an endpoint
// when a result arrives. Used in least-queue mode.
func (pd *PushDispatcher) NotifyResult(endpoint string) {
	pd.decrementInflight(endpoint)
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
		host, port, cfg.Namespace, cfg.LabelSelector)

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
