package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

const sglangKVVersion = "0.5.15"

type publisherEndpoint struct {
	rank      int
	eventURL  string
	replayURL string
}

type pendingEndpoint struct {
	rank int
	host string
	port int
}

func resolveTopic(engine, configured, pod, model string) string {
	value := strings.TrimSpace(configured)
	if engine != "sglang" {
		if value == "" {
			return "kv@"
		}
		return value
	}
	if value == "" || value == "kv@" {
		return fmt.Sprintf("kv@%s@%s", pod, model)
	}
	return value
}

func discoverSGLang(ctx context.Context, cfg *Config, client *http.Client, topic string) ([]publisherEndpoint, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.InferenceURL+"/server_info", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("server_info returned %d", resp.StatusCode)
	}
	var info struct {
		Version  string `json:"version"`
		KVEvents *struct {
			Publisher        string `json:"publisher"`
			BlockSize        int    `json:"block_size"`
			Topic            string `json:"topic"`
			EndpointHost     string `json:"endpoint_host"`
			EndpointPortBase int    `json:"endpoint_port_base"`
			DPSize           int    `json:"dp_size"`
		} `json:"kv_events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	if info.KVEvents == nil {
		return nil, fmt.Errorf("server_info.kv_events is missing")
	}
	var mismatches []string
	check := func(ok bool, detail string) {
		if !ok {
			mismatches = append(mismatches, detail)
		}
	}
	check(info.Version == sglangKVVersion, fmt.Sprintf("version=%q (expected %q)", info.Version, sglangKVVersion))
	check(info.KVEvents.Publisher == "zmq", fmt.Sprintf("publisher=%q (expected %q)", info.KVEvents.Publisher, "zmq"))
	check(info.KVEvents.BlockSize == cfg.KVEventExpectedPageSize, fmt.Sprintf("block_size=%d (expected %d)", info.KVEvents.BlockSize, cfg.KVEventExpectedPageSize))
	check(info.KVEvents.Topic == topic, fmt.Sprintf("topic=%q (expected %q)", info.KVEvents.Topic, topic))
	check(info.KVEvents.EndpointPortBase == cfg.KVEventPort, fmt.Sprintf("endpoint_port_base=%d (expected %d)", info.KVEvents.EndpointPortBase, cfg.KVEventPort))
	check(info.KVEvents.DPSize == cfg.DPSize, fmt.Sprintf("dp_size=%d (expected %d)", info.KVEvents.DPSize, cfg.DPSize))
	dialHost, hostErr := sglangDialHost(info.KVEvents.EndpointHost, cfg.InferenceHost)
	check(hostErr == nil, fmt.Sprintf("endpoint_host=%q (%v)", info.KVEvents.EndpointHost, hostErr))
	check(validRankPorts(info.KVEvents.EndpointPortBase, info.KVEvents.DPSize),
		fmt.Sprintf("event base port %d with %d ranks exceeds 1..65535",
			info.KVEvents.EndpointPortBase, info.KVEvents.DPSize))
	check(validRankPorts(cfg.KVEventReplayPort, cfg.DPSize),
		fmt.Sprintf("replay base port %d with %d ranks exceeds 1..65535",
			cfg.KVEventReplayPort, cfg.DPSize))
	if len(mismatches) > 0 {
		return nil, fmt.Errorf("SGLang KV descriptor mismatch: %s", strings.Join(mismatches, "; "))
	}
	out := make([]publisherEndpoint, 0, cfg.DPSize)
	for rank := 0; rank < cfg.DPSize; rank++ {
		out = append(out, publisherEndpoint{
			rank:      rank,
			eventURL:  zmqTCPEndpoint(dialHost, cfg.KVEventPort+rank),
			replayURL: zmqTCPEndpoint(dialHost, cfg.KVEventReplayPort+rank),
		})
	}
	return out, nil
}

func sglangDialHost(advertised, configured string) (string, error) {
	advertised = normalizeEndpointHost(advertised)
	configured = normalizeEndpointHost(configured)
	if advertised == "" {
		return "", fmt.Errorf("must be a non-empty string")
	}
	if isWildcardHost(advertised) {
		if configured == "" || isWildcardHost(configured) || !isConcreteHost(configured) {
			return "", fmt.Errorf("wildcard bind requires a concrete configured inference host")
		}
		return configured, nil
	}
	if !isConcreteHost(advertised) {
		return "", fmt.Errorf("is not a supported IP address or DNS hostname")
	}
	if !sameEndpointHost(advertised, configured) {
		return "", fmt.Errorf("concrete host must match configured inference host %q", configured)
	}
	return advertised, nil
}

func normalizeEndpointHost(host string) string {
	host = strings.TrimSpace(host)
	if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	return strings.TrimSuffix(strings.ToLower(host), ".")
}

func isWildcardHost(host string) bool {
	return host == "*" || host == "0.0.0.0" || host == "::"
}

func sameEndpointHost(left, right string) bool {
	leftIP, rightIP := net.ParseIP(left), net.ParseIP(right)
	if leftIP != nil || rightIP != nil {
		return leftIP != nil && rightIP != nil && leftIP.Equal(rightIP)
	}
	return left == right
}

func isConcreteHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func validRankPorts(base, ranks int) bool {
	return ranks > 0 && base > 0 && base <= 65535 && ranks-1 <= 65535-base
}

func zmqTCPEndpoint(host string, port int) string {
	return "tcp://" + net.JoinHostPort(host, fmt.Sprintf("%d", port))
}

func initialPublishers(ctx context.Context, cfg *Config, client *http.Client, topic string) ([]publisherEndpoint, []pendingEndpoint, error) {
	if cfg.InferenceEngine == "sglang" {
		if cfg.KVEventDiscoveryEnabled {
			publishers, err := discoverSGLang(ctx, cfg, client, topic)
			return publishers, nil, err
		}
		out := make([]publisherEndpoint, 0, cfg.DPSize)
		for rank := 0; rank < cfg.DPSize; rank++ {
			out = append(out, publisherEndpoint{rank: rank,
				eventURL:  fmt.Sprintf("tcp://%s:%d", cfg.InferenceHost, cfg.KVEventPort+rank),
				replayURL: fmt.Sprintf("tcp://%s:%d", cfg.InferenceHost, cfg.KVEventReplayPort+rank)})
		}
		return out, nil, nil
	}

	resolved := []publisherEndpoint{{rank: 0,
		eventURL:  fmt.Sprintf("tcp://%s:%d", cfg.InferenceHost, cfg.KVEventPort),
		replayURL: fmt.Sprintf("tcp://%s:%d", cfg.InferenceHost, cfg.KVEventReplayPort)}}
	var pending []pendingEndpoint
	if cfg.DPSize <= 1 {
		return resolved, pending, nil
	}
	service := cfg.ContainerName
	if index := strings.LastIndex(service, "-"); index >= 0 {
		service = service[:index]
	}
	for rank := 1; rank < cfg.DPSize; rank++ {
		host := fmt.Sprintf("%s-%d.%s.vllm.svc.cluster.local", cfg.ContainerName, rank, service)
		port := cfg.KVEventPort + rank*cfg.DPSizeLocal
		ips, err := net.DefaultResolver.LookupHost(ctx, host)
		if err != nil || len(ips) == 0 {
			pending = append(pending, pendingEndpoint{rank: rank, host: host, port: port})
			continue
		}
		resolved = append(resolved, publisherEndpoint{rank: rank,
			eventURL:  fmt.Sprintf("tcp://%s:%d", ips[0], port),
			replayURL: fmt.Sprintf("tcp://%s:%d", cfg.InferenceHost, cfg.KVEventReplayPort)})
	}
	return resolved, pending, nil
}

func discoveryHTTPClient(cfg *Config) *http.Client {
	return &http.Client{Timeout: time.Duration(cfg.KVEventDiscoveryTimeoutS * float64(time.Second))}
}
