package router

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Host string
	Port int

	RedisHost string
	RedisPort int
	ModelName string

	Namespace     string
	LabelSelector string
	VllmPort      int
	SidecarPort   int

	RouterMode string // pull | push-rr | push-random | push-leastq

	KVAware         bool
	LenAware        bool
	LenPolicy       string // short_first | long_first
	PoolFactor      int
	DefaultMaxToks  int

	ResultTimeoutS     float64
	ResultPollInterval float64

	HashServiceURL string
	HashTimeoutS   float64

	PushHTTPTimeoutS float64
	PushLeastqMode   string // health | local

	TransportMode    string // sync | async_pubsub
	SubmitPath       string
	ResultsZMQBind   string
	ResultsZMQTopic  string
	ResultsZMQHWM    int
	ResultsGraceS    float64

	ResultTransportMode string // poll | submit_ack
	ResultSubmitPath    string

	TraceEnabled bool

	KVWatchIntervalS     float64
	KVWatchMaxKeys       int64
	KVDiscoveryIntervalS float64
}

func LoadConfig() *Config {
	return &Config{
		Host: envStr("HOST", "0.0.0.0"),
		Port: envInt("PORT", 8080),

		RedisHost: envStr("REDIS_HOST", "redis"),
		RedisPort: envInt("REDIS_PORT", 6379),
		ModelName: envStr("MODEL_NAME", "served-model"),

		Namespace:     envStr("NAMESPACE", "vllm"),
		LabelSelector: envStr("LABEL_SELECTOR", "app=vllm-qwen"),
		VllmPort:      envInt("VLLM_PORT", 8200),
		SidecarPort:   envInt("SIDECAR_PORT", 9000),

		RouterMode: strings.ToLower(envStr("ROUTER_MODE", "pull")),

		KVAware:        envBool("KV_AWARE", true),
		LenAware:       envBool("LEN_AWARE", true),
		LenPolicy:      envStr("LEN_POLICY", "short_first"),
		PoolFactor:     envInt("POOL_FACTOR", 4),
		DefaultMaxToks: envInt("DEFAULT_MAX_TOKENS", 1024),

		ResultTimeoutS:     envFloat("RESULT_TIMEOUT_S", 1000.0),
		ResultPollInterval: envFloat("RESULT_POLL_INTERVAL_S", 0.02),

		HashServiceURL: envStr("HASH_SERVICE_URL", "http://vllm-cpu-hash:9095"),
		HashTimeoutS:   envFloat("HASH_TIMEOUT_S", 2.0),

		PushHTTPTimeoutS: envFloat("PUSH_HTTP_TIMEOUT_S", 2.0),
		PushLeastqMode:   envStr("PUSH_LEASTQ_MODE", "local"),

		TransportMode:   envStr("TRANSPORT_MODE", "sync"),
		SubmitPath:      envStr("SUBMIT_PATH", "/submit"),
		ResultsZMQBind:  envStr("RESULTS_ZMQ_BIND", "tcp://0.0.0.0:5559"),
		ResultsZMQTopic: envStr("RESULTS_ZMQ_TOPIC", "results"),
		ResultsZMQHWM:   envInt("RESULTS_ZMQ_HWM", 500000),
		ResultsGraceS:   envFloat("RESULTS_GRACE_S", 5.0),

		ResultTransportMode: envStr("RESULT_TRANSPORT_MODE", "poll"),
		ResultSubmitPath:    envStr("RESULT_SUBMIT_PATH", "/result_submit"),

		TraceEnabled: envBool("TRACE_ENABLED", false),

		KVWatchIntervalS:     envFloat("KV_WATCH_INTERVAL_S", 1.0),
		KVWatchMaxKeys:       int64(envInt("KV_WATCH_MAX_KEYS", 200)),
		KVDiscoveryIntervalS: envFloat("KV_DISCOVERY_INTERVAL_S", 5.0),
	}
}

func (c *Config) IsPullMode() bool  { return c.RouterMode == "pull" }
func (c *Config) IsPushMode() bool  { return !c.IsPullMode() }
func (c *Config) IsAsyncPub() bool  { return c.TransportMode == "async_pubsub" }

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	v := strings.ToLower(os.Getenv(key))
	if v == "" {
		return def
	}
	return v == "true" || v == "1" || v == "yes"
}
