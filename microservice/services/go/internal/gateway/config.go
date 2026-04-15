package gateway

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/saeid/kv-serving-go/internal/common"
)

type Config struct {
	Host string
	Port int

	RedisHost string
	RedisPort int
	ModelName string

	KVAware   bool
	LenAware  bool
	LenPolicy string

	HashServiceURL string
	HashTimeoutS   float64

	RouterMode  string
	SidecarPort int

	ResultTimeoutS      float64
	ResultPollIntervalS float64

	TransportMode string
	SubmitPath    string

	ResultsZMQBind  string
	ResultsZMQTopic string
	ResultsZMQHWM   int

	ResultTransportMode string
	ResultSubmitPath    string

	SLOAware          bool
	SLOWithKV         bool
	AdmissionThrottle bool

	FixedBatchSize     int
	OutputLenPredictor string
	BatchSizeEstimate  string
	FixedBatchEstimate int

	LatencyPredictor   string
	LatencyOnlineUpdate bool
	LatencyProfilePath string

	QueueWaitModel       string
	ChunkedPrefillAware  bool
	MaxNumBatchedTokens  int

	Namespace     string
	LabelSelector string
	VLLMPort      int

	PoolFactor      float64
	DefaultMaxTokens int

	PushHTTPTimeoutS float64
	PushLeastQMode   string

	TraceEnabled      bool
	TraceSamplingRate float64

	ReqLogMode string
}

func LoadConfig() *Config {
	cfg := &Config{
		Host: common.EnvStr("HOST", "0.0.0.0"),
		Port: common.EnvInt("PORT", 8080),

		RedisHost: common.EnvStr("REDIS_HOST", "redis"),
		RedisPort: common.EnvInt("REDIS_PORT", 6379),
		ModelName: common.EnvStr("MODEL_NAME", "served-model"),

		KVAware:   common.EnvBool("KV_AWARE", true),
		LenAware:  common.EnvBool("LEN_AWARE", true),
		LenPolicy: common.EnvStr("LEN_POLICY", "short_first"),

		HashServiceURL: common.EnvStr("HASH_SERVICE_URL", "http://prefix-hash-service:9095"),
		HashTimeoutS:   common.EnvFloat("HASH_TIMEOUT_S", 5.0),

		RouterMode:  common.EnvStr("ROUTER_MODE", "pull"),
		SidecarPort: common.EnvInt("SIDECAR_PORT", 9000),

		ResultTimeoutS:      common.EnvFloat("RESULT_TIMEOUT_S", 120.0),
		ResultPollIntervalS: common.EnvFloat("RESULT_POLL_INTERVAL_S", 0.05),

		TransportMode: common.EnvStr("TRANSPORT_MODE", "sync"),
		SubmitPath:    common.EnvStr("SUBMIT_PATH", "/submit"),

		ResultsZMQBind:  common.EnvStr("RESULTS_ZMQ_BIND", "tcp://0.0.0.0:5559"),
		ResultsZMQTopic: common.EnvStr("RESULTS_ZMQ_TOPIC", "results"),
		ResultsZMQHWM:   common.EnvInt("RESULTS_ZMQ_HWM", 10000),

		ResultTransportMode: common.EnvStr("RESULT_TRANSPORT_MODE", "sync"),
		ResultSubmitPath:    common.EnvStr("RESULT_SUBMIT_PATH", "/result_submit"),

		SLOAware:          common.EnvBool("SLO_AWARE", false),
		SLOWithKV:         common.EnvBool("SLO_WITH_KV", true),
		AdmissionThrottle: common.EnvBool("ADMISSION_THROTTLE", false),

		FixedBatchSize:     common.EnvInt("FIXED_BATCH_SIZE", 0),
		OutputLenPredictor: common.EnvStr("OUTPUT_LEN_PREDICTOR", "simple"),
		BatchSizeEstimate:  common.EnvStr("BATCH_SIZE_ESTIMATE", "fixed"),
		FixedBatchEstimate: common.EnvInt("FIXED_BATCH_ESTIMATE", 8),

		LatencyPredictor:    common.EnvStr("LATENCY_PREDICTOR", "linear"),
		LatencyOnlineUpdate: common.EnvBool("LATENCY_ONLINE_UPDATE", false),
		LatencyProfilePath:  common.EnvStr("LATENCY_PROFILE_PATH", ""),

		QueueWaitModel:      common.EnvStr("QUEUE_WAIT_MODEL", "none"),
		ChunkedPrefillAware: common.EnvBool("CHUNKED_PREFILL_AWARE", false),
		MaxNumBatchedTokens:  common.EnvInt("MAX_NUM_BATCHED_TOKENS", 0),

		Namespace:     common.EnvStr("NAMESPACE", "vllm"),
		LabelSelector: common.EnvStr("LABEL_SELECTOR", "app=vllm-qwen"),
		VLLMPort:      common.EnvInt("VLLM_PORT", 8200),

		PoolFactor:       common.EnvFloat("POOL_FACTOR", 2.0),
		DefaultMaxTokens: common.EnvInt("DEFAULT_MAX_TOKENS", 256),

		PushHTTPTimeoutS: common.EnvFloat("PUSH_HTTP_TIMEOUT_S", 5.0),
		PushLeastQMode:   common.EnvStr("PUSH_LEASTQ_MODE", "false"),

		TraceEnabled:      common.EnvBool("TRACE_ENABLED", false),
		TraceSamplingRate: common.EnvFloat("TRACE_SAMPLING_RATE", 1.0),

		ReqLogMode: common.EnvStr("REQ_LOG_MODE", "summary"),
	}

	cfg.normalize()
	return cfg
}

func (c *Config) normalize() {
	if c.SubmitPath != "" && !strings.HasPrefix(c.SubmitPath, "/") {
		c.SubmitPath = "/" + c.SubmitPath
	}
	if c.ResultSubmitPath != "" && !strings.HasPrefix(c.ResultSubmitPath, "/") {
		c.ResultSubmitPath = "/" + c.ResultSubmitPath
	}

	rm := strings.TrimSpace(strings.ToLower(c.RouterMode))
	switch rm {
	case "push-least-queue", "push_least_queue", "push-leastqueue", "push_leastqueue":
		rm = "push-leastq"
	}
	allowed := map[string]bool{"pull": true, "push-rr": true, "push-random": true, "push-leastq": true}
	if !allowed[rm] {
		rm = "pull"
	}
	c.RouterMode = rm

	lp := strings.TrimSpace(strings.ToLower(c.LenPolicy))
	if lp != "short_first" && lp != "long_first" {
		lp = "short_first"
	}
	c.LenPolicy = lp

	c.PoolFactor = math.Max(1.0, c.PoolFactor)
	if c.DefaultMaxTokens < 1 {
		c.DefaultMaxTokens = 1
	}
	if c.FixedBatchSize < 0 {
		c.FixedBatchSize = 0
	}
	if c.FixedBatchEstimate < 1 {
		c.FixedBatchEstimate = 1
	}
	if c.ResultsZMQHWM < 1 {
		c.ResultsZMQHWM = 1
	}
	if c.HashTimeoutS < 0.001 {
		c.HashTimeoutS = 0.001
	}
	if c.PushHTTPTimeoutS < 0.001 {
		c.PushHTTPTimeoutS = 0.001
	}

	if c.TraceSamplingRate <= 0 || c.TraceSamplingRate > 1.0 {
		c.TraceSamplingRate = 1.0
	}

	logMode := strings.TrimSpace(strings.ToLower(c.ReqLogMode))
	if logMode != "off" && logMode != "summary" && logMode != "full" {
		logMode = "summary"
	}
	c.ReqLogMode = logMode
}

func (c *Config) IsPushMode() bool {
	return strings.HasPrefix(c.RouterMode, "push-")
}

func (c *Config) PrintBanner() {
	fields := map[string]interface{}{
		"HOST":                   c.Host,
		"PORT":                   c.Port,
		"REDIS_HOST":             c.RedisHost,
		"REDIS_PORT":             c.RedisPort,
		"MODEL_NAME":             c.ModelName,
		"KV_AWARE":               c.KVAware,
		"LEN_AWARE":              c.LenAware,
		"LEN_POLICY":             c.LenPolicy,
		"HASH_SERVICE_URL":       c.HashServiceURL,
		"HASH_TIMEOUT_S":         c.HashTimeoutS,
		"ROUTER_MODE":            c.RouterMode,
		"SIDECAR_PORT":           c.SidecarPort,
		"RESULT_TIMEOUT_S":       c.ResultTimeoutS,
		"RESULT_POLL_INTERVAL_S": c.ResultPollIntervalS,
		"TRANSPORT_MODE":         c.TransportMode,
		"SUBMIT_PATH":            c.SubmitPath,
		"RESULTS_ZMQ_BIND":       c.ResultsZMQBind,
		"RESULTS_ZMQ_TOPIC":      c.ResultsZMQTopic,
		"RESULTS_ZMQ_HWM":        c.ResultsZMQHWM,
		"RESULT_TRANSPORT_MODE":  c.ResultTransportMode,
		"RESULT_SUBMIT_PATH":     c.ResultSubmitPath,
		"SLO_AWARE":              c.SLOAware,
		"SLO_WITH_KV":            c.SLOWithKV,
		"ADMISSION_THROTTLE":     c.AdmissionThrottle,
		"FIXED_BATCH_SIZE":       c.FixedBatchSize,
		"OUTPUT_LEN_PREDICTOR":   c.OutputLenPredictor,
		"BATCH_SIZE_ESTIMATE":    c.BatchSizeEstimate,
		"FIXED_BATCH_ESTIMATE":   c.FixedBatchEstimate,
		"LATENCY_PREDICTOR":      c.LatencyPredictor,
		"LATENCY_ONLINE_UPDATE":  c.LatencyOnlineUpdate,
		"LATENCY_PROFILE_PATH":   c.LatencyProfilePath,
		"QUEUE_WAIT_MODEL":       c.QueueWaitModel,
		"CHUNKED_PREFILL_AWARE":  c.ChunkedPrefillAware,
		"MAX_NUM_BATCHED_TOKENS": c.MaxNumBatchedTokens,
		"NAMESPACE":              c.Namespace,
		"LABEL_SELECTOR":         c.LabelSelector,
		"VLLM_PORT":              c.VLLMPort,
		"POOL_FACTOR":            c.PoolFactor,
		"DEFAULT_MAX_TOKENS":     c.DefaultMaxTokens,
		"PUSH_HTTP_TIMEOUT_S":    c.PushHTTPTimeoutS,
		"PUSH_LEASTQ_MODE":       c.PushLeastQMode,
		"TRACE_ENABLED":          c.TraceEnabled,
		"TRACE_SAMPLING_RATE":    c.TraceSamplingRate,
		"REQ_LOG_MODE":           c.ReqLogMode,
	}

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Println()
	fmt.Println("================== RouterConfig (effective) ==================")
	for _, k := range keys {
		fmt.Printf(" %-30s = %v\n", k, fields[k])
	}
	accessLog := os.Getenv("ACCESS_LOG")
	if accessLog == "" {
		accessLog = "true"
	}
	fmt.Printf(" %-30s = %s\n", "ACCESS_LOG", accessLog)
	fmt.Println("=============================================================")
	fmt.Println()
}
