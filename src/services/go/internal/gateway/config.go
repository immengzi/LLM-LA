package gateway

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/saeid/kv-serving-go/internal/common"
)

// Config mirrors RouterConfig in
// src/services/router_service/router/config.py. Field defaults match the
// Python defaults exactly so that, absent any env overrides, the Go router
// behaves identically to the Python router.
type Config struct {
	Host string
	Port int

	APIKey string

	RedisHost string
	RedisPort int
	ModelName string
	Namespace string

	LabelSelector string
	VLLMPort      int

	KVLogKeys            string
	KVWatchIntervalS     float64
	KVWatchMaxKeys       int
	KVDiscoveryIntervalS float64

	HashServiceURL       string
	HashTimeoutS         float64
	HashMaxKeepalive     int
	HashKeepaliveExpiryS float64

	KVAware   bool
	LenAware  bool
	LenPolicy string

	PoolFactor       float64
	DefaultMaxTokens int

	RouterMode  string
	SidecarPort int

	PushLeastQMode       string
	PushHTTPTimeoutS     float64
	PushMaxKeepalive     int
	PushKeepaliveExpiryS float64

	PushDecoupleDispatch  bool
	PushDispatchQueueMax  int
	PushDispatchWorkers   int
	PushDispatchMaxDelayS float64

	ResultTimeoutS       float64
	ResultPollIntervalS  float64
	PollResultTTLS       float64
	PollCleanupIntervalS float64

	ResultTransportMode string
	ResultSubmitPath    string

	TransportMode   string
	SubmitPath      string
	ResultsZMQBind  string
	ResultsZMQTopic string
	ResultsZMQHWM   int
	ResultsGraceS   float64

	ReqLogMode string

	TraceEnabled      bool
	TraceSamplingRate float64

	SLOAware          bool
	SLOWithKV         bool
	AdmissionThrottle bool
	FixedBatchSize    int

	OutputLenPredictor string
	BatchSizeEstimate  string
	FixedBatchEstimate int

	LatencyPredictor    string
	LatencyOnlineUpdate bool
	LatencyProfilePath  string

	QueueWaitModel      string
	ChunkedPrefillAware bool
	MaxNumBatchedTokens int

	ModelConfigPath string
}

func LoadConfig() *Config {
	cfg := &Config{
		Host: common.EnvStr("HOST", "0.0.0.0"),
		Port: common.EnvInt("PORT", 8080),

		APIKey: common.EnvStr("API_KEY", ""),

		RedisHost: common.EnvStr("REDIS_HOST", "redis"),
		RedisPort: common.EnvInt("REDIS_PORT", 6379),
		ModelName: common.EnvStr("MODEL_NAME", "served-model"),
		Namespace: common.EnvStr("NAMESPACE", "vllm"),

		LabelSelector: common.EnvStr("LABEL_SELECTOR", "app=vllm-qwen"),
		VLLMPort:      common.EnvInt("VLLM_PORT", 8200),

		KVLogKeys:            common.EnvStr("KV_LOG_KEYS", "off"),
		KVWatchIntervalS:     common.EnvFloat("KV_WATCH_INTERVAL_S", 1.0),
		KVWatchMaxKeys:       common.EnvInt("KV_WATCH_MAX_KEYS", 200),
		KVDiscoveryIntervalS: common.EnvFloat("KV_DISCOVERY_INTERVAL_S", 5.0),

		HashServiceURL:       common.EnvStr("HASH_SERVICE_URL", "http://prefix-hash-service:9095"),
		HashTimeoutS:         common.EnvFloat("HASH_TIMEOUT_S", 2.0),
		HashMaxKeepalive:     common.EnvInt("HASH_MAX_KEEPALIVE", 50),
		HashKeepaliveExpiryS: common.EnvFloat("HASH_KEEPALIVE_EXPIRY_S", 30.0),

		KVAware:   common.EnvBool("KV_AWARE", true),
		LenAware:  common.EnvBool("LEN_AWARE", true),
		LenPolicy: common.EnvStr("LEN_POLICY", "short_first"),

		PoolFactor:       common.EnvFloat("POOL_FACTOR", 4.0),
		DefaultMaxTokens: common.EnvInt("DEFAULT_MAX_TOKENS", 1024),

		RouterMode:  common.EnvStr("ROUTER_MODE", "pull"),
		SidecarPort: common.EnvInt("SIDECAR_PORT", 9000),

		PushLeastQMode:       common.EnvStr("PUSH_LEASTQ_MODE", "health"),
		PushHTTPTimeoutS:     common.EnvFloat("PUSH_HTTP_TIMEOUT_S", 2.0),
		PushMaxKeepalive:     common.EnvInt("PUSH_MAX_KEEPALIVE", 200),
		PushKeepaliveExpiryS: common.EnvFloat("PUSH_KEEPALIVE_EXPIRY_S", 30.0),

		PushDecoupleDispatch:  common.EnvBool("PUSH_DECOUPLE_DISPATCH", true),
		PushDispatchQueueMax:  common.EnvInt("PUSH_DISPATCH_QUEUE_MAX", 100000),
		PushDispatchWorkers:   common.EnvInt("PUSH_DISPATCH_WORKERS", 32),
		PushDispatchMaxDelayS: common.EnvFloat("PUSH_DISPATCH_MAX_DELAY_S", 60.0),

		ResultTimeoutS:       common.EnvFloat("RESULT_TIMEOUT_S", 60.0),
		ResultPollIntervalS:  common.EnvFloat("RESULT_POLL_INTERVAL_S", 0.02),
		PollResultTTLS:       common.EnvFloat("POLL_RESULT_TTL_S", 300.0),
		PollCleanupIntervalS: common.EnvFloat("POLL_CLEANUP_INTERVAL_S", 1.0),

		ResultTransportMode: common.EnvStr("RESULT_TRANSPORT_MODE", "sync"),
		ResultSubmitPath:    common.EnvStr("RESULT_SUBMIT_PATH", "/result_submit"),

		TransportMode:   common.EnvStr("TRANSPORT_MODE", "sync"),
		SubmitPath:      common.EnvStr("SUBMIT_PATH", "/submit"),
		ResultsZMQBind:  common.EnvStr("RESULTS_ZMQ_BIND", "tcp://0.0.0.0:5559"),
		ResultsZMQTopic: common.EnvStr("RESULTS_ZMQ_TOPIC", "results"),
		ResultsZMQHWM:   common.EnvInt("RESULTS_ZMQ_HWM", 100000),
		ResultsGraceS:   common.EnvFloat("RESULTS_GRACE_S", 30.0),

		ReqLogMode: common.EnvStr("REQ_LOG_MODE", "off"),

		TraceEnabled:      common.EnvBool("TRACE_ENABLED", false),
		TraceSamplingRate: common.EnvFloat("TRACE_SAMPLING_RATE", 1.0),

		SLOAware:          common.EnvBool("SLO_AWARE", false),
		SLOWithKV:         common.EnvBool("SLO_WITH_KV", true),
		AdmissionThrottle: common.EnvBool("ADMISSION_THROTTLE", false),
		FixedBatchSize:    common.EnvInt("FIXED_BATCH_SIZE", 0),

		OutputLenPredictor: common.EnvStr("OUTPUT_LEN_PREDICTOR", "simple"),
		BatchSizeEstimate:  common.EnvStr("BATCH_SIZE_ESTIMATE", "fixed"),
		FixedBatchEstimate: common.EnvInt("FIXED_BATCH_ESTIMATE", 8),

		LatencyPredictor:    common.EnvStr("LATENCY_PREDICTOR", "linear"),
		LatencyOnlineUpdate: common.EnvBool("LATENCY_ONLINE_UPDATE", false),
		LatencyProfilePath:  common.EnvStr("LATENCY_PROFILE_PATH", ""),

		QueueWaitModel:      common.EnvStr("QUEUE_WAIT_MODEL", "none"),
		ChunkedPrefillAware: common.EnvBool("CHUNKED_PREFILL_AWARE", false),
		MaxNumBatchedTokens: common.EnvInt("MAX_NUM_BATCHED_TOKENS", 0),

		ModelConfigPath: common.EnvStr("MODEL_CONFIG_PATH", ""),
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
	case "push-least-queue", "push_least_queue", "push-leastqueue", "push_leastqueue", "push-lq":
		rm = "push-leastq"
	}
	allowed := map[string]bool{"pull": true, "push-rr": true, "push-random": true, "push-leastq": true}
	if !allowed[rm] {
		rm = "pull"
	}
	c.RouterMode = rm

	plq := strings.TrimSpace(strings.ToLower(c.PushLeastQMode))
	if plq != "health" && plq != "local" {
		plq = "health"
	}
	c.PushLeastQMode = plq

	lp := strings.TrimSpace(strings.ToLower(c.LenPolicy))
	if lp != "short_first" && lp != "long_first" {
		lp = "short_first"
	}
	c.LenPolicy = lp

	tm := strings.TrimSpace(strings.ToLower(c.TransportMode))
	if tm != "sync" && tm != "async_pubsub" {
		tm = "sync"
	}
	c.TransportMode = tm

	rtm := strings.TrimSpace(strings.ToLower(c.ResultTransportMode))
	if rtm != "sync" && rtm != "submit_ack" {
		rtm = "sync"
	}
	c.ResultTransportMode = rtm

	olp := strings.TrimSpace(strings.ToLower(c.OutputLenPredictor))
	if olp != "simple" && olp != "distribution" && olp != "regression" && olp != "hint_only" {
		olp = "simple"
	}
	c.OutputLenPredictor = olp

	bse := strings.TrimSpace(strings.ToLower(c.BatchSizeEstimate))
	if bse != "fixed" && bse != "inflight" && bse != "reported" {
		bse = "fixed"
	}
	c.BatchSizeEstimate = bse

	lpd := strings.TrimSpace(strings.ToLower(c.LatencyPredictor))
	if lpd != "linear" && lpd != "piecewise" && lpd != "bayesian" && lpd != "hybrid" {
		lpd = "linear"
	}
	c.LatencyPredictor = lpd

	qwm := strings.TrimSpace(strings.ToLower(c.QueueWaitModel))
	if qwm != "none" && qwm != "simple" && qwm != "drain_rate" {
		qwm = "none"
	}
	c.QueueWaitModel = qwm

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
	if c.PollCleanupIntervalS <= 0 {
		c.PollCleanupIntervalS = 1.0
	}
	if c.PollResultTTLS < 1.0 {
		c.PollResultTTLS = 1.0
	}
	if c.PushDispatchWorkers < 1 {
		c.PushDispatchWorkers = 1
	}
	if c.PushDispatchQueueMax < 1 {
		c.PushDispatchQueueMax = 1
	}

	if c.TraceSamplingRate <= 0 || c.TraceSamplingRate > 1.0 {
		c.TraceSamplingRate = 1.0
	}

	logMode := strings.TrimSpace(strings.ToLower(c.ReqLogMode))
	if logMode != "off" && logMode != "summary" && logMode != "full" {
		logMode = "off"
	}
	c.ReqLogMode = logMode
}

func (c *Config) IsPushMode() bool {
	return strings.HasPrefix(c.RouterMode, "push-")
}

func (c *Config) PrintBanner() {
	fields := map[string]interface{}{
		"HOST":                    c.Host,
		"PORT":                    c.Port,
		"API_KEY":                 maskSecret(c.APIKey),
		"REDIS_HOST":              c.RedisHost,
		"REDIS_PORT":              c.RedisPort,
		"MODEL_NAME":              c.ModelName,
		"NAMESPACE":               c.Namespace,
		"LABEL_SELECTOR":          c.LabelSelector,
		"VLLM_PORT":               c.VLLMPort,
		"KV_LOG_KEYS":             c.KVLogKeys,
		"KV_WATCH_INTERVAL_S":     c.KVWatchIntervalS,
		"KV_WATCH_MAX_KEYS":       c.KVWatchMaxKeys,
		"KV_DISCOVERY_INTERVAL_S": c.KVDiscoveryIntervalS,
		"HASH_SERVICE_URL":        c.HashServiceURL,
		"HASH_TIMEOUT_S":          c.HashTimeoutS,
		"KV_AWARE":                c.KVAware,
		"LEN_AWARE":               c.LenAware,
		"LEN_POLICY":              c.LenPolicy,
		"POOL_FACTOR":             c.PoolFactor,
		"DEFAULT_MAX_TOKENS":      c.DefaultMaxTokens,
		"ROUTER_MODE":             c.RouterMode,
		"SIDECAR_PORT":            c.SidecarPort,
		"PUSH_LEASTQ_MODE":        c.PushLeastQMode,
		"PUSH_HTTP_TIMEOUT_S":     c.PushHTTPTimeoutS,
		"PUSH_DECOUPLE_DISPATCH":  c.PushDecoupleDispatch,
		"PUSH_DISPATCH_WORKERS":   c.PushDispatchWorkers,
		"RESULT_TIMEOUT_S":        c.ResultTimeoutS,
		"POLL_RESULT_TTL_S":       c.PollResultTTLS,
		"RESULT_TRANSPORT_MODE":   c.ResultTransportMode,
		"RESULT_SUBMIT_PATH":      c.ResultSubmitPath,
		"TRANSPORT_MODE":          c.TransportMode,
		"SUBMIT_PATH":             c.SubmitPath,
		"RESULTS_ZMQ_BIND":        c.ResultsZMQBind,
		"RESULTS_ZMQ_TOPIC":       c.ResultsZMQTopic,
		"RESULTS_ZMQ_HWM":         c.ResultsZMQHWM,
		"REQ_LOG_MODE":            c.ReqLogMode,
		"TRACE_ENABLED":           c.TraceEnabled,
		"SLO_AWARE":               c.SLOAware,
		"SLO_WITH_KV":             c.SLOWithKV,
		"ADMISSION_THROTTLE":      c.AdmissionThrottle,
		"FIXED_BATCH_SIZE":        c.FixedBatchSize,
		"OUTPUT_LEN_PREDICTOR":    c.OutputLenPredictor,
		"BATCH_SIZE_ESTIMATE":     c.BatchSizeEstimate,
		"FIXED_BATCH_ESTIMATE":    c.FixedBatchEstimate,
		"LATENCY_PREDICTOR":       c.LatencyPredictor,
		"LATENCY_ONLINE_UPDATE":   c.LatencyOnlineUpdate,
		"LATENCY_PROFILE_PATH":    c.LatencyProfilePath,
		"QUEUE_WAIT_MODEL":        c.QueueWaitModel,
		"CHUNKED_PREFILL_AWARE":   c.ChunkedPrefillAware,
		"MAX_NUM_BATCHED_TOKENS":  c.MaxNumBatchedTokens,
		"MODEL_CONFIG_PATH":       c.ModelConfigPath,
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

func maskSecret(s string) string {
	if s == "" {
		return ""
	}
	return "***set***"
}
