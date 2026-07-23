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
// src/core/services/router_service/router/config.py. Field defaults match the
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

	// Block-owner source for prefix routing: "lookup" (targeted per-request
	// HGETALL of the request's own blocks; exact, fresh, eviction-aware) or
	// "watcher" (legacy blind background scan). KVLookupMaxBlocks caps the
	// HGETALL fan-out per request.
	KVOwnerSource     string
	KVLookupMaxBlocks int

	KVHashSource         string
	HashServiceURL       string
	HashTimeoutS         float64
	HashMaxKeepalive     int
	HashKeepaliveExpiryS float64

	KVBlockSize    int
	MeasurePrefix  bool
	LogBlockHashes bool

	// When true, the /latency_log ring includes the full request body under
	// "request_body". Opt-in; LogRequestBodyMaxBytes caps each body (0 =
	// unlimited). Bodies live in the bounded ring so they evict automatically.
	LogRequestBody         bool
	LogRequestBodyMaxBytes int

	RouterStrategy string

	KVAware   bool
	LenAware  bool
	LenPolicy string

	AffinityEnabled      bool
	AffinityMode         string
	AffinityTTLS         float64
	AffinityHardTimeoutS float64

	PoolFactor       float64
	DefaultMaxTokens int

	RouterMode  string
	SidecarPort int

	// Central-push mode (admit like pull + deliver like push). Router decides
	// per-endpoint dispatch of CAP - in-flight items via POST /push; sidecar
	// never pulls. CentralPushCap should track sidecar BATCH_SIZE + PREFETCH.
	CentralPushCap       int
	CentralPushIntervalS float64

	// External-push mode (static external vLLM endpoints; no k8s pods, no
	// sidecar). Admits + schedules like central-push, but the router delivers
	// directly to each external vLLM's /v1/chat/completions and ingests the
	// response inline. Prefix routing still works via a router-side KV-events
	// subscriber. StaticEndpointsRaw is the ROUTER_STATIC_ENDPOINTS JSON array;
	// StaticEndpoints is the parsed form. Mirrors router/config.py.
	StaticEndpointsRaw      string
	StaticEndpoints         []ExternalEndpointConfig
	ExternalKVEvents        bool
	ExternalVLLMTimeoutS    float64
	ExternalPushCap         int
	ExternalPushIntervalS   float64
	ExternalHealthIntervalS float64

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

	// Pull-mode fairness (load-aware grant throttle). Off by default; mirrors
	// FAIR_* in src/core/services/router_service/router/config.py.
	FairPull               bool
	FairMargin             float64
	FairFloor              int
	StuckPullSeconds       int
	AffinityReleaseOnStuck bool

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

		KVOwnerSource:     common.EnvStr("KV_OWNER_SOURCE", "lookup"),
		KVLookupMaxBlocks: common.EnvInt("KV_LOOKUP_MAX_BLOCKS", 512),

		KVHashSource:         common.EnvStr("KV_HASH_SOURCE", "inline"),
		HashServiceURL:       common.EnvStr("HASH_SERVICE_URL", "http://127.0.0.1:9095"),
		HashTimeoutS:         common.EnvFloat("HASH_TIMEOUT_S", 2.0),
		HashMaxKeepalive:     common.EnvInt("HASH_MAX_KEEPALIVE", 50),
		HashKeepaliveExpiryS: common.EnvFloat("HASH_KEEPALIVE_EXPIRY_S", 30.0),

		KVBlockSize:    common.EnvInt("KV_BLOCK_SIZE", 128),
		MeasurePrefix:  common.EnvBool("ROUTER_MEASURE_PREFIX", false),
		LogBlockHashes: common.EnvBool("ROUTER_LOG_BLOCK_HASHES", false),

		LogRequestBody:         common.EnvBool("ROUTER_LOG_REQUEST_BODY", false),
		LogRequestBodyMaxBytes: common.EnvInt("ROUTER_LOG_REQUEST_BODY_MAX_BYTES", 16384),

		RouterStrategy: common.EnvStr("ROUTER_STRATEGY", ""),

		KVAware:   common.EnvBool("KV_AWARE", true),
		LenAware:  common.EnvBool("LEN_AWARE", true),
		LenPolicy: common.EnvStr("LEN_POLICY", "short_first"),

		AffinityEnabled:      common.EnvBool("AFFINITY_ENABLED", false),
		AffinityMode:         common.EnvStr("AFFINITY_MODE", "soft"),
		AffinityTTLS:         common.EnvFloat("AFFINITY_TTL_S", 300.0),
		AffinityHardTimeoutS: common.EnvFloat("AFFINITY_HARD_TIMEOUT_S", 5.0),

		PoolFactor:       common.EnvFloat("POOL_FACTOR", 4.0),
		DefaultMaxTokens: common.EnvInt("DEFAULT_MAX_TOKENS", 1024),

		RouterMode:  common.EnvStr("ROUTER_MODE", "pull"),
		SidecarPort: common.EnvInt("SIDECAR_PORT", 9000),

		CentralPushCap:       common.EnvInt("ROUTER_CENTRAL_PUSH_CAP", 8),
		CentralPushIntervalS: common.EnvFloat("ROUTER_CENTRAL_PUSH_INTERVAL_S", 0.05),

		StaticEndpointsRaw:      common.EnvStr("ROUTER_STATIC_ENDPOINTS", ""),
		ExternalKVEvents:        common.EnvBool("ROUTER_EXTERNAL_KV_EVENTS", true),
		ExternalVLLMTimeoutS:    common.EnvFloat("ROUTER_EXTERNAL_VLLM_TIMEOUT_S", 300.0),
		ExternalPushCap:         common.EnvInt("ROUTER_EXTERNAL_PUSH_CAP", 8),
		ExternalPushIntervalS:   common.EnvFloat("ROUTER_EXTERNAL_PUSH_INTERVAL_S", 0.05),
		ExternalHealthIntervalS: common.EnvFloat("ROUTER_EXTERNAL_HEALTH_INTERVAL_S", 5.0),

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

		FairPull:               common.EnvBool("ROUTER_FAIR_PULL", false),
		FairMargin:             common.EnvFloat("ROUTER_FAIR_MARGIN", 1.25),
		FairFloor:              common.EnvInt("ROUTER_FAIR_FLOOR", 1),
		StuckPullSeconds:       common.EnvInt("ROUTER_STUCK_PULL_SECONDS", 0),
		AffinityReleaseOnStuck: common.EnvBool("ROUTER_AFFINITY_RELEASE_ON_STUCK", false),

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
	switch rm {
	case "central_push", "centralpush":
		rm = "central-push"
	}
	switch rm {
	case "external_push", "externalpush", "external", "direct-external":
		rm = "external-push"
	}
	allowed := map[string]bool{"pull": true, "push-rr": true, "push-random": true, "push-leastq": true, "central-push": true, "external-push": true}
	if !allowed[rm] {
		rm = "pull"
	}
	c.RouterMode = rm

	if c.CentralPushCap < 1 {
		c.CentralPushCap = 1
	}
	if c.CentralPushIntervalS <= 0 {
		c.CentralPushIntervalS = 0.05
	}

	c.StaticEndpoints = parseStaticEndpoints(c.StaticEndpointsRaw)
	if c.ExternalPushCap < 1 {
		c.ExternalPushCap = 1
	}
	if c.ExternalPushIntervalS <= 0 {
		c.ExternalPushIntervalS = 0.05
	}
	if c.ExternalHealthIntervalS < 0 {
		c.ExternalHealthIntervalS = 0
	}

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

	am := strings.TrimSpace(strings.ToLower(c.AffinityMode))
	if am != "soft" && am != "hard" {
		am = "soft"
	}
	c.AffinityMode = am

	hs := strings.TrimSpace(strings.ToLower(c.KVHashSource))
	if hs != "inline" && hs != "external" {
		hs = "inline"
	}
	c.KVHashSource = hs

	ownerSrc := strings.TrimSpace(strings.ToLower(c.KVOwnerSource))
	if ownerSrc != "lookup" && ownerSrc != "watcher" {
		ownerSrc = "lookup"
	}
	c.KVOwnerSource = ownerSrc

	// Unified routing strategy: when set, derives KVAware / AffinityEnabled from
	// a single knob and overrides the individual flags above. Ports
	// ROUTER_STRATEGY handling from src/core/services/router_service/router/config.py.
	c.RouterStrategy = strings.TrimSpace(strings.ToLower(c.RouterStrategy))
	switch c.RouterStrategy {
	case "none":
		c.KVAware, c.AffinityEnabled = false, false
	case "prefix":
		c.KVAware, c.AffinityEnabled = true, false
	case "affinity":
		c.KVAware, c.AffinityEnabled = false, true
	case "both":
		c.KVAware, c.AffinityEnabled = true, true
	}

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
	if c.KVBlockSize < 1 {
		c.KVBlockSize = 1
	}
	if c.DefaultMaxTokens < 1 {
		c.DefaultMaxTokens = 1
	}
	if c.FixedBatchSize < 0 {
		c.FixedBatchSize = 0
	}
	if c.FairMargin < 1.0 {
		c.FairMargin = 1.0
	}
	if c.FairFloor < 0 {
		c.FairFloor = 0
	}
	if c.StuckPullSeconds < 0 {
		c.StuckPullSeconds = 0
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

// IsCentralPush reports whether the router runs the central-push mode.
func (c *Config) IsCentralPush() bool {
	return c.RouterMode == "central-push"
}

// IsExternalPush reports whether the router runs the external-push mode (static
// external vLLM endpoints, no k8s pods, no sidecar; router delivers directly).
func (c *Config) IsExternalPush() bool {
	return c.RouterMode == "external-push"
}

// UsesCentralQueue reports whether requests are admitted into the central
// queue (pull scheduling path): pull, central-push and external-push. Push-*
// skip the queue.
func (c *Config) UsesCentralQueue() bool {
	return c.RouterMode == "pull" || c.RouterMode == "central-push" || c.RouterMode == "external-push"
}

// UsesPushDelivery reports whether the router delivers to sidecars via POST
// /push (needs PushRouter/discovery): push-* and central-push.
func (c *Config) UsesPushDelivery() bool {
	return strings.HasPrefix(c.RouterMode, "push-") || c.RouterMode == "central-push"
}

// MeasurePrefixEnabled reports whether per-request prefix blocks should be
// computed/registered. Mirrors the guard in
// src/core/services/router_service/router/api.py: prefix blocks are needed when KV
// routing uses them (KVAware) OR when measurement-only logging is requested
// (MeasurePrefix) OR when the full block-hash list is being logged
// (LogBlockHashes). Routing itself still keys off KVAware.
func (c *Config) MeasurePrefixEnabled() bool {
	return c.KVAware || c.MeasurePrefix || c.LogBlockHashes
}

func (c *Config) PrintBanner() {
	fields := map[string]interface{}{
		"HOST":                              c.Host,
		"PORT":                              c.Port,
		"API_KEY":                           maskSecret(c.APIKey),
		"REDIS_HOST":                        c.RedisHost,
		"REDIS_PORT":                        c.RedisPort,
		"MODEL_NAME":                        c.ModelName,
		"NAMESPACE":                         c.Namespace,
		"LABEL_SELECTOR":                    c.LabelSelector,
		"VLLM_PORT":                         c.VLLMPort,
		"KV_LOG_KEYS":                       c.KVLogKeys,
		"KV_WATCH_INTERVAL_S":               c.KVWatchIntervalS,
		"KV_WATCH_MAX_KEYS":                 c.KVWatchMaxKeys,
		"KV_DISCOVERY_INTERVAL_S":           c.KVDiscoveryIntervalS,
		"KV_OWNER_SOURCE":                   c.KVOwnerSource,
		"KV_LOOKUP_MAX_BLOCKS":              c.KVLookupMaxBlocks,
		"KV_HASH_SOURCE":                    c.KVHashSource,
		"HASH_SERVICE_URL":                  c.HashServiceURL,
		"HASH_TIMEOUT_S":                    c.HashTimeoutS,
		"KV_BLOCK_SIZE":                     c.KVBlockSize,
		"ROUTER_MEASURE_PREFIX":             c.MeasurePrefix,
		"ROUTER_LOG_BLOCK_HASHES":           c.LogBlockHashes,
		"ROUTER_LOG_REQUEST_BODY":           c.LogRequestBody,
		"ROUTER_LOG_REQUEST_BODY_MAX_BYTES": c.LogRequestBodyMaxBytes,
		"ROUTER_STRATEGY":                   c.RouterStrategy,
		"KV_AWARE":                          c.KVAware,
		"LEN_AWARE":                         c.LenAware,
		"LEN_POLICY":                        c.LenPolicy,
		"AFFINITY_ENABLED":                  c.AffinityEnabled,
		"AFFINITY_MODE":                     c.AffinityMode,
		"AFFINITY_TTL_S":                    c.AffinityTTLS,
		"AFFINITY_HARD_TIMEOUT_S":           c.AffinityHardTimeoutS,
		"POOL_FACTOR":                       c.PoolFactor,
		"DEFAULT_MAX_TOKENS":                c.DefaultMaxTokens,
		"ROUTER_MODE":                       c.RouterMode,
		"SIDECAR_PORT":                      c.SidecarPort,
		"ROUTER_CENTRAL_PUSH_CAP":           c.CentralPushCap,
		"ROUTER_CENTRAL_PUSH_INTERVAL_S":    c.CentralPushIntervalS,
		"ROUTER_STATIC_ENDPOINTS":           len(c.StaticEndpoints),
		"ROUTER_EXTERNAL_KV_EVENTS":         c.ExternalKVEvents,
		"ROUTER_EXTERNAL_VLLM_TIMEOUT_S":    c.ExternalVLLMTimeoutS,
		"ROUTER_EXTERNAL_PUSH_CAP":          c.ExternalPushCap,
		"ROUTER_EXTERNAL_PUSH_INTERVAL_S":   c.ExternalPushIntervalS,
		"ROUTER_EXTERNAL_HEALTH_INTERVAL_S": c.ExternalHealthIntervalS,
		"PUSH_LEASTQ_MODE":                  c.PushLeastQMode,
		"PUSH_HTTP_TIMEOUT_S":               c.PushHTTPTimeoutS,
		"PUSH_DECOUPLE_DISPATCH":            c.PushDecoupleDispatch,
		"PUSH_DISPATCH_WORKERS":             c.PushDispatchWorkers,
		"RESULT_TIMEOUT_S":                  c.ResultTimeoutS,
		"POLL_RESULT_TTL_S":                 c.PollResultTTLS,
		"RESULT_TRANSPORT_MODE":             c.ResultTransportMode,
		"RESULT_SUBMIT_PATH":                c.ResultSubmitPath,
		"TRANSPORT_MODE":                    c.TransportMode,
		"SUBMIT_PATH":                       c.SubmitPath,
		"RESULTS_ZMQ_BIND":                  c.ResultsZMQBind,
		"RESULTS_ZMQ_TOPIC":                 c.ResultsZMQTopic,
		"RESULTS_ZMQ_HWM":                   c.ResultsZMQHWM,
		"REQ_LOG_MODE":                      c.ReqLogMode,
		"TRACE_ENABLED":                     c.TraceEnabled,
		"SLO_AWARE":                         c.SLOAware,
		"SLO_WITH_KV":                       c.SLOWithKV,
		"ADMISSION_THROTTLE":                c.AdmissionThrottle,
		"FIXED_BATCH_SIZE":                  c.FixedBatchSize,
		"ROUTER_FAIR_PULL":                  c.FairPull,
		"ROUTER_FAIR_MARGIN":                c.FairMargin,
		"ROUTER_FAIR_FLOOR":                 c.FairFloor,
		"ROUTER_STUCK_PULL_SECONDS":         c.StuckPullSeconds,
		"ROUTER_AFFINITY_RELEASE_ON_STUCK":  c.AffinityReleaseOnStuck,
		"OUTPUT_LEN_PREDICTOR":              c.OutputLenPredictor,
		"BATCH_SIZE_ESTIMATE":               c.BatchSizeEstimate,
		"FIXED_BATCH_ESTIMATE":              c.FixedBatchEstimate,
		"LATENCY_PREDICTOR":                 c.LatencyPredictor,
		"LATENCY_ONLINE_UPDATE":             c.LatencyOnlineUpdate,
		"LATENCY_PROFILE_PATH":              c.LatencyProfilePath,
		"QUEUE_WAIT_MODEL":                  c.QueueWaitModel,
		"CHUNKED_PREFILL_AWARE":             c.ChunkedPrefillAware,
		"MAX_NUM_BATCHED_TOKENS":            c.MaxNumBatchedTokens,
		"MODEL_CONFIG_PATH":                 c.ModelConfigPath,
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
