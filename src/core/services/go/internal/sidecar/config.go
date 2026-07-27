package sidecar

import (
	"strings"

	"github.com/saeid/kv-serving-go/internal/common"
)

// Config mirrors SidecarConfig in
// src/core/services/sidecar/sidecar/config.py. Defaults match the Python
// defaults exactly.
type Config struct {
	RouterURL string
	VLLMURL   string
	ModelName string

	BatchSize     int
	Prefetch      int
	PullIntervalS float64

	VLLMHost    string
	VLLMSubPort int

	RedisHost      string
	RedisPort      int
	ContainerName  string
	ModelNameRedis string

	SidecarPort int
	SidecarMode string

	RouterPullTimeoutS   float64
	VLLMTimeoutS         float64
	ResultTransportMode  string
	ResultSubmitPath     string
	RouterResultTimeoutS float64

	RouterPoolConnections int
	RouterPoolMaxsize     int

	ResultPostRetry        bool
	ResultPostMaxRetries   int
	ResultPostBackoffBaseS float64
	ResultPostBackoffCapS  float64

	TraceEnabled    bool
	TraceSampleRate float64

	ForceIgnoreEos bool
	StreamingMode  bool

	// SLO-driven dynamic pull backpressure (default OFF). When disabled the
	// sidecar behaves exactly as before: no monitor goroutine is started and
	// PullCap() stays BatchSize + Prefetch. Mirrors the Python SLO_* knobs in
	// src/core/services/sidecar/sidecar/config.py.
	SLODynamicPullEnabled bool
	SLOTpotSLOS           float64
	SLOEvalIntervalS      float64
	SLOWindowSamples      int
	SLOWindowAgg          string
	SLOMinPull            int
	SLOMaxPull            int
	SLODecreaseMode       string
	SLODecreaseStep       int
	SLODecreaseFactor     float64
	SLORecoverStep        int
	SLOCooldownS          float64
	SLOTpotMetric         string
	SLOScrapeTimeoutS     float64

	// GPU KV usage reporting for router soft divert (default OFF).
	KVUsageReport          bool
	KVUsageScrapeIntervalS float64
	KVUsageScrapeTimeoutS  float64

	// KV-memory pull gate (P1; default OFF). Shrink/stop pulling when the local
	// vLLM GPU KV fill fraction is high, to avoid preemption/OOM under long
	// decode. Needs KVUsageReport for samples; fails open when no sample exists.
	// Only ever reduces the pull. Mirrors the Python KV_PULL_GATE_* knobs.
	KVPullGateEnabled bool
	KVPullGateHigh    float64
	KVPullGateLow     float64

	LogLevel string
}

func LoadConfig() *Config {
	cfg := &Config{
		RouterURL: common.EnvStr("ROUTER_URL", "http://router-service:8080"),
		VLLMURL:   common.EnvStr("VLLM_URL", "http://127.0.0.1:8000"),
		ModelName: common.EnvStr("MODEL_NAME", "served-model"),

		BatchSize:     common.EnvInt("BATCH_SIZE", 8),
		Prefetch:      common.EnvInt("PREFETCH", 0),
		PullIntervalS: common.EnvFloat("PULL_INTERVAL_S", 0.05),

		VLLMHost:    common.EnvStr("VLLM_HOST", "127.0.0.1"),
		VLLMSubPort: common.EnvInt("VLLM_SUB_PORT", 5557),

		RedisHost:      common.EnvStr("REDIS_HOST", "redis"),
		RedisPort:      common.EnvInt("REDIS_PORT", 6379),
		ContainerName:  common.EnvStr("CONTAINER_NAME", "vllm-pod"),
		ModelNameRedis: common.EnvStr("MODEL_NAME_REDIS", "served-model"),

		SidecarPort: common.EnvInt("SIDECAR_PORT", 9000),
		SidecarMode: common.EnvStr("SIDECAR_MODE", "pull"),

		RouterPullTimeoutS:   common.EnvFloat("ROUTER_PULL_TIMEOUT_S", 1.0),
		VLLMTimeoutS:         common.EnvFloat("VLLM_TIMEOUT_S", 30.0),
		ResultTransportMode:  common.EnvStr("RESULT_TRANSPORT_MODE", "sync"),
		ResultSubmitPath:     common.EnvStr("RESULT_SUBMIT_PATH", "/result_submit"),
		RouterResultTimeoutS: common.EnvFloat("ROUTER_RESULT_TIMEOUT_S", 5.0),

		RouterPoolConnections: common.EnvInt("ROUTER_POOL_CONNECTIONS", 50),
		RouterPoolMaxsize:     common.EnvInt("ROUTER_POOL_MAXSIZE", 200),

		ResultPostRetry:        common.EnvBool("RESULT_POST_RETRY", false),
		ResultPostMaxRetries:   common.EnvInt("RESULT_POST_MAX_RETRIES", 0),
		ResultPostBackoffBaseS: common.EnvFloat("RESULT_POST_BACKOFF_BASE_S", 0.05),
		ResultPostBackoffCapS:  common.EnvFloat("RESULT_POST_BACKOFF_CAP_S", 2.0),

		TraceEnabled:    common.EnvBool("TRACE_ENABLED", false),
		TraceSampleRate: common.EnvFloat("TRACE_SAMPLE_RATE", 1.0),

		ForceIgnoreEos: common.EnvBool("FORCE_IGNORE_EOS", false),
		StreamingMode:  common.EnvBool("STREAMING_MODE", false),

		SLODynamicPullEnabled: common.EnvBool("SLO_DYNAMIC_PULL_ENABLED", false),
		SLOTpotSLOS:           common.EnvFloat("SLO_TPOT_SLO_S", 0.05),
		SLOEvalIntervalS:      common.EnvFloat("SLO_EVAL_INTERVAL_S", 5.0),
		SLOWindowSamples:      common.EnvInt("SLO_WINDOW_SAMPLES", 6),
		SLOWindowAgg:          common.EnvStr("SLO_WINDOW_AGG", "mean"),
		SLOMinPull:            common.EnvInt("SLO_MIN_PULL", 1),
		SLOMaxPull:            common.EnvInt("SLO_MAX_PULL", 0),
		SLODecreaseMode:       common.EnvStr("SLO_DECREASE_MODE", "additive"),
		SLODecreaseStep:       common.EnvInt("SLO_DECREASE_STEP", 1),
		SLODecreaseFactor:     common.EnvFloat("SLO_DECREASE_FACTOR", 0.5),
		SLORecoverStep:        common.EnvInt("SLO_RECOVER_STEP", 1),
		SLOCooldownS:          common.EnvFloat("SLO_COOLDOWN_S", 10.0),
		SLOTpotMetric:         common.EnvStr("SLO_TPOT_METRIC", "vllm:time_per_output_token_seconds"),
		SLOScrapeTimeoutS:     common.EnvFloat("SLO_SCRAPE_TIMEOUT_S", 2.0),

		KVUsageReport:          common.EnvBool("KV_USAGE_REPORT", false),
		KVUsageScrapeIntervalS: common.EnvFloat("KV_USAGE_SCRAPE_INTERVAL_S", 5.0),
		KVUsageScrapeTimeoutS:  common.EnvFloat("KV_USAGE_SCRAPE_TIMEOUT_S", 2.0),

		KVPullGateEnabled: common.EnvBool("KV_PULL_GATE_ENABLED", false),
		KVPullGateHigh:    common.EnvFloat("KV_PULL_GATE_HIGH", 0.90),
		KVPullGateLow:     common.EnvFloat("KV_PULL_GATE_LOW", 0.70),

		LogLevel: common.EnvStr("LOG_LEVEL", "info"),
	}
	cfg.normalize()
	return cfg
}

func (c *Config) normalize() {
	c.SidecarMode = strings.TrimSpace(strings.ToLower(c.SidecarMode))
	if c.SidecarMode != "pull" && c.SidecarMode != "push" {
		c.SidecarMode = "pull"
	}
	c.ResultTransportMode = strings.TrimSpace(strings.ToLower(c.ResultTransportMode))
	if c.ResultTransportMode != "sync" && c.ResultTransportMode != "submit_ack" {
		c.ResultTransportMode = "sync"
	}
	if c.ResultSubmitPath != "" && !strings.HasPrefix(c.ResultSubmitPath, "/") {
		c.ResultSubmitPath = "/" + c.ResultSubmitPath
	}
	c.LogLevel = strings.TrimSpace(strings.ToLower(c.LogLevel))
	if c.BatchSize < 1 {
		c.BatchSize = 1
	}
	if c.Prefetch < 0 {
		c.Prefetch = 0
	}
	c.SLOWindowAgg = strings.TrimSpace(strings.ToLower(c.SLOWindowAgg))
	if c.SLOWindowAgg != "mean" && c.SLOWindowAgg != "p90" {
		c.SLOWindowAgg = "mean"
	}
	c.SLODecreaseMode = strings.TrimSpace(strings.ToLower(c.SLODecreaseMode))
	if c.SLODecreaseMode != "additive" && c.SLODecreaseMode != "multiplicative" {
		c.SLODecreaseMode = "additive"
	}
	// Keep the KV pull gate window well-formed: 0 <= LOW <= HIGH <= 1.
	if c.KVPullGateHigh > 1.0 {
		c.KVPullGateHigh = 1.0
	}
	if c.KVPullGateHigh < 0 {
		c.KVPullGateHigh = 0
	}
	if c.KVPullGateLow < 0 {
		c.KVPullGateLow = 0
	}
	if c.KVPullGateLow > c.KVPullGateHigh {
		c.KVPullGateLow = c.KVPullGateHigh
	}
}

// PullCap is the total local capacity used to compute pull `want`.
func (c *Config) PullCap() int {
	return c.BatchSize + c.Prefetch
}

// DebugEnabled reports whether verbose ("debug") logging is on.
func (c *Config) DebugEnabled() bool {
	return c.LogLevel == "debug"
}
