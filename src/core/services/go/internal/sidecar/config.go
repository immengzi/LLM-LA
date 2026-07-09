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
}

// PullCap is the total local capacity used to compute pull `want`.
func (c *Config) PullCap() int {
	return c.BatchSize + c.Prefetch
}

// DebugEnabled reports whether verbose ("debug") logging is on.
func (c *Config) DebugEnabled() bool {
	return c.LogLevel == "debug"
}
