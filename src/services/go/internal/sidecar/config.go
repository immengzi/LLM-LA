package sidecar

import (
	"github.com/saeid/kv-serving-go/internal/common"
)

type Config struct {
	RouterURL            string
	VLLMURL              string
	ModelName            string
	BatchSize            int
	PullIntervalS        float64
	VLLMHost             string
	VLLMSubPort          int
	RedisHost            string
	RedisPort            int
	ContainerName        string
	ModelNameRedis       string
	SidecarPort          int
	SidecarMode          string
	RouterPullTimeoutS   float64
	VLLMTimeoutS         float64
	ResultTransportMode  string
	ResultSubmitPath     string
	RouterResultTimeoutS float64
	ResultPostRetry      bool
	ResultPostMaxRetries int
	ResultPostBackoffBaseS float64
	ResultPostBackoffCapS  float64
	TraceEnabled         bool
	TraceSampleRate      float64
}

func LoadConfig() *Config {
	return &Config{
		RouterURL:              common.EnvStr("ROUTER_URL", "http://router-service:8080"),
		VLLMURL:                common.EnvStr("VLLM_URL", "http://127.0.0.1:8000"),
		ModelName:              common.EnvStr("MODEL_NAME", "served-model"),
		BatchSize:              common.EnvInt("BATCH_SIZE", 8),
		PullIntervalS:          common.EnvFloat("PULL_INTERVAL_S", 0.05),
		VLLMHost:               common.EnvStr("VLLM_HOST", "127.0.0.1"),
		VLLMSubPort:            common.EnvInt("VLLM_SUB_PORT", 5557),
		RedisHost:              common.EnvStr("REDIS_HOST", "redis"),
		RedisPort:              common.EnvInt("REDIS_PORT", 6379),
		ContainerName:          common.EnvStr("CONTAINER_NAME", "vllm-pod"),
		ModelNameRedis:         common.EnvStr("MODEL_NAME_REDIS", "served-model"),
		SidecarPort:            common.EnvInt("SIDECAR_PORT", 9000),
		SidecarMode:            common.EnvStr("SIDECAR_MODE", "pull"),
		RouterPullTimeoutS:     common.EnvFloat("ROUTER_PULL_TIMEOUT_S", 1.0),
		VLLMTimeoutS:           common.EnvFloat("VLLM_TIMEOUT_S", 30.0),
		ResultTransportMode:    common.EnvStr("RESULT_TRANSPORT_MODE", "sync"),
		ResultSubmitPath:       common.EnvStr("RESULT_SUBMIT_PATH", "/result_submit"),
		RouterResultTimeoutS:   common.EnvFloat("ROUTER_RESULT_TIMEOUT_S", 5.0),
		ResultPostRetry:        common.EnvBool("RESULT_POST_RETRY", false),
		ResultPostMaxRetries:   common.EnvInt("RESULT_POST_MAX_RETRIES", 0),
		ResultPostBackoffBaseS: common.EnvFloat("RESULT_POST_BACKOFF_BASE_S", 0.05),
		ResultPostBackoffCapS:  common.EnvFloat("RESULT_POST_BACKOFF_CAP_S", 2.0),
		TraceEnabled:           common.EnvBool("TRACE_ENABLED", false),
		TraceSampleRate:        common.EnvFloat("TRACE_SAMPLE_RATE", 1.0),
	}
}
