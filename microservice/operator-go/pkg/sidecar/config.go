package sidecar

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	RouterURL string
	VllmURL   string
	ModelName string

	BatchSize      int
	PullIntervalS  float64
	SidecarPort    int
	SidecarMode    string // pull | push

	VllmHost    string
	VllmSubPort int
	RedisHost   string
	RedisPort   int

	ContainerName  string
	ModelNameRedis string

	RouterPullTimeoutS   float64
	VllmTimeoutS         float64
	RouterResultTimeoutS float64

	ResultTransportMode string // poll | submit_ack
	ResultSubmitPath    string

	TraceEnabled bool
}

func LoadConfig() *Config {
	return &Config{
		RouterURL: envStr("ROUTER_URL", "http://router-service:8080"),
		VllmURL:   envStr("VLLM_URL", "http://127.0.0.1:8200"),
		ModelName: envStr("MODEL_NAME", "served-model"),

		BatchSize:     envInt("BATCH_SIZE", 8),
		PullIntervalS: envFloat("PULL_INTERVAL_S", 0.05),
		SidecarPort:   envInt("SIDECAR_PORT", 9000),
		SidecarMode:   strings.ToLower(envStr("SIDECAR_MODE", "pull")),

		VllmHost:    envStr("VLLM_HOST", "127.0.0.1"),
		VllmSubPort: envInt("VLLM_SUB_PORT", 5557),
		RedisHost:   envStr("REDIS_HOST", "redis"),
		RedisPort:   envInt("REDIS_PORT", 6379),

		ContainerName:  envStr("CONTAINER_NAME", "unknown"),
		ModelNameRedis: envStr("MODEL_NAME_REDIS", "served-model"),

		RouterPullTimeoutS:   envFloat("ROUTER_PULL_TIMEOUT_S", 1000.0),
		VllmTimeoutS:         envFloat("VLLM_TIMEOUT_S", 1000.0),
		RouterResultTimeoutS: envFloat("ROUTER_RESULT_TIMEOUT_S", 1000.0),

		ResultTransportMode: envStr("RESULT_TRANSPORT_MODE", "poll"),
		ResultSubmitPath:    envStr("RESULT_SUBMIT_PATH", "/result_submit"),

		TraceEnabled: envBool("TRACE_ENABLED", false),
	}
}

func (c *Config) IsPullMode() bool { return c.SidecarMode == "pull" }

func (c *Config) ResultURL() string {
	if c.ResultTransportMode == "submit_ack" {
		return c.RouterURL + c.ResultSubmitPath
	}
	return c.RouterURL + "/result"
}

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
