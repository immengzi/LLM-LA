package sidecar

import "strings"

// EngineHealthProfile centralizes engine-specific probe policy.
type EngineHealthProfile struct {
	EngineType    string
	HealthPath    string
	ReadinessPath string
}

func NewEngineHealthProfile(cfg *Config) EngineHealthProfile {
	health := normalizePath(cfg.InferenceHealthPath, "/health")
	// /health_generate performs generation in SGLang and is never a liveness
	// probe, even when inherited through the legacy VLLM_HEALTH_PATH alias.
	if strings.TrimRight(health, "/") == "/health_generate" {
		health = "/health"
	}
	ready := cfg.InferenceReadinessPath
	if ready == "" {
		ready = health
	}
	ready = normalizePath(ready, health)
	return EngineHealthProfile{
		EngineType:    cfg.InferenceEngine,
		HealthPath:    health,
		ReadinessPath: ready,
	}
}

func (p EngineHealthProfile) Path(readiness bool) string {
	if readiness {
		return p.ReadinessPath
	}
	return p.HealthPath
}
