package sidecar

import "testing"

func TestLoadConfigDefaults(t *testing.T) {
	cfg := LoadConfig()
	if cfg.SidecarMode != "pull" {
		t.Fatalf("default SidecarMode = %q, want pull", cfg.SidecarMode)
	}
	if cfg.BatchSize != 8 {
		t.Fatalf("default BatchSize = %d, want 8", cfg.BatchSize)
	}
	if cfg.ResultTransportMode != "sync" {
		t.Fatalf("default ResultTransportMode = %q, want sync", cfg.ResultTransportMode)
	}
	if cfg.PullCap() != 8 {
		t.Fatalf("default PullCap = %d, want 8", cfg.PullCap())
	}
	if cfg.KVRedisProbeIntervalS != 1 {
		t.Fatalf("default KVRedisProbeIntervalS = %v, want 1", cfg.KVRedisProbeIntervalS)
	}
}

func TestLoadConfigEnvOverrides(t *testing.T) {
	t.Setenv("SIDECAR_MODE", "PUSH")
	t.Setenv("BATCH_SIZE", "4")
	t.Setenv("PREFETCH", "2")
	t.Setenv("RESULT_SUBMIT_PATH", "custom")
	t.Setenv("LOG_LEVEL", "DEBUG")
	t.Setenv("KV_REDIS_PROBE_INTERVAL_S", "0.25")

	cfg := LoadConfig()
	if cfg.SidecarMode != "push" {
		t.Fatalf("SidecarMode = %q, want push (lowercased)", cfg.SidecarMode)
	}
	if cfg.PullCap() != 6 {
		t.Fatalf("PullCap = %d, want 6 (batch 4 + prefetch 2)", cfg.PullCap())
	}
	if cfg.ResultSubmitPath != "/custom" {
		t.Fatalf("ResultSubmitPath = %q, want /custom (leading slash added)", cfg.ResultSubmitPath)
	}
	if !cfg.DebugEnabled() {
		t.Fatal("DebugEnabled should be true when LOG_LEVEL=debug")
	}
	if cfg.KVRedisProbeIntervalS != 0.25 {
		t.Fatalf("KVRedisProbeIntervalS = %v, want 0.25", cfg.KVRedisProbeIntervalS)
	}
}

func TestGenericConfigTakesPriorityAndSynchronizesAliases(t *testing.T) {
	t.Setenv("INFERENCE_ENGINE", "SGLANG")
	t.Setenv("INFERENCE_URL", "http://generic:30000/")
	t.Setenv("VLLM_URL", "http://legacy:8000")
	t.Setenv("INFERENCE_HOST", "generic")
	t.Setenv("VLLM_HOST", "legacy")
	t.Setenv("KV_EVENT_PORT", "7001")
	t.Setenv("VLLM_SUB_PORT", "6001")
	t.Setenv("KV_EVENT_REPLAY_PORT", "7002")
	t.Setenv("KV_EVENT_TOPIC", "topic")
	t.Setenv("INFERENCE_TIMEOUT_S", "90")
	t.Setenv("VLLM_TIMEOUT_S", "45")
	cfg := LoadConfig()
	if cfg.InferenceEngine != "sglang" || cfg.InferenceURL != "http://generic:30000" {
		t.Fatalf("generic inference config not normalized: %+v", cfg)
	}
	if cfg.VLLMURL != cfg.InferenceURL || cfg.VLLMHost != cfg.InferenceHost ||
		cfg.VLLMSubPort != cfg.KVEventPort || cfg.VLLMTimeoutS != cfg.InferenceTimeoutS {
		t.Fatal("vLLM compatibility aliases are not synchronized")
	}
	if cfg.KVEventPort != 7001 || cfg.KVEventReplayPort != 7002 ||
		cfg.KVEventTopic != "topic" || cfg.InferenceTimeoutS != 90 {
		t.Fatalf("generic KV config not applied: %+v", cfg)
	}
}

func TestLegacyVLLMConfigAliases(t *testing.T) {
	t.Setenv("VLLM_URL", "http://legacy:8000")
	t.Setenv("VLLM_HOST", "legacy")
	t.Setenv("VLLM_SUB_PORT", "6001")
	t.Setenv("VLLM_REPLAY_PORT", "6002")
	t.Setenv("VLLM_EVENT_TOPIC", "legacy-kv")
	t.Setenv("VLLM_TIMEOUT_S", "45")
	cfg := LoadConfig()
	if cfg.InferenceURL != "http://legacy:8000" || cfg.InferenceHost != "legacy" ||
		cfg.KVEventPort != 6001 || cfg.KVEventReplayPort != 6002 ||
		cfg.KVEventTopic != "legacy-kv" || cfg.InferenceTimeoutS != 45 {
		t.Fatalf("legacy aliases not loaded: %+v", cfg)
	}
}

func TestConfigRejectsUnknownEngine(t *testing.T) {
	t.Setenv("INFERENCE_ENGINE", "unknown")
	if err := LoadConfig().Validate(); err == nil {
		t.Fatal("unknown inference engine should fail validation")
	}
}

func TestNormalizeClampsInvalidValues(t *testing.T) {
	t.Setenv("SIDECAR_MODE", "bogus")
	t.Setenv("RESULT_TRANSPORT_MODE", "weird")
	t.Setenv("BATCH_SIZE", "0")
	t.Setenv("PREFETCH", "-5")
	t.Setenv("KV_REDIS_PROBE_INTERVAL_S", "99")

	cfg := LoadConfig()
	if cfg.SidecarMode != "pull" {
		t.Fatalf("invalid SidecarMode should fall back to pull, got %q", cfg.SidecarMode)
	}
	if cfg.ResultTransportMode != "sync" {
		t.Fatalf("invalid ResultTransportMode should fall back to sync, got %q", cfg.ResultTransportMode)
	}
	if cfg.BatchSize != 1 {
		t.Fatalf("BatchSize should clamp to 1, got %d", cfg.BatchSize)
	}
	if cfg.Prefetch != 0 {
		t.Fatalf("Prefetch should clamp to 0, got %d", cfg.Prefetch)
	}
	if cfg.KVRedisProbeIntervalS != 5 {
		t.Fatalf("KVRedisProbeIntervalS should clamp to 5, got %v", cfg.KVRedisProbeIntervalS)
	}
}
