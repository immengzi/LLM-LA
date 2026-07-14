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
}

func TestLoadConfigEnvOverrides(t *testing.T) {
	t.Setenv("SIDECAR_MODE", "PUSH")
	t.Setenv("BATCH_SIZE", "4")
	t.Setenv("PREFETCH", "2")
	t.Setenv("RESULT_SUBMIT_PATH", "custom")
	t.Setenv("LOG_LEVEL", "DEBUG")

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
}

func TestNormalizeClampsInvalidValues(t *testing.T) {
	t.Setenv("SIDECAR_MODE", "bogus")
	t.Setenv("RESULT_TRANSPORT_MODE", "weird")
	t.Setenv("BATCH_SIZE", "0")
	t.Setenv("PREFETCH", "-5")

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
}
