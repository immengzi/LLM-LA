package gateway

import "testing"

// TestRouterStrategyMapping mirrors the Python router config test: the unified
// ROUTER_STRATEGY knob overrides the individual KV_AWARE / AFFINITY_ENABLED
// flags.
func TestRouterStrategyMapping(t *testing.T) {
	cases := []struct {
		strategy string
		kv       bool
		affinity bool
	}{
		{"none", false, false},
		{"prefix", true, false},
		{"affinity", false, true},
		{"both", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.strategy, func(t *testing.T) {
			t.Setenv("ROUTER_STRATEGY", tc.strategy)
			// Set the individual flags to the opposite so we prove the strategy wins.
			t.Setenv("KV_AWARE", "false")
			t.Setenv("AFFINITY_ENABLED", "false")
			cfg := LoadConfig()
			if cfg.KVAware != tc.kv {
				t.Errorf("KVAware = %v, want %v", cfg.KVAware, tc.kv)
			}
			if cfg.AffinityEnabled != tc.affinity {
				t.Errorf("AffinityEnabled = %v, want %v", cfg.AffinityEnabled, tc.affinity)
			}
		})
	}
}

func TestRouterStrategyUnsetKeepsFlags(t *testing.T) {
	t.Setenv("ROUTER_STRATEGY", "")
	t.Setenv("KV_AWARE", "false")
	t.Setenv("AFFINITY_ENABLED", "true")
	cfg := LoadConfig()
	if cfg.KVAware {
		t.Error("KVAware should stay false when no strategy is set")
	}
	if !cfg.AffinityEnabled {
		t.Error("AffinityEnabled should stay true when no strategy is set")
	}
}

func TestRouterModeNormalization(t *testing.T) {
	cases := map[string]string{
		"pull":             "pull",
		"PUSH-RR":          "push-rr",
		"push_least_queue": "push-leastq",
		"push-lq":          "push-leastq",
		"central_push":     "central-push",
		"centralpush":      "central-push",
		"bogus-mode":       "pull",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			t.Setenv("ROUTER_MODE", in)
			cfg := LoadConfig()
			if cfg.RouterMode != want {
				t.Errorf("RouterMode(%q) = %q, want %q", in, cfg.RouterMode, want)
			}
		})
	}
}

func TestEnumNormalizationFallbacks(t *testing.T) {
	t.Setenv("LEN_POLICY", "sideways")
	t.Setenv("AFFINITY_MODE", "medium")
	t.Setenv("KV_HASH_SOURCE", "magic")
	t.Setenv("OUTPUT_LEN_PREDICTOR", "psychic")
	t.Setenv("LATENCY_PREDICTOR", "vibes")
	cfg := LoadConfig()
	if cfg.LenPolicy != "short_first" {
		t.Errorf("LenPolicy = %q, want short_first", cfg.LenPolicy)
	}
	if cfg.AffinityMode != "soft" {
		t.Errorf("AffinityMode = %q, want soft", cfg.AffinityMode)
	}
	if cfg.KVHashSource != "inline" {
		t.Errorf("KVHashSource = %q, want inline", cfg.KVHashSource)
	}
	if cfg.OutputLenPredictor != "simple" {
		t.Errorf("OutputLenPredictor = %q, want simple", cfg.OutputLenPredictor)
	}
	if cfg.LatencyPredictor != "linear" {
		t.Errorf("LatencyPredictor = %q, want linear", cfg.LatencyPredictor)
	}
}

func TestNumericClamps(t *testing.T) {
	t.Setenv("POOL_FACTOR", "0.1")
	t.Setenv("KV_BLOCK_SIZE", "0")
	t.Setenv("DEFAULT_MAX_TOKENS", "0")
	t.Setenv("TRACE_SAMPLING_RATE", "5")
	cfg := LoadConfig()
	if cfg.PoolFactor != 1.0 {
		t.Errorf("PoolFactor = %v, want clamp to 1.0", cfg.PoolFactor)
	}
	if cfg.KVBlockSize != 1 {
		t.Errorf("KVBlockSize = %d, want clamp to 1", cfg.KVBlockSize)
	}
	if cfg.DefaultMaxTokens != 1 {
		t.Errorf("DefaultMaxTokens = %d, want clamp to 1", cfg.DefaultMaxTokens)
	}
	if cfg.TraceSamplingRate != 1.0 {
		t.Errorf("TraceSamplingRate = %v, want reset to 1.0", cfg.TraceSamplingRate)
	}
}
