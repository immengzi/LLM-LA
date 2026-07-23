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
		"external-push":    "external-push",
		"external_push":    "external-push",
		"externalpush":     "external-push",
		"external":         "external-push",
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

// TestStaticEndpointsParsing verifies ROUTER_STATIC_ENDPOINTS parsing +
// defaults mirror the Python _parse_static_endpoints: url required (trailing
// slash trimmed), id defaults to ext-N, topic defaults to "kv@".
func TestStaticEndpointsParsing(t *testing.T) {
	t.Setenv("ROUTER_MODE", "external-push")
	t.Setenv("ROUTER_STATIC_ENDPOINTS", `[
		{"id":"a","url":"http://1.2.3.4:8200/","model":"m","kv_events_endpoints":["tcp://1.2.3.4:5556"]},
		{"url":"http://5.6.7.8:8200"},
		{"model":"no-url"}
	]`)
	cfg := LoadConfig()
	if cfg.RouterMode != "external-push" {
		t.Fatalf("RouterMode = %q, want external-push", cfg.RouterMode)
	}
	if len(cfg.StaticEndpoints) != 2 {
		t.Fatalf("StaticEndpoints len = %d, want 2 (malformed entry dropped)", len(cfg.StaticEndpoints))
	}
	a := cfg.StaticEndpoints[0]
	if a.ID != "a" || a.URL != "http://1.2.3.4:8200" || a.Model != "m" || a.KVEventsTopic != "kv@" {
		t.Errorf("endpoint[0] = %+v, want id=a url trimmed model=m topic=kv@", a)
	}
	if len(a.KVEventsEndpoints) != 1 || a.KVEventsEndpoints[0] != "tcp://1.2.3.4:5556" {
		t.Errorf("endpoint[0] kv_events = %v", a.KVEventsEndpoints)
	}
	b := cfg.StaticEndpoints[1]
	if b.ID != "ext-2" || b.URL != "http://5.6.7.8:8200" {
		t.Errorf("endpoint[1] = %+v, want id=ext-2 (default)", b)
	}
}

func TestExternalPushClamps(t *testing.T) {
	t.Setenv("ROUTER_EXTERNAL_PUSH_CAP", "0")
	t.Setenv("ROUTER_EXTERNAL_PUSH_INTERVAL_S", "0")
	cfg := LoadConfig()
	if cfg.ExternalPushCap != 1 {
		t.Errorf("ExternalPushCap = %d, want clamp to 1", cfg.ExternalPushCap)
	}
	if cfg.ExternalPushIntervalS != 0.05 {
		t.Errorf("ExternalPushIntervalS = %v, want default 0.05", cfg.ExternalPushIntervalS)
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
