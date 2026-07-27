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

// TestSidecarEnabledDefaultAndKvEvents mirrors the Python config tests for the
// sidecar-optional central-push knob.
func TestSidecarEnabledDefaultAndKvEvents(t *testing.T) {
	cfg := LoadConfig()
	if !cfg.SidecarEnabled {
		t.Error("SidecarEnabled should default to true")
	}
	if cfg.VLLMKvEventsPort != 5557 {
		t.Errorf("VLLMKvEventsPort = %d, want 5557", cfg.VLLMKvEventsPort)
	}
	if cfg.VLLMKvEventsTopic != "kv@" {
		t.Errorf("VLLMKvEventsTopic = %q, want kv@", cfg.VLLMKvEventsTopic)
	}
}

// TestSidecarDisabledForPushAndCentralPush: the disable flag takes effect for
// push-* and central-push, but is ignored (kept on) for pull / external-push.
func TestSidecarDisabledForPushAndCentralPush(t *testing.T) {
	pushModes := []string{
		"central-push",
		"push-rr", "push-random", "push-leastq",
		"push-throughput", "push-p2c", "push-kv-cost",
		"push-least-kv", "push-least-latency", "push-least-busy",
	}
	for _, mode := range pushModes {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("ROUTER_MODE", mode)
			t.Setenv("ROUTER_SIDECAR_ENABLED", "false")
			cfg := LoadConfig()
			if cfg.SidecarEnabled {
				t.Errorf("SidecarEnabled should stay false for mode %q", mode)
			}
			if mode == "central-push" {
				if !cfg.IsCentralPushDirect() {
					t.Error("IsCentralPushDirect() should be true")
				}
				if cfg.UsesPushDelivery() {
					t.Error("UsesPushDelivery() should be false (central-queue direct)")
				}
				if !cfg.UsesDirectDelivery() {
					t.Error("UsesDirectDelivery() should be true")
				}
			} else {
				if !cfg.IsPushDirect() {
					t.Errorf("IsPushDirect() should be true for mode %q", mode)
				}
				if !cfg.UsesPushDelivery() {
					t.Errorf("UsesPushDelivery() should be true for push-* direct (PushDispatcher)")
				}
				if cfg.UsesDirectDelivery() {
					t.Errorf("UsesDirectDelivery() should be false for push-* (not ExternalPushDispatcher)")
				}
			}
		})
	}
	for _, mode := range []string{"pull", "external-push"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("ROUTER_MODE", mode)
			t.Setenv("ROUTER_SIDECAR_ENABLED", "false")
			cfg := LoadConfig()
			if !cfg.SidecarEnabled {
				t.Errorf("SidecarEnabled should be forced true for mode %q", mode)
			}
			if cfg.IsCentralPushDirect() || cfg.IsPushDirect() {
				t.Errorf("direct predicates should be false for mode %q", mode)
			}
		})
	}
}

// TestSidecarCentralPushUsesPushDelivery: default central-push (sidecar on)
// still delivers via the sidecar PushRouter.
func TestSidecarCentralPushUsesPushDelivery(t *testing.T) {
	t.Setenv("ROUTER_MODE", "central-push")
	t.Setenv("ROUTER_SIDECAR_ENABLED", "true")
	cfg := LoadConfig()
	if cfg.IsCentralPushDirect() {
		t.Error("IsCentralPushDirect() should be false when sidecar enabled")
	}
	if !cfg.UsesPushDelivery() {
		t.Error("UsesPushDelivery() should be true for sidecar central-push")
	}
	if cfg.UsesDirectDelivery() {
		t.Error("UsesDirectDelivery() should be false for sidecar central-push")
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

// TestSoftDivertConfigDefaults verifies the soft-divert knobs default to the
// same values as the Python router config.
func TestSoftDivertConfigDefaults(t *testing.T) {
	cfg := LoadConfig()
	if cfg.KVSoftDivert {
		t.Error("KVSoftDivert should default off")
	}
	if cfg.KVPressureHigh != 0.85 || cfg.KVPressureLow != 0.75 || cfg.KVPressurePeerOK != 0.70 {
		t.Errorf("pressure defaults = %v/%v/%v, want 0.85/0.75/0.70", cfg.KVPressureHigh, cfg.KVPressureLow, cfg.KVPressurePeerOK)
	}
	if cfg.KVSoftMinHits != 1 {
		t.Errorf("KVSoftMinHits = %d, want 1", cfg.KVSoftMinHits)
	}
	if cfg.KVUsageStaleS != 30.0 {
		t.Errorf("KVUsageStaleS = %v, want 30.0", cfg.KVUsageStaleS)
	}
	if cfg.KVHealthPollIntervalS != 5.0 {
		t.Errorf("KVHealthPollIntervalS = %v, want 5.0", cfg.KVHealthPollIntervalS)
	}
}

func TestSoftDivertConfigOverride(t *testing.T) {
	t.Setenv("ROUTER_KV_SOFT_DIVERT", "true")
	t.Setenv("ROUTER_KV_PRESSURE_HIGH", "0.9")
	t.Setenv("ROUTER_KV_SOFT_MIN_HITS", "-3") // clamps to 0
	cfg := LoadConfig()
	if !cfg.KVSoftDivert {
		t.Error("KVSoftDivert should be true")
	}
	if cfg.KVPressureHigh != 0.9 {
		t.Errorf("KVPressureHigh = %v, want 0.9", cfg.KVPressureHigh)
	}
	if cfg.KVSoftMinHits != 0 {
		t.Errorf("KVSoftMinHits = %d, want clamp to 0", cfg.KVSoftMinHits)
	}
}

// TestAffinityPersistConfigDefaults verifies persistent-affinity defaults match
// the Python router config.
func TestAffinityPersistConfigDefaults(t *testing.T) {
	cfg := LoadConfig()
	if cfg.AffinityPersistEnabled {
		t.Error("AffinityPersistEnabled should default off")
	}
	if cfg.AffinityRedisTTLSeconds != 0 {
		t.Errorf("AffinityRedisTTLSeconds = %d, want 0", cfg.AffinityRedisTTLSeconds)
	}
	if cfg.AffinityRedisKeyPrefix != "affinity" {
		t.Errorf("AffinityRedisKeyPrefix = %q, want affinity", cfg.AffinityRedisKeyPrefix)
	}
	if cfg.AffinityCacheMax != 100000 {
		t.Errorf("AffinityCacheMax = %d, want 100000", cfg.AffinityCacheMax)
	}
	if cfg.AffinityEndpointStaleS != 1800.0 {
		t.Errorf("AffinityEndpointStaleS = %v, want 1800.0", cfg.AffinityEndpointStaleS)
	}
}

func TestAffinityPersistConfigOverride(t *testing.T) {
	t.Setenv("AFFINITY_PERSIST_ENABLED", "true")
	t.Setenv("AFFINITY_REDIS_KEY_PREFIX", "  ")  // blank -> falls back to "affinity"
	t.Setenv("AFFINITY_REDIS_TTL_SECONDS", "-5") // clamps to 0
	t.Setenv("CLUSTER", " bz ")
	cfg := LoadConfig()
	if !cfg.AffinityPersistEnabled {
		t.Error("AffinityPersistEnabled should be true")
	}
	if cfg.AffinityRedisKeyPrefix != "affinity" {
		t.Errorf("blank prefix should fall back to affinity, got %q", cfg.AffinityRedisKeyPrefix)
	}
	if cfg.AffinityRedisTTLSeconds != 0 {
		t.Errorf("negative TTL should clamp to 0, got %d", cfg.AffinityRedisTTLSeconds)
	}
	if cfg.Cluster != "bz" {
		t.Errorf("CLUSTER should be trimmed to bz, got %q", cfg.Cluster)
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
