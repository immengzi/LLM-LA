package sidecar

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseKvUsagePrefersV1MaxEngines(t *testing.T) {
	text := `
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0"} 0.40
vllm:kv_cache_usage_perc{engine="1"} 0.92
lmcache:local_cache_usage{engine="0"} 12345
`
	v, ok := ParseKvUsageFromText(strings.NewReader(text))
	if !ok {
		t.Fatal("expected ok")
	}
	if v < 0.91 || v > 0.93 {
		t.Fatalf("got %v, want ~0.92", v)
	}
}

func TestParseKvUsageFallbackV0(t *testing.T) {
	text := `
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc{engine="0"} 0.55
`
	v, ok := ParseKvUsageFromText(strings.NewReader(text))
	if !ok || v < 0.54 || v > 0.56 {
		t.Fatalf("got %v ok=%v", v, ok)
	}
}

func TestParseKvUsagePreferV1WhenBoth(t *testing.T) {
	text := `
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0"} 0.33
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc{engine="0"} 0.99
`
	v, ok := ParseKvUsageFromText(strings.NewReader(text))
	if !ok || v < 0.32 || v > 0.34 {
		t.Fatalf("got %v ok=%v", v, ok)
	}
}

func TestParseKvUsageMissing(t *testing.T) {
	text := `# TYPE lmcache:local_cache_usage gauge
lmcache:local_cache_usage 1
`
	if _, ok := ParseKvUsageFromText(strings.NewReader(text)); ok {
		t.Fatal("expected missing")
	}
}

func TestParseKvUsagePercentScale(t *testing.T) {
	text := `
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0"} 85
`
	v, ok := ParseKvUsageFromText(strings.NewReader(text))
	if !ok || v < 0.84 || v > 0.86 {
		t.Fatalf("got %v ok=%v", v, ok)
	}
}

func TestPullRequestOmitsKvWhenUnset(t *testing.T) {
	req := pullRequest{Endpoint: "ep", Want: 1, Model: "m"}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if strings.Contains(s, "kv_usage") {
		t.Fatalf("unexpected kv_usage in %s", s)
	}
}

func TestPullRequestIncludesKvWhenSet(t *testing.T) {
	kv := 0.42
	req := pullRequest{Endpoint: "ep", Want: 1, Model: "m", KvUsage: &kv}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.Contains(s, "kv_usage") || !strings.Contains(s, "0.42") {
		t.Fatalf("missing kv_usage in %s", s)
	}
}
