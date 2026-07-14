package sidecar

import "testing"

func TestBuildPayloadFlatPrompt(t *testing.T) {
	w := &VLLMWorker{cfg: &Config{ModelName: "m"}}
	payload, forwardStream := w.buildPayload("hello", map[string]any{})
	if forwardStream {
		t.Fatal("flat prompt should not request stream forwarding")
	}
	if payload["model"] != "m" {
		t.Fatalf("model = %v, want m", payload["model"])
	}
	if payload["max_tokens"].(int) != 128 {
		t.Fatalf("max_tokens = %v, want 128", payload["max_tokens"])
	}
	if _, ok := payload["messages"]; !ok {
		t.Fatal("flat prompt payload missing messages")
	}
}

func TestBuildPayloadChatRequestForwardsStream(t *testing.T) {
	w := &VLLMWorker{cfg: &Config{ModelName: "m"}}
	meta := map[string]any{
		"__chat_request__": map[string]any{
			"messages": []any{map[string]any{"role": "user", "content": "hi"}},
			"stream":   true,
		},
	}
	payload, forwardStream := w.buildPayload("ignored", meta)
	if !forwardStream {
		t.Fatal("chat request with stream=true should forward stream")
	}
	// vLLM is always queried non-streaming for the aggregated result path,
	// so stream is forced to false on the outbound payload.
	if payload["stream"] != false {
		t.Fatalf("outbound stream = %v, want false", payload["stream"])
	}
	if payload["model"] != "m" {
		t.Fatalf("model override failed: %v", payload["model"])
	}
}

func TestBuildPayloadForceIgnoreEos(t *testing.T) {
	w := &VLLMWorker{cfg: &Config{ModelName: "m", ForceIgnoreEos: true}}
	payload, _ := w.buildPayload("hi", map[string]any{})
	if payload["ignore_eos"] != true {
		t.Fatalf("ForceIgnoreEos not applied: %v", payload["ignore_eos"])
	}
}

func TestMergeAndCompleteToolCalls(t *testing.T) {
	acc := map[int]map[string]any{}
	mergeToolCallDelta(acc, []any{
		map[string]any{"index": 0, "id": "call_1", "type": "function",
			"function": map[string]any{"name": "get_weather", "arguments": `{"ci`}},
	})
	mergeToolCallDelta(acc, []any{
		map[string]any{"index": 0,
			"function": map[string]any{"arguments": `ty":"SF"}`}},
	})
	out := completeToolCalls(acc)
	if len(out) != 1 {
		t.Fatalf("completeToolCalls len = %d, want 1", len(out))
	}
	tc := out[0].(map[string]any)
	fn := tc["function"].(map[string]any)
	if fn["name"] != "get_weather" {
		t.Fatalf("name = %v, want get_weather", fn["name"])
	}
	if fn["arguments"] != `{"city":"SF"}` {
		t.Fatalf("arguments = %v, want concatenated json", fn["arguments"])
	}
}

func TestCompleteToolCallsSkipsNameless(t *testing.T) {
	acc := map[int]map[string]any{}
	mergeToolCallDelta(acc, []any{
		map[string]any{"index": 0, "function": map[string]any{"arguments": "{}"}},
	})
	if out := completeToolCalls(acc); len(out) != 0 {
		t.Fatalf("expected nameless tool call to be dropped, got %v", out)
	}
}

func TestCompleteToolCallsOrderedByIndex(t *testing.T) {
	acc := map[int]map[string]any{}
	mergeToolCallDelta(acc, []any{
		map[string]any{"index": 2, "function": map[string]any{"name": "c"}},
		map[string]any{"index": 0, "function": map[string]any{"name": "a"}},
		map[string]any{"index": 1, "function": map[string]any{"name": "b"}},
	})
	out := completeToolCalls(acc)
	names := []string{}
	for _, o := range out {
		names = append(names, o.(map[string]any)["function"].(map[string]any)["name"].(string))
	}
	if len(names) != 3 || names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Fatalf("order = %v, want [a b c]", names)
	}
}

func TestExtractToolCalls(t *testing.T) {
	raw := map[string]any{
		"choices": []any{
			map[string]any{
				"message": map[string]any{
					"tool_calls": []any{map[string]any{"id": "call_1"}},
				},
			},
		},
	}
	tc := extractToolCalls(raw)
	if len(tc) != 1 {
		t.Fatalf("extractToolCalls len = %d, want 1", len(tc))
	}
	if extractToolCalls(map[string]any{}) != nil {
		t.Fatal("extractToolCalls on empty map should be nil")
	}
}

func TestToBoolAndToInt(t *testing.T) {
	if !toBool(true) || !toBool(1.0) || !toBool("true") {
		t.Fatal("toBool truthy cases failed")
	}
	if toBool("false") || toBool("0") || toBool("") || toBool(0.0) {
		t.Fatal("toBool falsy cases failed")
	}
	if toInt(5, 0) != 5 || toInt(5.9, 0) != 5 || toInt("x", 7) != 7 {
		t.Fatal("toInt cases failed")
	}
}
