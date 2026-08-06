package sidecar

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

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
	kwargs, ok := payload["chat_template_kwargs"].(map[string]any)
	if !ok || kwargs["enable_thinking"] != false {
		t.Fatalf("legacy vLLM thinking override = %v, want false", payload["chat_template_kwargs"])
	}
}

func TestBuildPayloadSGLangThinkingPolicy(t *testing.T) {
	w := &InferenceWorker{cfg: &Config{InferenceEngine: "sglang", ModelName: "m"}}

	payload, _ := w.buildPayload("hello", map[string]any{})
	if _, ok := payload["chat_template_kwargs"]; ok {
		t.Fatalf("flat SGLang payload implicitly overrides tokenizer default: %v", payload)
	}

	for _, value := range []bool{false, true} {
		payload, _ = w.buildPayload("hello", map[string]any{"enable_thinking": value})
		kwargs, ok := payload["chat_template_kwargs"].(map[string]any)
		if !ok || kwargs["enable_thinking"] != value {
			t.Fatalf("explicit SGLang enable_thinking=%t produced %v", value, payload)
		}
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

func TestBuildPayloadPreservesMinTokens(t *testing.T) {
	w := &InferenceWorker{cfg: &Config{ModelName: "m"}}
	payload, _ := w.buildPayload("hi", map[string]any{"min_tokens": 7})
	if payload["min_tokens"] != 7 {
		t.Fatalf("min_tokens = %v, want 7", payload["min_tokens"])
	}
}

func TestInferenceWorkerUsesGenericURL(t *testing.T) {
	var received map[string]any
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer engine.Close()
	w := &InferenceWorker{
		cfg:    &Config{InferenceURL: engine.URL, ModelName: "m"},
		client: &http.Client{Timeout: time.Second},
	}
	result, err := w.callNonStreaming("r1", map[string]any{"model": "m"})
	if err != nil || result.outputText != "ok" || received["model"] != "m" {
		t.Fatalf("result=%+v err=%v request=%v", result, err, received)
	}
}

func TestStreamingToolCallsAreAccumulated(t *testing.T) {
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"id\":\"chat-1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"weather\",\"arguments\":\"{\\\"ci\"}}]},\"finish_reason\":null}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"ty\\\":\\\"SF\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[],\"usage\":{\"completion_tokens\":3}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer engine.Close()
	w := &InferenceWorker{
		cfg:    &Config{InferenceEngine: "sglang", InferenceURL: engine.URL, ModelName: "m"},
		client: &http.Client{Timeout: time.Second}, chunkClient: &http.Client{Timeout: time.Second},
	}
	result, err := w.callStreaming("r1", map[string]any{"model": "m", "stream": true}, false)
	if err != nil {
		t.Fatal(err)
	}
	calls := extractToolCalls(result.rawVllm)
	if len(calls) != 1 {
		t.Fatalf("tool calls = %v", calls)
	}
	function := calls[0].(map[string]any)["function"].(map[string]any)
	if function["name"] != "weather" || function["arguments"] != `{"city":"SF"}` ||
		result.finishReason != "tool_calls" || result.usage["completion_tokens"] != float64(3) {
		t.Fatalf("stream result = %+v", result)
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
