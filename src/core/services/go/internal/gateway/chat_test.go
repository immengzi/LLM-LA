package gateway

import (
	"encoding/json"
	"strings"
	"testing"
)

func parseSSEObjects(t *testing.T, stream string) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	for _, part := range strings.Split(stream, "\n\n") {
		if !strings.HasPrefix(part, "data: ") || part == "data: [DONE]" {
			continue
		}
		var value map[string]interface{}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(part, "data: ")), &value); err != nil {
			t.Fatalf("invalid SSE JSON %q: %v", part, err)
		}
		out = append(out, value)
	}
	return out
}

func sseDelta(t *testing.T, object map[string]interface{}) map[string]interface{} {
	t.Helper()
	choices := object["choices"].([]interface{})
	return choices[0].(map[string]interface{})["delta"].(map[string]interface{})
}

func TestRecoverSSETextWithoutDuplication(t *testing.T) {
	result := map[string]interface{}{
		"output": "hello", "finish_reason": "stop",
		"usage": map[string]interface{}{"completion_tokens": 1},
	}
	tests := []struct {
		name     string
		emitted  string
		missing  string
		objCount int
	}{
		{name: "zero emitted", emitted: "", missing: "hello", objCount: 2},
		{name: "partial emitted", emitted: "hel", missing: "lo", objCount: 2},
		{name: "full emitted", emitted: "hello", missing: "", objCount: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream := recoverSSEFromFullResult("r1", "m", 1, result, test.emitted, nil, "pod")
			objects := parseSSEObjects(t, stream)
			if len(objects) != test.objCount || !strings.HasSuffix(stream, "data: [DONE]\n\n") {
				t.Fatalf("recovered stream objects=%d stream=%q", len(objects), stream)
			}
			if test.missing != "" {
				if got := sseDelta(t, objects[0])["content"]; got != test.missing {
					t.Fatalf("missing content delta = %v, want %q", got, test.missing)
				}
			}
			final := objects[len(objects)-1]
			if final["system_fingerprint"] != "pod" {
				t.Fatalf("final metadata missing endpoint: %v", final)
			}
			usage := final["usage"].(map[string]interface{})
			if usage["completion_tokens"] != float64(1) {
				t.Fatalf("final usage = %v", usage)
			}
		})
	}
}

func TestRecoverSSEPartialToolCall(t *testing.T) {
	result := map[string]interface{}{
		"output": "", "finish_reason": "tool_calls",
		"tool_calls": []interface{}{map[string]interface{}{
			"id": "call-1", "type": "function",
			"function": map[string]interface{}{"name": "weather", "arguments": `{"city":"SF"}`},
		}},
	}
	emitted := map[int]map[string]interface{}{
		0: {
			"id": "call-1", "type": "function",
			"function": map[string]interface{}{"name": "wea", "arguments": `{"city":`},
		},
	}
	objects := parseSSEObjects(t, recoverSSEFromFullResult("r1", "m", 1, result, "", emitted, nil))
	if len(objects) != 2 {
		t.Fatalf("objects = %v", objects)
	}
	calls := sseDelta(t, objects[0])["tool_calls"].([]interface{})
	call := calls[0].(map[string]interface{})
	function := call["function"].(map[string]interface{})
	if _, duplicated := call["id"]; duplicated || function["name"] != "ther" || function["arguments"] != `"SF"}` {
		t.Fatalf("tool recovery delta duplicated or incomplete: %v", call)
	}
	choices := objects[1]["choices"].([]interface{})
	if got := choices[0].(map[string]interface{})["finish_reason"]; got != "tool_calls" {
		t.Fatalf("finish reason = %v", got)
	}
}

func TestRecoverSSECompleteToolStreamOnlyFinalizes(t *testing.T) {
	result := map[string]interface{}{
		"output": "", "finish_reason": "tool_calls",
		"tool_calls": []interface{}{map[string]interface{}{
			"id": "call-1", "type": "function",
			"function": map[string]interface{}{"name": "weather", "arguments": "{}"},
		}},
	}
	emitted := map[int]map[string]interface{}{
		0: {
			"id": "call-1", "type": "function",
			"function": map[string]interface{}{"name": "weather", "arguments": "{}"},
		},
	}
	objects := parseSSEObjects(t, recoverSSEFromFullResult("r1", "m", 1, result, "", emitted, nil))
	if len(objects) != 1 || len(sseDelta(t, objects[0])) != 0 {
		t.Fatalf("complete stream was duplicated: %v", objects)
	}
}

func TestRecoverSSEImpossibleMismatchEmitsError(t *testing.T) {
	stream := recoverSSEFromFullResult(
		"r1", "m", 1, map[string]interface{}{"output": "different"}, "already sent", nil, nil,
	)
	objects := parseSSEObjects(t, stream)
	if len(objects) != 1 {
		t.Fatalf("error stream = %q", stream)
	}
	errObject := objects[0]["error"].(map[string]interface{})
	if errObject["code"] != "stream_recovery_failed" || !strings.HasSuffix(stream, "data: [DONE]\n\n") {
		t.Fatalf("missing explicit stream error: %v", errObject)
	}
}
