package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestSGLangRequestIneligibilityParity(t *testing.T) {
	tests := []struct {
		name    string
		request map[string]interface{}
		want    string
	}{
		{"cache salt", map[string]interface{}{"messages": []interface{}{}, "cache_salt": "x"}, "cache_salt"},
		{"extra key", map[string]interface{}{"messages": []interface{}{}, "extra-key": "x"}, "extra_key"},
		{"extra keys", map[string]interface{}{"messages": []interface{}{}, "extra_keys": []interface{}{"x"}}, "extra_keys"},
		{"lora", map[string]interface{}{"messages": []interface{}{}, "lora_path": "/adapter"}, "lora_or_adapter"},
		{"adapter", map[string]interface{}{"messages": []interface{}{}, "adapter": "a"}, "lora_or_adapter"},
		{"speculative", map[string]interface{}{"messages": []interface{}{}, "speculative_num_steps": 2}, "speculative_or_bigram"},
		{"bigram", map[string]interface{}{"messages": []interface{}{}, "bigram_index": 4}, "speculative_or_bigram"},
		{"chat template", map[string]interface{}{"messages": []interface{}{}, "chat_template": "custom"}, "chat_template_override"},
		{"thinking", map[string]interface{}{"enable_thinking": false}, "chat_template_override"},
		{"tokenizer", map[string]interface{}{"messages": []interface{}{}, "tokenizer_kwargs": map[string]interface{}{}}, "tokenizer_override"},
		{"special tokens", map[string]interface{}{"messages": []interface{}{}, "add_special_tokens": false}, "special_tokenization_override"},
		{"continue final", map[string]interface{}{"messages": []interface{}{}, "continue_final_message": true}, "special_tokenization_override"},
		{"alternate prompt", map[string]interface{}{"messages": []interface{}{}, "prompt": "x"}, "alternate_chat_input"},
		{"alternate token ids", map[string]interface{}{"messages": []interface{}{}, "input_ids": []interface{}{1}}, "alternate_chat_input"},
		{"draft", map[string]interface{}{"messages": []interface{}{}, "draft_model": "x"}, "speculative_or_bigram"},
		{"processor", map[string]interface{}{"messages": []interface{}{}, "sampling_params": map[string]interface{}{"logits_processors": []interface{}{"x"}}}, "custom_processor_override"},
		{
			"multimodal",
			map[string]interface{}{
				"messages": []interface{}{
					map[string]interface{}{
						"role": "user",
						"content": []interface{}{
							map[string]interface{}{"type": "image_url", "image_url": map[string]interface{}{"url": "x"}},
						},
					},
				},
			},
			"multimodal_message_part",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sglangRequestIneligibility(test.request); got != test.want {
				t.Fatalf("ineligibility = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSGLangPlainChatAndToolSchemasAreEligible(t *testing.T) {
	request := map[string]interface{}{
		"messages": []interface{}{
			map[string]interface{}{
				"role": "user",
				"content": []interface{}{
					map[string]interface{}{"type": "text", "text": "use the tool"},
				},
			},
		},
		"tools": []interface{}{
			map[string]interface{}{
				"type": "function",
				"function": map[string]interface{}{
					"name": "run",
					"parameters": map[string]interface{}{
						"properties": map[string]interface{}{
							"prompt":         map[string]interface{}{"type": "string"},
							"processor":      map[string]interface{}{"type": "string"},
							"tokenizer_mode": map[string]interface{}{"type": "string"},
						},
					},
				},
			},
		},
		"temperature": 0.2,
	}
	if got := sglangRequestIneligibility(request); got != "" {
		t.Fatalf("supported chat rejected: %s", got)
	}
}

func TestRegisterKVBlocksSkipsUnsafeSGLangWithoutRejectingRequest(t *testing.T) {
	cfg := validSGLangTestConfig()
	server := &Server{cfg: cfg, kv: newKVAware()}
	meta := map[string]interface{}{"enable_thinking": false, "tenant": "kept"}

	got := server.registerKVBlocks("r1", "prompt", meta, false)
	if got["tenant"] != "kept" {
		t.Fatalf("request metadata changed: %#v", got)
	}
	if got["kv_hash_skip_reason"] != "sglang_request_unsupported:chat_template_override" {
		t.Fatalf("skip reason = %#v", got["kv_hash_skip_reason"])
	}
}

func TestRegisterKVBlocksFailsClosedOnSGLangContractMismatch(t *testing.T) {
	cfg := validSGLangTestConfig()
	cfg.SGLangContractVersion = "0.5.16"
	server := &Server{cfg: cfg, kv: newKVAware()}
	meta := map[string]interface{}{"tenant": "kept"}

	got := server.registerKVBlocks("r1", "prompt", meta, false)
	if got["tenant"] != "kept" {
		t.Fatalf("request metadata changed: %#v", got)
	}
	want := "sglang_contract:sglang_contract_version_mismatch:expected=0.5.15,actual=0.5.16"
	if got["kv_hash_skip_reason"] != want {
		t.Fatalf("skip reason = %#v, want %q", got["kv_hash_skip_reason"], want)
	}
}

func TestRegisterKVBlocksPreservesSupportedChatAndSGLangGoldenHashes(t *testing.T) {
	var received map[string]interface{}
	hasher := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode hasher request: %v", err)
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"block_hashes": []interface{}{3817746824117602890, -4216701448867210342},
		})
	}))
	defer hasher.Close()

	cfg := validSGLangTestConfig()
	cfg.HashServiceURL = hasher.URL
	hashClient := NewHashClient(cfg)
	server := &Server{cfg: cfg, kv: newKVAware(), hashClient: hashClient}
	chatRequest := map[string]interface{}{
		"messages": []interface{}{
			map[string]interface{}{"role": "user", "content": "use the tool"},
		},
		"tools": []interface{}{
			map[string]interface{}{
				"type": "function",
				"function": map[string]interface{}{
					"name": "run",
					"parameters": map[string]interface{}{
						"properties": map[string]interface{}{
							"processor": map[string]interface{}{"type": "string"},
						},
					},
				},
			},
		},
	}
	before, err := json.Marshal(chatRequest)
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]interface{}{"__chat_request__": chatRequest}

	got := server.registerKVBlocks("r1", "flattened prompt", meta, false)
	if _, skipped := got["kv_hash_skip_reason"]; skipped {
		t.Fatalf("supported request was skipped: %#v", got)
	}
	after, err := json.Marshal(chatRequest)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("chat request mutated:\n got %s\nwant %s", after, before)
	}
	if received["backend"] != "sglang" {
		t.Fatalf("hasher backend = %#v, want sglang", received["backend"])
	}
	if !reflect.DeepEqual(received["messages"], chatRequest["messages"]) ||
		!reflect.DeepEqual(received["tools"], chatRequest["tools"]) {
		t.Fatalf("hasher request did not preserve messages/tools: %#v", received)
	}
	if gotHashes := server.kv.reqBlocks["r1"]; !reflect.DeepEqual(
		gotHashes,
		[]string{"3817746824117602890", "-4216701448867210342"},
	) {
		t.Fatalf("registered hashes = %#v", gotHashes)
	}
}

func TestVLLMHashRequestRegression(t *testing.T) {
	var received map[string]interface{}
	hasher := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"block_hashes": []interface{}{json.Number("12009346384364793183")},
		})
	}))
	defer hasher.Close()

	cfg := &Config{
		InferenceEngine: "vllm",
		KVHashBackend:   "vllm",
		KVHashSource:    "inline",
		HashServiceURL:  hasher.URL,
		HashTimeoutS:    1,
	}
	client := NewHashClient(cfg)
	messages := []interface{}{map[string]interface{}{"role": "user", "content": "hello"}}
	tools := []interface{}{map[string]interface{}{"type": "function"}}
	hashes, err := client.ComputeHashes(t.Context(), "flat", messages, tools)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hashes, []string{"12009346384364793183"}) {
		t.Fatalf("vLLM hashes = %#v", hashes)
	}
	if received["backend"] != "vllm" ||
		!reflect.DeepEqual(received["messages"], messages) ||
		!reflect.DeepEqual(received["tools"], tools) {
		t.Fatalf("vLLM inline body changed: %#v", received)
	}
}

func validSGLangTestConfig() *Config {
	return &Config{
		InferenceEngine:       "sglang",
		KVHashBackend:         "sglang",
		SGLangContractVersion: sglangHashContractVersion,
		KVHashSource:          "inline",
		KVBlockSize:           2,
		KVAware:               true,
		HashTimeoutS:          1,
	}
}
