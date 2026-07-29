package gateway

import (
	"sort"
	"strings"
)

// sglangRequestIneligibility mirrors the pinned SGLang v0.5.15 Python
// request_ineligibility contract. Tool schemas are intentionally excluded from
// recursive inspection because they are represented in router-side hashing.
func sglangRequestIneligibility(request map[string]interface{}) string {
	messages, hasMessages := request["messages"].([]interface{})
	if hasMessages {
		for _, rawMessage := range messages {
			message, ok := rawMessage.(map[string]interface{})
			if !ok {
				continue
			}
			parts, ok := message["content"].([]interface{})
			if !ok {
				continue
			}
			for _, rawPart := range parts {
				if _, ok := rawPart.(string); ok {
					continue
				}
				part, ok := rawPart.(map[string]interface{})
				if !ok {
					return "multimodal_message_part"
				}
				partType := strings.TrimSpace(strings.ToLower(stringValue(part["type"])))
				if partType != "text" && partType != "input_text" {
					return "multimodal_message_part"
				}
				for _, key := range []string{
					"image", "image_url", "input_image",
					"audio", "audio_url", "input_audio",
					"video", "video_url", "input_video",
				} {
					if _, exists := part[key]; exists {
						return "multimodal_message_part"
					}
				}
			}
		}

		for _, field := range []string{"prompt", "token_ids", "input_ids"} {
			if _, exists := request[field]; exists {
				return "alternate_chat_input"
			}
		}
	}

	unrepresented := make(map[string]interface{}, len(request))
	for key, value := range request {
		normalized := strings.TrimSpace(strings.ToLower(key))
		if normalized != "messages" && normalized != "tools" {
			unrepresented[key] = value
		}
	}
	return walkSGLangUnsupportedFields(unrepresented)
}

func walkSGLangUnsupportedFields(value interface{}) string {
	switch typed := value.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			normalized := strings.ReplaceAll(
				strings.TrimSpace(strings.ToLower(key)),
				"-",
				"_",
			)
			switch normalized {
			case "cache_salt":
				return "cache_salt"
			case "extra_key":
				return "extra_key"
			case "extra_keys":
				return "extra_keys"
			case "chat_template", "chat_template_kwargs", "enable_thinking":
				return "chat_template_override"
			case "add_special_tokens", "continue_final_message":
				return "special_tokenization_override"
			}
			if normalized == "tokenizer" || strings.HasPrefix(normalized, "tokenizer_") {
				return "tokenizer_override"
			}
			if strings.Contains(normalized, "processor") {
				return "custom_processor_override"
			}
			if strings.Contains(normalized, "lora") ||
				normalized == "adapter" ||
				normalized == "adapters" ||
				normalized == "adapter_id" ||
				normalized == "adapter_name" ||
				normalized == "adapter_path" ||
				normalized == "adapter_request" {
				return "lora_or_adapter"
			}
			if strings.Contains(normalized, "speculative") ||
				strings.Contains(normalized, "bigram") ||
				strings.HasPrefix(normalized, "draft_") {
				return "speculative_or_bigram"
			}
			if reason := walkSGLangUnsupportedFields(typed[key]); reason != "" {
				return reason
			}
		}
	case []interface{}:
		for _, child := range typed {
			if reason := walkSGLangUnsupportedFields(child); reason != "" {
				return reason
			}
		}
	}
	return ""
}

func stringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
