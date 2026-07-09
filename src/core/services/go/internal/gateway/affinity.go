package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"
)

// deriveAffinityKey mirrors router/affinity.py:derive_affinity_key. It derives a
// stable per-conversation key from the model and the conversation's opening
// (system prompt + first user message), which is identical across every turn
// because each turn resends the full message history. Returns "" when there is
// no user message to key on.
func deriveAffinityKey(model string, messages []interface{}) string {
	if len(messages) == 0 {
		return ""
	}

	parts := []string{"model:" + model}
	sawUser := false
	for _, mi := range messages {
		m, ok := mi.(map[string]interface{})
		if !ok {
			continue
		}
		role, _ := m["role"].(string)
		content := messageContentText(m["content"])
		switch role {
		case "system":
			parts = append(parts, "system:"+content)
		case "user":
			parts = append(parts, "user:"+content)
			sawUser = true
		}
		if sawUser {
			break // first user message only -> stable across turns
		}
	}

	if !sawUser {
		return ""
	}

	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])[:16]
}

// messageContentText flattens an OpenAI/Anthropic message content (string or
// content-block array) into its text.
func messageContentText(content interface{}) string {
	switch c := content.(type) {
	case string:
		return c
	case []interface{}:
		var b strings.Builder
		for _, blk := range c {
			bm, ok := blk.(map[string]interface{})
			if !ok {
				continue
			}
			if t, _ := bm["type"].(string); t == "text" {
				if txt, ok := bm["text"].(string); ok {
					b.WriteString(txt)
				}
			}
		}
		return b.String()
	default:
		return ""
	}
}

type affinityEntry struct {
	endpoint string
	lastSeen time.Time
}

// AffinityMap is a thread-safe conversation-key -> endpoint map with TTL expiry,
// mirroring router/affinity.py:AffinityMap.
type AffinityMap struct {
	mu  sync.Mutex
	m   map[string]affinityEntry
	ttl time.Duration
}

func NewAffinityMap(ttlS float64) *AffinityMap {
	return &AffinityMap{
		m:   make(map[string]affinityEntry),
		ttl: time.Duration(ttlS * float64(time.Second)),
	}
}

// Lookup returns the mapped endpoint if present and not expired, else "".
func (a *AffinityMap) Lookup(key string) string {
	if key == "" {
		return ""
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.m[key]
	if !ok {
		return ""
	}
	if time.Since(e.lastSeen) > a.ttl {
		delete(a.m, key)
		return ""
	}
	return e.endpoint
}

// Claim records (or refreshes) that this conversation key is served by endpoint.
func (a *AffinityMap) Claim(key, endpoint string) {
	if key == "" || endpoint == "" {
		return
	}
	a.mu.Lock()
	a.m[key] = affinityEntry{endpoint: endpoint, lastSeen: time.Now()}
	a.mu.Unlock()
}

// Size returns the number of live mappings.
func (a *AffinityMap) Size() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.m)
}
