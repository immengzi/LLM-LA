package gateway

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

// ModelEntry mirrors config.ModelEntry: one entry from model_list in the
// shared models.yaml.
type ModelEntry struct {
	Name          string
	LabelSelector string
	BatchSize     int
}

// ModelRegistry mirrors the _MODEL_REGISTRY in config.py. When disabled
// (single-model mode), Enabled() returns false and Resolve() always returns
// the default model name.
type ModelRegistry struct {
	defaultModel string
	entries      map[string]ModelEntry
}

// LoadModelRegistry parses models.yaml (shared ConfigMap format). Returns a
// registry, or nil + error.
func LoadModelRegistry(path, defaultModel string) (*ModelRegistry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc struct {
		ModelList []struct {
			ModelName    string `yaml:"model_name"`
			RouterParams struct {
				LabelSelector string `yaml:"label_selector"`
				BatchSize     int    `yaml:"batch_size"`
			} `yaml:"router_params"`
		} `yaml:"model_list"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	entries := make(map[string]ModelEntry)
	for _, item := range doc.ModelList {
		name := item.ModelName
		if name == "" {
			continue
		}
		entries[name] = ModelEntry{
			Name:          name,
			LabelSelector: item.RouterParams.LabelSelector,
			BatchSize:     item.RouterParams.BatchSize,
		}
	}
	return &ModelRegistry{defaultModel: defaultModel, entries: entries}, nil
}

// MaybeLoadModelRegistry loads the registry only if MODEL_CONFIG_PATH is set
// and points to an existing file. Returns nil if multi-model is not enabled.
func MaybeLoadModelRegistry(cfg *Config) *ModelRegistry {
	if cfg.ModelConfigPath == "" {
		return nil
	}
	if fi, err := os.Stat(cfg.ModelConfigPath); err != nil || fi.IsDir() {
		return nil
	}
	reg, err := LoadModelRegistry(cfg.ModelConfigPath, cfg.ModelName)
	if err != nil {
		fmt.Printf("[router] WARNING: failed to load model registry: %v\n", err)
		return nil
	}
	fmt.Printf("[router] Loaded model registry from %s: %v\n", cfg.ModelConfigPath, reg.Names())
	return reg
}

func (r *ModelRegistry) Enabled() bool { return r != nil && len(r.entries) > 0 }

func (r *ModelRegistry) Names() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.entries))
	for k := range r.entries {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (r *ModelRegistry) Entries() []ModelEntry {
	if r == nil {
		return nil
	}
	out := make([]ModelEntry, 0, len(r.entries))
	for _, name := range r.Names() {
		out = append(out, r.entries[name])
	}
	return out
}

func (r *ModelRegistry) Has(name string) bool {
	if r == nil {
		return false
	}
	_, ok := r.entries[name]
	return ok
}

// Resolve mirrors api._resolve_model: validates against the registry and
// returns (model, ok). ok=false means 404 (unknown model). In single-model
// mode it always returns the default model with ok=true.
func (r *ModelRegistry) Resolve(model, defaultModel string) (string, bool) {
	if !r.Enabled() {
		return defaultModel, true
	}
	if model == "" {
		return defaultModel, true
	}
	if !r.Has(model) {
		return "", false
	}
	return model, true
}
