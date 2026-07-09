package gateway

import (
	"strings"
	"sync"
)

// slo_state.go ports src/core/services/router_service/router/slo_state.py:
// a thread-safe in-memory registry of per-request SLO state.

type sloEntry struct {
	ReqID string `json:"req_id"`

	SLOType        *string `json:"slo_type"`
	DeadlineTTFT   *float64 `json:"deadline_ttft"`
	DeadlineTPOTs  *float64 `json:"deadline_tpot_s"`
	DeadlineE2E    *float64 `json:"deadline_e2e"`
	TaskType       *string  `json:"task_type"`
	OutputLenHint  *int     `json:"output_len_hint"`

	ArrivalTS          float64 `json:"arrival_ts"`
	InputTokens        int     `json:"input_tokens"`
	PredictedOutputLen *int    `json:"predicted_output_len"`

	PredictedTTFT     *float64 `json:"predicted_ttft"`
	PredictedTPOT     *float64 `json:"predicted_tpot"`
	PredictedE2E      *float64 `json:"predicted_e2e"`
	Slack             *float64 `json:"slack"`
	BindingConstraint *string  `json:"binding_constraint"`
	AssignedEndpoint  *string  `json:"assigned_endpoint"`
	DispatchTS        *float64 `json:"dispatch_ts"`

	ActualTTFT      *float64 `json:"actual_ttft"`
	ActualOutputLen *int     `json:"actual_output_len"`
	ActualE2E       *float64 `json:"actual_e2e"`
	ResultTS        *float64 `json:"result_ts"`
}

type sloRegistryStore struct {
	mu      sync.RWMutex
	entries map[string]*sloEntry
	ttlS    float64
}

func newSLORegistryStore(ttlS float64) *sloRegistryStore {
	if ttlS < 1.0 {
		ttlS = 1.0
	}
	return &sloRegistryStore{
		entries: make(map[string]*sloEntry),
		ttlS:    ttlS,
	}
}

type sloRegisterArgs struct {
	sloType       *string
	sloTtftMs     *float64
	sloTpotMs     *float64
	sloE2eMs      *float64
	taskType      *string
	outputLenHint *int
	inputTokens   int
	arrivalTS     float64
}

func (r *sloRegistryStore) register(reqID string, a sloRegisterArgs) *sloEntry {
	now := a.arrivalTS
	if now == 0 {
		now = nowS()
	}

	e := &sloEntry{
		ReqID:         reqID,
		ArrivalTS:     now,
		InputTokens:   a.inputTokens,
		TaskType:      a.taskType,
		OutputLenHint: a.outputLenHint,
	}

	if a.sloType != nil && *a.sloType != "" {
		st := strings.ToLower(strings.TrimSpace(*a.sloType))
		e.SLOType = &st
		switch st {
		case "ttft":
			if a.sloTtftMs != nil {
				v := now + *a.sloTtftMs/1000.0
				e.DeadlineTTFT = &v
			}
		case "tpot":
			if a.sloTpotMs != nil {
				v := *a.sloTpotMs / 1000.0
				e.DeadlineTPOTs = &v
			}
		case "ttft+tpot":
			if a.sloTtftMs != nil {
				v := now + *a.sloTtftMs/1000.0
				e.DeadlineTTFT = &v
			}
			if a.sloTpotMs != nil {
				v := *a.sloTpotMs / 1000.0
				e.DeadlineTPOTs = &v
			}
		case "e2e":
			if a.sloE2eMs != nil {
				v := now + *a.sloE2eMs/1000.0
				e.DeadlineE2E = &v
			}
		}
	}

	r.mu.Lock()
	r.entries[reqID] = e
	r.mu.Unlock()
	return e
}

func (r *sloRegistryStore) get(reqID string) *sloEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.entries[reqID]
}

func (r *sloRegistryStore) setPredictedOutputLen(reqID string, tokens int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.entries[reqID]; e != nil {
		e.PredictedOutputLen = &tokens
	}
}

func (r *sloRegistryStore) updateDispatch(reqID, endpoint string, predTTFT, predTPOT, predE2E, slack *float64, binding *string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[reqID]
	if e == nil {
		return
	}
	ep := endpoint
	e.AssignedEndpoint = &ep
	now := nowS()
	e.DispatchTS = &now
	e.PredictedTTFT = predTTFT
	e.PredictedTPOT = predTPOT
	e.PredictedE2E = predE2E
	e.Slack = slack
	e.BindingConstraint = binding
}

func (r *sloRegistryStore) ingestResult(reqID string, actualTTFT, actualE2E *float64, actualOutputLen *int) *sloEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[reqID]
	if e == nil {
		return nil
	}
	now := nowS()
	e.ResultTS = &now
	if actualTTFT != nil {
		e.ActualTTFT = actualTTFT
	}
	if actualOutputLen != nil {
		e.ActualOutputLen = actualOutputLen
	}
	if actualE2E != nil {
		e.ActualE2E = actualE2E
	}
	return e
}

func (r *sloRegistryStore) remove(reqID string) {
	r.mu.Lock()
	delete(r.entries, reqID)
	r.mu.Unlock()
}

func (r *sloRegistryStore) cleanupExpired() int {
	now := nowS()
	r.mu.Lock()
	defer r.mu.Unlock()
	var toDel []string
	for rid, e := range r.entries {
		if now-e.ArrivalTS >= r.ttlS {
			toDel = append(toDel, rid)
		}
	}
	for _, rid := range toDel {
		delete(r.entries, rid)
	}
	return len(toDel)
}

func (r *sloRegistryStore) size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

func (r *sloRegistryStore) debugEntry(reqID string) (*sloEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[reqID]
	return e, ok
}

func (r *sloRegistryStore) debugSummary() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := len(r.entries)
	withSLO := 0
	byType := map[string]int{}
	for _, e := range r.entries {
		t := "none"
		if e.SLOType != nil {
			t = *e.SLOType
			withSLO++
		}
		byType[t]++
	}
	return map[string]interface{}{
		"total":    total,
		"with_slo": withSLO,
		"by_type":  byType,
	}
}
