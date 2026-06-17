package gateway

import (
	"math"
	"sort"
)

// slo.go ties together the SLO subsystem: it implements both the sloEngine
// interface (used by CentralQueue for sort/admission/dispatch) and the
// sloRegistry interface (used by the HTTP handler layer for register / ingest /
// debug). It mirrors the SLO wiring in router_state.py + api.py.

type sloEngineImpl struct {
	cfg      *Config
	registry *sloRegistryStore
	lp       latencyPredictor
	batch    *batchSizeEstimator
	queue    *queueWaitEstimator
	pred     OutputLengthPredictor
}

// NewSLOEngine builds the SLO subsystem wired to the shared output-length
// predictor singleton (exported for main wiring). The returned value satisfies
// both the sloEngine and sloRegistry interfaces.
func NewSLOEngine(cfg *Config) *sloEngineImpl {
	return newSLOEngine(cfg, getOutputLengthPredictor(cfg))
}

// newSLOEngine builds the SLO engine. The output-length predictor must be the
// same singleton used by the central queue so stateful predictors share data.
func newSLOEngine(cfg *Config, pred OutputLengthPredictor) *sloEngineImpl {
	return &sloEngineImpl{
		cfg:      cfg,
		registry: newSLORegistryStore(cfg.PollResultTTLS),
		lp:       getLatencyPredictor(cfg),
		batch:    newBatchSizeEstimator(cfg.BatchSizeEstimate, cfg.FixedBatchEstimate),
		queue:    newQueueWaitEstimator(cfg.QueueWaitModel),
		pred:     pred,
	}
}

// ---------------- sloRegistry interface (handlers) ----------------

func (s *sloEngineImpl) register(rid string, req *EnqueueRequest, arrivalTS float64) {
	hasAnnotations := req.SLOType != nil || req.TaskType != nil || req.OutputLenHint != nil
	if !hasAnnotations && !s.cfg.SLOAware {
		return
	}

	inputTokens := maxInt(1, len(req.Prompt)/4)
	s.registry.register(rid, sloRegisterArgs{
		sloType:       req.SLOType,
		sloTtftMs:     req.SLOTtftMs,
		sloTpotMs:     req.SLOTpotMs,
		sloE2eMs:      req.SLOE2eMs,
		taskType:      req.TaskType,
		outputLenHint: req.OutputLenHint,
		inputTokens:   inputTokens,
		arrivalTS:     arrivalTS,
	})

	var predictedLen int
	if req.OutputLenHint != nil && *req.OutputLenHint > 0 {
		predictedLen = *req.OutputLenHint
	} else {
		tt := ""
		if req.TaskType != nil {
			tt = *req.TaskType
		}
		predictedLen = s.pred.Predict(req.Prompt, inputTokens, tt, rid)
	}
	s.registry.setPredictedOutputLen(rid, predictedLen)
}

func (s *sloEngineImpl) ingestResult(rid string, result map[string]interface{}) {
	if result == nil {
		return
	}

	trace, _ := result["trace"].(map[string]interface{})
	if trace == nil {
		trace, _ = result["__trace__"].(map[string]interface{})
	}
	usage, _ := result["usage"].(map[string]interface{})

	var actualTTFT, actualE2E *float64
	var actualOutputLen *int

	if trace != nil {
		if v, ok := trace["ttft_s"]; ok {
			f := toFloat(v)
			actualTTFT = &f
		} else if t1, ok1 := trace["t_first_token"]; ok1 {
			if t0, ok0 := trace["t_prefill_start"]; ok0 {
				f := toFloat(t1) - toFloat(t0)
				actualTTFT = &f
			}
		}
		if v, ok := trace["e2e_s"]; ok {
			f := toFloat(v)
			actualE2E = &f
		}
	}
	if usage != nil {
		if v, ok := usage["completion_tokens"]; ok {
			n := int(toFloat(v))
			actualOutputLen = &n
		}
	}

	if actualOutputLen != nil && *actualOutputLen > 0 {
		e := s.registry.get(rid)
		tt := ""
		it := 0
		if e != nil {
			if e.TaskType != nil {
				tt = *e.TaskType
			}
			it = e.InputTokens
		}
		s.pred.Update(rid, *actualOutputLen, tt, it)
	}

	e := s.registry.ingestResult(rid, actualTTFT, actualE2E, actualOutputLen)
	if e == nil {
		return
	}

	if e.PredictedOutputLen != nil && e.ActualOutputLen != nil && *e.ActualOutputLen > 0 {
		ratio := float64(*e.PredictedOutputLen-*e.ActualOutputLen) / float64(*e.ActualOutputLen)
		observeOutputLenError(ratio)
	}
	if e.PredictedTTFT != nil && e.ActualTTFT != nil {
		observeTTFTPredictionError(*e.PredictedTTFT - *e.ActualTTFT)
	}
	if e.PredictedE2E != nil && e.ActualE2E != nil {
		observeE2EPredictionError(*e.PredictedE2E - *e.ActualE2E)
	}

	if e.SLOType != nil {
		met := true
		switch *e.SLOType {
		case "ttft":
			if e.DeadlineTTFT != nil && e.ActualTTFT != nil && e.ArrivalTS+*e.ActualTTFT > *e.DeadlineTTFT {
				met = false
			}
		case "e2e":
			if e.DeadlineE2E != nil && e.ActualE2E != nil && e.ArrivalTS+*e.ActualE2E > *e.DeadlineE2E {
				met = false
			}
		case "ttft+tpot":
			if e.DeadlineTTFT != nil && e.ActualTTFT != nil && e.ArrivalTS+*e.ActualTTFT > *e.DeadlineTTFT {
				met = false
			}
		}
		if met {
			incSLOActualMet()
		} else {
			incSLOActualMiss()
		}
	}

	setSLORegistrySize(s.registry.size())
}

// onResult mirrors the SLO inflight / completion / online-update bookkeeping in
// _ingest_result_payload. Called only when an endpoint is known.
func (s *sloEngineImpl) onResult(rid, endpoint string) {
	if !s.cfg.SLOAware {
		return
	}
	if endpoint != "" {
		s.batch.decrementInflight(endpoint, 1)
		s.queue.recordCompletion(endpoint)
	}
	if s.cfg.LatencyOnlineUpdate {
		e := s.registry.get(rid)
		if e != nil && e.ActualTTFT != nil {
			out := 0
			if e.ActualOutputLen != nil {
				out = *e.ActualOutputLen
			}
			obs := latencyObservation{
				inputTokens:  e.InputTokens,
				cachedTokens: 0,
				outputTokens: out,
				batchSize:    s.cfg.FixedBatchEstimate,
				actualTTFTs:  *e.ActualTTFT,
			}
			if e.ActualE2E != nil {
				obs.actualE2Es = *e.ActualE2E
			}
			s.lp.update(obs)
		}
	}
}

func (s *sloEngineImpl) size() int { return s.registry.size() }

func (s *sloEngineImpl) debugEntry(rid string) (interface{}, bool) {
	e, ok := s.registry.debugEntry(rid)
	if !ok {
		return nil, false
	}
	return e, true
}

func (s *sloEngineImpl) debugSummary() interface{} { return s.registry.debugSummary() }

func (s *sloEngineImpl) cleanupExpired() int { return s.registry.cleanupExpired() }

// ---------------- sloEngine interface (CentralQueue) ----------------

func (s *sloEngineImpl) sloAwareSort(pool []queueItem, endpoint string, want int, kv *kvAware) ([]queueItem, map[string]int) {
	kvEnabled := s.cfg.KVAware
	sloWithKV := s.cfg.SLOWithKV

	kvHitsMap := make(map[string]int, len(pool))
	if kvEnabled {
		for _, it := range pool {
			kvHitsMap[it.reqID] = kv.prefixLen(endpoint, it.reqID)
		}
	}

	type scoredItem struct {
		item    queueItem
		slack   float64
		binding string
		cached  int
	}
	scored := make([]scoredItem, 0, len(pool))

	batchSize := s.batch.estimate(endpoint)
	queueWait := s.queue.estimate(endpoint, 0)

	for _, it := range pool {
		slack := sloInf
		binding := ""
		cached := kvHitsMap[it.reqID]

		entry := s.registry.get(it.reqID)
		if entry != nil && entry.SLOType != nil {
			slack, binding = computeSlack(entry, s.lp, batchSize, cached, queueWait, 16)
			sl := slack
			entry.Slack = &sl
			if binding != "" {
				b := binding
				entry.BindingConstraint = &b
			} else {
				entry.BindingConstraint = nil
			}
			predTTFT := s.lp.predictTTFT(entry.InputTokens, cached*16, batchSize)
			entry.PredictedTTFT = &predTTFT
		}

		scored = append(scored, scoredItem{item: it, slack: slack, binding: binding, cached: cached})
	}

	const slackBandMs = 100.0
	bandOf := func(slack float64) float64 {
		if math.IsInf(slack, 1) || slack >= sloInf {
			return math.Inf(1)
		}
		if math.IsInf(slack, -1) {
			return math.Inf(-1)
		}
		return math.Round(slack*1000/slackBandMs) * slackBandMs
	}
	secondaryOf := func(si scoredItem) int {
		if si.slack < 0 {
			return 0
		}
		if sloWithKV && kvEnabled {
			if si.binding == "tpot" {
				return 0
			}
			return -si.cached
		}
		return 0
	}

	sort.SliceStable(scored, func(i, j int) bool {
		bi, bj := bandOf(scored[i].slack), bandOf(scored[j].slack)
		if bi != bj {
			return bi < bj
		}
		si, sj := secondaryOf(scored[i]), secondaryOf(scored[j])
		if si != sj {
			return si < sj
		}
		return scored[i].item.reqID < scored[j].item.reqID
	})

	ordered := make([]queueItem, len(scored))
	for i, si := range scored {
		ordered[i] = si.item
	}
	return ordered, kvHitsMap
}

func (s *sloEngineImpl) applyAdmissionThrottle(currentWant int, endpoint string, allItems []queueItem) int {
	inflight := s.batch.getInflight(endpoint)

	var tpotBudget *float64
	limit := len(allItems)
	if limit > 50 {
		limit = 50
	}
	for _, it := range allItems[:limit] {
		e := s.registry.get(it.reqID)
		if e != nil && e.DeadlineTPOTs != nil {
			if tpotBudget == nil || *e.DeadlineTPOTs < *tpotBudget {
				tpotBudget = e.DeadlineTPOTs
			}
		}
	}
	if tpotBudget == nil {
		return currentWant
	}

	const avgAccum = 2048
	maxAdmit := computeMaxSafeAdmit(s.lp, inflight, *tpotBudget, avgAccum)
	if maxAdmit < currentWant {
		return maxAdmit
	}
	return currentWant
}

func (s *sloEngineImpl) onDispatch(endpoint string, chosen []queueItem, kvHits map[string]int) {
	for _, it := range chosen {
		e := s.registry.get(it.reqID)
		if e == nil {
			continue
		}

		if s.cfg.TraceEnabled {
			if tr, ok := it.meta["__trace__"].(map[string]interface{}); ok {
				tr["slo_type"] = strOrNil(e.SLOType)
				tr["slo_slack"] = floatOrNil(e.Slack)
				tr["slo_binding"] = strOrNil(e.BindingConstraint)
				tr["slo_predicted_output_len"] = intOrNil(e.PredictedOutputLen)
			}
		}

		s.registry.updateDispatch(it.reqID, endpoint, e.PredictedTTFT, e.PredictedTPOT, e.PredictedE2E, e.Slack, e.BindingConstraint)

		if e.Slack != nil {
			observeSLOSlack(*e.Slack)
			if *e.Slack < 0 {
				incSLOPredictedMiss()
			}
		}
	}
}

func (s *sloEngineImpl) incrementInflight(endpoint string, n int) {
	s.batch.incrementInflight(endpoint, n)
}

// ---------------- helpers ----------------

func toFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	}
	return 0
}

func strOrNil(p *string) interface{} {
	if p == nil {
		return nil
	}
	return *p
}

func floatOrNil(p *float64) interface{} {
	if p == nil {
		return nil
	}
	return *p
}

func intOrNil(p *int) interface{} {
	if p == nil {
		return nil
	}
	return *p
}
