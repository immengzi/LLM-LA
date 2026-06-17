package gateway

import (
	"math"
	"sync"
)

// slo_scoring.go ports src/services/router_service/router/slo_scoring.py:
// batch-size / queue-wait estimation and per-(request,endpoint) slack.

const sloInf = math.MaxFloat64

// ---------------- BatchSizeEstimator ----------------

type batchSizeEstimator struct {
	mode     string
	fixed    int
	mu       sync.Mutex
	inflight map[string]int
	reported map[string]int
}

func newBatchSizeEstimator(mode string, fixedValue int) *batchSizeEstimator {
	if fixedValue < 1 {
		fixedValue = 1
	}
	return &batchSizeEstimator{
		mode:     mode,
		fixed:    fixedValue,
		inflight: make(map[string]int),
		reported: make(map[string]int),
	}
}

func (b *batchSizeEstimator) estimate(endpoint string) int {
	switch b.mode {
	case "inflight":
		b.mu.Lock()
		defer b.mu.Unlock()
		if v, ok := b.inflight[endpoint]; ok {
			return maxInt(1, v)
		}
		return maxInt(1, b.fixed)
	case "reported":
		b.mu.Lock()
		defer b.mu.Unlock()
		if v, ok := b.reported[endpoint]; ok {
			return maxInt(1, v)
		}
		return maxInt(1, b.fixed)
	}
	return b.fixed
}

func (b *batchSizeEstimator) incrementInflight(endpoint string, n int) {
	b.mu.Lock()
	b.inflight[endpoint] += n
	b.mu.Unlock()
}

func (b *batchSizeEstimator) decrementInflight(endpoint string, n int) {
	b.mu.Lock()
	cur := b.inflight[endpoint]
	b.inflight[endpoint] = maxInt(0, cur-n)
	b.mu.Unlock()
}

func (b *batchSizeEstimator) getInflight(endpoint string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inflight[endpoint]
}

func (b *batchSizeEstimator) setReported(endpoint string, batchSize int) {
	b.mu.Lock()
	b.reported[endpoint] = maxInt(1, batchSize)
	b.mu.Unlock()
}

// ---------------- QueueWaitEstimator ----------------

type queueWaitEstimator struct {
	mode            string
	mu              sync.Mutex
	interCompletion map[string]float64
	lastCompletion  map[string]float64
}

func newQueueWaitEstimator(mode string) *queueWaitEstimator {
	return &queueWaitEstimator{
		mode:            mode,
		interCompletion: make(map[string]float64),
		lastCompletion:  make(map[string]float64),
	}
}

func (q *queueWaitEstimator) estimate(endpoint string, queuePosition int) float64 {
	if q.mode == "none" {
		return 0.0
	}
	if q.mode == "simple" {
		q.mu.Lock()
		avg := q.interCompletion[endpoint]
		q.mu.Unlock()
		return math.Max(0.0, float64(queuePosition)*avg)
	}
	return 0.0
}

func (q *queueWaitEstimator) recordCompletion(endpoint string) {
	now := nowS()
	q.mu.Lock()
	defer q.mu.Unlock()
	if last, ok := q.lastCompletion[endpoint]; ok {
		ict := now - last
		oldAvg, has := q.interCompletion[endpoint]
		if !has {
			oldAvg = ict
		}
		q.interCompletion[endpoint] = 0.9*oldAvg + 0.1*ict
	}
	q.lastCompletion[endpoint] = now
}

// ---------------- computeSlack ----------------

// computeSlack mirrors slo_scoring.compute_slack. Returns (slack, binding).
// binding is "" for None. block_size_tokens default is 16.
func computeSlack(
	e *sloEntry,
	lp latencyPredictor,
	batchSize int,
	cachedTokens int,
	queueWaitS float64,
	blockSizeTokens int,
) (float64, string) {
	if e.SLOType == nil {
		return sloInf, ""
	}
	if blockSizeTokens <= 0 {
		blockSizeTokens = 16
	}

	now := nowS()
	inputTokens := maxInt(1, e.InputTokens)
	lCached := maxInt(0, cachedTokens) * blockSizeTokens

	predictedOutput := 256
	if e.PredictedOutputLen != nil && *e.PredictedOutputLen != 0 {
		predictedOutput = *e.PredictedOutputLen
	} else if e.OutputLenHint != nil && *e.OutputLenHint != 0 {
		predictedOutput = *e.OutputLenHint
	}

	switch *e.SLOType {
	case "ttft":
		if e.DeadlineTTFT == nil {
			return sloInf, ""
		}
		predTTFT := lp.predictTTFT(inputTokens, lCached, batchSize)
		predCompletion := now + queueWaitS + predTTFT
		return *e.DeadlineTTFT - predCompletion, "ttft"

	case "tpot":
		if e.DeadlineTPOTs == nil {
			return sloInf, ""
		}
		avgAccum := inputTokens + predictedOutput/2
		predTPOT := lp.predictTPOT(batchSize, avgAccum)
		return *e.DeadlineTPOTs - predTPOT, "tpot"

	case "ttft+tpot":
		ttftSlack := sloInf
		tpotSlack := sloInf
		if e.DeadlineTTFT != nil {
			predTTFT := lp.predictTTFT(inputTokens, lCached, batchSize)
			predCompletion := now + queueWaitS + predTTFT
			ttftSlack = *e.DeadlineTTFT - predCompletion
		}
		if e.DeadlineTPOTs != nil {
			avgAccum := inputTokens + predictedOutput/2
			predTPOT := lp.predictTPOT(batchSize, avgAccum)
			tpotSlack = *e.DeadlineTPOTs - predTPOT
		}
		slack := math.Min(ttftSlack, tpotSlack)
		binding := "tpot"
		if ttftSlack <= tpotSlack {
			binding = "ttft"
		}
		return slack, binding

	case "e2e":
		if e.DeadlineE2E == nil {
			return sloInf, ""
		}
		predE2E := lp.predictE2E(inputTokens, lCached, predictedOutput, batchSize)
		predCompletion := now + queueWaitS + predE2E
		slack := *e.DeadlineE2E - predCompletion

		predTTFT := lp.predictTTFT(inputTokens, lCached, batchSize)
		avgAccum := inputTokens + predictedOutput/2
		predDecodeTotal := float64(predictedOutput) * lp.predictTPOT(batchSize, avgAccum)
		binding := "tpot"
		if predTTFT >= predDecodeTotal {
			binding = "ttft"
		}
		return slack, binding
	}

	return sloInf, ""
}
