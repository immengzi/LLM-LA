package gateway

import (
	"encoding/json"
	"log"
	"math"
	"os"
	"sync"
)

// latency_predictor.go ports
// src/services/router_service/router/latency_predictor.py.
//
// Predictors expose:
//   predictTTFT(inputTokens, cachedTokens, batchSize) -> seconds
//   predictTPOT(batchSize, accumulatedLen)            -> seconds
//   predictE2E(inputTokens, cachedTokens, outputTokens, batchSize) -> seconds
//   update(observation)

type latencyObservation struct {
	inputTokens  int
	cachedTokens int
	outputTokens int
	batchSize    int
	actualTTFTs  float64
	actualTPOTs  float64
	actualE2Es   float64
}

type latencyPredictor interface {
	predictTTFT(inputTokens, cachedTokens, batchSize int) float64
	predictTPOT(batchSize, accumulatedLen int) float64
	predictE2E(inputTokens, cachedTokens, outputTokens, batchSize int) float64
	update(obs latencyObservation)
}

// ---------------- coefficient sets ----------------

type computeCoeffs struct {
	Alpha float64 `json:"alpha"`
	Beta  float64 `json:"beta"`
	Gamma float64 `json:"gamma"`
	Delta float64 `json:"delta"`
}

type loadCoeffs struct {
	Alpha float64 `json:"alpha"`
	Beta  float64 `json:"beta"`
	Delta float64 `json:"delta"`
}

type decodeCoeffs struct {
	Alpha float64 `json:"alpha"`
	Beta  float64 `json:"beta"`
	Gamma float64 `json:"gamma"`
	Delta float64 `json:"delta"`
}

type latencyProfile struct {
	compute         computeCoeffs
	load            loadCoeffs
	decode          decodeCoeffs
	blockSizeTokens int
}

func defaultLatencyProfile() latencyProfile {
	return latencyProfile{
		compute:         computeCoeffs{Alpha: 1e-7, Beta: 1e-4, Gamma: 5e-6, Delta: 0.01},
		load:            loadCoeffs{Alpha: 1e-8, Beta: 1e-6, Delta: 0.001},
		decode:          decodeCoeffs{Alpha: 1e-8, Beta: 5e-4, Gamma: 1e-7, Delta: 0.005},
		blockSizeTokens: 16,
	}
}

func latencyProfileFromJSON(path string) (latencyProfile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return latencyProfile{}, err
	}
	var d struct {
		Compute         computeCoeffs `json:"compute"`
		Load            loadCoeffs    `json:"load"`
		Decode          decodeCoeffs  `json:"decode"`
		BlockSizeTokens *int          `json:"block_size_tokens"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return latencyProfile{}, err
	}
	bst := 16
	if d.BlockSizeTokens != nil {
		bst = *d.BlockSizeTokens
	}
	return latencyProfile{
		compute:         d.Compute,
		load:            d.Load,
		decode:          d.Decode,
		blockSizeTokens: bst,
	}, nil
}

// ---------------- LinearLatencyPredictor ----------------

type linearLatencyPredictor struct {
	p           latencyProfile
	chunked     bool
	maxBatched  int
}

func newLinearLatencyPredictor(p latencyProfile, chunkedPrefill bool, maxNumBatchedTokens int) *linearLatencyPredictor {
	chunked := chunkedPrefill && maxNumBatchedTokens > 0
	maxBatched := 0
	if chunked {
		maxBatched = maxNumBatchedTokens
		if maxBatched < 1 {
			maxBatched = 1
		}
	}
	return &linearLatencyPredictor{p: p, chunked: chunked, maxBatched: maxBatched}
}

func (l *linearLatencyPredictor) tCompute(b, lCold int) float64 {
	c := l.p.compute
	return math.Max(0.0, c.Alpha*float64(b)*float64(lCold)+c.Beta*float64(b)+c.Gamma*float64(lCold)+c.Delta)
}

func (l *linearLatencyPredictor) tLoad(b, lCached int) float64 {
	if lCached <= 0 {
		return 0.0
	}
	lo := l.p.load
	return math.Max(0.0, lo.Alpha*float64(b)*float64(lCached)+lo.Beta*float64(lCached)+lo.Delta)
}

func (l *linearLatencyPredictor) tauDecode(b, lA int) float64 {
	d := l.p.decode
	return math.Max(0.0, d.Alpha*float64(b)*float64(lA)+d.Beta*float64(b)+d.Gamma*float64(lA)+d.Delta)
}

func (l *linearLatencyPredictor) predictTTFT(inputTokens, cachedTokens, batchSize int) float64 {
	b := maxInt(1, batchSize)
	lCached := maxInt(0, cachedTokens)
	lCold := maxInt(0, inputTokens-lCached)

	tPrefill := l.tCompute(b, lCold) + l.tLoad(b, lCached)

	if l.chunked && lCold > l.maxBatched {
		nChunks := int(math.Ceil(float64(lCold) / float64(l.maxBatched)))
		avgAccum := inputTokens + 50
		tInterleave := float64(nChunks-1) * l.tauDecode(b, avgAccum)
		tPrefill += tInterleave
	}
	return tPrefill
}

func (l *linearLatencyPredictor) predictTPOT(batchSize, accumulatedLen int) float64 {
	return l.tauDecode(maxInt(1, batchSize), maxInt(1, accumulatedLen))
}

func (l *linearLatencyPredictor) predictE2E(inputTokens, cachedTokens, outputTokens, batchSize int) float64 {
	ttft := l.predictTTFT(inputTokens, cachedTokens, batchSize)
	avgAccum := inputTokens + outputTokens/2
	tpot := l.predictTPOT(batchSize, avgAccum)
	return ttft + float64(maxInt(1, outputTokens))*tpot
}

func (l *linearLatencyPredictor) update(latencyObservation) {}

// ---------------- BayesianLatencyPredictor ----------------

type bayesianLatencyPredictor struct {
	base      *linearLatencyPredictor
	mu        sync.Mutex
	ff        float64
	ttftScale float64
	tpotScale float64
	nObs      int
}

func newBayesianLatencyPredictor(base *linearLatencyPredictor) *bayesianLatencyPredictor {
	return &bayesianLatencyPredictor{
		base:      base,
		ff:        0.995,
		ttftScale: 1.0,
		tpotScale: 1.0,
	}
}

func (b *bayesianLatencyPredictor) predictTTFT(inputTokens, cachedTokens, batchSize int) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.base.predictTTFT(inputTokens, cachedTokens, batchSize) * b.ttftScale
}

func (b *bayesianLatencyPredictor) predictTPOT(batchSize, accumulatedLen int) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.base.predictTPOT(batchSize, accumulatedLen) * b.tpotScale
}

func (b *bayesianLatencyPredictor) predictE2E(inputTokens, cachedTokens, outputTokens, batchSize int) float64 {
	ttft := b.predictTTFT(inputTokens, cachedTokens, batchSize)
	avgAccum := inputTokens + outputTokens/2
	tpot := b.predictTPOT(batchSize, avgAccum)
	return ttft + float64(maxInt(1, outputTokens))*tpot
}

func (b *bayesianLatencyPredictor) update(obs latencyObservation) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nObs++
	if obs.actualTTFTs > 0 {
		predicted := b.base.predictTTFT(obs.inputTokens, obs.cachedTokens, obs.batchSize)
		if predicted > 0 {
			ratio := obs.actualTTFTs / predicted
			b.ttftScale = b.ff*b.ttftScale + (1-b.ff)*ratio
		}
	}
	if obs.actualTPOTs > 0 {
		accum := obs.inputTokens + obs.outputTokens/2
		predicted := b.base.predictTPOT(obs.batchSize, accum)
		if predicted > 0 {
			ratio := obs.actualTPOTs / predicted
			b.tpotScale = b.ff*b.tpotScale + (1-b.ff)*ratio
		}
	}
}

// ---------------- factory ----------------

var (
	latencyPredictorOnce sync.Once
	latencyPredictorInst latencyPredictor
)

// getLatencyPredictor returns the singleton latency predictor based on config.
func getLatencyPredictor(cfg *Config) latencyPredictor {
	latencyPredictorOnce.Do(func() {
		var profile latencyProfile
		path := cfg.LatencyProfilePath
		if path != "" {
			if fi, err := os.Stat(path); err == nil && !fi.IsDir() {
				if p, err := latencyProfileFromJSON(path); err == nil {
					profile = p
				} else {
					log.Printf("[latency_predictor] WARNING: failed to load profile from %s: %v", path, err)
					profile = defaultLatencyProfile()
				}
			} else {
				profile = defaultLatencyProfile()
			}
		} else {
			profile = defaultLatencyProfile()
		}

		base := newLinearLatencyPredictor(profile, cfg.ChunkedPrefillAware, cfg.MaxNumBatchedTokens)

		switch cfg.LatencyPredictor {
		case "bayesian", "hybrid":
			latencyPredictorInst = newBayesianLatencyPredictor(base)
		default:
			latencyPredictorInst = base
		}
	})
	return latencyPredictorInst
}
