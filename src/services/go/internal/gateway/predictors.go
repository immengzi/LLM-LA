package gateway

import (
	"math"
	"sort"
	"sync"
)

// predictors.go ports src/services/router_service/router/predictors.py:
// output-length predictors behind a common interface.
//
//   - predict()      is called at enqueue time for every request.
//   - update()       is called from /result with the actual output token count.
//   - predictOutTokens() is the legacy accessor used by length-aware selection.

type OutputLengthPredictor interface {
	Name() string
	Predict(prompt string, inputTokens int, taskType, reqID string) int
	Update(reqID string, actualOutputTokens int, taskType string, inputTokens int)
	PredictOutTokens(prompt, reqID string) int
}

// ---------------- SimpleLengthPredictor (legacy / "char-len") ----------------

type SimpleLengthPredictor struct{}

func (p *SimpleLengthPredictor) Name() string { return "char-len" }

func (p *SimpleLengthPredictor) Predict(prompt string, inputTokens int, taskType, reqID string) int {
	if n := len(prompt) / 2; n > 1 {
		return n
	}
	return 1
}

func (p *SimpleLengthPredictor) Update(string, int, string, int) {}

func (p *SimpleLengthPredictor) PredictOutTokens(prompt, reqID string) int {
	return p.Predict(prompt, 0, "", "")
}

// ---------------- HintOnlyPredictor ----------------

type HintOnlyPredictor struct {
	defaultTokens int
}

func newHintOnlyPredictor(defaultTokens int) *HintOnlyPredictor {
	if defaultTokens < 1 {
		defaultTokens = 1
	}
	return &HintOnlyPredictor{defaultTokens: defaultTokens}
}

func (p *HintOnlyPredictor) Name() string { return "hint_only" }

func (p *HintOnlyPredictor) Predict(prompt string, inputTokens int, taskType, reqID string) int {
	return p.defaultTokens
}

func (p *HintOnlyPredictor) Update(string, int, string, int) {}

func (p *HintOnlyPredictor) PredictOutTokens(prompt, reqID string) int {
	// HintOnly has no predict_out_tokens in Python; len_select falls back to
	// the predictor's predict() via SimpleLengthPredictor wrapping. Python's
	// get_length_predictor returns the predictor only if it has
	// predict_out_tokens; HintOnly does NOT, so it returns SimpleLengthPredictor.
	return (&SimpleLengthPredictor{}).PredictOutTokens(prompt, reqID)
}

// ---------------- running stats ----------------

type runningStats struct {
	maxWindow int
	samples   []int
	sum       float64
	sumSq     float64
}

func newRunningStats(maxWindow int) *runningStats {
	if maxWindow < 10 {
		maxWindow = 10
	}
	return &runningStats{maxWindow: maxWindow}
}

func (s *runningStats) add(val int) {
	if len(s.samples) == s.maxWindow {
		old := s.samples[0]
		s.samples = s.samples[1:]
		s.sum -= float64(old)
		s.sumSq -= float64(old) * float64(old)
	}
	s.samples = append(s.samples, val)
	s.sum += float64(val)
	s.sumSq += float64(val) * float64(val)
}

func (s *runningStats) count() int { return len(s.samples) }

func (s *runningStats) median() float64 {
	n := len(s.samples)
	if n == 0 {
		return 0.0
	}
	cp := make([]int, n)
	copy(cp, s.samples)
	sort.Ints(cp)
	if n%2 == 1 {
		return float64(cp[n/2])
	}
	return float64(cp[n/2-1]+cp[n/2]) / 2.0
}

// ---------------- TaskTypeDistributionPredictor ----------------

type TaskTypeDistributionPredictor struct {
	mu         sync.Mutex
	minSamples int
	maxWindow  int
	defaultTok int
	perType    map[string]*runningStats
	global     *runningStats
}

func newDistributionPredictor(defaultTokens int) *TaskTypeDistributionPredictor {
	if defaultTokens < 1 {
		defaultTokens = 1
	}
	return &TaskTypeDistributionPredictor{
		minSamples: 5,
		maxWindow:  500,
		defaultTok: defaultTokens,
		perType:    make(map[string]*runningStats),
		global:     newRunningStats(500),
	}
}

func (p *TaskTypeDistributionPredictor) Name() string { return "distribution" }

func (p *TaskTypeDistributionPredictor) Predict(prompt string, inputTokens int, taskType, reqID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if taskType != "" {
		if st := p.perType[taskType]; st != nil && st.count() >= p.minSamples {
			return maxInt(1, int(st.median()))
		}
	}
	if p.global.count() >= p.minSamples {
		return maxInt(1, int(p.global.median()))
	}
	return p.defaultTok
}

func (p *TaskTypeDistributionPredictor) Update(reqID string, actual int, taskType string, inputTokens int) {
	if actual <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.global.add(actual)
	if taskType != "" {
		st := p.perType[taskType]
		if st == nil {
			st = newRunningStats(p.maxWindow)
			p.perType[taskType] = st
		}
		st.add(actual)
	}
}

func (p *TaskTypeDistributionPredictor) PredictOutTokens(prompt, reqID string) int {
	return p.Predict(prompt, 0, "", "")
}

// ---------------- InputLengthRegressionPredictor ----------------

type xy struct{ x, y int }

type InputLengthRegressionPredictor struct {
	mu         sync.Mutex
	minSamples int
	defaultTok int
	maxWindow  int
	perType    map[string][]xy
	global     []xy
}

func newRegressionPredictor(defaultTokens int) *InputLengthRegressionPredictor {
	if defaultTokens < 1 {
		defaultTokens = 1
	}
	return &InputLengthRegressionPredictor{
		minSamples: 20,
		defaultTok: defaultTokens,
		maxWindow:  500,
		perType:    make(map[string][]xy),
	}
}

func (p *InputLengthRegressionPredictor) Name() string { return "regression" }

func (p *InputLengthRegressionPredictor) fit(samples []xy) (float64, float64, bool) {
	n := len(samples)
	if n < p.minSamples {
		return 0, 0, false
	}
	var sx, sy, sxx, sxy float64
	for _, s := range samples {
		sx += float64(s.x)
		sy += float64(s.y)
		sxx += float64(s.x) * float64(s.x)
		sxy += float64(s.x) * float64(s.y)
	}
	denom := float64(n)*sxx - sx*sx
	if math.Abs(denom) < 1e-12 {
		return 0, 0, false
	}
	a := (float64(n)*sxy - sx*sy) / denom
	b := (sy - a*sx) / float64(n)
	return a, b, true
}

func (p *InputLengthRegressionPredictor) Predict(prompt string, inputTokens int, taskType, reqID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if taskType != "" {
		if samples := p.perType[taskType]; samples != nil {
			if a, b, ok := p.fit(samples); ok {
				return maxInt(1, int(a*float64(inputTokens)+b))
			}
		}
	}
	if a, b, ok := p.fit(p.global); ok {
		return maxInt(1, int(a*float64(inputTokens)+b))
	}
	return p.defaultTok
}

func (p *InputLengthRegressionPredictor) Update(reqID string, actual int, taskType string, inputTokens int) {
	if actual <= 0 || inputTokens <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.global = appendCapped(p.global, xy{inputTokens, actual}, p.maxWindow)
	if taskType != "" {
		p.perType[taskType] = appendCapped(p.perType[taskType], xy{inputTokens, actual}, p.maxWindow)
	}
}

func (p *InputLengthRegressionPredictor) PredictOutTokens(prompt, reqID string) int {
	return p.Predict(prompt, maxInt(1, len(prompt)/4), "", "")
}

func appendCapped(s []xy, v xy, max int) []xy {
	s = append(s, v)
	if len(s) > max {
		s = s[len(s)-max:]
	}
	return s
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ---------------- factory ----------------

var (
	predictorOnce sync.Once
	predictorInst OutputLengthPredictor
)

// getOutputLengthPredictor returns the singleton output-length predictor based
// on cfg.OUTPUT_LEN_PREDICTOR.
func getOutputLengthPredictor(cfg *Config) OutputLengthPredictor {
	predictorOnce.Do(func() {
		switch cfg.OutputLenPredictor {
		case "distribution":
			predictorInst = newDistributionPredictor(cfg.DefaultMaxTokens)
		case "regression":
			predictorInst = newRegressionPredictor(cfg.DefaultMaxTokens)
		case "hint_only":
			predictorInst = newHintOnlyPredictor(cfg.DefaultMaxTokens)
		default:
			predictorInst = &SimpleLengthPredictor{}
		}
	})
	return predictorInst
}

// getLengthPredictor mirrors get_length_predictor: returns something compatible
// with the length-aware selector's PredictOutTokens. HintOnly has no
// predict_out_tokens in Python, so it falls back to SimpleLengthPredictor.
func getLengthPredictor(cfg *Config) OutputLengthPredictor {
	pred := getOutputLengthPredictor(cfg)
	if _, ok := pred.(*HintOnlyPredictor); ok {
		return &SimpleLengthPredictor{}
	}
	return pred
}
