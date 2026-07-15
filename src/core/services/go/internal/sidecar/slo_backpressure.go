package sidecar

// SLO-driven dynamic pull backpressure for the Go sidecar (default OFF).
//
// Parity port of src/core/services/sidecar/sidecar/slo_backpressure.py. The
// sidecar pulls up to PullCap() = BatchSize + Prefetch requests per pod. When
// enabled, this watches vLLM's TPOT (time-per-output-token) and shrinks that
// cap while the TPOT SLO is violated, then slowly restores it once TPOT is back
// inside SLO (AIMD-style control).
//
// Three decoupled pieces (each independently testable):
//   1. NextPullCap        — a pure function (no I/O, no clock, no goroutines).
//   2. PullCapController   — thin stateful wrapper owning cap / lastAdjust.
//   3. vllmTpotScraper + SloBackpressureMonitor — scrape vLLM /metrics, keep a
//      sliding TPOT window, and drive the controller on a timer.
//
// When SLODynamicPullEnabled is false, main.go never constructs a monitor and
// never sets a cap provider, so the pull path keeps the static cap and the code
// path is unchanged with zero extra cost.

import (
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/common/expfmt"
)

// ControllerParams is the decoupled parameter set the pure controller needs.
type ControllerParams struct {
	SLOTargetS     float64
	MinPull        int
	MaxPull        int
	DecreaseMode   string // "additive" | "multiplicative"
	DecreaseStep   int
	DecreaseFactor float64
	RecoverStep    int
	CooldownS      float64
}

// ControllerParamsFromConfig builds params, resolving MaxPull (<=0 means "use
// the static pull cap") and keeping the [MinPull, MaxPull] window well-formed.
func ControllerParamsFromConfig(cfg *Config, defaultMaxPull int) ControllerParams {
	maxPull := cfg.SLOMaxPull
	if maxPull <= 0 {
		maxPull = defaultMaxPull
	}
	minPull := cfg.SLOMinPull
	if minPull < 1 {
		minPull = 1
	}
	if maxPull < minPull {
		maxPull = minPull
	}
	mode := cfg.SLODecreaseMode
	if mode != "additive" && mode != "multiplicative" {
		mode = "additive"
	}
	step := cfg.SLODecreaseStep
	if step < 1 {
		step = 1
	}
	recover := cfg.SLORecoverStep
	if recover < 1 {
		recover = 1
	}
	cooldown := cfg.SLOCooldownS
	if cooldown < 0 {
		cooldown = 0
	}
	return ControllerParams{
		SLOTargetS:     cfg.SLOTpotSLOS,
		MinPull:        minPull,
		MaxPull:        maxPull,
		DecreaseMode:   mode,
		DecreaseStep:   step,
		DecreaseFactor: cfg.SLODecreaseFactor,
		RecoverStep:    recover,
		CooldownS:      cooldown,
	}
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// NextPullCap decides the pull cap for this tick. Pure: no side effects.
//
// Priority order (first match wins):
//
//	no_data -> cooldown -> violation(decrease) -> in-SLO(recover)
//
// observedTpot<0 signals "no data" (window not ready / scrape failed). The
// returned cap is always clamped to [MinPull, MaxPull]. The caller advances
// lastAdjust only when the returned cap differs from currentCap.
func NextPullCap(observedTpot float64, hasTpot bool, currentCap int, now, lastAdjust float64, p ControllerParams) (int, string) {
	// Re-clamp the incoming cap so a config change (e.g. lowered MaxPull) is
	// respected even while holding.
	currentCap = clampInt(currentCap, p.MinPull, p.MaxPull)

	if !hasTpot {
		return currentCap, "no_data"
	}

	if (now - lastAdjust) < p.CooldownS {
		return currentCap, "hold_cooldown"
	}

	if observedTpot > p.SLOTargetS {
		var next int
		if p.DecreaseMode == "multiplicative" {
			next = int(float64(currentCap) * p.DecreaseFactor)
		} else {
			next = currentCap - p.DecreaseStep
		}
		if next < p.MinPull {
			next = p.MinPull
		}
		if next != currentCap {
			return next, "decrease"
		}
		return currentCap, "hold_at_min"
	}

	// Inside SLO -> recover (additive slow rise).
	next := currentCap + p.RecoverStep
	if next > p.MaxPull {
		next = p.MaxPull
	}
	if next != currentCap {
		return next, "recover"
	}
	return currentCap, "hold_at_max"
}

// PullCapController owns the mutable cap state and delegates to NextPullCap.
type PullCapController struct {
	mu         sync.Mutex
	params     ControllerParams
	cap        int
	lastAdjust float64
}

func NewPullCapController(params ControllerParams, initialCap int) *PullCapController {
	return &PullCapController{
		params: params,
		cap:    clampInt(initialCap, params.MinPull, params.MaxPull),
		// -inf so the first evaluation is never gated by cooldown, regardless of
		// the absolute value of the clock passed in.
		lastAdjust: math.Inf(-1),
	}
}

func (c *PullCapController) Cap() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cap
}

// Update advances one control step. Returns (oldCap, newCap, reason).
func (c *PullCapController) Update(observedTpot float64, hasTpot bool, now float64) (int, int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.cap
	next, reason := NextPullCap(observedTpot, hasTpot, old, now, c.lastAdjust, c.params)
	if next != old {
		c.cap = next
		c.lastAdjust = now
	}
	return old, next, reason
}

// tpotSampler returns the interval-average TPOT (seconds). ok=false means no
// usable sample this tick (idle / scrape failure / need a baseline).
type tpotSampler interface {
	Sample() (float64, bool)
}

// vllmTpotScraper scrapes vLLM /metrics and returns delta(sum)/delta(count) of
// the TPOT histogram since the last scrape.
type vllmTpotScraper struct {
	url        string
	metricName string
	client     *http.Client

	prevSum   float64
	prevCount float64
	havePrev  bool
}

func newVLLMTpotScraper(cfg *Config) *vllmTpotScraper {
	return &vllmTpotScraper{
		url:        strings.TrimRight(cfg.VLLMURL, "/") + "/metrics",
		metricName: cfg.SLOTpotMetric,
		client:     &http.Client{Timeout: time.Duration(cfg.SLOScrapeTimeoutS * float64(time.Second))},
	}
}

func (s *vllmTpotScraper) Sample() (float64, bool) {
	sum, count, ok := s.scrapeSumCount()
	if !ok {
		return 0, false
	}
	prevSum, prevCount, havePrev := s.prevSum, s.prevCount, s.havePrev
	s.prevSum, s.prevCount, s.havePrev = sum, count, true
	if !havePrev {
		return 0, false // need a baseline
	}
	dSum := sum - prevSum
	dCount := count - prevCount
	if dCount <= 0 || dSum < 0 {
		// No new tokens (idle) or a counter reset (vLLM restart).
		return 0, false
	}
	return dSum / dCount, true
}

// scrapeSumCount fetches /metrics and aggregates the histogram _sum and _count
// across all label sets for the configured metric name.
func (s *vllmTpotScraper) scrapeSumCount() (float64, float64, bool) {
	resp, err := s.client.Get(s.url)
	if err != nil {
		return 0, 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, false
	}
	return parseTpotSumCount(resp.Body, s.metricName)
}

// parseTpotSumCount parses Prometheus text and aggregates the histogram _sum and
// _count across all label sets for the given metric name.
func parseTpotSumCount(r io.Reader, metricName string) (float64, float64, bool) {
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return 0, 0, false
	}
	fam, ok := families[metricName]
	if !ok {
		return 0, 0, false
	}
	var totalSum, totalCount float64
	seen := false
	for _, m := range fam.GetMetric() {
		if h := m.GetHistogram(); h != nil {
			totalSum += h.GetSampleSum()
			totalCount += float64(h.GetSampleCount())
			seen = true
		}
	}
	if !seen {
		return 0, 0, false
	}
	return totalSum, totalCount, true
}

// tpotWindow is a fixed-size sliding window over interval-average TPOT samples.
type tpotWindow struct {
	buf []float64
	max int
	agg string
}

func newTpotWindow(maxSamples int, agg string) *tpotWindow {
	if maxSamples < 1 {
		maxSamples = 1
	}
	if agg != "mean" && agg != "p90" {
		agg = "mean"
	}
	return &tpotWindow{max: maxSamples, agg: agg}
}

func (w *tpotWindow) Add(v float64, ok bool) {
	if !ok {
		return
	}
	w.buf = append(w.buf, v)
	if len(w.buf) > w.max {
		w.buf = w.buf[len(w.buf)-w.max:]
	}
}

func (w *tpotWindow) Value() (float64, bool) {
	if len(w.buf) == 0 {
		return 0, false
	}
	if w.agg == "p90" {
		return percentile(w.buf, 0.90), true
	}
	var sum float64
	for _, v := range w.buf {
		sum += v
	}
	return sum / float64(len(w.buf)), true
}

func percentile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	s := make([]float64, len(values))
	copy(s, values)
	sort.Float64s(s)
	if len(s) == 1 {
		return s[0]
	}
	idx := q * float64(len(s)-1)
	lo := int(idx)
	hi := lo + 1
	if hi > len(s)-1 {
		hi = len(s) - 1
	}
	frac := idx - float64(lo)
	return s[lo] + (s[hi]-s[lo])*frac
}

// SloBackpressureMonitor scrapes TPOT, updates the window, drives the
// controller, and publishes the current cap via GetCap for the pull path.
type SloBackpressureMonitor struct {
	cfg        *Config
	endpointID string
	params     ControllerParams
	controller *PullCapController
	window     *tpotWindow
	scraper    tpotSampler
	emit       bool

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewSloBackpressureMonitor builds a monitor using the real vLLM scraper.
func NewSloBackpressureMonitor(cfg *Config, defaultCap int, endpointID string) *SloBackpressureMonitor {
	return newSloBackpressureMonitorWithSampler(cfg, defaultCap, endpointID, newVLLMTpotScraper(cfg), true)
}

// newSloBackpressureMonitorWithSampler is the injectable constructor used by tests.
func newSloBackpressureMonitorWithSampler(cfg *Config, defaultCap int, endpointID string, sampler tpotSampler, emit bool) *SloBackpressureMonitor {
	params := ControllerParamsFromConfig(cfg, defaultCap)
	if emit {
		// Register the SLO gauges lazily, exactly when the feature is in use, so
		// the disabled path's /metrics output is unchanged.
		initSLOMetrics()
	}
	return &SloBackpressureMonitor{
		cfg:        cfg,
		endpointID: endpointID,
		params:     params,
		// Start optimistic at MaxPull so the enabled path matches the static cap
		// until the first violation is observed.
		controller: NewPullCapController(params, params.MaxPull),
		window:     newTpotWindow(cfg.SLOWindowSamples, cfg.SLOWindowAgg),
		scraper:    sampler,
		emit:       emit,
		stopCh:     make(chan struct{}),
	}
}

// GetCap returns the current dynamic pull cap (wired into RouterPullWorker).
func (m *SloBackpressureMonitor) GetCap() int {
	return m.controller.Cap()
}

// Params exposes the resolved controller params (used by tests).
func (m *SloBackpressureMonitor) Params() ControllerParams {
	return m.params
}

func (m *SloBackpressureMonitor) Start() {
	log.Printf("[sidecar][slo] dynamic pull backpressure ENABLED "+
		"(slo_tpot=%.4gs, min_pull=%d, max_pull=%d, eval_interval=%gs, "+
		"window=%dx%s, decrease=%s, cooldown=%gs)",
		m.params.SLOTargetS, m.params.MinPull, m.params.MaxPull,
		m.cfg.SLOEvalIntervalS, m.cfg.SLOWindowSamples, m.cfg.SLOWindowAgg,
		m.params.DecreaseMode, m.params.CooldownS)
	go m.loop()
}

func (m *SloBackpressureMonitor) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
}

// tick runs one evaluation step (also used by tests). Returns
// (windowedTpot, hasTpot, oldCap, newCap, reason).
func (m *SloBackpressureMonitor) tick(now float64) (float64, bool, int, int, string) {
	sample, ok := m.scraper.Sample()
	m.window.Add(sample, ok)
	windowed, hasTpot := m.window.Value()
	old, next, reason := m.controller.Update(windowed, hasTpot, now)
	if m.emit {
		setSLOBackpressureState(m.endpointID, next, windowed, hasTpot, m.params.SLOTargetS)
	}
	if next != old {
		wt := "n/a"
		if hasTpot {
			wt = fmt.Sprintf("%.4fs", windowed)
		}
		log.Printf("[sidecar][slo] adjust endpoint=%s tpot=%s slo=%gs old_cap=%d new_cap=%d reason=%s",
			m.endpointID, wt, m.params.SLOTargetS, old, next, reason)
	}
	return windowed, hasTpot, old, next, reason
}

func (m *SloBackpressureMonitor) loop() {
	interval := m.cfg.SLOEvalIntervalS
	if interval < 0.1 {
		interval = 0.1
	}
	d := time.Duration(interval * float64(time.Second))
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.tick(float64(time.Now().UnixNano()) / 1e9)
		}
	}
}
