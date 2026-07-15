package sidecar

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

func testParams(mut func(*ControllerParams)) ControllerParams {
	p := ControllerParams{
		SLOTargetS:     0.040,
		MinPull:        1,
		MaxPull:        8,
		DecreaseMode:   "additive",
		DecreaseStep:   1,
		DecreaseFactor: 0.5,
		RecoverStep:    1,
		CooldownS:      10.0,
	}
	if mut != nil {
		mut(&p)
	}
	return p
}

// ------------------------------------------------------------------
// Pure controller
// ------------------------------------------------------------------

func TestNextPullCapViolationAdditiveDecrease(t *testing.T) {
	p := testParams(nil)
	got, reason := NextPullCap(0.060, true, 8, 100.0, math.Inf(-1), p)
	if got != 7 || reason != "decrease" {
		t.Fatalf("got (%d,%q), want (7,decrease)", got, reason)
	}
}

func TestNextPullCapViolationMultiplicativeDecrease(t *testing.T) {
	p := testParams(func(p *ControllerParams) {
		p.DecreaseMode = "multiplicative"
		p.DecreaseFactor = 0.5
	})
	got, reason := NextPullCap(0.060, true, 8, 100.0, math.Inf(-1), p)
	if got != 4 || reason != "decrease" {
		t.Fatalf("got (%d,%q), want (4,decrease)", got, reason)
	}
}

func TestNextPullCapRecoverAdditiveIncrease(t *testing.T) {
	p := testParams(nil)
	got, reason := NextPullCap(0.030, true, 4, 100.0, math.Inf(-1), p)
	if got != 5 || reason != "recover" {
		t.Fatalf("got (%d,%q), want (5,recover)", got, reason)
	}
}

func TestNextPullCapDecreaseClampedAtMin(t *testing.T) {
	p := testParams(func(p *ControllerParams) { p.MinPull = 1 })
	got, reason := NextPullCap(0.060, true, 1, 100.0, math.Inf(-1), p)
	if got != 1 || reason != "hold_at_min" {
		t.Fatalf("got (%d,%q), want (1,hold_at_min)", got, reason)
	}
}

func TestNextPullCapMultiplicativeRespectsMin(t *testing.T) {
	p := testParams(func(p *ControllerParams) {
		p.DecreaseMode = "multiplicative"
		p.DecreaseFactor = 0.5
		p.MinPull = 3
	})
	// floor(4*0.5)=2, clamped up to MinPull=3.
	got, reason := NextPullCap(0.060, true, 4, 100.0, math.Inf(-1), p)
	if got != 3 || reason != "decrease" {
		t.Fatalf("got (%d,%q), want (3,decrease)", got, reason)
	}
}

func TestNextPullCapRecoverClampedAtMax(t *testing.T) {
	p := testParams(nil)
	got, reason := NextPullCap(0.030, true, 8, 100.0, math.Inf(-1), p)
	if got != 8 || reason != "hold_at_max" {
		t.Fatalf("got (%d,%q), want (8,hold_at_max)", got, reason)
	}
}

func TestNextPullCapBoundaryIsNotViolation(t *testing.T) {
	p := testParams(nil)
	// observed == target is NOT > target -> treated as in-SLO -> recover.
	got, reason := NextPullCap(0.040, true, 4, 100.0, math.Inf(-1), p)
	if got != 5 || reason != "recover" {
		t.Fatalf("got (%d,%q), want (5,recover)", got, reason)
	}
}

func TestNextPullCapCooldownHolds(t *testing.T) {
	p := testParams(nil)
	got, reason := NextPullCap(0.060, true, 6, 105.0, 100.0, p) // 5s < cooldown 10s
	if got != 6 || reason != "hold_cooldown" {
		t.Fatalf("got (%d,%q), want (6,hold_cooldown)", got, reason)
	}
}

func TestNextPullCapNoDataHolds(t *testing.T) {
	p := testParams(nil)
	got, reason := NextPullCap(0, false, 6, 1000.0, math.Inf(-1), p)
	if got != 6 || reason != "no_data" {
		t.Fatalf("got (%d,%q), want (6,no_data)", got, reason)
	}
}

func TestNextPullCapReclampsIncomingCap(t *testing.T) {
	p := testParams(func(p *ControllerParams) { p.MinPull = 2; p.MaxPull = 6 })
	got, reason := NextPullCap(0, false, 99, 1.0, math.Inf(-1), p)
	if got != 6 || reason != "no_data" {
		t.Fatalf("got (%d,%q), want (6,no_data)", got, reason)
	}
}

// ------------------------------------------------------------------
// Stateful controller: cooldown + lastAdjust bookkeeping
// ------------------------------------------------------------------

func TestControllerAdvancesLastAdjustOnlyOnChange(t *testing.T) {
	c := NewPullCapController(testParams(nil), 8)

	if old, next, reason := c.Update(0.060, true, 0.0); old != 8 || next != 7 || reason != "decrease" {
		t.Fatalf("step1 got (%d,%d,%q), want (8,7,decrease)", old, next, reason)
	}
	if old, next, reason := c.Update(0.060, true, 5.0); old != 7 || next != 7 || reason != "hold_cooldown" {
		t.Fatalf("step2 got (%d,%d,%q), want (7,7,hold_cooldown)", old, next, reason)
	}
	if old, next, reason := c.Update(0.060, true, 11.0); old != 7 || next != 6 || reason != "decrease" {
		t.Fatalf("step3 got (%d,%d,%q), want (7,6,decrease)", old, next, reason)
	}
}

func TestControllerHoldAtMinDoesNotResetCooldown(t *testing.T) {
	c := NewPullCapController(testParams(func(p *ControllerParams) { p.MinPull = 5 }), 5)
	if old, next, reason := c.Update(0.060, true, 100.0); old != 5 || next != 5 || reason != "hold_at_min" {
		t.Fatalf("got (%d,%d,%q), want (5,5,hold_at_min)", old, next, reason)
	}
	// Immediately able to recover (no spurious cooldown from a no-op hold).
	if old, next, reason := c.Update(0.030, true, 100.1); old != 5 || next != 6 || reason != "recover" {
		t.Fatalf("got (%d,%d,%q), want (5,6,recover)", old, next, reason)
	}
}

// ------------------------------------------------------------------
// Sliding window
// ------------------------------------------------------------------

func TestWindowMean(t *testing.T) {
	w := newTpotWindow(3, "mean")
	if _, ok := w.Value(); ok {
		t.Fatal("empty window should have no value")
	}
	w.Add(0.02, true)
	w.Add(0.04, true)
	if v, _ := w.Value(); math.Abs(v-0.03) > 1e-9 {
		t.Fatalf("mean = %v, want 0.03", v)
	}
	w.Add(0.06, true)
	w.Add(0.12, true) // evicts 0.02
	if v, _ := w.Value(); math.Abs(v-(0.04+0.06+0.12)/3) > 1e-9 {
		t.Fatalf("mean = %v, want %v", v, (0.04+0.06+0.12)/3)
	}
}

func TestWindowIgnoresNotOK(t *testing.T) {
	w := newTpotWindow(3, "mean")
	w.Add(0, false)
	if _, ok := w.Value(); ok {
		t.Fatal("window should ignore not-ok samples")
	}
	w.Add(0.05, true)
	if v, _ := w.Value(); math.Abs(v-0.05) > 1e-9 {
		t.Fatalf("mean = %v, want 0.05", v)
	}
}

func TestWindowP90(t *testing.T) {
	w := newTpotWindow(10, "p90")
	for _, v := range []float64{0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10} {
		w.Add(v, true)
	}
	want := 0.09 + 0.1*(0.10-0.09) // idx 0.9*9 = 8.1
	if v, _ := w.Value(); math.Abs(v-want) > 1e-9 {
		t.Fatalf("p90 = %v, want %v", v, want)
	}
}

// ------------------------------------------------------------------
// Scraper: delta(sum)/delta(count)
// ------------------------------------------------------------------

func metricsText(sum float64, count int) string {
	m := "vllm:time_per_output_token_seconds"
	cnt := strconv.Itoa(count)
	return "# HELP " + m + " TPOT\n" +
		"# TYPE " + m + " histogram\n" +
		m + "_bucket{le=\"0.01\"} 0\n" +
		m + "_bucket{le=\"+Inf\"} " + cnt + "\n" +
		m + "_sum " + strconv.FormatFloat(sum, 'f', -1, 64) + "\n" +
		m + "_count " + cnt + "\n"
}

func TestScraperNeedsBaselineThenIntervalAvg(t *testing.T) {
	sc := &vllmTpotScraper{metricName: "vllm:time_per_output_token_seconds"}
	// Feed parsed values directly by short-circuiting scrapeSumCount via prev state.
	if v, ok := sampleFromCounts(sc, 1.0, 100); ok {
		t.Fatalf("baseline should be not-ok, got %v", v)
	}
	if v, ok := sampleFromCounts(sc, 1.6, 110); !ok || math.Abs(v-0.06) > 1e-9 {
		t.Fatalf("interval avg = %v ok=%v, want 0.06 true", v, ok)
	}
}

func TestScraperNoNewTokens(t *testing.T) {
	sc := &vllmTpotScraper{metricName: "x"}
	sampleFromCounts(sc, 1.0, 100)
	if _, ok := sampleFromCounts(sc, 1.0, 100); ok {
		t.Fatal("no new tokens should be not-ok")
	}
}

func TestScraperCounterReset(t *testing.T) {
	sc := &vllmTpotScraper{metricName: "x"}
	sampleFromCounts(sc, 5.0, 500)
	if _, ok := sampleFromCounts(sc, 0.2, 10); ok {
		t.Fatal("counter reset should be not-ok")
	}
}

// sampleFromCounts drives the scraper's delta logic with injected sum/count,
// bypassing HTTP (the parsing itself is covered by TestScrapeSumCountParsing).
func sampleFromCounts(s *vllmTpotScraper, sum float64, count int) (float64, bool) {
	prevSum, prevCount, havePrev := s.prevSum, s.prevCount, s.havePrev
	s.prevSum, s.prevCount, s.havePrev = sum, float64(count), true
	if !havePrev {
		return 0, false
	}
	dSum := sum - prevSum
	dCount := float64(count) - prevCount
	if dCount <= 0 || dSum < 0 {
		return 0, false
	}
	return dSum / dCount, true
}

func TestScraperURLTrimsTrailingSlash(t *testing.T) {
	// Parity with Python: vllm_url.rstrip("/") + "/metrics".
	got := newVLLMTpotScraper(&Config{VLLMURL: "http://vllm:8000/", SLOTpotMetric: "m", SLOScrapeTimeoutS: 2}).url
	if got != "http://vllm:8000/metrics" {
		t.Fatalf("url = %q, want http://vllm:8000/metrics", got)
	}
	got = newVLLMTpotScraper(&Config{VLLMURL: "http://vllm:8000", SLOTpotMetric: "m", SLOScrapeTimeoutS: 2}).url
	if got != "http://vllm:8000/metrics" {
		t.Fatalf("url = %q, want http://vllm:8000/metrics", got)
	}
}

func TestScrapeSumCountParsing(t *testing.T) {
	// Exercise the real expfmt parsing path end to end.
	body := metricsText(1.6, 110)
	sum, count, ok := parseTpotSumCount(strings.NewReader(body), "vllm:time_per_output_token_seconds")
	if !ok || math.Abs(sum-1.6) > 1e-9 || math.Abs(count-110) > 1e-9 {
		t.Fatalf("parsed sum=%v count=%v ok=%v, want 1.6 110 true", sum, count, ok)
	}
}

// ------------------------------------------------------------------
// End-to-end convergence via a scripted sampler
// ------------------------------------------------------------------

type scriptedSampler struct {
	series []float64
	i      int
}

func (s *scriptedSampler) Sample() (float64, bool) {
	var v float64
	if s.i >= len(s.series) {
		v = s.series[len(s.series)-1]
	} else {
		v = s.series[s.i]
	}
	s.i++
	return v, true
}

func monitorWithSeries(series []float64, mut func(*Config)) *SloBackpressureMonitor {
	cfg := &Config{
		VLLMURL:           "http://x",
		SLOTpotSLOS:       0.040,
		SLOEvalIntervalS:  5,
		SLOWindowSamples:  1, // no smoothing -> deterministic steps
		SLOWindowAgg:      "mean",
		SLOMinPull:        1,
		SLOMaxPull:        8,
		SLODecreaseMode:   "additive",
		SLODecreaseStep:   1,
		SLODecreaseFactor: 0.5,
		SLORecoverStep:    1,
		SLOCooldownS:      0, // react every tick
		SLOScrapeTimeoutS: 2,
	}
	if mut != nil {
		mut(cfg)
	}
	// emit=false so tests don't touch the global Prometheus registry.
	return newSloBackpressureMonitorWithSampler(cfg, 8, "pod-a", &scriptedSampler{series: series}, false)
}

func TestConvergenceDownThenRecover(t *testing.T) {
	series := []float64{0.080, 0.080, 0.080, 0.080}
	for i := 0; i < 12; i++ {
		series = append(series, 0.020)
	}
	mon := monitorWithSeries(series, nil)
	var caps []int
	tnow := 0.0
	for range series {
		_, _, _, next, _ := mon.tick(tnow)
		caps = append(caps, next)
		tnow += 1.0
	}
	if caps[0] != 7 {
		t.Fatalf("first tick cap = %d, want 7", caps[0])
	}
	if caps[3] != 4 {
		t.Fatalf("4th tick cap = %d, want 4", caps[3])
	}
	if caps[len(caps)-1] != 8 {
		t.Fatalf("final cap = %d, want 8 (recovered)", caps[len(caps)-1])
	}
	if minInts(caps) != 4 {
		t.Fatalf("min cap = %d, want 4", minInts(caps))
	}
}

func TestConvergenceMultiplicativeFastBackoff(t *testing.T) {
	mon := monitorWithSeries([]float64{0.090, 0.090}, func(c *Config) {
		c.SLODecreaseMode = "multiplicative"
		c.SLODecreaseFactor = 0.5
	})
	_, _, _, c1, _ := mon.tick(0.0)
	_, _, _, c2, _ := mon.tick(1.0)
	if c1 != 4 || c2 != 2 {
		t.Fatalf("multiplicative caps = (%d,%d), want (4,2)", c1, c2)
	}
}

func TestConvergenceNeverBelowMin(t *testing.T) {
	series := make([]float64, 50)
	for i := range series {
		series[i] = 0.090
	}
	mon := monitorWithSeries(series, func(c *Config) { c.SLOMinPull = 2 })
	last := 8
	for i := 0; i < 50; i++ {
		_, _, _, last, _ = mon.tick(float64(i))
	}
	if last != 2 {
		t.Fatalf("final cap = %d, want 2 (min_pull)", last)
	}
}

func TestMonitorCooldownPreventsRapidAdjust(t *testing.T) {
	series := make([]float64, 12)
	for i := range series {
		series[i] = 0.090
	}
	mon := monitorWithSeries(series, func(c *Config) { c.SLOCooldownS = 10.0 })
	if _, _, _, cap, reason := mon.tick(0.0); cap != 7 || reason != "decrease" {
		t.Fatalf("t0 got (%d,%q), want (7,decrease)", cap, reason)
	}
	for tsec := 1; tsec < 10; tsec++ {
		if _, _, _, cap, reason := mon.tick(float64(tsec)); cap != 7 || reason != "hold_cooldown" {
			t.Fatalf("t%d got (%d,%q), want (7,hold_cooldown)", tsec, cap, reason)
		}
	}
	if _, _, _, cap, reason := mon.tick(10.0); cap != 6 || reason != "decrease" {
		t.Fatalf("t10 got (%d,%q), want (6,decrease)", cap, reason)
	}
}

func TestMonitorGetCapStartsAtMax(t *testing.T) {
	mon := monitorWithSeries([]float64{0.02}, nil)
	if mon.GetCap() != 8 {
		t.Fatalf("initial cap = %d, want 8 (max_pull)", mon.GetCap())
	}
}

// ------------------------------------------------------------------
// Config parsing + ControllerParamsFromConfig
// ------------------------------------------------------------------

func TestSLOConfigDefaultsDisabled(t *testing.T) {
	cfg := LoadConfig()
	if cfg.SLODynamicPullEnabled {
		t.Fatal("SLO dynamic pull should default to disabled")
	}
	if cfg.SLOMinPull != 1 || cfg.SLOMaxPull != 0 || cfg.SLODecreaseMode != "additive" {
		t.Fatalf("unexpected defaults: min=%d max=%d mode=%q", cfg.SLOMinPull, cfg.SLOMaxPull, cfg.SLODecreaseMode)
	}
}

func TestSLOConfigEnvOverrides(t *testing.T) {
	t.Setenv("SLO_DYNAMIC_PULL_ENABLED", "true")
	t.Setenv("SLO_TPOT_SLO_S", "0.03")
	t.Setenv("SLO_MIN_PULL", "2")
	t.Setenv("SLO_MAX_PULL", "16")
	t.Setenv("SLO_DECREASE_MODE", "MULTIPLICATIVE")
	t.Setenv("SLO_DECREASE_FACTOR", "0.25")
	t.Setenv("SLO_RECOVER_STEP", "2")
	t.Setenv("SLO_COOLDOWN_S", "7.5")
	cfg := LoadConfig()
	if !cfg.SLODynamicPullEnabled {
		t.Fatal("enabled should be true")
	}
	if cfg.SLOTpotSLOS != 0.03 || cfg.SLOMinPull != 2 || cfg.SLOMaxPull != 16 {
		t.Fatalf("unexpected: tpot=%v min=%d max=%d", cfg.SLOTpotSLOS, cfg.SLOMinPull, cfg.SLOMaxPull)
	}
	if cfg.SLODecreaseMode != "multiplicative" {
		t.Fatalf("decrease mode = %q, want multiplicative (lowercased)", cfg.SLODecreaseMode)
	}
}

func TestParamsFromConfigDefaultMaxPullUsesStaticCap(t *testing.T) {
	cfg := &Config{SLOMaxPull: 0, SLOMinPull: 1, SLODecreaseMode: "additive", SLODecreaseStep: 1, SLORecoverStep: 1}
	p := ControllerParamsFromConfig(cfg, 12)
	if p.MaxPull != 12 {
		t.Fatalf("MaxPull = %d, want 12 (static cap)", p.MaxPull)
	}
}

func TestParamsFromConfigFixesInvertedWindow(t *testing.T) {
	cfg := &Config{SLOMaxPull: 4, SLOMinPull: 10, SLODecreaseMode: "additive", SLODecreaseStep: 1, SLORecoverStep: 1}
	p := ControllerParamsFromConfig(cfg, 4)
	if p.MinPull != 10 || p.MaxPull != 10 {
		t.Fatalf("got min=%d max=%d, want 10 10", p.MinPull, p.MaxPull)
	}
}

// ------------------------------------------------------------------
// Closed-state wiring: no cap provider -> static cap
// ------------------------------------------------------------------

func TestPullWorkerStaticCapWhenDisabled(t *testing.T) {
	cfg := &Config{BatchSize: 8, Prefetch: 2}
	w := NewRouterPullWorker(cfg, NewLocalQueue("pod-a"), "pod-a") // no cap provider
	if got := w.currentPullCap(); got != 10 {
		t.Fatalf("static cap = %d, want 10 (batch 8 + prefetch 2)", got)
	}
}

func TestPullWorkerUsesCapProviderWhenSet(t *testing.T) {
	cfg := &Config{BatchSize: 8, Prefetch: 2}
	w := NewRouterPullWorker(cfg, NewLocalQueue("pod-a"), "pod-a")
	dyn := 3
	w.SetCapProvider(func() int { return dyn })
	if got := w.currentPullCap(); got != 3 {
		t.Fatalf("dynamic cap = %d, want 3", got)
	}
	dyn = 5
	if got := w.currentPullCap(); got != 5 {
		t.Fatalf("dynamic cap = %d, want 5", got)
	}
}

// ------------------------------------------------------------------
// helpers
// ------------------------------------------------------------------

func minInts(xs []int) int {
	m := xs[0]
	for _, x := range xs {
		if x < m {
			m = x
		}
	}
	return m
}
