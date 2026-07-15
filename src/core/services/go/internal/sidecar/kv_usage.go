package sidecar

import (
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// Preferred then fallback metric names (mirrors Python kv_usage.py).
var kvMetricNames = []string{
	"vllm:kv_cache_usage_perc",
	"vllm:gpu_cache_usage_perc",
}

// ParseKvUsageFromText parses Prometheus text and returns max GPU KV usage
// fraction across engines. ok=false when neither series is present.
func ParseKvUsageFromText(r io.Reader) (float64, bool) {
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return 0, false
	}
	for _, name := range kvMetricNames {
		fam, ok := families[name]
		if !ok {
			continue
		}
		vals := make([]float64, 0, 4)
		for _, m := range fam.GetMetric() {
			var v float64
			switch fam.GetType() {
			case dto.MetricType_GAUGE:
				if g := m.GetGauge(); g != nil {
					v = g.GetValue()
				} else {
					continue
				}
			case dto.MetricType_UNTYPED:
				if u := m.GetUntyped(); u != nil {
					v = u.GetValue()
				} else {
					continue
				}
			default:
				continue
			}
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			vals = append(vals, v)
		}
		if len(vals) == 0 {
			continue
		}
		mx := vals[0]
		for _, x := range vals[1:] {
			if x > mx {
				mx = x
			}
		}
		if mx < 0 {
			mx = 0
		}
		if mx > 1.0 {
			if mx <= 100.0 {
				mx = mx / 100.0
			} else {
				mx = 1.0
			}
		}
		return mx, true
	}
	return 0, false
}

// VLLMKvUsageScraper GETs local vLLM /metrics for a KV usage sample.
type VLLMKvUsageScraper struct {
	url    string
	client *http.Client
}

func NewVLLMKvUsageScraper(cfg *Config) *VLLMKvUsageScraper {
	timeout := time.Duration(cfg.KVUsageScrapeTimeoutS * float64(time.Second))
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &VLLMKvUsageScraper{
		url:    strings.TrimRight(cfg.VLLMURL, "/") + "/metrics",
		client: &http.Client{Timeout: timeout},
	}
}

func (s *VLLMKvUsageScraper) Sample() (float64, bool) {
	resp, err := s.client.Get(s.url)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	return ParseKvUsageFromText(resp.Body)
}

// KvUsageMonitor periodically scrapes and caches KV usage.
type KvUsageMonitor struct {
	scraper   *VLLMKvUsageScraper
	interval  time.Duration
	mu        sync.RWMutex
	value     float64
	ok        bool
	ts        time.Time
	stopCh    chan struct{}
	stoppedCh chan struct{}
	startOnce sync.Once
}

func NewKvUsageMonitor(cfg *Config) *KvUsageMonitor {
	interval := time.Duration(cfg.KVUsageScrapeIntervalS * float64(time.Second))
	if interval < 200*time.Millisecond {
		interval = 5 * time.Second
	}
	return &KvUsageMonitor{
		scraper:   NewVLLMKvUsageScraper(cfg),
		interval:  interval,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

func (m *KvUsageMonitor) Get() (float64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.value, m.ok
}

func (m *KvUsageMonitor) Tick() (float64, bool) {
	v, ok := m.scraper.Sample()
	m.mu.Lock()
	defer m.mu.Unlock()
	if ok {
		m.value = v
		m.ok = true
		m.ts = time.Now()
	}
	return m.value, m.ok
}

func (m *KvUsageMonitor) Start() {
	m.startOnce.Do(func() {
		go m.loop()
	})
}

func (m *KvUsageMonitor) Stop() {
	select {
	case <-m.stopCh:
		return
	default:
		close(m.stopCh)
	}
	<-m.stoppedCh
}

func (m *KvUsageMonitor) loop() {
	defer close(m.stoppedCh)
	m.Tick()
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
			m.Tick()
		}
	}
}

// Process-wide optional monitor (bound from main).
var (
	kvMonitorMu sync.RWMutex
	kvMonitor   *KvUsageMonitor
)

func BindKvUsageMonitor(m *KvUsageMonitor) {
	kvMonitorMu.Lock()
	kvMonitor = m
	kvMonitorMu.Unlock()
}

func GetCachedKvUsage() (float64, bool) {
	kvMonitorMu.RLock()
	m := kvMonitor
	kvMonitorMu.RUnlock()
	if m == nil {
		return 0, false
	}
	return m.Get()
}
