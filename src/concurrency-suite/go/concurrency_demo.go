// go/concurrency_demo.go
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TrialResult struct {
	Workers      int
	Pulled       int
	Unique       int
	Duplicates   int
	Misses       int
	PullSpreadMS float64
	SuccessRate  float64
}

type AggRow struct {
	Workers        int
	AvgSuccessRate float64
	P95SuccessRate float64
	P99SuccessRate float64
	AvgDuplicates  float64
	P95Duplicates  float64
	P99Duplicates  float64
	AvgMisses      float64
	P95Misses      float64
	P99Misses      float64
	AvgSpreadMS    float64
	P95SpreadMS    float64
	P99SpreadMS    float64
}

func runTrial(n int) TrialResult {
	ch := make(chan int, n)
	for i := 0; i < n; i++ { ch <- i }

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)

	seen := make(map[int]bool)
	var mu sync.Mutex
	pulled, unique, dupes, misses := 0, 0, 0, 0

	worker := func() {
		defer wg.Done()
		<-start
		v, ok := <-ch
		if !ok {
			mu.Lock(); misses++; mu.Unlock()
			return
		}
		mu.Lock()
		pulled++
		if seen[v] { dupes++ } else { seen[v] = true; unique++ }
		mu.Unlock()
	}

	for i := 0; i < n; i++ { go worker() }

	t0 := time.Now()
	close(start)
	wg.Wait()
	spread := time.Since(t0).Seconds() * 1000.0

	return TrialResult{
		Workers: n, Pulled: pulled, Unique: unique, Duplicates: dupes, Misses: misses,
		PullSpreadMS: spread, SuccessRate: float64(pulled) / float64(n),
	}
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 { return 0 }
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	k := int(p*float64(len(cp)-1) + 0.5)
	if k < 0 { k = 0 }
	if k >= len(cp) { k = len(cp)-1 }
	return cp[k]
}
func avg(xs []float64) float64 {
	if len(xs) == 0 { return 0 }
	s := 0.0
	for _, v := range xs { s += v }
	return s / float64(len(xs))
}

func aggregate(workers int, trials []TrialResult) AggRow {
	succ, dups, miss, spread := []float64{}, []float64{}, []float64{}, []float64{}
	for _, t := range trials {
		succ = append(succ, t.SuccessRate)
		dups = append(dups, float64(t.Duplicates))
		miss = append(miss, float64(t.Misses))
		spread = append(spread, t.PullSpreadMS)
	}
	return AggRow{
		Workers:        workers,
		AvgSuccessRate: avg(succ),
		P95SuccessRate: pct(succ, 0.95),
		P99SuccessRate: pct(succ, 0.99),
		AvgDuplicates:  avg(dups),
		P95Duplicates:  pct(dups, 0.95),
		P99Duplicates:  pct(dups, 0.99),
		AvgMisses:      avg(miss),
		P95Misses:      pct(miss, 0.95),
		P99Misses:      pct(miss, 0.99),
		AvgSpreadMS:    avg(spread),
		P95SpreadMS:    pct(spread, 0.95),
		P99SpreadMS:    pct(spread, 0.99),
	}
}

func parseWorkers(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" { continue }
		n, err := strconv.Atoi(p)
		if err != nil { return nil, err }
		if n <= 0 { return nil, fmt.Errorf("workers must be > 0: %d", n) }
		out = append(out, n)
	}
	return out, nil
}

func main() {
	workersFlag := flag.String("workers", "10,20,40,80,160,320", "Comma-separated worker counts")
	reps := flag.Int("reps", 20, "Trials per worker count")
	csvPath := flag.String("csv", "go_concurrency_results.csv", "CSV output path")
	flag.Parse()

	workersList, err := parseWorkers(*workersFlag)
	if err != nil { fmt.Println("bad -workers:", err); os.Exit(1) }

	fmt.Printf("GO  workers | avg_succ%% | p95_succ%% | p99_succ%% | avg_spread | p95_spread | p99_spread\n")

	rows := make([]AggRow, 0, len(workersList))
	for _, w := range workersList {
		trials := make([]TrialResult, 0, *reps)
		for i := 0; i < *reps; i++ { trials = append(trials, runTrial(w)) }
		agg := aggregate(w, trials)
		rows = append(rows, agg)
		fmt.Printf("GO %7d | %9.2f | %10.2f | %10.2f | %10.3f | %10.3f | %10.3f\n",
			w, 100.0*agg.AvgSuccessRate, 100.0*agg.P95SuccessRate, 100.0*agg.P99SuccessRate,
			agg.AvgSpreadMS, agg.P95SpreadMS, agg.P99SpreadMS)
	}

	f, err := os.Create(*csvPath); if err != nil { fmt.Println("create CSV:", err); os.Exit(1) }
	defer f.Close()
	wr := csv.NewWriter(f); defer wr.Flush()

	wr.Write([]string{
		"workers","avg_success_rate","p95_success_rate","p99_success_rate",
		"avg_duplicates","p95_duplicates","p99_duplicates",
		"avg_misses","p95_misses","p99_misses",
		"avg_spread_ms","p95_spread_ms","p99_spread_ms",
	})
	for _, r := range rows {
		wr.Write([]string{
			strconv.Itoa(r.Workers),
			fmt.Sprintf("%.6f", r.AvgSuccessRate),
			fmt.Sprintf("%.6f", r.P95SuccessRate),
			fmt.Sprintf("%.6f", r.P99SuccessRate),
			fmt.Sprintf("%.6f", r.AvgDuplicates),
			fmt.Sprintf("%.6f", r.P95Duplicates),
			fmt.Sprintf("%.6f", r.P99Duplicates),
			fmt.Sprintf("%.6f", r.AvgMisses),
			fmt.Sprintf("%.6f", r.P95Misses),
			fmt.Sprintf("%.6f", r.P99Misses),
			fmt.Sprintf("%.3f", r.AvgSpreadMS),
			fmt.Sprintf("%.3f", r.P95SpreadMS),
			fmt.Sprintf("%.3f", r.P99SpreadMS),
		})
	}
	fmt.Printf("\nSaved CSV -> %s\n", *csvPath)
}
