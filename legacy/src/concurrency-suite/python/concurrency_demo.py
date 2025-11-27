# python/concurrency_demo.py
import argparse
import csv
import time
import threading
from queue import SimpleQueue, Empty
import matplotlib.pyplot as plt


def run_trial(num_workers: int):
    q = SimpleQueue()
    for i in range(num_workers):
        q.put(i)

    start_evt = threading.Event()
    pulled = uniques = dupes = misses = 0
    seen = set()
    lock = threading.Lock()

    def worker():
        nonlocal pulled, uniques, dupes, misses
        start_evt.wait()
        try:
            item = q.get_nowait()
        except Empty:
            with lock:
                misses += 1
            return
        with lock:
            pulled += 1
            if item in seen:
                dupes += 1
            else:
                seen.add(item)
                uniques += 1

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(num_workers)]
    for t in threads:
        t.start()
    t0 = time.perf_counter_ns()
    start_evt.set()
    for t in threads:
        t.join()
    t1 = time.perf_counter_ns()

    return {
        "workers": num_workers,
        "pulled": pulled,
        "unique": uniques,
        "duplicates": dupes,
        "misses": misses,
        "avg_spread_ms": (t1 - t0) / 1e6,  # one-shot spread for this trial
        "success_rate": pulled / num_workers if num_workers else 1.0,
    }


def _pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    # nearest-rank on [0..n-1]
    k = int(round(p * (len(xs) - 1)))
    if k < 0:
        k = 0
    if k >= len(xs):
        k = len(xs) - 1
    return xs[k]


def aggregate(trials):
    def avg(xs):
        return sum(xs) / len(xs) if xs else float("nan")

    spreads = [t["avg_spread_ms"] for t in trials]
    succ = [t["success_rate"] for t in trials]
    dups = [t["duplicates"] for t in trials]
    miss = [t["misses"] for t in trials]
    return {
        "avg_spread_ms": avg(spreads),
        "p95_spread_ms": _pct(spreads, 0.95),
        "p99_spread_ms": _pct(spreads, 0.99),
        "avg_success_rate": avg(succ),
        "p95_success_rate": _pct(succ, 0.95),
        "p99_success_rate": _pct(succ, 0.99),
        "avg_duplicates": avg(dups),
        "p95_duplicates": _pct(dups, 0.95),
        "p99_duplicates": _pct(dups, 0.99),
        "avg_misses": avg(miss),
        "p95_misses": _pct(miss, 0.95),
        "p99_misses": _pct(miss, 0.99),
    }


def main():
    ap = argparse.ArgumentParser(
        description="Central queue concurrent pull demo (efficient)."
    )
    ap.add_argument("--workers", type=str, default="10,20,30,40,60,80,100,200")
    ap.add_argument("--reps", type=int, default=20)
    ap.add_argument("--csv", type=str, default="concurrency_results_python.csv")
    ap.add_argument("--png1", type=str, default="success_vs_workers_python.png")
    ap.add_argument("--png2", type=str, default="spread_vs_workers_python.png")
    args = ap.parse_args()

    counts = [int(x.strip()) for x in args.workers.split(",") if x.strip()]

    rows = []
    print(
        "PY  workers | avg_succ% | p95_succ% | p99_succ% | avg_spread | p95_spread | p99_spread"
    )
    for n in counts:
        trials = [run_trial(n) for _ in range(args.reps)]
        agg = aggregate(trials)
        row = {
            "workers": n,
            "avg_success_rate": agg["avg_success_rate"],
            "p95_success_rate": agg["p95_success_rate"],
            "p99_success_rate": agg["p99_success_rate"],
            "avg_duplicates": agg["avg_duplicates"],
            "p95_duplicates": agg["p95_duplicates"],
            "p99_duplicates": agg["p99_duplicates"],
            "avg_misses": agg["avg_misses"],
            "p95_misses": agg["p95_misses"],
            "p99_misses": agg["p99_misses"],
            "avg_spread_ms": agg["avg_spread_ms"],
            "p95_spread_ms": agg["p95_spread_ms"],
            "p99_spread_ms": agg["p99_spread_ms"],
        }
        rows.append(row)
        print(
            f"PY {n:7d} | {100*row['avg_success_rate']:9.2f} | {100*row['p95_success_rate']:10.2f} | {100*row['p99_success_rate']:10.2f} | "
            f"{row['avg_spread_ms']:10.3f} | {row['p95_spread_ms']:10.3f} | {row['p99_spread_ms']:10.3f}"
        )

    # CSV
    with open(args.csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    # Optional standalone plots (avg only, since run_all.py makes full combined ones)
    # Keep small to reduce clutter:
    # Success (avg)
    xs = [r["workers"] for r in rows]
    succ_avg = [100 * r["avg_success_rate"] for r in rows]
    plt.figure(figsize=(7, 4.2))
    plt.plot(xs, succ_avg, marker="o")
    plt.xlabel("Workers")
    plt.ylabel("Success rate (%)")
    plt.ylim(0, 105)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(args.png1, dpi=140)
    # Spread (avg)
    spread_avg = [r["avg_spread_ms"] for r in rows]
    plt.figure(figsize=(7, 4.2))
    plt.plot(xs, spread_avg, marker="o")
    plt.xlabel("Workers")
    plt.ylabel("Avg spread (ms)")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(args.png2, dpi=140)


if __name__ == "__main__":
    main()
