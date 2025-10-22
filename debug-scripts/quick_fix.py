#!/usr/bin/env python3
# Fix all experiments under /home/saeid/llm-lb/experiments
# - Sort each latencies.csv by end_time (arrival) ascending
# - Keep request_id as a column
# - Regenerate summary.json in each batch_* folder

from pathlib import Path
from datetime import datetime
import csv, json

EXPERIMENTS_ROOT = Path("/home/saeid/llm-lb/experiments").resolve()

HEADER = [
    "request_id",
    "status",
    "latency_s",
    "start_time",
    "end_time",          # arrival time
    "response_raw_len",
    "output_tokens",
    "error",
]

def parse_ts(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        # fallback if format is unexpected
        return datetime(1970, 1, 1)

def load_rows(csv_path: Path):
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = [dict(x) for x in r]
    # coerce numeric fields where present
    for x in rows:
        for k in ("request_id","status","response_raw_len","output_tokens"):
            if k in x and x[k] not in (None, "",):
                try: x[k] = int(x[k])
                except: pass
        for k in ("latency_s",):
            if k in x and x[k] not in (None, "",):
                try: x[k] = float(x[k])
                except: pass
    return rows

def write_rows(csv_path: Path, rows):
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in HEADER})

def summarize(rows, batch_size_hint=None):
    ok = [r for r in rows if str(r.get("status")) == "200"]
    errs = [r for r in rows if str(r.get("status")) != "200"]
    summ = {
        "batch_size": batch_size_hint,
        "requests": len(rows),
        "success": len(ok),
        "errors": len(errs),
    }
    if ok:
        lats = sorted(float(r.get("latency_s", 0.0)) for r in ok)
        n = len(lats)
        p50 = lats[n//2] if n % 2 else 0.5*(lats[n//2-1] + lats[n//2])
        p95 = lats[max(0, int(0.95*(n-1)))]
        summ["latency_median_s"] = round(p50, 3)
        summ["latency_p95_s"] = round(p95, 3)
    if errs:
        summ["errors_preview"] = [
            {"status": r.get("status"), "error": r.get("error")}
            for r in errs[:5]
        ]
    return summ

def fix_one(csv_path: Path):
    rows = load_rows(csv_path)
    if not rows:
        print(f"skip (empty): {csv_path}")
        return

    # sort by end_time (arrival)
    rows_sorted = sorted(rows, key=lambda r: parse_ts(r.get("end_time", "1970-01-01T00:00:00.000")))
    write_rows(csv_path, rows_sorted)

    # derive batch size from parent folder name "batch_<N>"
    batch_dir = csv_path.parent
    bs = None
    try:
        name = batch_dir.name
        if name.startswith("batch_"):
            bs = int(name.split("_", 1)[1])
    except Exception:
        pass

    # write summary.json
    summary_path = batch_dir / "summary.json"
    summ = summarize(rows_sorted, batch_size_hint=bs)
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summ, f, indent=2)

    print(f"fixed: {csv_path}  (rows={len(rows_sorted)})")

def main():
    if not EXPERIMENTS_ROOT.exists():
        print(f"root not found: {EXPERIMENTS_ROOT}")
        return
    targets = sorted(EXPERIMENTS_ROOT.rglob("latencies.csv"))
    if not targets:
        print("no latencies.csv found under", EXPERIMENTS_ROOT)
        return
    print(f"found {len(targets)} files")
    for p in targets:
        fix_one(p)

if __name__ == "__main__":
    main()
