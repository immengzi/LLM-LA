#!/usr/bin/env python3
"""Patch run_meta.json after a Locust cell (avoids fragile bash heredocs)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("meta_path", type=Path)
    ap.add_argument("--locust-exit-code", type=int, default=0)
    ap.add_argument("--out-dir", type=Path, required=True)
    args = ap.parse_args()

    meta: dict = {}
    if args.meta_path.exists():
        meta = json.loads(args.meta_path.read_text())
    meta["locust_exit_code"] = args.locust_exit_code

    fail_csv = args.out_dir / "locust_failures.csv"
    meta["had_request_failures"] = False
    fail_count = 0
    if fail_csv.exists():
        lines = [ln for ln in fail_csv.read_text().splitlines() if ln.strip()]
        meta["had_request_failures"] = len(lines) > 1
        # Sum Occurrences when CSV has the Locust failures schema.
        try:
            import csv

            for row in csv.DictReader(fail_csv.open()):
                fail_count += int(row.get("Occurrences") or 0)
        except Exception:
            fail_count = max(0, len(lines) - 1)
    meta["failure_occurrences"] = fail_count
    args.meta_path.write_text(json.dumps(meta, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
