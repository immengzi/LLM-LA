#!/usr/bin/env python3
"""Write initial run_meta.json for a bench_mock cell."""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("meta_path", type=Path)
    ap.add_argument("--path", required=True)
    ap.add_argument("--instances", type=int, required=True)
    args = ap.parse_args()

    meta = {
        "path": args.path,
        "instances": args.instances,
        "users": int(os.environ.get("USERS", "1000")),
        "spawn_rate": int(os.environ.get("SPAWN_RATE", "500")),
        "run_time": os.environ.get("RUN_TIME", "5m"),
        "model": os.environ.get("BENCH_MODEL"),
        "chat_path": os.environ.get("CHAT_PATH"),
        "mock_latency_ms": os.environ.get("MOCK_LATENCY_MS", "0"),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "machine_spec_target": {"cpu": 4, "ram_gb": 8},
    }
    args.meta_path.parent.mkdir(parents=True, exist_ok=True)
    args.meta_path.write_text(json.dumps(meta, indent=2) + "\n")
    print(json.dumps(meta, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
