#!/usr/bin/env python3
import os
import time
from datetime import datetime

INTERVAL = 5  # seconds
OUT_FILE = "thread_dump.txt"

def list_pids():
    return [pid for pid in os.listdir("/proc") if pid.isdigit()]

def read_comm(pid):
    try:
        with open(f"/proc/{pid}/comm") as f:
            return f.read().strip()
    except Exception:
        return "?"

def list_threads(pid):
    try:
        return os.listdir(f"/proc/{pid}/task")
    except Exception:
        return []

def snapshot():
    pids = list_pids()
    total_threads = 0
    processes = []

    for pid in pids:
        tids = list_threads(pid)
        if not tids:
            continue

        total_threads += len(tids)
        processes.append({
            "pid": pid,
            "comm": read_comm(pid),
            "threads": tids,
        })

    return total_threads, processes

def main():
    with open(OUT_FILE, "a") as out:
        out.write("# Thread dump started\n")

    while True:
        ts = datetime.utcnow().isoformat() + "Z"
        total_threads, processes = snapshot()

        with open(OUT_FILE, "a") as out:
            out.write("\n")
            out.write(f"=== SNAPSHOT {ts} ===\n")
            out.write(f"TOTAL_THREADS {total_threads}\n\n")

            for p in sorted(processes, key=lambda x: len(x["threads"]), reverse=True):
                out.write(
                    f"PID={p['pid']} COMM={p['comm']} "
                    f"THREADS={len(p['threads'])}\n"
                )
                out.write("  TIDS: " + " ".join(p["threads"]) + "\n")

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
