#!/usr/bin/env python3
"""
launch_monitors.py — 从 instances.yaml 启动/管理所有 vllm_monitor 子进程。

Usage:
    python launch_monitors.py                  # 前台运行，Ctrl-C 统一停止
    python launch_monitors.py --daemon         # 后台运行（nohup 模式）
    python launch_monitors.py --stop           # 停止所有运行中的 monitor
    python launch_monitors.py --status         # 查看各进程状态
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = Path(__file__).parent / "instances.yaml"
MONITOR_SCRIPT = Path(__file__).parent / "vllm_monitor.py"
PID_DIR        = Path("/tmp/vllm_monitor_pids")


def load_config(path: Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ─────────────────────────────────────────────────────────────────────────────
# PID file helpers
# ─────────────────────────────────────────────────────────────────────────────

def pid_file(name: str) -> Path:
    PID_DIR.mkdir(parents=True, exist_ok=True)
    return PID_DIR / f"{name}.pid"


def write_pid(name: str, pid: int) -> None:
    pid_file(name).write_text(str(pid))


def read_pid(name: str) -> int | None:
    p = pid_file(name)
    if not p.exists():
        return None
    try:
        return int(p.read_text().strip())
    except ValueError:
        return None


def clear_pid(name: str) -> None:
    pid_file(name).unlink(missing_ok=True)


def is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Ops log
# ─────────────────────────────────────────────────────────────────────────────

_ops_log_path: Path | None = None


def ops_log(msg: str) -> None:
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"{ts}  {msg}"
    print(line, flush=True)
    if _ops_log_path:
        with open(_ops_log_path, "a") as f:
            f.write(line + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Launch / stop
# ─────────────────────────────────────────────────────────────────────────────

def launch_one(name: str, host: str, cfg: dict) -> subprocess.Popen:
    log_dir = Path(cfg["log_dir"]) / name
    log_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, str(MONITOR_SCRIPT),
        "--host",     host,
        "--interval", str(cfg.get("interval", 15)),
        "--timeout",  str(cfg.get("timeout", 10)),
        "--log-dir",  str(log_dir),
        "--mode",     name,
    ]

    # stdout/stderr → per-instance ops log (vllm_monitor.py also writes this
    # file internally, but subprocess stdout captures the ops_log prints too)
    stdout_log = open(log_dir / "vllm_monitor.log", "a")

    proc = subprocess.Popen(
        cmd,
        stdout=stdout_log,
        stderr=subprocess.STDOUT,
        # New process group so Ctrl-C on launcher doesn't cascade immediately
        start_new_session=True,
    )
    write_pid(name, proc.pid)
    return proc


def stop_one(name: str) -> None:
    pid = read_pid(name)
    if pid is None:
        ops_log(f"[{name}] no PID file, skipping")
        return
    if not is_running(pid):
        ops_log(f"[{name}] PID {pid} not running, cleaning up")
        clear_pid(name)
        return
    os.kill(pid, signal.SIGTERM)
    # Wait up to 5 s for graceful exit
    for _ in range(50):
        time.sleep(0.1)
        if not is_running(pid):
            break
    else:
        ops_log(f"[{name}] PID {pid} did not exit cleanly, sending SIGKILL")
        os.kill(pid, signal.SIGKILL)
    clear_pid(name)
    ops_log(f"[{name}] stopped (PID {pid})")


# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────

def cmd_start(cfg: dict, daemon: bool) -> None:
    instances = cfg["instances"]
    procs: list[tuple[str, subprocess.Popen]] = []

    for inst in instances:
        name = inst["name"]
        host = inst["host"]

        # Skip if already running
        existing_pid = read_pid(name)
        if existing_pid and is_running(existing_pid):
            ops_log(f"[{name}] already running (PID {existing_pid}), skipping")
            continue

        proc = launch_one(name, host, cfg)
        procs.append((name, proc))
        ops_log(f"[{name}] started — PID {proc.pid}  host={host}")

    if not procs:
        ops_log("All instances already running.")
        return

    if daemon:
        ops_log(f"All {len(procs)} monitors running in background.")
        ops_log(f"  Logs : {cfg['log_dir']}/<instance>/metrics.jsonl")
        ops_log(f"  Stop : python {Path(__file__).name} --stop")
        return

    # Foreground: wait and forward Ctrl-C
    ops_log("Running in foreground — press Ctrl-C to stop all monitors.")
    try:
        while True:
            # Restart any crashed child
            for i, (name, proc) in enumerate(procs):
                if proc.poll() is not None:
                    ops_log(f"[{name}] exited (rc={proc.returncode}), restarting...")
                    inst_cfg = next(x for x in cfg["instances"] if x["name"] == name)
                    new_proc = launch_one(name, inst_cfg["host"], cfg)
                    procs[i] = (name, new_proc)
                    ops_log(f"[{name}] restarted — PID {new_proc.pid}")
            time.sleep(5)
    except KeyboardInterrupt:
        ops_log("Ctrl-C received, stopping all monitors...")
        for name, proc in procs:
            proc.send_signal(signal.SIGTERM)
        for name, proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            clear_pid(name)
            ops_log(f"[{name}] stopped")


def cmd_stop(cfg: dict) -> None:
    for inst in cfg["instances"]:
        stop_one(inst["name"])


def cmd_status(cfg: dict) -> None:
    print(f"\n{'NAME':<20} {'PID':>7}  {'STATUS':<12}  HOST")
    print("─" * 66)
    for inst in cfg["instances"]:
        name = inst["name"]
        host = inst["host"]
        pid  = read_pid(name)
        if pid is None:
            status = "not started"
        elif is_running(pid):
            status = "running"
        else:
            status = "dead (stale)"
        print(f"{name:<20} {str(pid or '-'):>7}  {status:<12}  {host}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    global _ops_log_path

    parser = argparse.ArgumentParser(description="vLLM multi-instance monitor launcher")
    parser.add_argument("--config",  type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--start",   action="store_true", default=True)
    parser.add_argument("--stop",    action="store_true")
    parser.add_argument("--status",  action="store_true")
    parser.add_argument("--daemon",  action="store_true",
                        help="Detach all child processes and return immediately")
    args = parser.parse_args()

    cfg = load_config(args.config)

    log_dir = Path(cfg["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)
    _ops_log_path = log_dir / "launcher.log"

    if args.stop:
        cmd_stop(cfg)
    elif args.status:
        cmd_status(cfg)
    else:
        cmd_start(cfg, daemon=args.daemon)


if __name__ == "__main__":
    main()
