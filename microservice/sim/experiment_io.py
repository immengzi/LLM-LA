# sim/experiment_io.py
from __future__ import annotations
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import time
import yaml

from config import ClientConfig


def _next_experiment_id(root: Path) -> int:
    root.mkdir(parents=True, exist_ok=True)
    mx = 0
    for p in root.iterdir():
        if p.is_dir():
            try:
                mx = max(mx, int(p.name))
            except Exception:
                pass
    return mx + 1


def init_experiment_sim(cfg: ClientConfig, config_path: str, sim_cfg: Dict[str, Any]) -> Path:
    root = Path("experiments_sim")
    exp_id = _next_experiment_id(root)
    exp_dir = root / str(exp_id)
    exp_dir.mkdir(parents=True, exist_ok=True)

    # Persist config.json (client config) and config_used.yaml (original yaml + sim section)
    (exp_dir / "config.json").write_text(json.dumps(asdict(cfg), indent=2, sort_keys=True), encoding="utf-8")

    raw_yaml = Path(config_path).read_text(encoding="utf-8")
    # Write exact YAML used (unchanged)
    (exp_dir / "config_used.yaml").write_text(raw_yaml, encoding="utf-8")

    # sim settings snapshot
    (exp_dir / "sim_config.json").write_text(json.dumps(sim_cfg, indent=2, sort_keys=True), encoding="utf-8")

    return exp_dir


def write_logs(exp_dir: Path, records: List[Dict[str, Any]]):
    (exp_dir / "logs.json").write_text(json.dumps(records, indent=2), encoding="utf-8")


def write_metrics_summary(exp_dir: Path, metrics_summary: Dict[str, Any]):
    (exp_dir / "metrics_summary.json").write_text(json.dumps(metrics_summary, indent=2, sort_keys=True), encoding="utf-8")


def write_run_summary(exp_dir: Path, run_summary: Dict[str, Any]):
    (exp_dir / "run_summary.json").write_text(json.dumps(run_summary, indent=2, sort_keys=True), encoding="utf-8")
