# Helpers for experiment I/O:
# - Choose next experiment directory under ./experiments
# - Persist the effective client config as config.json
# - Provide a thread-safe ExperimentLogger for per-request logs (logs.json)

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Tuple, Optional
import json
import threading
import time

from config import ClientConfig


class ExperimentLogger:
    """
    Thread-safe JSON-lines logger for per-request records.

    Each call to log_request(record) appends one JSON object as a line into
    logs.json. The file content is thus NDJSON/JSONL (one JSON per line),
    even though the extension is .json for convenience.
    """

    def __init__(self, logs_path: Path):
        self._logs_path = logs_path
        self._lock = threading.Lock()
        self._fh = None  # type: Optional[object]

    def open(self) -> None:
        # Append mode so multiple runs could be resumed if desired.
        self._fh = open(self._logs_path, "a", encoding="utf-8")

    def log_request(self, record: dict) -> None:
        if self._fh is None:
            return
        line = json.dumps(record, ensure_ascii=False)
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def _next_experiment_dir(root: Path) -> Path:
    root.mkdir(exist_ok=True)
    existing_ids = []
    for p in root.iterdir():
        if p.is_dir() and p.name.isdigit():
            try:
                existing_ids.append(int(p.name))
            except ValueError:
                continue
    next_id = max(existing_ids) + 1 if existing_ids else 1
    exp_dir = root / str(next_id)
    exp_dir.mkdir(parents=True, exist_ok=False)
    return exp_dir


def init_experiment(
    cfg: ClientConfig,
    *,
    config_path: str,
    config_name: Optional[str] = None,
    configs_root: str = "configs",
    experiments_root: str = "experiments",
) -> Tuple[Path, ExperimentLogger]:
    """
    Create a new experiment directory and persist:
      - config.json : frozen view of the effective ClientConfig.
      - logs.json   : per-request JSON-lines (append-only via ExperimentLogger).

    Returns:
      (experiment_dir_path, ExperimentLogger)
    """
    cfgs_root = Path(configs_root)
    exps_root = Path(experiments_root)
    cfgs_root.mkdir(exist_ok=True)
    exps_root.mkdir(exist_ok=True)

    exp_dir = _next_experiment_dir(exps_root)

    # Persist the effective config for this run.
    config_out = {
        "created_at_unix": time.time(),
        "config_path": str(Path(config_path).resolve()),
        "config_name": config_name,
        "client_config": asdict(cfg),
    }
    config_json_path = exp_dir / "config.json"
    with config_json_path.open("w", encoding="utf-8") as f:
        json.dump(config_out, f, indent=2, sort_keys=True)

    # Prepare logger for per-request logs.
    logs_path = exp_dir / "logs.json"
    logger = ExperimentLogger(logs_path)
    logger.open()

    return exp_dir, logger
