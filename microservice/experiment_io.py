# experiment_io.py
# Helpers for experiment I/O:
# - Choose next experiment directory under ./experiments
# - Persist the effective client config as config.json
# - Copy reproducibility artifacts (exact YAML used, deployment manifest)
# - Provide a thread-safe ExperimentLogger for per-request logs (logs.json)

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Tuple, Optional
import json
import shutil
import threading
import time
import os

from config import ClientConfig
from k8s_time_offsets import measure_k8s_node_time_offsets


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

        # (backward compatible): optionally reduce flush frequency to avoid
        # pathological slowdowns/hangs on some filesystems.
        #
        # Default is 1 => flush every line (IDENTICAL to old behavior).
        try:
            self._flush_every_n = int(os.environ.get("EXP_LOG_FLUSH_EVERY_N", "1") or "1")
        except Exception:
            self._flush_every_n = 1
        self._flush_every_n = max(1, self._flush_every_n)
        self._n_since_flush = 0

    def open(self) -> None:
        # Append mode so multiple runs could be resumed if desired.
        self._fh = open(self._logs_path, "a", encoding="utf-8")

    def log_request(self, record: dict) -> None:
        # Fast path: if closed/detached, do nothing.
        if self._fh is None:
            return

        try:
            line = json.dumps(record, ensure_ascii=False)
        except Exception:
            # If record is non-serializable, skip rather than crashing the run.
            return

        with self._lock:
            fh = self._fh
            if fh is None:
                return
            try:
                fh.write(line + "\n")
                self._n_since_flush += 1
                if self._flush_every_n <= 1 or (self._n_since_flush % self._flush_every_n) == 0:
                    fh.flush()
            except Exception:
                # Best-effort: never let logging wedge the experiment.
                pass

    def close(self) -> None:
        # Detach file handle under lock so writers immediately stop,
        # then close outside the lock to avoid blocking other threads.
        fh = None
        with self._lock:
            fh = self._fh
            self._fh = None
        if fh is not None:
            try:
                fh.close()
            except Exception:
                pass


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


def _copy_file_if_exists(src: Path, dst: Path) -> bool:
    """
    Best-effort copy that preserves metadata (mtime) via copy2.
    Returns True if copied, False otherwise.
    """
    try:
        if src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            return True
    except Exception:
        pass
    return False


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
      - config.json        : frozen view of the effective ClientConfig + node time offsets snapshot.
      - config_used.yaml   : exact YAML file used to run this experiment.
      - vllm-k8s.yaml      : deployment manifest snapshot (now: Helm-rendered manifest).
      - logs.json          : per-request JSON-lines (append-only via ExperimentLogger).

    Returns:
      (experiment_dir_path, ExperimentLogger)
    """
    cfgs_root = Path(configs_root)
    exps_root = Path(experiments_root)
    cfgs_root.mkdir(exist_ok=True)
    exps_root.mkdir(exist_ok=True)

    exp_dir = _next_experiment_dir(exps_root)

    # --- Measure node clock offsets at run start (best-effort) ---
    # We assume the client runs on the master node and treat the local clock as reference.
    # Offsets are measured from the time-probe DaemonSet logs using a midpoint estimator.
    #
    # Only ONE scalar per node is stored:
    #   offset_ns = remote_epoch_ns - local_midpoint_ns
    #
    # To convert a remote timestamp into master time:
    #   master_ns_est = remote_ns - offset_ns
    #
    # Controlled by env vars:
    #   TIME_PROBE_NAMESPACE (default: kube-system)
    #   TIME_PROBE_LABEL     (default: app=time-probe)
    #   TIME_PROBE_SAMPLES   (default: 15)
    #   TIME_PROBE_TIMEOUT_S (default: 5)
    #   TIME_PROBE_KUBECTL   (default: kubectl)
    try:
        time_offsets_ns = measure_k8s_node_time_offsets(
            namespace=os.environ.get("TIME_PROBE_NAMESPACE", "kube-system"),
            label_selector=os.environ.get("TIME_PROBE_LABEL", "app=time-probe"),
            samples=int(os.environ.get("TIME_PROBE_SAMPLES", "15") or "15"),
            kubectl=os.environ.get("TIME_PROBE_KUBECTL", "kubectl"),
            timeout_s=float(os.environ.get("TIME_PROBE_TIMEOUT_S", "5") or "5"),
        )
    except Exception:
        time_offsets_ns = {}

    # Persist the effective config for this run (resolved + defaults applied).
    config_out = {
        "created_at_unix": time.time(),
        "config_path": str(Path(config_path).resolve()),
        "config_name": config_name,
        "client_config": asdict(cfg),
        # New entry (simple):
        "time_offsets_ns": time_offsets_ns,
    }
    config_json_path = exp_dir / "config.json"
    with config_json_path.open("w", encoding="utf-8") as f:
        json.dump(config_out, f, indent=2, sort_keys=True)

    # --- Reproducibility artifacts ---
    # 1) Copy the *exact* YAML file used (byte-for-byte).
    src_cfg = Path(config_path).expanduser()
    if not src_cfg.is_absolute():
        src_cfg = (Path.cwd() / src_cfg)
    src_cfg = src_cfg.resolve()
    _copy_file_if_exists(src_cfg, exp_dir / "config_used.yaml")

    # 2) Copy deployment manifest snapshot (best-effort).
    # The sweeper writes repo_root/vllm-k8s.yaml = Helm rendered manifest before running the client.
    here = Path(__file__).resolve().parent
    candidates = [
        (here / "vllm-k8s.yaml").resolve(),
        (Path.cwd() / "vllm-k8s.yaml").resolve(),
    ]
    copied = False
    for c in candidates:
        if _copy_file_if_exists(c, exp_dir / "vllm-k8s.yaml"):
            copied = True
            break

    if not copied:
        (exp_dir / "vllm-k8s.yaml").write_text(
            "# WARN: vllm-k8s.yaml snapshot missing. Sweeper did not write repo_root/vllm-k8s.yaml.\n",
            encoding="utf-8",
        )

    # Prepare logger for per-request logs.
    logs_path = exp_dir / "logs.json"
    logger = ExperimentLogger(logs_path)
    logger.open()

    return exp_dir, logger
