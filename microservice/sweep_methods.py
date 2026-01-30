#!/usr/bin/env python3
# sweep_methods.py
#
# Helm-based sweep runner with same external interface/behavior as the old YAML-patching sweeper:
# - Reads configs/<master_config>.yaml mapping: { client_config: [methods...] }
# - BEFORE EACH EXPERIMENT: clean cluster (helm uninstall, ignore errors)
# - Deploy via Helm with per-experiment knobs read from the client config YAML:
#     cfg.helm.replicas, cfg.helm.batch_size, cfg.helm.autoscaling_* and cfg.helm.autoscaling_prometheus_query
# - Also sets router mode per job (method)
# - Waits for readiness
# - Writes repo_root/vllm-k8s.yaml = helm template output so experiment snapshot stays identical
# - Runs main.py unchanged and snapshots sweep_meta.json

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click
import yaml

from config import load_config  # <-- NEW: to read cfg.helm from each client config


REPO_ROOT = Path(__file__).resolve().parent
CONFIGS_DIR = REPO_ROOT / "configs"
EXPERIMENTS_ROOT = REPO_ROOT / "experiments"


# ---------------------------
# subprocess helpers
# ---------------------------

def _run(cmd: List[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    click.echo(f"[cmd] {' '.join(cmd)}")
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def _kubectl(args: List[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return _run(["kubectl", *args], check=check, capture=capture)


def _helm(args: List[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return _run(["helm", *args], check=check, capture=capture)


# ---------------------------
# experiment dir detection
# ---------------------------

def _snapshot_existing_experiments() -> set[str]:
    if not EXPERIMENTS_ROOT.exists():
        return set()
    return {p.name for p in EXPERIMENTS_ROOT.iterdir() if p.is_dir() and p.name.isdigit()}


def _newest_experiment_dir(before: set[str]) -> Optional[Path]:
    if not EXPERIMENTS_ROOT.exists():
        return None
    after = {p.name for p in EXPERIMENTS_ROOT.iterdir() if p.is_dir() and p.name.isdigit()}
    new = sorted(list(after - before), key=lambda s: int(s))
    if not new:
        return None
    return EXPERIMENTS_ROOT / new[-1]


# ---------------------------
# master plan parsing
# ---------------------------

def _resolve_master_path(master_config: str) -> Path:
    p = Path(master_config).expanduser()
    if p.suffix == "":
        p = p.with_suffix(".yaml")

    if not p.is_absolute():
        parts = p.parts
        if parts and parts[0] == "configs":
            p = REPO_ROOT / p
        else:
            p = CONFIGS_DIR / p

    return p.resolve()


def _resolve_client_config_path(key: str) -> Path:
    p = Path(key).expanduser()
    if p.suffix == "":
        p = p.with_suffix(".yaml")

    if not p.is_absolute():
        parts = p.parts
        if parts and parts[0] == "configs":
            p = REPO_ROOT / p
        else:
            p = CONFIGS_DIR / p

    return p.resolve()


def _load_master_plan(master_path: Path) -> Dict[Path, List[str]]:
    raw = yaml.safe_load(master_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict) or not raw:
        raise RuntimeError("master_config.yaml must be a non-empty mapping: {config: [methods...] }")

    plan: Dict[Path, List[str]] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not k.strip():
            raise RuntimeError(f"Invalid config key: {k!r}")
        if not isinstance(v, list) or not all(isinstance(x, str) and x.strip() for x in v):
            raise RuntimeError(f"Invalid methods list for {k!r}. Expected a YAML list of strings.")

        cfg_path = _resolve_client_config_path(k)
        methods = [x.strip() for x in v]
        plan[cfg_path] = methods

    return plan


# ---------------------------
# Helm helpers
# ---------------------------

def _coerce_set_value(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return None
    return str(v)


def _helm_template(
    *,
    release: str,
    chart_dir: Path,
    namespace: str,
    values_file: Optional[Path],
    set_values: Dict[str, object],
) -> str:
    cmd: List[str] = [
        "template",
        release,
        str(chart_dir),
        "--namespace",
        namespace,
    ]
    if values_file is not None and values_file.is_file():
        cmd.extend(["-f", str(values_file)])

    for k in sorted(set_values.keys()):
        vs = _coerce_set_value(set_values[k])
        if vs is None:
            continue
        cmd.extend(["--set", f"{k}={vs}"])

    proc = _helm(cmd, check=True, capture=True)
    return proc.stdout or ""


def _helm_uninstall(*, release: str, namespace: str) -> None:
    _helm(["uninstall", release, "-n", namespace], check=False, capture=True)


def _helm_install_or_upgrade(
    *,
    release: str,
    chart_dir: Path,
    namespace: str,
    values_file: Optional[Path],
    set_values: Dict[str, object],
) -> None:
    cmd: List[str] = [
        "upgrade",
        "--install",
        release,
        str(chart_dir),
        "-n",
        namespace,
        "--create-namespace",
    ]
    if values_file is not None and values_file.is_file():
        cmd.extend(["-f", str(values_file)])

    for k in sorted(set_values.keys()):
        vs = _coerce_set_value(set_values[k])
        if vs is None:
            continue
        cmd.extend(["--set", f"{k}={vs}"])

    _helm(cmd, check=True, capture=False)


def _wait_ready(namespace: str, timeout_s: float = 900.0) -> None:
    deadline = time.time() + float(timeout_s)

    for kind in ("deploy", "sts", "ds"):
        try:
            out = _kubectl(["get", kind, "-n", namespace, "-o", "name"], capture=True).stdout or ""
        except subprocess.CalledProcessError:
            continue

        names = [ln.strip() for ln in out.splitlines() if ln.strip()]
        for name in names:
            remaining = max(1, int(deadline - time.time()))
            try:
                _kubectl(["rollout", "status", name, "-n", namespace, f"--timeout={remaining}s"], check=True)
            except subprocess.CalledProcessError:
                click.echo(f"[warn] rollout status failed for {name}, continuing...")

    remaining = max(1, int(deadline - time.time()))
    _kubectl(
        ["wait", "-n", namespace, "--for=condition=Ready", "pod", "--all", f"--timeout={remaining}s"],
        check=True,
    )


# ---------------------------
# run client
# ---------------------------

def _run_client(config_path: Path) -> None:
    _run([sys.executable, str(REPO_ROOT / "main.py"), "--config", str(config_path)], check=True)


# ---------------------------
# Click CLI (same interface)
# ---------------------------

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--config",
    "master_config",
    default="1-master_config",
    show_default=True,
    help="Master sweep config file (suffix .yaml optional; relative to configs/ or absolute path).",
)
def cli(master_config: str) -> None:
    """
    Reads configs/<master_config>.yaml (mapping: config -> methods),
    cleans cluster before each experiment, deploys via Helm, then runs main.py.
    """
    master_path = _resolve_master_path(master_config)
    if not master_path.is_file():
        raise click.ClickException(f"Missing {master_path}")

    plan = _load_master_plan(master_path)
    for cfg in plan.keys():
        if not cfg.is_file():
            raise click.ClickException(f"Client config not found: {cfg}")

    # HELM defaults (hardcoded)
    release = "vllm"
    namespace = "vllm"
    chart_dir = (REPO_ROOT / "vllm-kv-stack").resolve()
    values_file = chart_dir / "values.yaml"
    if not chart_dir.is_dir():
        raise click.ClickException(f"Chart dir not found: {chart_dir}")

    jobs: List[Tuple[Path, str]] = []
    for cfg, methods in plan.items():
        for m in methods:
            jobs.append((cfg, m))

    click.echo(f"[sweep] master_config={master_path}")
    click.echo(f"[sweep] chart_dir={chart_dir}")
    click.echo(f"[sweep] release={release} namespace={namespace}")
    click.echo(f"[sweep] jobs={len(jobs)}")

    for i, (cfg_path, method) in enumerate(jobs, start=1):
        click.echo("\n" + "=" * 90)
        click.echo(f"[sweep] job {i}/{len(jobs)}  config={cfg_path.name}  method={method}")
        click.echo("=" * 90)

        # Load client config to read Helm knobs (NEW)
        cfg = load_config(str(cfg_path))
        h = getattr(cfg, "helm", None)
        if h is None:
            raise click.ClickException(f"Config has no 'helm' section: {cfg_path}")

        # ---- EXACT OLD BEHAVIOR: clean cluster before each experiment ----
        _helm_uninstall(release=release, namespace=namespace)

        # ---- Deploy via Helm using cfg.helm knobs ----
        #
        # IMPORTANT: these keys must match your chart values.yaml!
        set_values: Dict[str, object] = {
            # routing method for router
            "router.mode": method,

            # init replicas (needed even if autoscaling is enabled)
            "vllm.replicas": int(h.replicas),

            # sidecar batching
            "sidecar.batchSize": int(h.batch_size),
        }

        # Autoscaling toggle + parameters
        set_values["autoscaling.enabled"] = bool(h.autoscaling_enabled)

        if bool(h.autoscaling_enabled):
            set_values["autoscaling.minReplicaCount"] = int(h.autoscaling_min)
            set_values["autoscaling.maxReplicaCount"] = int(h.autoscaling_max)
            set_values["autoscaling.threshold"] = str(h.autoscaling_threshold)

            # Query can be multiline; helm --set needs a single line.
            # We normalize whitespace while preserving semantics.
            q = str(h.autoscaling_prometheus_query or "").strip()
            q = " ".join(q.split())
            set_values["autoscaling.prometheusQuery"] = q

        _helm_install_or_upgrade(
            release=release,
            chart_dir=chart_dir,
            namespace=namespace,
            values_file=values_file if values_file.is_file() else None,
            set_values=set_values,
        )

        _wait_ready(namespace)

        # Render and snapshot manifest (same artifact name)
        rendered_text = _helm_template(
            release=release,
            chart_dir=chart_dir,
            namespace=namespace,
            values_file=values_file if values_file.is_file() else None,
            set_values=set_values,
        )
        (REPO_ROOT / "vllm-k8s.yaml").write_text(rendered_text, encoding="utf-8")

        before = _snapshot_existing_experiments()
        _run_client(cfg_path)
        exp_dir = _newest_experiment_dir(before)

        if exp_dir is None:
            click.echo("[warn] could not detect new experiment dir; skipping artifact snapshot")
            continue

        shutil.copy2(REPO_ROOT / "vllm-k8s.yaml", exp_dir / "vllm-k8s.yaml")
        meta = {
            "client_config": str(cfg_path),
            "router_method": method,
            "master_config": str(master_path),
            "ts_unix": time.time(),
            "helm_release": release,
            "helm_namespace": namespace,
            "helm_chart_dir": str(chart_dir),
            "helm_set_values": set_values,
            "helm_knobs_from_config": {
                "replicas": int(h.replicas),
                "batch_size": int(h.batch_size),
                "autoscaling_enabled": bool(h.autoscaling_enabled),
                "autoscaling_min": int(h.autoscaling_min),
                "autoscaling_max": int(h.autoscaling_max),
                "autoscaling_threshold": str(h.autoscaling_threshold),
                "autoscaling_prometheus_query": str(h.autoscaling_prometheus_query),
            },
        }
        (exp_dir / "sweep_meta.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
        )

        click.echo(f"[sweep] experiment_dir={exp_dir}")

    click.echo("\n[sweep] done.")


if __name__ == "__main__":
    cli()
