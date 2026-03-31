#!/usr/bin/env python3
# deploy_vllm.py
#
# Standalone vLLM deployer — decoupled from the sweep runner.
# Deploys ONLY the vLLM component (deploy.vllm=true, all others false)
# using the same client config YAML format as sweep_methods.py.
#
# Usage:
#   python deploy_vllm.py --config configs/router-tp8.yaml
#
# Uses the same Helm release "vllm" as sweep_methods.py to avoid RBAC
# ownership conflicts. deploy.vllm=true, deploy.router/redis/cpuHash=false
# means only vLLM pods are created/updated.
#
# Once vLLM is up, run sweeps without restarting it:
#   python sweep_methods.py --config master_config.yaml --skip-vllm

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import click
import yaml

from config import load_config


REPO_ROOT = Path(__file__).resolve().parent
CONFIGS_DIR = REPO_ROOT / "configs"

RELEASE_VLLM = "vllm"   # same release as sweep_methods.py — avoids RBAC ownership conflicts
NAMESPACE = "vllm"


# ---------------------------
# subprocess helpers (same as sweep_methods.py)
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
# Helm helpers
# ---------------------------

def _coerce_set_value(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return None
    return str(v)


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


def _resolve_config_path(config: str) -> Path:
    p = Path(config).expanduser()
    if p.suffix == "":
        p = p.with_suffix(".yaml")
    if not p.is_absolute():
        parts = p.parts
        if parts and parts[0] == "configs":
            p = REPO_ROOT / p
        else:
            p = CONFIGS_DIR / p
    return p.resolve()


def _wait_vllm_ready(namespace: str, timeout_s: float = 36000) -> None:
    """Wait for all vllm-qwen pods to be Ready."""
    deadline = time.time() + float(timeout_s)

    # Wait for rollout
    remaining = max(1, int(deadline - time.time()))
    try:
        _kubectl([
            "rollout", "status", "deployment/vllm-qwen",
            "-n", namespace,
            f"--timeout={remaining}s",
        ], check=True)
    except subprocess.CalledProcessError:
        click.echo("[warn] rollout status failed, falling through to pod wait...")

    # Wait for pods Ready
    remaining = max(1, int(deadline - time.time()))
    _kubectl([
        "wait", "-n", namespace,
        "--for=condition=Ready", "pod",
        "-l", "app=vllm-qwen",
        f"--timeout={remaining}s",
    ], check=True)


def _debug_wait_failure(namespace: str) -> None:
    """Best-effort diagnostics when wait fails."""
    try:
        click.echo("[diag] vllm-qwen pods (wide):")
        out = _kubectl([
            "get", "pods", "-n", namespace, "-l", "app=vllm-qwen", "-o", "wide"
        ], check=False, capture=True).stdout or ""
        click.echo(out.strip())
    except Exception:
        pass

    try:
        click.echo("\n[diag] recent events:")
        ev = _kubectl([
            "get", "events", "-n", namespace, "--sort-by=.lastTimestamp"
        ], check=False, capture=True).stdout or ""
        lines = ev.splitlines()
        click.echo("\n".join(lines[-100:]))
    except Exception:
        pass


# ---------------------------
# Click CLI
# ---------------------------

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--config",
    "client_config",
    required=True,
    help="Client config YAML (same format as sweep_methods.py, e.g. configs/router-tp8.yaml).",
)
@click.option(
    "--reinstall",
    is_flag=True,
    default=False,
    help="Uninstall existing vllm release before deploying (forces fresh pod creation).",
)
@click.option(
    "--timeout",
    "timeout_s",
    default=36000,
    show_default=True,
    help="Seconds to wait for vLLM pods to become Ready (default 10 hours).",
)
def cli(client_config: str, reinstall: bool, timeout_s: int) -> None:
    """
    Deploy vLLM only using an existing client config YAML.

    Uses the same Helm release 'vllm' as sweep_methods.py.
    Sets deploy.vllm=true, deploy.router=false, deploy.redis=false, deploy.cpuHash=false.

    After this completes, run sweeps without restarting vLLM:
        python sweep_methods.py --config master_config.yaml --skip-vllm
    """
    cfg_path = _resolve_config_path(client_config)
    if not cfg_path.is_file():
        raise click.ClickException(f"Config not found: {cfg_path}")

    chart_dir = (REPO_ROOT / "vllm-kv-stack").resolve()
    values_file = chart_dir / "values.yaml"
    if not chart_dir.is_dir():
        raise click.ClickException(f"Chart dir not found: {chart_dir}")

    cfg = load_config(str(cfg_path))
    h = getattr(cfg, "helm", None)
    if h is None:
        raise click.ClickException(f"Config has no 'helm' section: {cfg_path}")

    backend = str(getattr(cfg, "backend", "router") or "router").strip().lower()

    click.echo(f"[deploy-vllm] config={cfg_path}")
    click.echo(f"[deploy-vllm] release={RELEASE_VLLM} namespace={NAMESPACE}")
    click.echo(f"[deploy-vllm] reinstall={reinstall}")

    if reinstall:
        click.echo(f"[deploy-vllm] uninstalling existing release {RELEASE_VLLM}...")
        _helm_uninstall(release=RELEASE_VLLM, namespace=NAMESPACE)
        time.sleep(5)

    # Build set_values — same vLLM knobs as sweep_methods.py, but deploy flags are vllm-only
    set_values: Dict[str, object] = {
        "backend": backend,
        "replicas.vllm": int(h.replicas),
        "batchSize": int(h.batch_size),
        "tensorParallelSize": int(getattr(h, "tensor_parallel_size", 1)),

        # Deploy flags — vLLM only
        "deploy.vllm": True,
        "deploy.router": False,
        "deploy.redis": False,
        "deploy.cpuHash": False,

        # AIBrix labels (pass through in case aibrix is enabled)
        "aibrix.enabled": bool(getattr(h, "aibrix_enabled", False)),
        "aibrix.modelName": str(getattr(h, "aibrix_model_name", "served-model")),
        "aibrix.port": int(getattr(h, "aibrix_port", 8200)),
    }

    # vLLM runtime flags — same logic as sweep_methods.py
    if getattr(h, "vllm_gpu_memory_utilization", None) is not None:
        set_values["vllm.gpuMemoryUtilization"] = float(h.vllm_gpu_memory_utilization)
    if getattr(h, "vllm_quantization", None) is not None:
        set_values["vllm.quantization"] = str(h.vllm_quantization)
    set_values["vllm.enableExpertParallel"] = bool(getattr(h, "vllm_enable_expert_parallel", True))
    if getattr(h, "vllm_compilation_config", None) is not None:
        try:
            cc = json.loads(h.vllm_compilation_config)
            set_values["vllm.compilationConfig.cudagraphMode"] = cc.get("cudagraph_mode", "FULL_DECODE_ONLY")
        except Exception:
            set_values["vllm.compilationConfig.cudagraphMode"] = "FULL_DECODE_ONLY"
    set_values["vllm.trustRemoteCode"] = bool(getattr(h, "vllm_trust_remote_code", False))
    if getattr(h, "vllm_max_num_batched_tokens", None) is not None:
        set_values["vllm.maxNumBatchedTokens"] = int(h.vllm_max_num_batched_tokens)
    if getattr(h, "vllm_seed", None) is not None:
        set_values["vllm.seed"] = int(h.vllm_seed)
    if getattr(h, "vllm_additional_config", None) is not None:
        set_values["vllm.additionalConfig"] = str(h.vllm_additional_config)
    if getattr(h, "vllm_speculative_config", None) is not None:
        set_values["vllm.speculativeConfig"] = str(h.vllm_speculative_config)

    click.echo("[deploy-vllm] set values:")
    for k in sorted(set_values):
        click.echo(f"  - {k}={_coerce_set_value(set_values[k])}")

    _helm_install_or_upgrade(
        release=RELEASE_VLLM,
        chart_dir=chart_dir,
        namespace=NAMESPACE,
        values_file=values_file if values_file.is_file() else None,
        set_values=set_values,
    )

    click.echo(f"[deploy-vllm] waiting for vllm-qwen pods Ready (timeout={timeout_s}s)...")
    try:
        _wait_vllm_ready(NAMESPACE, timeout_s=float(timeout_s))
        click.echo("[deploy-vllm] vLLM is Ready.")
        click.echo("[deploy-vllm] You can now run sweeps without restarting vLLM:")
        click.echo("[deploy-vllm]   python sweep_methods.py --config master_config.yaml --skip-vllm")
    except subprocess.CalledProcessError as e:
        click.echo(f"[deploy-vllm] ERROR: wait failed: {e}")
        _debug_wait_failure(NAMESPACE)
        sys.exit(1)


if __name__ == "__main__":
    cli()
