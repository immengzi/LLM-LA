#!/usr/bin/env python3
# deploy_vllm.py
#
# Standalone vLLM deployer — decoupled from the sweep runner.
# Deploys ONLY the vLLM component (deploy.vllm=true, all others false)
# using the same client config YAML format as sweep_methods.py.
#
# Usage:
#   # Regular deployment (no mooncake)
#   python deploy_vllm.py --config configs/router-tp8.yaml
#
#   # Mooncake deployment (auto-detected from helm.mooncake_enabled in config)
#   python deploy_vllm.py --config configs/router-tp8-glm-mooncake.yaml
#
#   # Deploy into a separate namespace to avoid conflicts
#   python deploy_vllm.py --config configs/router-tp8-glm-mooncake.yaml \
#       --release vllm-moon --namespace vllm-moon --reinstall
#
# Mooncake is auto-detected from helm.mooncake_enabled in the config YAML.
# When enabled, deploy.mooncakeMaster=true and vllm.hostNetwork=true are set
# automatically, along with all mooncake.* Helm values.
#
# Once vLLM is up, run sweeps without restarting it:
#   python sweep_methods.py --config master_config.yaml --skip-vllm

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional

import click
import yaml

from config import load_config


REPO_ROOT = Path(__file__).resolve().parent
CONFIGS_DIR = REPO_ROOT / "configs"

DEFAULT_RELEASE = "vllm"
DEFAULT_NAMESPACE = "vllm"


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

    remaining = max(1, int(deadline - time.time()))
    try:
        _kubectl([
            "rollout", "status", "deployment/vllm-qwen",
            "-n", namespace,
            f"--timeout={remaining}s",
        ], check=True)
    except subprocess.CalledProcessError:
        click.echo("[warn] rollout status failed, falling through to pod wait...")

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
# Mooncake values builder
# ---------------------------

def _build_mooncake_values(h, set_values: Dict[str, object]) -> None:
    """Populate Helm set_values with Mooncake-specific config."""
    set_values["mooncake.enabled"] = True
    set_values["deploy.mooncakeMaster"] = True

    mooncake_fields = {
        "mooncake.masterServerAddress": ("mooncake_master_server_address", None),
        "mooncake.masterPort": ("mooncake_master_port", 50088),
        "mooncake.globalSegmentSize": ("mooncake_global_segment_size", 60000000000),
        "mooncake.ascendBufferPool": ("mooncake_ascend_buffer_pool", "4:8"),
        "mooncake.lookupRpcPort": ("mooncake_lookup_rpc_port", "10010"),
    }
    for helm_key, (cfg_attr, default) in mooncake_fields.items():
        val = getattr(h, cfg_attr, default)
        if val is not None:
            set_values[helm_key] = val

    mooncake_host_network = bool(getattr(h, "mooncake_host_network", True))
    set_values["vllm.hostNetwork"] = mooncake_host_network

    click.echo(f"[deploy-vllm] mooncake: hostNetwork={mooncake_host_network}")
    for k in sorted(set_values):
        if k.startswith("mooncake."):
            click.echo(f"[deploy-vllm] {k}={_coerce_set_value(set_values[k])}")


# ---------------------------
# vLLM runtime flags builder
# ---------------------------

def _build_vllm_runtime_values(h, set_values: Dict[str, object]) -> None:
    """Populate Helm set_values with vLLM runtime flags."""
    if getattr(h, "vllm_gpu_memory_utilization", None) is not None:
        set_values["vllm.gpuMemoryUtilization"] = float(h.vllm_gpu_memory_utilization)

    # CRITICAL: --quantization flag (e.g. "ascend" for W4A8 models)
    # Without this, vLLM creates weights in float16 → OOM on MoE models
    if getattr(h, "vllm_quantization", None) is not None:
        set_values["vllm.quantization"] = str(h.vllm_quantization)
        click.echo(f"[deploy-vllm] quantization={h.vllm_quantization}")

    # CRITICAL: --enable-expert-parallel (distributes MoE experts across cards)
    set_values["vllm.enableExpertParallel"] = bool(getattr(h, "vllm_enable_expert_parallel", False))
    if set_values["vllm.enableExpertParallel"]:
        click.echo("[deploy-vllm] expert-parallel=enabled")

    if getattr(h, "vllm_max_model_len", None) is not None:
        set_values["vllm.maxModelLen"] = int(h.vllm_max_model_len)
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


# ---------------------------
# Click CLI
# ---------------------------

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--config",
    "client_config",
    required=True,
    help="Client config YAML (e.g. configs/router-tp8.yaml).",
)
@click.option(
    "--reinstall",
    is_flag=True,
    default=False,
    help="Uninstall existing release before deploying (forces fresh pod creation).",
)
@click.option(
    "--timeout",
    "timeout_s",
    default=36000,
    show_default=True,
    help="Seconds to wait for vLLM pods to become Ready.",
)
@click.option(
    "--release",
    "release_name",
    default=None,
    help=f"Helm release name (default: '{DEFAULT_RELEASE}').",
)
@click.option(
    "--namespace", "-n",
    "namespace",
    default=None,
    help=f"Kubernetes namespace (default: '{DEFAULT_NAMESPACE}').",
)
def cli(
    client_config: str,
    reinstall: bool,
    timeout_s: int,
    release_name: Optional[str],
    namespace: Optional[str],
) -> None:
    """
    Deploy vLLM using an existing client config YAML.

    Mooncake is auto-detected from helm.mooncake_enabled in the config.
    Use --release and --namespace to deploy into a separate namespace.

    \b
    Examples:
      # Regular (no mooncake)
      python deploy_vllm.py --config router-tp8-glm

      # Mooncake (auto-detected), separate namespace
      python deploy_vllm.py --config router-tp8-glm-mooncake \\
          --release vllm-moon --namespace vllm-moon --reinstall
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
    mooncake_enabled = bool(getattr(h, "mooncake_enabled", False))

    rel = release_name or DEFAULT_RELEASE
    ns = namespace or DEFAULT_NAMESPACE

    click.echo(f"[deploy-vllm] config={cfg_path}")
    click.echo(f"[deploy-vllm] release={rel} namespace={ns}")
    click.echo(f"[deploy-vllm] mooncake_enabled={mooncake_enabled}")
    click.echo(f"[deploy-vllm] reinstall={reinstall}")

    if reinstall:
        click.echo(f"[deploy-vllm] uninstalling existing release {rel}...")
        _helm_uninstall(release=rel, namespace=ns)
        time.sleep(5)

    # ---------------------------------------------------------------
    # Base set_values
    # ---------------------------------------------------------------
    set_values: Dict[str, object] = {
        "backend": backend,
        "replicas.vllm": int(h.replicas),
        "batchSize": int(h.batch_size),
        "tensorParallelSize": int(getattr(h, "tensor_parallel_size", 1)),

        "deploy.vllm": True,
        "deploy.router": False,
        "deploy.redis": False,
        "deploy.cpuHash": False,
        "deploy.mooncakeMaster": mooncake_enabled,

        "aibrix.enabled": bool(getattr(h, "aibrix_enabled", False)),
        "aibrix.modelName": str(getattr(h, "aibrix_model_name", "served-model")),
        "aibrix.port": int(getattr(h, "aibrix_port", 8200)),

        "mooncake.enabled": mooncake_enabled,
    }

    # ---------------------------------------------------------------
    # Mooncake (conditional on config)
    # ---------------------------------------------------------------
    if mooncake_enabled:
        _build_mooncake_values(h, set_values)

    # ---------------------------------------------------------------
    # Model volume
    # ---------------------------------------------------------------
    nfs_path = str(getattr(h, "nfs_path", "")).strip()
    if nfs_path:
        model_subpath = PurePosixPath(nfs_path).name
        set_values["modelVolume.modelSubPath"] = model_subpath
        set_values["modelVolume.create"] = True
        click.echo(f"[deploy-vllm] modelVolume.modelSubPath={model_subpath} (derived from nfs_path)")
    else:
        raise click.ClickException(
            "helm.nfs_path must be set in the client config to derive modelVolume.modelSubPath. "
            "Example: nfs_path: /saeid/models/GLM-5-w4a8-mtp-QuaRot"
        )

    # ---------------------------------------------------------------
    # vLLM runtime flags
    # ---------------------------------------------------------------
    _build_vllm_runtime_values(h, set_values)

    # ---------------------------------------------------------------
    # Deploy
    # ---------------------------------------------------------------
    click.echo("[deploy-vllm] set values:")
    for k in sorted(set_values):
        click.echo(f"  - {k}={_coerce_set_value(set_values[k])}")

    _helm_install_or_upgrade(
        release=rel,
        chart_dir=chart_dir,
        namespace=ns,
        values_file=values_file if values_file.is_file() else None,
        set_values=set_values,
    )

    click.echo(f"[deploy-vllm] waiting for vllm-qwen pods Ready (timeout={timeout_s}s)...")
    try:
        _wait_vllm_ready(ns, timeout_s=float(timeout_s))
        click.echo("[deploy-vllm] vLLM is Ready.")
        click.echo("[deploy-vllm] You can now run sweeps without restarting vLLM:")
        click.echo(f"[deploy-vllm]   python sweep_methods.py --config master_config.yaml --skip-vllm")
    except subprocess.CalledProcessError as e:
        click.echo(f"[deploy-vllm] ERROR: wait failed: {e}")
        _debug_wait_failure(ns)
        sys.exit(1)


if __name__ == "__main__":
    cli()