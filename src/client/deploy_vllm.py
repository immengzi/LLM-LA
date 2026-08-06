#!/usr/bin/env python3
# deploy_vllm.py
#
# Standalone vLLM deployer — decoupled from the sweep runner.
# Deploys ONLY the vLLM component (deploy.vllm=true, all others false)
# using the same client config YAML format as sweep_methods.py.
#
# Usage:
#   python src/client/deploy_vllm.py --config configs/router-tp8.yaml
#
# Uses the same Helm release "vllm" as sweep_methods.py to avoid RBAC
# ownership conflicts. deploy.vllm=true, deploy.router/redis/cpuHash=false
# means only vLLM pods are created/updated.
#
# Once vLLM is up, run sweeps without restarting it:
#   python src/client/sweep_methods.py --config master_config.yaml --skip-vllm

from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional

import click
import yaml

from config import load_config, migrate_legacy_helm_to_models


CLIENT_DIR = Path(__file__).resolve().parent          # src/client
SRC_DIR = CLIENT_DIR.parent                            # src
REPO_ROOT = SRC_DIR.parent                             # repo root
CONFIGS_DIR = CLIENT_DIR / "configs"
CHART_DIR = SRC_DIR / "core" / "vllm-kv-stack"

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


def _flatten_helm_values(prefix: str, value) -> Dict[str, object]:
    """Flatten a nested helm.values mapping into Helm --set dot paths.

    Mirrors sweep_methods._flatten_helm_values so a config's helm.values
    (typically supplied per cluster via configs/clusters.yaml) apply the same
    way for a standalone vLLM deploy — e.g. hardware: nvidia, images.*.
    """
    if not isinstance(value, dict):
        return {prefix: value} if prefix else {}

    flattened: Dict[str, object] = {}
    for key, child in value.items():
        if not isinstance(key, str) or not key.strip():
            raise click.ClickException(f"Invalid helm.values key: {key!r}")
        path = f"{prefix}.{key}" if prefix else key
        flattened.update(_flatten_helm_values(path, child))
    return flattened


def _helm_uninstall(*, release: str, namespace: str) -> None:
    _helm(["uninstall", release, "-n", namespace], check=False, capture=True)


def _helm_install_or_upgrade(
    *,
    release: str,
    chart_dir: Path,
    namespace: str,
    values_file: Optional[Path],
    set_values: Dict[str, object],
    extra_values_files: Optional[List[Path]] = None,
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
    for evf in (extra_values_files or []):
        if evf is not None and evf.is_file():
            cmd.extend(["-f", str(evf)])

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
            p = CLIENT_DIR / p
        else:
            p = CONFIGS_DIR / p
    return p.resolve()


def _wait_vllm_ready(
    namespace: str,
    model_name: str = "qwen",
    timeout_s: float = 36000,
    *,
    engine_type: str = "vllm",
) -> None:
    """Wait for <engine>-{model_name} pods to be Ready."""
    engine = str(engine_type or "vllm").strip().lower() or "vllm"
    deploy_name = f"{engine}-{model_name}"
    deadline = time.time() + float(timeout_s)

    remaining = max(1, int(deadline - time.time()))
    try:
        _kubectl([
            "rollout", "status", f"deployment/{deploy_name}",
            "-n", namespace,
            f"--timeout={remaining}s",
        ], check=True)
    except subprocess.CalledProcessError:
        click.echo(f"[warn] rollout status for {deploy_name} failed, falling through to pod wait...")

    remaining = max(1, int(deadline - time.time()))
    _kubectl([
        "wait", "-n", namespace,
        "--for=condition=Ready", "pod",
        "-l", f"app={deploy_name}",
        f"--timeout={remaining}s",
    ], check=True)


def _debug_wait_failure(namespace: str) -> None:
    """Best-effort diagnostics when wait fails."""
    try:
        click.echo("[diag] vllm pods (wide):")
        out = _kubectl([
            "get", "pods", "-n", namespace, "-l", "model", "-o", "wide"
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
        python src/client/sweep_methods.py --config master_config.yaml --skip-vllm
    """
    cfg_path = _resolve_config_path(client_config)
    if not cfg_path.is_file():
        raise click.ClickException(f"Config not found: {cfg_path}")

    chart_dir = CHART_DIR.resolve()
    values_file = chart_dir / "values.yaml"
    if not chart_dir.is_dir():
        raise click.ClickException(f"Chart dir not found: {chart_dir}")

    cfg = load_config(str(cfg_path))
    h = getattr(cfg, "helm", None)
    if h is None:
        raise click.ClickException(f"Config has no 'helm' section: {cfg_path}")
    engine_type = str(getattr(h, "engine_type", "vllm") or "vllm").strip().lower()
    if engine_type not in ("vllm", "sglang"):
        raise click.ClickException(
            f"Invalid engine_type {engine_type!r}; expected 'vllm' or 'sglang'"
        )
    service_impl = str(
        getattr(h, "service_impl", "python") or "python"
    ).strip().lower()
    if service_impl not in ("python", "go"):
        raise click.ClickException(
            f"Invalid service_impl {service_impl!r}; expected 'python' or 'go'"
        )
    if engine_type == "sglang":
        unsupported = []
        if bool(getattr(h, "sglang_trust_remote_code", False)):
            unsupported.append("sglang_trust_remote_code")
        for model in list(getattr(h, "models", None) or []):
            if bool((model.get("sglang") or {}).get("trustRemoteCode", False)):
                unsupported.append("models[].sglang.trustRemoteCode")
                break
        if bool(getattr(h, "data_parallel_enabled", False)):
            unsupported.append("data_parallel/LWS")
        if bool(getattr(h, "mooncake_enabled", False)):
            unsupported.append("Mooncake")
        if bool(getattr(h, "lmcache_enabled", False)):
            unsupported.append("LMCache")
        if unsupported:
            raise click.ClickException(
                "Unsupported SGLang engine-only deployment: " + ", ".join(unsupported)
            )

    backend = str(getattr(cfg, "backend", "router") or "router").strip().lower()

    click.echo(f"[deploy-vllm] config={cfg_path}")
    click.echo(f"[deploy-vllm] release={RELEASE_VLLM} namespace={NAMESPACE}")
    click.echo(f"[deploy-vllm] reinstall={reinstall}")

    if reinstall:
        click.echo(f"[deploy-vllm] uninstalling existing release {RELEASE_VLLM}...")
        _helm_uninstall(release=RELEASE_VLLM, namespace=NAMESPACE)
        time.sleep(5)

    # ---- Unified models[] — auto-migrate legacy flat config if needed ----
    migrate_legacy_helm_to_models(h)
    models_list = list(h.models or [])
    if not models_list:
        raise click.ClickException(f"No models defined in {cfg_path}")

    first_model = models_list[0]
    model_names = [str(model.get("name", "qwen")) for model in models_list]

    set_values: Dict[str, object] = {
        "backend": backend,
        "engine.type": engine_type,
        "serviceImpl": service_impl,
        "replicas.vllm": int(first_model.get("replicas", h.replicas)),
        "batchSize": int(first_model.get("batchSize", h.batch_size)),
        "tensorParallelSize": int(first_model.get("tensorParallelSize", h.tensor_parallel_size)),
        "deploy.vllm": True,
        "deploy.router": False,
        "deploy.redis": False,
        "deploy.cpuHash": False,
        "modelVolume.create": False,
        "aibrix.enabled": bool(getattr(h, "aibrix_enabled", False)),
        "aibrix.modelName": str(getattr(h, "aibrix_model_name", "served-model")),
        "aibrix.port": int(getattr(h, "aibrix_port", 8200)),
    }

    model_sub_path = first_model.get("modelSubPath", "")
    nfs_path = str(getattr(h, "nfs_path", "")).strip()
    if not model_sub_path and nfs_path:
        model_sub_path = PurePosixPath(nfs_path.rstrip("/")).name
    if model_sub_path:
        set_values["modelVolume.modelSubPath"] = model_sub_path

    model_host_path = str(getattr(h, "model_host_path", "")).strip()
    if model_host_path:
        set_values["modelVolume.hostPath"] = model_host_path

    # Explicit Helm dot-path overlay (helm.values), e.g. the per-cluster
    # hardware switch and image registry from configs/clusters.yaml. Config
    # values win over the generated defaults above, matching sweep_methods.py.
    raw_helm_values = getattr(h, "values", {}) or {}
    if raw_helm_values:
        if not isinstance(raw_helm_values, dict):
            raise click.ClickException("helm.values must be a mapping of Helm dot-paths to values")
        explicit_values = _flatten_helm_values("", raw_helm_values)
        set_values.update(explicit_values)
        click.echo(f"[deploy-vllm] applied {len(explicit_values)} explicit helm.values override(s)")

    _models_tmp = tempfile.NamedTemporaryFile(
        mode="w", prefix="deploy_models_", suffix=".yaml",
        delete=False, encoding="utf-8",
    )
    yaml.safe_dump({"models": models_list}, _models_tmp, sort_keys=False)
    _models_tmp.close()
    _models_values_file = Path(_models_tmp.name)

    click.echo("[deploy-vllm] set values:")
    for k in sorted(set_values):
        click.echo(f"  - {k}={_coerce_set_value(set_values[k])}")
    click.echo(f"[deploy-vllm] models overlay: {_models_values_file}")

    _helm_install_or_upgrade(
        release=RELEASE_VLLM,
        chart_dir=chart_dir,
        namespace=NAMESPACE,
        values_file=values_file if values_file.is_file() else None,
        set_values=set_values,
        extra_values_files=[_models_values_file],
    )

    try:
        _models_values_file.unlink(missing_ok=True)
    except Exception:
        pass

    try:
        for model_name in model_names:
            click.echo(
                f"[deploy-vllm] waiting for {engine_type}-{model_name} pods Ready "
                f"(timeout={timeout_s}s)..."
            )
            _wait_vllm_ready(
                NAMESPACE,
                model_name=model_name,
                timeout_s=float(timeout_s),
                engine_type=engine_type,
            )
        click.echo(f"[deploy-vllm] {engine_type} is Ready.")
        click.echo("[deploy-vllm] You can now run sweeps without restarting the engine:")
        click.echo("[deploy-vllm]   python src/client/sweep_methods.py --config master_config.yaml --skip-vllm")
    except subprocess.CalledProcessError as e:
        click.echo(f"[deploy-vllm] ERROR: wait failed: {e}")
        _debug_wait_failure(NAMESPACE)
        sys.exit(1)


if __name__ == "__main__":
    cli()
