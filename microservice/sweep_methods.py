#!/usr/bin/env python3
# sweep_methods.py
#
# Helm-based sweep runner with same external interface/behavior as the old YAML-patching sweeper:
# - Reads configs/<master_config>.yaml mapping: { client_config: [methods...] }
# - BEFORE EACH EXPERIMENT: clean cluster (helm uninstall, ignore errors)
# - Deploy via Helm with per-experiment knobs read from the client config YAML:
#     cfg.helm.replicas, cfg.helm.batch_size, cfg.helm.autoscaling_* and cfg.helm.autoscaling_prometheus_query
# - Interprets "method" based on backend:
#     backend=router  -> router.mode = method
#     backend=aibrix -> aibrix.routing_strategy = method
#     backend=litellm -> no method override (litellm has no routing strategy knob;
#                        method is stored in sweep_meta.json for labelling only)
# - Waits for readiness
# - Writes repo_root/vllm-k8s.yaml = helm template output so experiment snapshot stays identical
# - Runs main.py using a temporary per-job config and snapshots sweep_meta.json
#
# --skip-vllm mode:
# - Uses the same release "vllm" to avoid RBAC ownership conflicts
# - Checks if vllm-qwen pods are already running; sets deploy.vllm=true if so
#   (prevents Helm from deleting them during upgrade)
# - Only redeploys router + redis + cpu-hash
# - vLLM must already be running (deployed via deploy_vllm.py)
#
# PV/PVC lifecycle:
# - PV and PVC are deployed ONCE externally (via deploy_vllm.py or manually) on the
#   parent NFS directory (e.g. /saeid/models/).
# - The sweep never touches modelVolume.create — it always sets it to False so Helm
#   never attempts to create or reconcile the PV/PVC.
# - Per-experiment model selection is done via modelVolume.modelSubPath, derived from
#   the last path component of cfg.helm.nfs_path (e.g. /saeid/models/glm5 -> "glm5").

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
import tempfile
from dataclasses import asdict
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Tuple

import click
import yaml

from config import load_config


REPO_ROOT = Path(__file__).resolve().parent
CONFIGS_DIR = REPO_ROOT / "configs"
EXPERIMENTS_ROOT = REPO_ROOT / "experiments"

# Single release name for all modes — avoids RBAC ownership conflicts
RELEASE = "vllm"


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
# temp config helper
# ---------------------------

def _write_temp_job_config(cfg, method: str) -> Path:
    """
    Write a per-job temporary config so main.py sees the correct method for the chosen backend.

    backend=router  -> method applied only through Helm router.mode (no config override needed)
    backend=aibrix  -> override cfg.aibrix.routing_strategy = method
    backend=litellm -> no method override; method is a label only (stored in sweep_meta.json)
                       litellm has no routing strategy knob — routing decisions happen inside
                       the router, which is already configured via Helm router.mode separately.
    backend=boom    -> same as litellm (label only)
    """
    cfg_dict = asdict(cfg)
    backend = str(cfg_dict.get("backend", "router") or "router").strip().lower()

    if backend == "aibrix":
        cfg_dict.setdefault("aibrix", {})
        cfg_dict["aibrix"]["routing_strategy"] = method

    # backend=litellm/boom: no config mutation needed — method is informational only

    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        prefix="sweep_job_",
        suffix=".yaml",
        delete=False,
        encoding="utf-8",
    )
    try:
        yaml.safe_dump(cfg_dict, tmp, sort_keys=False)
        tmp_path = Path(tmp.name)
    finally:
        tmp.close()

    return tmp_path


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
    timeout: str = "240m",
) -> None:
    cmd: List[str] = [
        "upgrade",
        "--install",
        release,
        str(chart_dir),
        "-n",
        namespace,
        "--create-namespace",
        "--timeout",
        timeout,
    ]
    if values_file is not None and values_file.is_file():
        cmd.extend(["-f", str(values_file)])

    for k in sorted(set_values.keys()):
        vs = _coerce_set_value(set_values[k])
        if vs is None:
            continue
        cmd.extend(["--set", f"{k}={vs}"])

    _helm(cmd, check=True, capture=False)


def _wait_ready(namespace: str, timeout_s: float = 36000, label_selector: Optional[str] = None) -> None:
    deadline = time.time() + float(timeout_s)

    for kind in ("deploy", "sts", "ds"):
        try:
            cmd = ["get", kind, "-n", namespace, "-o", "name"]
            if label_selector:
                cmd.extend(["-l", label_selector])
            out = _kubectl(cmd, capture=True).stdout or ""
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
    wait_cmd = ["wait", "-n", namespace, "--for=condition=Ready", "pod", f"--timeout={remaining}s"]
    if label_selector:
        wait_cmd.extend(["-l", label_selector])
    else:
        wait_cmd.append("--all")
    _kubectl(wait_cmd, check=True)


# ---------------------------
# vLLM pod detection
# ---------------------------

def _vllm_pods_exist(namespace: str) -> bool:
    """Return True if any vllm-qwen pods exist (any phase) in the namespace."""
    try:
        out = _kubectl(
            ["get", "pods", "-n", namespace, "-l", "app=vllm-qwen", "-o", "name"],
            check=False, capture=True,
        ).stdout or ""
        return bool(out.strip())
    except Exception:
        return False


# ---------------------------
# redeploy helpers (on wait failure)
# ---------------------------

def _debug_wait_failure(namespace: str) -> None:
    """Best-effort diagnostics when kubectl wait fails."""
    try:
        click.echo("[diag] pods (wide):")
        out = _kubectl(["get", "pods", "-n", namespace, "-o", "wide"], check=False, capture=True).stdout or ""
        click.echo(out.strip())
    except Exception:
        pass

    try:
        click.echo("\n[diag] not-ready pods (describe):")
        out = _kubectl(["get", "pods", "-n", namespace, "--no-headers"], check=False, capture=True).stdout or ""
        for ln in out.splitlines():
            parts = ln.split()
            if len(parts) < 2:
                continue
            name, ready = parts[0], parts[1]
            try:
                a, b = ready.split("/")
                if int(a) == int(b):
                    continue
            except Exception:
                pass
            click.echo(f"\n--- describe pod/{name} ---")
            desc = _kubectl(["describe", "pod", name, "-n", namespace], check=False, capture=True).stdout or ""
            click.echo(desc.strip())
    except Exception:
        pass

    try:
        click.echo("\n[diag] recent events (tail):")
        ev = _kubectl(["get", "events", "-n", namespace, "--sort-by=.lastTimestamp"], check=False, capture=True).stdout or ""
        lines = ev.splitlines()
        click.echo("\n".join(lines[-200:]))
    except Exception:
        pass


# ---------------------------
# operator-mode helpers
# ---------------------------

def _flat_to_nested(flat: Dict[str, object]) -> Dict[str, object]:
    """Convert {'a.b.c': 1, 'a.b.d': 2} to {'a': {'b': {'c': 1, 'd': 2}}}."""
    result: Dict[str, object] = {}
    for key, val in flat.items():
        parts = key.split(".")
        d = result
        for part in parts[:-1]:
            if part not in d or not isinstance(d[part], dict):
                d[part] = {}
            d = d[part]
        d[parts[-1]] = val
    return result


def _operator_delete(*, cr_name: str, namespace: str) -> None:
    _kubectl(
        ["delete", "vllmkvstack", cr_name, "-n", namespace, "--ignore-not-found"],
        check=False,
        capture=True,
    )
    time.sleep(5)


def _operator_apply(
    *,
    cr_name: str,
    namespace: str,
    set_values: Dict[str, object],
) -> None:
    """Generate a VllmKvStack CR and kubectl-apply it."""
    nested = _flat_to_nested(set_values)
    cr = {
        "apiVersion": "kvstack.llm.io/v1alpha1",
        "kind": "VllmKvStack",
        "metadata": {
            "name": cr_name,
            "namespace": namespace,
        },
        "spec": nested,
    }

    tmp = tempfile.NamedTemporaryFile(
        mode="w", prefix="sweep_cr_", suffix=".yaml", delete=False, encoding="utf-8",
    )
    try:
        yaml.safe_dump(cr, tmp, sort_keys=False)
        tmp.close()
        _kubectl(["apply", "-f", tmp.name], check=True)
    finally:
        try:
            Path(tmp.name).unlink(missing_ok=True)
        except Exception:
            pass


def _wait_cr_phase(cr_name: str, namespace: str, timeout_s: float = 120.0) -> None:
    """Poll until VllmKvStack .status.phase == Ready (best-effort)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        proc = _kubectl(
            ["get", "vllmkvstack", cr_name, "-n", namespace,
             "-o", "jsonpath={.status.phase}"],
            check=False,
            capture=True,
        )
        phase = (proc.stdout or "").strip()
        if phase == "Ready":
            click.echo(f"[operator] CR {cr_name} phase=Ready")
            return
        if phase == "Error":
            click.echo(f"[operator] CR {cr_name} phase=Error; continuing to pod wait")
            return
        time.sleep(5)
    click.echo("[operator] CR phase timeout; falling through to pod readiness check")


# ---------------------------
# run client
# ---------------------------

def _run_client(config_path: Path) -> None:
    _run([sys.executable, str(REPO_ROOT / "main.py"), "--config", str(config_path)], check=True)


# ---------------------------
# Click CLI
# ---------------------------

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--config",
    "master_config",
    default="1-master_config",
    show_default=True,
    help="Master sweep config file (suffix .yaml optional; relative to configs/ or absolute path).",
)
@click.option(
    "--skip-vllm",
    is_flag=True,
    default=False,
    help=(
        "Skip vLLM deployment. Checks if vllm-qwen pods already exist and preserves them. "
        "vLLM must already be running (deployed via deploy_vllm.py). "
        "Only router, redis, and cpu-hash are deployed/redeployed between experiments."
    ),
)
def cli(master_config: str, skip_vllm: bool) -> None:
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

    release = RELEASE
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
    click.echo(f"[sweep] skip_vllm={skip_vllm}")
    click.echo(f"[sweep] jobs={len(jobs)}")

    for i, (cfg_path, method) in enumerate(jobs, start=1):
        click.echo("\n" + "=" * 90)
        click.echo(f"[sweep] job {i}/{len(jobs)}  config={cfg_path.name}  method={method}")
        click.echo("=" * 90)

        cfg = load_config(str(cfg_path))
        h = getattr(cfg, "helm", None)
        if h is None:
            raise click.ClickException(f"Config has no 'helm' section: {cfg_path}")

        backend = str(getattr(cfg, "backend", "router") or "router").strip().lower()

        if backend not in ("router", "aibrix", "litellm", "boom"):
            raise click.ClickException(f"Invalid backend '{backend}' in {cfg_path}")

        deploy_mode = str(getattr(h, "deploy_mode", "helm")).strip().lower()
        cr_name = str(getattr(h, "operator_cr_name", "vllm")).strip() or release

        if deploy_mode == "operator":
            _operator_delete(cr_name=cr_name, namespace=namespace)
        else:
            if not skip_vllm:
                _helm_uninstall(release=release, namespace=namespace)

        set_values: Dict[str, object] = {
            "backend": backend,
            "replicas.vllm": int(h.replicas),
            "batchSize": int(h.batch_size),
            "tensorParallelSize": int(getattr(h, "tensor_parallel_size", 1)),
            "router.kvAware": bool(getattr(h, "router_kv_aware", True)),
            "router.lenAware": bool(getattr(h, "router_len_aware", True)),
            "router.lenPolicy": str(getattr(h, "router_len_policy", "short_first")),
            "router.apiKey": str(getattr(h, "router_api_key", "")),
            "aibrix.enabled": bool(getattr(h, "aibrix_enabled", False)),
            "aibrix.modelName": str(getattr(h, "aibrix_model_name", "served-model")),
            "aibrix.port": int(getattr(h, "aibrix_port", 8200)),

            # SLO-aware routing knobs
            "router.sloAware": bool(getattr(h, "router_slo_aware", False)),
            "router.sloWithKv": bool(getattr(h, "router_slo_with_kv", True)),
            "router.admissionThrottle": bool(getattr(h, "router_admission_throttle", False)),
            "router.fixedBatchSize": int(getattr(h, "router_fixed_batch_size", 0)),
            "router.outputLenPredictor": str(getattr(h, "router_output_len_predictor", "simple")),
            "router.batchSizeEstimate": str(getattr(h, "router_batch_size_estimate", "fixed")),
            "router.fixedBatchEstimate": int(getattr(h, "router_fixed_batch_estimate", 8)),
            "router.latencyPredictor": str(getattr(h, "router_latency_predictor", "linear")),
            "router.latencyOnlineUpdate": bool(getattr(h, "router_latency_online_update", False)),
            "router.latencyProfilePath": str(getattr(h, "router_latency_profile_path", "")),
            "router.queueWaitModel": str(getattr(h, "router_queue_wait_model", "none")),
            "router.chunkedPrefillAware": bool(getattr(h, "router_chunked_prefill_aware", False)),
            "router.maxNumBatchedTokens": int(getattr(h, "router_max_num_batched_tokens", 0)),
        }

        # ---- PV/PVC: always disabled — deployed once externally on parent NFS dir ----
        set_values["modelVolume.create"] = False

        # ---- deploy component flags ----
        if skip_vllm:
            vllm_running = _vllm_pods_exist(namespace)
            set_values["deploy.vllm"] = vllm_running
            set_values["deploy.router"] = True
            set_values["deploy.redis"] = True
            set_values["deploy.cpuHash"] = True
            if vllm_running:
                click.echo("[sweep] vllm-qwen pods detected — deploy.vllm=true (pods preserved)")
            else:
                click.echo("[sweep] WARNING: --skip-vllm set but no vllm-qwen pods found")
        else:
            set_values["deploy.vllm"] = True
            set_values["deploy.router"] = True
            set_values["deploy.redis"] = True
            set_values["deploy.cpuHash"] = True

        # ---- method interpretation per backend ----
        if backend == "router":
            # method is the router mode (pull, push-rr, push-random, push-leastq)
            set_values["router.mode"] = method
        elif backend == "aibrix":
            # method is the AIBrix routing strategy — applied via temp config, not Helm
            pass
        elif backend == "litellm":
            # method is a label only (e.g. "litellm-pull", "litellm-v1")
            # actual routing is controlled by the router running behind LiteLLM,
            # which is configured separately via router.mode in Helm.
            # No Helm knob to set here.
            click.echo(
                f"[sweep] backend=litellm: method={method!r} is a label only; "
                f"routing is handled inside the router pod."
            )
        elif backend == "boom":
            click.echo(
                f"[sweep] backend=boom: method={method!r} is a label only; "
                f"routing is handled inside the router pod."
            )

        # ---- LiteLLM pod deployment toggle ----
        if backend == "litellm":
            litellm_cfg = getattr(cfg, "litellm", None)
            set_values["litellm.enabled"] = True
            set_values["litellm.masterKey"] = str(
                getattr(litellm_cfg, "api_key", "sk-litellm-master") if litellm_cfg else "sk-litellm-master"
            )
            click.echo(
                f"[sweep] litellm.enabled=true masterKey={set_values['litellm.masterKey']!r}"
            )
        else:
            set_values["litellm.enabled"] = False

        # ---- BooM Gateway pod deployment toggle ----
        if backend == "boom":
            boom_cfg = getattr(cfg, "boom", None)
            set_values["boom.enabled"] = True
            set_values["boom.masterKey"] = str(
                getattr(boom_cfg, "api_key", "sk-boom-master") if boom_cfg else "sk-boom-master"
            )
            # Claude Code aliases: map Claude model names to served-model in BooM config
            boom_claude_aliases = bool(getattr(h, "boom_claude_aliases", False))
            set_values["boom.claudeCodeAliases"] = boom_claude_aliases
            click.echo(
                f"[sweep] boom.enabled=true masterKey={set_values['boom.masterKey']!r} "
                f"claudeCodeAliases={boom_claude_aliases}"
            )
        else:
            set_values["boom.enabled"] = False

        # ---- NFS cache warm (pre-read model shards before vLLM starts) ----
        cache_warm = bool(getattr(h, "cache_warm_enabled", False))
        set_values["cacheWarm.enabled"] = cache_warm
        if cache_warm:
            set_values["cacheWarm.pvcName"] = str(getattr(h, "cache_warm_pvc_name", "models-nfs-pvc"))
            set_values["cacheWarm.modelSubPath"] = str(
                getattr(h, "cache_warm_model_sub_path", "") or model_sub_path
            )
            click.echo(
                f"[sweep] cacheWarm.enabled=true "
                f"pvc={set_values['cacheWarm.pvcName']!r} "
                f"subPath={set_values['cacheWarm.modelSubPath']!r}"
            )

        # ---- Mooncake KV cache transfer toggle ----
        mooncake_enabled = bool(getattr(h, "mooncake_enabled", False))
        set_values["mooncake.enabled"] = mooncake_enabled
        if mooncake_enabled:
            set_values["mooncake.masterPort"] = int(getattr(h, "mooncake_master_port", 50088))
            set_values["mooncake.masterServerAddress"] = str(
                getattr(h, "mooncake_master_server_address", "10.50.156.65:50088")
            )
            set_values["mooncake.globalSegmentSize"] = int(
                getattr(h, "mooncake_global_segment_size", 140000000000)
            )
            set_values["mooncake.evictionHighWatermark"] = float(
                getattr(h, "mooncake_eviction_high_watermark", 0.9)
            )
            set_values["mooncake.evictionRatio"] = float(
                getattr(h, "mooncake_eviction_ratio", 0.1)
            )
            set_values["mooncake.ascendBufferPool"] = str(
                getattr(h, "mooncake_ascend_buffer_pool", "4:8")
            )
            set_values["mooncake.lookupRpcPort"] = str(
                getattr(h, "mooncake_lookup_rpc_port", "10010")
            )
            set_values["vllm.hostNetwork"] = bool(getattr(h, "mooncake_host_network", False))
            set_values["deploy.mooncakeMaster"] = bool(getattr(h, "deploy_mooncake_master", True))
            click.echo(
                f"[sweep] mooncake.enabled=true "
                f"masterAddr={set_values['mooncake.masterServerAddress']!r} "
                f"hostNetwork={set_values['vllm.hostNetwork']}"
            )

        service_impl = str(getattr(h, "service_impl", "python")).strip().lower()
        if service_impl == "go":
            set_values["images.router"] = "kv-router-go:latest"
            set_values["images.sidecar"] = "kv-sidecar-go:latest"
            # prefix hash stays Python (Option C): HuggingFace tokenizers + vLLM block hashing
            # are too heavy to port to Go for v0.1. cpuHash image is NOT overridden.

        set_values["autoscaling.enabled"] = bool(h.autoscaling_enabled)

        if bool(h.autoscaling_enabled):
            set_values["autoscaling.minReplicaCount"] = int(h.autoscaling_min)
            set_values["autoscaling.maxReplicaCount"] = int(h.autoscaling_max)
            set_values["autoscaling.threshold"] = str(h.autoscaling_threshold)

            q = str(h.autoscaling_prometheus_query or "").strip()
            q = " ".join(q.split())
            set_values["autoscaling.prometheusQuery"] = q

        # ---- vLLM model config: derive modelSubPath from nfs_path ----
        nfs_path = str(getattr(h, "nfs_path", "")).strip()
        if not nfs_path:
            raise click.ClickException(
                f"helm.nfs_path must be set in {cfg_path} "
                f"(e.g. /saeid/models/glm5) — used to derive modelVolume.modelSubPath"
            )
        model_sub_path = PurePosixPath(nfs_path.rstrip("/")).name
        if not model_sub_path:
            raise click.ClickException(
                f"Could not derive model subfolder from helm.nfs_path={nfs_path!r}. "
                f"Expected a path like /saeid/models/<model-name>."
            )
        set_values["modelVolume.modelSubPath"] = model_sub_path
        click.echo(f"[sweep] model subPath={model_sub_path!r} (derived from nfs_path={nfs_path!r})")

        model_host_path = str(getattr(h, "model_host_path", "")).strip()
        if model_host_path:
            set_values["modelVolume.hostPath"] = model_host_path
            click.echo(f"[sweep] using local hostPath={model_host_path!r} (bypassing NFS)")

        # ---- vLLM runtime flags ----
        if getattr(h, "vllm_gpu_memory_utilization", None) is not None:
            set_values["vllm.gpuMemoryUtilization"] = float(h.vllm_gpu_memory_utilization)
        if getattr(h, "vllm_quantization", None) is not None:
            set_values["vllm.quantization"] = str(h.vllm_quantization)
        set_values["vllm.enableExpertParallel"] = bool(getattr(h, "vllm_enable_expert_parallel", False))
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
            ac = h.vllm_additional_config
            if isinstance(ac, str):
                try:
                    ac = json.loads(ac)
                except Exception:
                    ac = None
            if isinstance(ac, dict):
                for ak, av in ac.items():
                    set_values[f"vllm.additionalConfig.{ak}"] = av
        kv_cache_dtype = str(getattr(h, "vllm_kv_cache_dtype", "auto")).strip()
        if kv_cache_dtype and kv_cache_dtype != "auto":
            set_values["vllm.kvCacheDtype"] = kv_cache_dtype
        if getattr(h, "vllm_cpu_offload_gb", None) is not None:
            set_values["vllm.cpuOffloadGb"] = float(h.vllm_cpu_offload_gb)
        if getattr(h, "vllm_enable_prefix_caching", False):
            set_values["vllm.enablePrefixCaching"] = "true"
        if getattr(h, "vllm_tool_call_parser", None) is not None:
            set_values["vllm.toolCallParser"] = str(h.vllm_tool_call_parser)
        if getattr(h, "vllm_reasoning_parser", None) is not None:
            set_values["vllm.reasoningParser"] = str(h.vllm_reasoning_parser)
        if getattr(h, "vllm_speculative_config", None) is not None:
            sc = h.vllm_speculative_config
            if isinstance(sc, str):
                try:
                    sc = json.loads(sc)
                except Exception:
                    sc = None
            if isinstance(sc, dict):
                for sk, sv in sc.items():
                    set_values[f"vllm.speculativeConfig.{sk}"] = sv

        click.echo(f"[sweep] backend={backend}  deploy_mode={deploy_mode}  skip_vllm={skip_vllm}")
        click.echo("[sweep] set values:")
        for k in sorted(set_values):
            click.echo(f"  - {k}={_coerce_set_value(set_values[k])}")

        max_redeploy_attempts = 3
        redeploy_sleep_s = 10

        last_err: Optional[Exception] = None
        for attempt in range(1, max_redeploy_attempts + 1):
            click.echo(f"[deploy] attempt {attempt}/{max_redeploy_attempts}")

            if deploy_mode == "operator":
                _operator_apply(cr_name=cr_name, namespace=namespace, set_values=set_values)
                _wait_cr_phase(cr_name, namespace)
            else:
                _helm_install_or_upgrade(
                    release=release,
                    chart_dir=chart_dir,
                    namespace=namespace,
                    values_file=values_file if values_file.is_file() else None,
                    set_values=set_values,
                )
                # Give Kubernetes time to schedule and create new pods before kubectl wait
                # runs. Without this sleep, pods are still terminating/pending when wait
                # fires and returns "no matching resources found" immediately.
                click.echo("[deploy] waiting 15s for pods to be scheduled...")
                time.sleep(15)

            try:
                if skip_vllm:
                    # Use app in (a,b,c) selector — quoted as separate -l arg, which
                    # is correctly parsed by kubectl on all versions.
                    _wait_ready(
                        namespace,
                        label_selector="app in (router-service,redis,vllm-cpu-hash)",
                    )
                else:
                    _wait_ready(namespace)
                last_err = None
                break
            except subprocess.CalledProcessError as e:
                last_err = e
                click.echo(f"[deploy] WARN: wait_ready failed: {e}")
                _debug_wait_failure(namespace)

                click.echo("[deploy] redeploying (teardown -> sleep -> retry)")
                if deploy_mode == "operator":
                    _operator_delete(cr_name=cr_name, namespace=namespace)
                else:
                    if not skip_vllm:
                        _helm_uninstall(release=release, namespace=namespace)
                time.sleep(redeploy_sleep_s)

        if last_err is not None:
            raise click.ClickException(
                f"Deployment not ready after {max_redeploy_attempts} attempts: {last_err}"
            )

        effective_release = cr_name if deploy_mode == "operator" else release
        try:
            out = _helm(["get", "values", effective_release, "-n", namespace, "--all"], capture=True).stdout or ""
            (REPO_ROOT / "helm-effective-values.yaml").write_text(out, encoding="utf-8")
            click.echo("[sweep] wrote helm-effective-values.yaml")
        except Exception as e:
            click.echo(f"[sweep] WARN: failed to helm get values: {e}")

        rendered_text = _helm_template(
            release=effective_release,
            chart_dir=chart_dir,
            namespace=namespace,
            values_file=values_file if values_file.is_file() else None,
            set_values=set_values,
        )
        (REPO_ROOT / "vllm-k8s.yaml").write_text(rendered_text, encoding="utf-8")

        tmp_cfg_path = _write_temp_job_config(cfg, method)

        before = _snapshot_existing_experiments()
        try:
            _run_client(tmp_cfg_path)
        finally:
            try:
                tmp_cfg_path.unlink(missing_ok=True)
            except Exception:
                pass

        exp_dir = _newest_experiment_dir(before)

        if exp_dir is None:
            click.echo("[warn] could not detect new experiment dir; skipping artifact snapshot")
            continue

        shutil.copy2(REPO_ROOT / "vllm-k8s.yaml", exp_dir / "vllm-k8s.yaml")
        hv_path = REPO_ROOT / "helm-effective-values.yaml"
        if hv_path.is_file():
            shutil.copy2(hv_path, exp_dir / "helm-effective-values.yaml")

        meta = {
            "client_config": str(cfg_path),
            "backend": backend,
            "method": method,
            "deploy_mode": deploy_mode,
            "skip_vllm": skip_vllm,
            "master_config": str(master_path),
            "ts_unix": time.time(),
            "helm_release": effective_release,
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
                "router_kv_aware": bool(getattr(h, "router_kv_aware", True)),
                "router_len_aware": bool(getattr(h, "router_len_aware", True)),
                "router_len_policy": str(getattr(h, "router_len_policy", "short_first")),
                "aibrix_enabled": bool(getattr(h, "aibrix_enabled", False)),
                "aibrix_model_name": str(getattr(h, "aibrix_model_name", "served-model")),
                "aibrix_port": int(getattr(h, "aibrix_port", 8200)),
                "service_impl": str(getattr(h, "service_impl", "python")),
                "nfs_path": nfs_path,
                "model_sub_path": model_sub_path,
                "vllm_gpu_memory_utilization": getattr(h, "vllm_gpu_memory_utilization", None),
                "vllm_quantization": getattr(h, "vllm_quantization", None),
                "vllm_enable_expert_parallel": bool(getattr(h, "vllm_enable_expert_parallel", False)),
                "vllm_max_model_len": getattr(h, "vllm_max_model_len", None),
                "vllm_compilation_config": getattr(h, "vllm_compilation_config", None),
                "vllm_trust_remote_code": bool(getattr(h, "vllm_trust_remote_code", False)),
                "vllm_max_num_batched_tokens": getattr(h, "vllm_max_num_batched_tokens", None),
                "vllm_seed": getattr(h, "vllm_seed", None),
                "vllm_additional_config": getattr(h, "vllm_additional_config", None),
                "vllm_speculative_config": getattr(h, "vllm_speculative_config", None),
                "vllm_tool_call_parser": getattr(h, "vllm_tool_call_parser", None),
                "vllm_reasoning_parser": getattr(h, "vllm_reasoning_parser", None),
                # litellm knobs
                "litellm_enabled": backend == "litellm",
                "litellm_base_url": getattr(getattr(cfg, "litellm", None), "base_url", None),
                "litellm_model": getattr(getattr(cfg, "litellm", None), "model", None),
                # boom knobs
                "boom_enabled": backend == "boom",
                "boom_claude_aliases": bool(getattr(h, "boom_claude_aliases", False)),
                "boom_base_url": getattr(getattr(cfg, "boom", None), "base_url", None),
                "boom_model": getattr(getattr(cfg, "boom", None), "model", None),
                # mooncake knobs
                "mooncake_enabled": bool(getattr(h, "mooncake_enabled", False)),
                "mooncake_master_server_address": str(getattr(h, "mooncake_master_server_address", "")),
                "mooncake_host_network": bool(getattr(h, "mooncake_host_network", False)),
                # SLO-aware knobs
                "router_slo_aware": bool(getattr(h, "router_slo_aware", False)),
                "router_admission_throttle": bool(getattr(h, "router_admission_throttle", False)),
                "router_fixed_batch_size": int(getattr(h, "router_fixed_batch_size", 0)),
                "router_output_len_predictor": str(getattr(h, "router_output_len_predictor", "simple")),
                "router_latency_predictor": str(getattr(h, "router_latency_predictor", "linear")),
                # SLO client config
                "slo_enabled": bool(getattr(getattr(cfg, "slo", None), "enabled", False)),
            },
        }
        (exp_dir / "sweep_meta.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
        )

        click.echo(f"[sweep] experiment_dir={exp_dir}")

    click.echo("\n[sweep] done.")


if __name__ == "__main__":
    cli()