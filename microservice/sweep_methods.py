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
from urllib.parse import urlparse

import click
import yaml

from config import load_config, migrate_legacy_helm_to_models


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
        "--timeout",
        timeout,
    ]
    if values_file is not None and values_file.is_file():
        cmd.extend(["-f", str(values_file)])

    if extra_values_files:
        for vf in extra_values_files:
            if vf is not None and vf.is_file():
                cmd.extend(["-f", str(vf)])

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
    """Return True if any vllm-qwen, vllm-dp, or multi-model vllm pods exist (any phase)."""
    try:
        out = _kubectl(
            ["get", "pods", "-n", namespace,
             "-l", "app in (vllm-qwen,vllm-dp-worker)",
             "-o", "name"],
            check=False, capture=True,
        ).stdout or ""
        if out.strip():
            return True
        out2 = _kubectl(
            ["get", "pods", "-n", namespace,
             "-l", "component=vllm",
             "-o", "name"],
            check=False, capture=True,
        ).stdout or ""
        return bool(out2.strip())
    except Exception:
        return False


def _detect_current_route_mode(namespace: str) -> str:
    """Detect the routing mode of the live deployment.

    Returns "direct" if no sidecar containers exist in vLLM pods,
    "router" if sidecars are present, or "unknown" if detection fails.
    """
    try:
        out = _kubectl(
            ["get", "pods", "-n", namespace,
             "-l", "component=vllm",
             "-o", "jsonpath={.items[0].spec.containers[*].name}"],
            check=False, capture=True,
        ).stdout or ""
        containers = out.strip().split()
        if not containers:
            return "unknown"
        return "router" if "kv-sidecar" in containers else "direct"
    except Exception:
        return "unknown"


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
# BooM config printer
# ---------------------------

def _print_boom_config(cfg, helm_cfg, models_list: list, namespace: str) -> None:
    """Print the BooM / external gateway config after deployment for sharing with maintainers."""
    router_api_key = str(getattr(helm_cfg, "router_api_key", "")).strip() or "dummy"

    # Detect node IP from router_url or boom base_url
    node_ip = "<node-ip>"
    for url_attr in ("router_url", ):
        raw_url = str(getattr(cfg, url_attr, "") or "").strip()
        if raw_url:
            try:
                parsed = urlparse(raw_url)
                if parsed.hostname and not parsed.hostname.startswith("127."):
                    node_ip = parsed.hostname
                    break
            except Exception:
                pass

    # Detect router NodePort
    router_port = "30080"
    try:
        out = subprocess.run(
            ["kubectl", "get", "svc", "router-service", "-n", namespace,
             "-o", "jsonpath={.spec.ports[0].nodePort}"],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
        if out.isdigit():
            router_port = out
    except Exception:
        pass

    click.echo("")
    click.echo("=" * 70)
    click.echo("  BooM / External Gateway Config  (share with your BooM maintainer)")
    click.echo("=" * 70)

    if models_list:
        click.echo("")
        click.echo("model_list:")
        for m in models_list:
            name = m.get("servedModelName") or m.get("name", "unknown")
            click.echo(f"  - model_name: {name}")
            click.echo(f"    litellm_params:")
            click.echo(f"      model: openai/{name}")
            click.echo(f"      api_base: http://{node_ip}:{router_port}/v1")
            click.echo(f'      api_key: "{router_api_key}"')
    else:
        model_name = str(getattr(helm_cfg, "model_name", "served-model")).strip() or "served-model"
        click.echo("")
        click.echo("model_list:")
        click.echo(f"  - model_name: {model_name}")
        click.echo(f"    litellm_params:")
        click.echo(f"      model: openai/{model_name}")
        click.echo(f"      api_base: http://{node_ip}:{router_port}/v1")
        click.echo(f'      api_key: "{router_api_key}"')

    click.echo("")
    click.echo(f"Router endpoint:  http://{node_ip}:{router_port}")

    # BooM NodePort
    try:
        out = subprocess.run(
            ["kubectl", "get", "svc", "boom-proxy", "-n", namespace,
             "-o", "jsonpath={.spec.ports[0].nodePort}"],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
        if out.isdigit():
            click.echo(f"BooM endpoint:    http://{node_ip}:{out}")
    except Exception:
        pass

    click.echo("=" * 70)
    click.echo("")


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
        # Determine the requested routing mode for this job.
        _requested_route = "direct" if (
            backend == "boom"
            and str(getattr(h, "boom_route_via", "router")).strip().lower() == "direct"
        ) else "router"

        if skip_vllm:
            vllm_running = _vllm_pods_exist(namespace)
            set_values["deploy.vllm"] = vllm_running
            set_values["deploy.router"] = True
            set_values["deploy.redis"] = True
            set_values["deploy.cpuHash"] = True
            if vllm_running:
                # Check that the live deployment's routing mode matches
                # the requested one. Mismatches would mutate the vLLM pod
                # spec (add/remove sidecars), defeating --skip-vllm.
                _live_route = _detect_current_route_mode(namespace)
                if _live_route != "unknown" and _live_route != _requested_route:
                    raise click.ClickException(
                        f"--skip-vllm: routing mode mismatch — "
                        f"live deployment is '{_live_route}' but config requests '{_requested_route}'. "
                        f"Run without --skip-vllm to do a full redeploy, or use a config "
                        f"with the same routing mode."
                    )
                click.echo(
                    f"[sweep] vllm pods detected (mode={_live_route}) — "
                    f"deploy.vllm=true (pods preserved)"
                )
            else:
                click.echo("[sweep] WARNING: --skip-vllm set but no vllm pods found")
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
            # method is the BooM routing strategy (round_robin, key_affinity)
            # used when routeVia=direct; ignored when routeVia=router.
            pass

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
            boom_claude_aliases = bool(getattr(h, "boom_claude_aliases", False))
            set_values["boom.claudeCodeAliases"] = boom_claude_aliases
            boom_route_via = str(getattr(h, "boom_route_via", "router")).strip().lower()
            set_values["boom.routeVia"] = boom_route_via
            if boom_route_via == "direct":
                # In direct mode, the method IS the BooM routing strategy.
                # Override helm config — method from master_config takes precedence.
                set_values["boom.directRoutingStrategy"] = method
                # No router/sidecar/redis/hash needed — BooM routes directly to vLLM.
                set_values["deploy.router"] = False
                set_values["deploy.redis"] = False
                set_values["deploy.cpuHash"] = False
                set_values["sidecar.enabled"] = False
                click.echo(
                    f"[sweep] boom direct mode: disabling router, redis, cpuHash, sidecars; "
                    f"BooM routing_strategy={method}"
                )
            boom_max_inflight = int(getattr(h, "boom_max_inflight", 0))
            if boom_max_inflight > 0:
                set_values["boom.maxInflight"] = boom_max_inflight
            click.echo(
                f"[sweep] boom.enabled=true masterKey={set_values['boom.masterKey']!r} "
                f"claudeCodeAliases={boom_claude_aliases} routeVia={boom_route_via}"
                f" maxInflight={boom_max_inflight}"
            )
        else:
            set_values["boom.enabled"] = False

        # ---- Sidecar log level ----
        sidecar_log_level = str(getattr(h, "sidecar_log_level", "info")).strip().lower()
        set_values["sidecar.logLevel"] = sidecar_log_level

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

        # ---- Unified models[] — auto-migrate legacy flat config if needed ----
        migrate_legacy_helm_to_models(h)
        models_list = list(h.models or [])
        if not models_list:
            raise click.ClickException(
                f"No models defined in {cfg_path}. "
                f"Add helm.models[] or legacy flat vllm_*/data_parallel_* fields."
            )

        click.echo(f"[sweep] models: {len(models_list)} model(s)")
        for mi, mdef in enumerate(models_list):
            dp_info = mdef.get("dataParallel", {})
            dp_tag = f" DP={dp_info.get('size', '-')}" if dp_info.get("enabled") else ""
            click.echo(
                f"  [{mi}] name={mdef.get('name')} replicas={mdef.get('replicas', 1)} "
                f"tp={mdef.get('tensorParallelSize', '?')}{dp_tag}"
            )

        first_model = models_list[0]
        set_values["replicas.vllm"] = int(first_model.get("replicas", h.replicas))
        set_values["batchSize"] = int(first_model.get("batchSize", h.batch_size))
        set_values["tensorParallelSize"] = int(first_model.get("tensorParallelSize", h.tensor_parallel_size))

        # Write models YAML overlay for Helm -f
        _models_tmp = tempfile.NamedTemporaryFile(
            mode="w", prefix="sweep_models_", suffix=".yaml",
            delete=False, encoding="utf-8",
        )
        yaml.safe_dump({"models": models_list}, _models_tmp, sort_keys=False)
        _models_tmp.close()
        _models_values_file: Optional[Path] = Path(_models_tmp.name)

        # ---- Model volume defaults (from nfs_path / first model) ----
        nfs_path = str(getattr(h, "nfs_path", "")).strip()
        model_sub_path = first_model.get("modelSubPath", "")
        if not model_sub_path and nfs_path:
            model_sub_path = PurePosixPath(nfs_path.rstrip("/")).name
        if model_sub_path:
            set_values["modelVolume.modelSubPath"] = model_sub_path
            click.echo(f"[sweep] model subPath={model_sub_path!r}")

        model_host_path = str(getattr(h, "model_host_path", "")).strip()
        if model_host_path:
            set_values["modelVolume.hostPath"] = model_host_path
            click.echo(f"[sweep] using local hostPath={model_host_path!r} (bypassing NFS)")

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
                    extra_values_files=[_models_values_file] if _models_values_file else None,
                )
                # Give Kubernetes time to schedule and create new pods before kubectl wait
                # runs. Without this sleep, pods are still terminating/pending when wait
                # fires and returns "no matching resources found" immediately.
                click.echo("[deploy] waiting 15s for pods to be scheduled...")
                time.sleep(15)

            try:
                if skip_vllm:
                    if _requested_route == "direct":
                        # Direct mode: only BooM proxy is deployed (no router/redis/hash).
                        _wait_ready(
                            namespace,
                            label_selector="app in (boom-proxy)",
                        )
                    else:
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

        # ---- Print BooM / external gateway config for maintainer ----
        _print_boom_config(cfg, h, models_list, namespace)

        tmp_cfg_path = _write_temp_job_config(cfg, method)

        before = _snapshot_existing_experiments()
        try:
            _run_client(tmp_cfg_path)
        finally:
            try:
                tmp_cfg_path.unlink(missing_ok=True)
            except Exception:
                pass
            if _models_values_file is not None:
                try:
                    _models_values_file.unlink(missing_ok=True)
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
                "autoscaling_enabled": bool(h.autoscaling_enabled),
                "autoscaling_min": int(h.autoscaling_min),
                "autoscaling_max": int(h.autoscaling_max),
                "router_kv_aware": bool(getattr(h, "router_kv_aware", True)),
                "router_len_aware": bool(getattr(h, "router_len_aware", True)),
                "router_len_policy": str(getattr(h, "router_len_policy", "short_first")),
                "service_impl": str(getattr(h, "service_impl", "python")),
                "nfs_path": nfs_path,
                "model_sub_path": model_sub_path,
                "boom_enabled": backend == "boom",
                "boom_claude_aliases": bool(getattr(h, "boom_claude_aliases", False)),
                "mooncake_enabled": bool(getattr(h, "mooncake_enabled", False)),
                "router_slo_aware": bool(getattr(h, "router_slo_aware", False)),
                "slo_enabled": bool(getattr(getattr(cfg, "slo", None), "enabled", False)),
                "models": models_list,
            },
        }
        (exp_dir / "sweep_meta.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
        )

        click.echo(f"[sweep] experiment_dir={exp_dir}")

    click.echo("\n[sweep] done.")


if __name__ == "__main__":
    cli()