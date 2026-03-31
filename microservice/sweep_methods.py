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
# - Waits for readiness
# - Writes repo_root/vllm-k8s.yaml = helm template output so experiment snapshot stays identical
# - Runs main.py using a temporary per-job config and snapshots sweep_meta.json

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click
import yaml

from config import load_config


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
# temp config helper
# ---------------------------

def _write_temp_job_config(cfg, method: str) -> Path:
    """
    Write a per-job temporary config so main.py sees the correct method for the chosen backend.

    backend=router  -> keep router config; method is applied only through Helm router.mode
    backend=aibrix -> override cfg.aibrix.routing_strategy = method
    """
    cfg_dict = asdict(cfg)
    backend = str(cfg_dict.get("backend", "router") or "router").strip().lower()

    if backend == "aibrix":
        cfg_dict.setdefault("aibrix", {})
        cfg_dict["aibrix"]["routing_strategy"] = method

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


def _wait_ready(namespace: str, timeout_s: float = 36000) -> None:
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

        cfg = load_config(str(cfg_path))
        h = getattr(cfg, "helm", None)
        if h is None:
            raise click.ClickException(f"Config has no 'helm' section: {cfg_path}")

        backend = str(getattr(cfg, "backend", "router") or "router").strip().lower()
        if backend not in ("router", "aibrix"):
            raise click.ClickException(f"Invalid backend '{backend}' in {cfg_path}")

        deploy_mode = str(getattr(h, "deploy_mode", "helm")).strip().lower()
        cr_name = str(getattr(h, "operator_cr_name", "vllm")).strip() or release

        if deploy_mode == "operator":
            _operator_delete(cr_name=cr_name, namespace=namespace)
        else:
            _helm_uninstall(release=release, namespace=namespace)

        set_values: Dict[str, object] = {
            "backend": backend,
            "replicas.vllm": int(h.replicas),
            "batchSize": int(h.batch_size),
            "tensorParallelSize": int(getattr(h, "tensor_parallel_size", 1)),
            "router.kvAware": bool(getattr(h, "router_kv_aware", True)),
            "router.lenAware": bool(getattr(h, "router_len_aware", True)),
            "router.lenPolicy": str(getattr(h, "router_len_policy", "short_first")),
            "aibrix.enabled": bool(getattr(h, "aibrix_enabled", False)),
            "aibrix.modelName": str(getattr(h, "aibrix_model_name", "served-model")),
            "aibrix.port": int(getattr(h, "aibrix_port", 8200)),
        }

        # Interpret method family from backend.
        if backend == "router":
            set_values["router.mode"] = method

        service_impl = str(getattr(h, "service_impl", "python")).strip().lower()
        if service_impl == "go":
            set_values["images.router"] = "kv-router-go:latest"
            set_values["images.sidecar"] = "kv-sidecar-go:latest"
            set_values["images.cpuHash"] = "kv-prefixhash-go:latest"

        set_values["autoscaling.enabled"] = bool(h.autoscaling_enabled)

        if bool(h.autoscaling_enabled):
            set_values["autoscaling.minReplicaCount"] = int(h.autoscaling_min)
            set_values["autoscaling.maxReplicaCount"] = int(h.autoscaling_max)
            set_values["autoscaling.threshold"] = str(h.autoscaling_threshold)

            q = str(h.autoscaling_prometheus_query or "").strip()
            q = " ".join(q.split())
            set_values["autoscaling.prometheusQuery"] = q

        # ---- vLLM runtime flags ----
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
        # ----------------------------

        click.echo(f"[sweep] backend={backend}  deploy_mode={deploy_mode}")
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

            try:
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
                "vllm_gpu_memory_utilization": getattr(h, "vllm_gpu_memory_utilization", None),
                "vllm_quantization": getattr(h, "vllm_quantization", None),
                "vllm_enable_expert_parallel": bool(getattr(h, "vllm_enable_expert_parallel", True)),
                "vllm_compilation_config": getattr(h, "vllm_compilation_config", None),
                "vllm_trust_remote_code": bool(getattr(h, "vllm_trust_remote_code", False)),
                "vllm_max_num_batched_tokens": getattr(h, "vllm_max_num_batched_tokens", None),
                "vllm_seed": getattr(h, "vllm_seed", None),
                "vllm_additional_config": getattr(h, "vllm_additional_config", None),
                "vllm_speculative_config": getattr(h, "vllm_speculative_config", None),
            },
        }
        (exp_dir / "sweep_meta.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
        )

        click.echo(f"[sweep] experiment_dir={exp_dir}")

    click.echo("\n[sweep] done.")


if __name__ == "__main__":
    cli()