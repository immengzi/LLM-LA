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
import os
import shutil
import subprocess
import sys
import threading
import time
import tempfile
from dataclasses import asdict
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Tuple, Union, cast
from urllib.parse import urlparse

import click
import yaml

from config import load_config, migrate_legacy_helm_to_models


REPO_ROOT = Path(__file__).resolve().parent
CONFIGS_DIR = REPO_ROOT / "configs"
EXPERIMENTS_ROOT = REPO_ROOT / "experiments"

# Single release name for all modes — avoids RBAC ownership conflicts
RELEASE = "vllm"

ConfigValue = Union[None, str, int, float, bool, List["ConfigValue"], Dict[str, "ConfigValue"]]


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

def _flatten_helm_values(prefix: str, value: ConfigValue) -> Dict[str, ConfigValue]:
    """Flatten nested Helm values into --set dot paths."""
    if not isinstance(value, dict):
        return {prefix: value} if prefix else {}

    flattened: Dict[str, ConfigValue] = {}
    for key, child in value.items():
        if not isinstance(key, str) or not key.strip():
            raise click.ClickException(f"Invalid helm.values key: {key!r}")
        path = f"{prefix}.{key}" if prefix else key
        flattened.update(_flatten_helm_values(path, child))
    return flattened


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


def _allocate_experiment_dir() -> Path:
    """Reserve the next numeric experiment dir (max+1) up front so logs/configs can
    start writing at deploy time. The client reuses it via FORCE_EXPERIMENT_DIR."""
    EXPERIMENTS_ROOT.mkdir(parents=True, exist_ok=True)
    ids = [int(p.name) for p in EXPERIMENTS_ROOT.iterdir() if p.is_dir() and p.name.isdigit()]
    next_id = (max(ids) + 1) if ids else 1
    exp_dir = EXPERIMENTS_ROOT / str(next_id)
    exp_dir.mkdir(parents=True, exist_ok=False)
    return exp_dir


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
    # cfg is already resolved by load_config(). Temp files live under /tmp, so
    # preserving switch_cluster would make main.py look for /tmp/clusters.yaml.
    cfg_dict["switch_cluster"] = None
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
    set_values: Dict[str, ConfigValue],
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


def _wait_pods_terminated(namespace: str, timeout_s: float = 300.0, poll_s: float = 3.0) -> None:
    """Block until no pod in the namespace is Terminating (deletionTimestamp set).

    Run right after `helm uninstall` so the previous release's pods are fully gone
    before the next deploy + log capture. This prevents stale/terminating pods from
    the prior sweep step leaking into the new experiment's vllm-logs/.
    """
    deadline = time.time() + float(timeout_s)
    while time.time() < deadline:
        try:
            res = subprocess.run(
                ["kubectl", "get", "pods", "-n", namespace, "-o", "json"],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
            )
            data = json.loads(res.stdout or "{}")
        except Exception:
            return  # namespace gone / kubectl error -> nothing to wait for
        terminating = [
            (i.get("metadata") or {}).get("name")
            for i in data.get("items", [])
            if (i.get("metadata") or {}).get("deletionTimestamp")
        ]
        if not terminating:
            return
        click.echo(f"[deploy] waiting for {len(terminating)} terminating pod(s) to clear...")
        time.sleep(poll_s)
    click.echo(f"[deploy] WARN: pods still terminating in ns={namespace} after {timeout_s:g}s; continuing")


def _helm_uninstall(*, release: str, namespace: str, wait: bool = True, timeout_s: float = 300.0) -> None:
    cmd = ["uninstall", release, "-n", namespace]
    if wait:
        # --wait makes helm block until it considers the release's resources deleted.
        cmd += ["--wait", "--timeout", f"{int(timeout_s)}s"]
    _helm(cmd, check=False, capture=True)
    if wait:
        # Belt-and-suspenders: CRD/controller-managed pods (e.g. LWS) can outlive
        # helm's own wait, so explicitly poll until nothing is Terminating.
        _wait_pods_terminated(namespace, timeout_s=timeout_s)


def _helm_install_or_upgrade(
    *,
    release: str,
    chart_dir: Path,
    namespace: str,
    values_file: Optional[Path],
    set_values: Dict[str, ConfigValue],
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

def _flat_to_nested(flat: Dict[str, ConfigValue]) -> Dict[str, ConfigValue]:
    """Convert {'a.b.c': 1, 'a.b.d': 2} to {'a': {'b': {'c': 1, 'd': 2}}}."""
    result: Dict[str, ConfigValue] = {}
    for key, val in flat.items():
        parts = key.split(".")
        d = result
        for part in parts[:-1]:
            if part not in d or not isinstance(d[part], dict):
                d[part] = {}
            d = cast(Dict[str, ConfigValue], d[part])
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
    set_values: Dict[str, ConfigValue],
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
# Pod log collection (cfg.collect_vllm_logs)
# ---------------------------

def _list_pods_and_containers(namespace: str) -> List[Tuple[str, List[str]]]:
    """Return [(pod_name, [container_names...]), ...] for all pods in the namespace."""
    try:
        res = subprocess.run(
            ["kubectl", "get", "pods", "-n", namespace, "-o", "json"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
        )
        data = json.loads(res.stdout or "{}")
    except Exception as e:
        click.echo(f"[logs] WARN: failed to list pods in ns={namespace}: {e}")
        return []

    out: List[Tuple[str, List[str]]] = []
    for item in data.get("items", []):
        meta = item.get("metadata") or {}
        pod = meta.get("name")
        if not pod:
            continue
        # Skip pods that are Terminating (deletionTimestamp set). These are leftovers
        # from the previous sweep step being torn down by the new deploy; capturing
        # them would pollute this experiment's vllm-logs/ with stale pod logs.
        if meta.get("deletionTimestamp"):
            continue
        spec = item.get("spec") or {}
        containers = [c.get("name") for c in spec.get("containers", []) if c.get("name")]
        if containers:
            out.append((pod, containers))
    return out


class _NamespaceLogCollector:
    """
    Continuously captures `kubectl logs -f` for EVERY container of EVERY pod in a
    namespace into dest_dir/<pod>/<container>.log — independent of pod names.

    A background thread re-scans the namespace every `poll_interval` seconds so pods
    that appear or restart mid-run (autoscaling, crashes, rollouts) are also captured.
    Logs are opened in append mode so a re-attach after a restart never clobbers what
    was already streamed. Best-effort: never raises into the caller.
    """

    def __init__(self, namespace: str, dest_dir: Path, poll_interval: float = 5.0) -> None:
        self.namespace = namespace
        self.dest_dir = Path(dest_dir)
        self.poll_interval = poll_interval
        self._streams: Dict[Tuple[str, str], Tuple[subprocess.Popen, "object"]] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _start_stream(self, pod: str, container: str) -> None:
        pod_dir = self.dest_dir / pod
        try:
            pod_dir.mkdir(parents=True, exist_ok=True)
            fh = open(pod_dir / f"{container}.log", "a", encoding="utf-8")
            proc = subprocess.Popen(
                ["kubectl", "logs", "-f", "--timestamps",
                 f"pod/{pod}", "-c", container, "-n", self.namespace],
                stdout=fh, stderr=subprocess.STDOUT, text=True,
                start_new_session=True,
            )
            self._streams[(pod, container)] = (proc, fh)
        except Exception as e:
            click.echo(f"[logs] WARN: failed to start collector {pod}/{container}: {e}")

    def _scan_once(self) -> None:
        for pod, containers in _list_pods_and_containers(self.namespace):
            for c in containers:
                key = (pod, c)
                existing = self._streams.get(key)
                if existing is not None:
                    proc, fh = existing
                    if proc.poll() is None:
                        continue  # still streaming
                    # streamer died (e.g. container restart) -> re-attach (append)
                    try:
                        fh.close()
                    except Exception:
                        pass
                    del self._streams[key]
                self._start_stream(pod, c)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._scan_once()
            except Exception as e:
                click.echo(f"[logs] WARN: namespace scan failed: {e}")
            self._stop.wait(self.poll_interval)

    def start(self) -> "_NamespaceLogCollector":
        self._scan_once()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        click.echo(
            f"[logs] watching ALL pods in ns={self.namespace} -> {self.dest_dir} "
            f"(rescan every {self.poll_interval:g}s; {len(self._streams)} stream(s) so far)"
        )
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.poll_interval + 5)
        for proc, _ in self._streams.values():
            try:
                proc.terminate()
            except Exception:
                pass
        for proc, fh in self._streams.values():
            try:
                proc.wait(timeout=10)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            try:
                fh.flush()
                fh.close()
            except Exception:
                pass
        click.echo(f"[logs] stopped; captured {len(self._streams)} container stream(s)")


def _start_log_collectors(namespace: str, dest_dir: Path) -> "_NamespaceLogCollector":
    """Begin namespace-wide log capture. Returns a collector to stop later."""
    return _NamespaceLogCollector(namespace, dest_dir).start()


def _stop_log_collectors(collector) -> None:
    """Stop a namespace-wide log collector. Best-effort."""
    if collector is not None:
        collector.stop()


# ---------------------------
# Per-pod vLLM NodePort services
# ---------------------------


def _create_per_pod_services(
    *,
    namespace: str,
    release: str,
    chart_dir: Path,
    port_offset: int = 0,
) -> List[Tuple[str, str]]:
    """Create a NodePort Service per externally-reachable vLLM pod via Helm.

    Discovers running pods, then runs ``helm upgrade --reuse-values`` with the
    pod list so the chart template renders one NodePort Service per pod.  Each
    Service selects its pod by the built-in ``statefulset.kubernetes.io/pod-name``
    label, which the StatefulSet controller re-applies automatically on every
    restart — so the per-pod metrics endpoint stays attached across pod
    restarts with no manual re-labeling or re-sweep.  Because the services are
    Helm-managed, ``helm uninstall`` cleans them up automatically.

    For DP pods only the leader (worker-index 0) is exposed.

    Returns list of (pod_name, nodeport) tuples.
    """
    try:
        raw = subprocess.run(
            ["kubectl", "get", "pods", "-n", namespace,
             "-l", "component=vllm",
             "--field-selector=status.phase=Running",
             "-o", "json"],
            capture_output=True, text=True, timeout=30,
        )
        if raw.returncode != 0:
            return []
        pods = json.loads(raw.stdout).get("items", [])
    except Exception:
        return []

    if not pods:
        return []

    discovered_pods: List[str] = []
    # DP pods are a LeaderWorkerSet (StatefulSet-backed) -> they carry the
    # controller-managed `statefulset.kubernetes.io/pod-name` label, which
    # survives restarts, so the per-pod Service can select on it with no manual
    # labeling. Non-DP pods are a plain Deployment with no such stable label,
    # so we fall back to the imperative `vllm-pod-id` label for those.
    use_ss_selector = True

    for pod in pods:
        pod_name: str = pod["metadata"]["name"]
        labels = pod["metadata"].get("labels", {})

        worker_idx = labels.get("leaderworkerset.sigs.k8s.io/worker-index")
        if worker_idx is not None and worker_idx != "0":
            continue

        is_lws = worker_idx is not None or "leaderworkerset.sigs.k8s.io/name" in labels
        if not is_lws:
            # Non-DP Deployment: no stable pod-name label -> apply vllm-pod-id.
            use_ss_selector = False
            try:
                _kubectl(
                    ["label", "pod", pod_name, "-n", namespace,
                     "vllm-pod-id=" + pod_name, "--overwrite"],
                    check=True, capture=True,
                )
            except Exception:
                click.echo(f"[per-pod] WARN: failed to label pod {pod_name}")
                continue

        discovered_pods.append(pod_name)

    if not discovered_pods:
        return []

    _selector = "statefulset.kubernetes.io/pod-name" if use_ss_selector else "vllm-pod-id"
    click.echo(
        f"[per-pod] Registering {len(discovered_pods)} pod(s) via helm upgrade "
        f"--reuse-values (selector={_selector})"
    )

    cmd: List[str] = [
        "upgrade", release, str(chart_dir),
        "-n", namespace,
        "--reuse-values",
        "--timeout", "5m",
        "--set", "perPodServices.enabled=true",
        "--set", f"perPodServices.useStatefulSetSelector={'true' if use_ss_selector else 'false'}",
    ]
    _per_pod_base_port = 31361 + port_offset
    for i, pname in enumerate(discovered_pods):
        cmd.extend(["--set", f"perPodServices.pods[{i}].name={pname}"])
        cmd.extend(["--set", f"perPodServices.pods[{i}].nodePort={_per_pod_base_port + i}"])

    try:
        _helm(cmd, check=True, capture=False)
    except Exception as exc:
        click.echo(f"[per-pod] WARN: helm upgrade for per-pod services failed: {exc}")
        return []

    endpoints: List[Tuple[str, str]] = []
    for pname in discovered_pods:
        svc_name = f"vllm-pp-{pname}"[:63]
        try:
            np_out = subprocess.run(
                ["kubectl", "get", "svc", svc_name, "-n", namespace,
                 "-o", "jsonpath={.spec.ports[0].nodePort}"],
                capture_output=True, text=True, timeout=10,
            ).stdout.strip()
        except Exception:
            np_out = "?"
        endpoints.append((pname, np_out))

    click.echo(f"[per-pod] Created {len(endpoints)} per-pod NodePort service(s)")
    return endpoints


# ---------------------------
# BooM config printer
# ---------------------------

def _print_boom_config(
    cfg,
    helm_cfg,
    models_list: list,
    namespace: str,
    per_pod_endpoints: Optional[List[Tuple[str, str]]] = None,
) -> str:
    """Print and return the BooM / external gateway config for sharing with maintainers."""
    lines: list[str] = []

    def _emit(line: str = "") -> None:
        lines.append(line)
        click.echo(line)

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

    _emit("")
    _emit("=" * 70)
    _emit("  BooM / External Gateway Config  (share with your BooM maintainer)")
    _emit("=" * 70)

    if models_list:
        _emit("")
        _emit("model_list:")
        for m in models_list:
            name = m.get("servedModelName") or m.get("name", "unknown")
            _emit(f"  - model_name: {name}")
            _emit(f"    litellm_params:")
            _emit(f"      model: openai/{name}")
            _emit(f"      api_base: http://{node_ip}:{router_port}/v1")
            _emit(f'      api_key: "{router_api_key}"')
    else:
        model_name = str(getattr(helm_cfg, "model_name", "served-model")).strip() or "served-model"
        _emit("")
        _emit("model_list:")
        _emit(f"  - model_name: {model_name}")
        _emit(f"    litellm_params:")
        _emit(f"      model: openai/{model_name}")
        _emit(f"      api_base: http://{node_ip}:{router_port}/v1")
        _emit(f'      api_key: "{router_api_key}"')

    _emit("")
    _emit(f"Router endpoint:  http://{node_ip}:{router_port}")

    # Per-pod vLLM endpoints (each pod gets its own NodePort)
    if per_pod_endpoints:
        _emit("")
        _emit(f"vLLM per-pod endpoints ({len(per_pod_endpoints)} instances):")
        for pod_name, nodeport in per_pod_endpoints:
            _emit(f"  {pod_name}: http://{node_ip}:{nodeport}/v1")
    else:
        try:
            pod_count = subprocess.run(
                ["kubectl", "get", "pods", "-n", namespace,
                 "-l", "component=vllm",
                 "--field-selector=status.phase=Running",
                 "-o", "jsonpath={range .items[*]}x{end}"],
                capture_output=True, text=True, timeout=10,
            ).stdout.strip()
            if pod_count:
                _emit(f"vLLM instances:   {len(pod_count)} running (via router at :{router_port})")
        except Exception:
            pass

    # BooM NodePort
    boom_port = "30401"
    try:
        out = subprocess.run(
            ["kubectl", "get", "svc", "boom-proxy", "-n", namespace,
             "-o", "jsonpath={.spec.ports[0].nodePort}"],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
        if out.isdigit():
            boom_port = out
            _emit(f"BooM endpoint:    http://{node_ip}:{out}")
    except Exception:
        pass

    # Prometheus endpoint
    prom_port = "31190"
    _emit(f"Prometheus:       http://{node_ip}:{prom_port}/metrics")

    # Served model name (first model)
    served_model = "served-model"
    if models_list:
        served_model = models_list[0].get("servedModelName") or models_list[0].get("name", "served-model")

    boom_api_key = "sk-boom-master"
    boom_cfg = getattr(cfg, "boom", None)
    if boom_cfg:
        boom_api_key = str(getattr(boom_cfg, "api_key", boom_api_key)).strip() or boom_api_key

    _emit("")
    _emit("-" * 70)
    _emit("  Prod Latency Collector (run in a separate terminal)")
    _emit("-" * 70)
    _emit(f"  python prod_latency_collector.py \\")
    _emit(f"      --router-url http://{node_ip}:{router_port} \\")
    _emit(f"      --prometheus-url http://{node_ip}:{prom_port} \\")
    _emit(f"      --poll-interval 5")

    _emit("")
    _emit("-" * 70)
    _emit('  Claude CLI  (~/.claude/settings.json  "env" block)')
    _emit("-" * 70)
    _emit(f'  "env": {{')
    _emit(f'      "ANTHROPIC_AUTH_TOKEN": "{boom_api_key}",')
    _emit(f'      "ANTHROPIC_BASE_URL": "http://{node_ip}:{boom_port}",')
    _emit(f'      "ANTHROPIC_DEFAULT_HAIKU_MODEL": "{served_model}",')
    _emit(f'      "ANTHROPIC_DEFAULT_OPUS_MODEL": "{served_model}",')
    _emit(f'      "ANTHROPIC_DEFAULT_SONNET_MODEL": "{served_model}",')
    _emit(f'      "ANTHROPIC_MODEL": "{served_model}",')
    _emit(f'      "ANTHROPIC_REASONING_MODEL": "{served_model}"')
    _emit(f'  }}')

    _emit("=" * 70)
    _emit("")

    return "\n".join(lines)


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

        # Per-job overrides for release / namespace / portOffset so shadow
        # deployments coexist with the primary deployment in the same master config.
        release = str(getattr(h, "release", "") or "").strip() or RELEASE
        namespace = str(getattr(h, "namespace", "") or "").strip() or "vllm"
        _port_offset = int(getattr(h, "port_offset", 0) or 0)
        _pin_node_override = str(getattr(h, "pin_node_name", "") or "").strip()
        _vllm_node_selector = getattr(cfg, "vllm_node_selector", None)
        _vllm_leader_node_selector = getattr(cfg, "vllm_leader_node_selector", None)
        _vllm_worker_node_selector = getattr(cfg, "vllm_worker_node_selector", None)
        _vllm_avoid_label = str(getattr(cfg, "vllm_avoid_label", "") or "").strip()

        click.echo(f"[sweep] release={release} namespace={namespace} portOffset={_port_offset}")

        backend = str(getattr(cfg, "backend", "router") or "router").strip().lower()

        if backend not in ("router", "aibrix", "litellm", "boom"):
            raise click.ClickException(f"Invalid backend '{backend}' in {cfg_path}")

        deploy_mode = str(getattr(h, "deploy_mode", "helm")).strip().lower()
        cr_name = str(getattr(h, "operator_cr_name", "vllm")).strip() or release

        expose_per_pod = bool(getattr(h, "expose_per_pod", False))

        if deploy_mode == "operator":
            _operator_delete(cr_name=cr_name, namespace=namespace)
        else:
            if not skip_vllm:
                _helm_uninstall(release=release, namespace=namespace)

        set_values: Dict[str, ConfigValue] = {
            "backend": backend,
            "replicas.vllm": int(h.replicas),
            "batchSize": int(h.batch_size),
            "tensorParallelSize": int(getattr(h, "tensor_parallel_size", 1)),
            "router.strategy": str(getattr(h, "router_strategy", "")),
            "router.hashSource": str(getattr(h, "router_hash_source", "inline")).strip().lower(),
            "router.ownerSource": str(getattr(h, "router_owner_source", "lookup")).strip().lower(),
            "router.lookupMaxBlocks": int(getattr(h, "router_lookup_max_blocks", 512)),
            "router.logBlockHashes": bool(getattr(h, "router_log_block_hashes", False)),
            "router.logRequestBody": bool(getattr(h, "router_log_request_body", False)),
            "router.logRequestBodyMaxBytes": int(getattr(h, "router_log_request_body_max_bytes", 16384)),
            "router.measurePrefix": bool(getattr(h, "router_measure_prefix", False)),
            "router.kvAware": bool(getattr(h, "router_kv_aware", True)),
            "router.lenAware": bool(getattr(h, "router_len_aware", True)),
            "router.lenPolicy": str(getattr(h, "router_len_policy", "short_first")),
            "router.apiKey": str(getattr(h, "router_api_key", "")),

            # Conversation key-affinity knobs
            "router.affinityEnabled": bool(getattr(h, "router_affinity_enabled", False)),
            "router.affinityMode": str(getattr(h, "router_affinity_mode", "soft")),
            "router.affinityTtlS": float(getattr(h, "router_affinity_ttl_s", 300.0)),
            "router.affinityHardTimeoutS": float(getattr(h, "router_affinity_hard_timeout_s", 5.0)),

            # Persistent affinity map (Redis-backed) knobs
            "router.affinityPersistEnabled": bool(getattr(h, "router_affinity_persist_enabled", False)),
            "router.affinityRedisTtlSeconds": int(getattr(h, "router_affinity_redis_ttl_seconds", 0)),
            "router.affinityRedisKeyPrefix": str(getattr(h, "router_affinity_redis_key_prefix", "affinity")),
            "router.affinityCacheMax": int(getattr(h, "router_affinity_cache_max", 100000)),
            "router.affinityCacheRefreshS": float(getattr(h, "router_affinity_cache_refresh_s", 0.0)),
            "router.affinityEndpointStaleS": float(getattr(h, "router_affinity_endpoint_stale_s", 1800.0)),
            "router.affinityCluster": str(getattr(h, "router_affinity_cluster", "")),

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

        # ---- Shadow deployment overrides (portOffset, pin, nodeSelector, avoidLabel) ----
        if _port_offset:
            set_values["portOffset"] = _port_offset
            set_values["boom.nodePort"] = 30401 + _port_offset
            set_values["litellm.nodePort"] = 30400 + _port_offset
        if _pin_node_override:
            set_values["pin.nodeName"] = _pin_node_override
        if _vllm_avoid_label:
            set_values["vllm.avoidLabelValue"] = _vllm_avoid_label
        if _vllm_node_selector and isinstance(_vllm_node_selector, dict):
            for k, v in _vllm_node_selector.items():
                set_values[f"vllm.nodeSelector.{k}"] = str(v)
        if _vllm_leader_node_selector and isinstance(_vllm_leader_node_selector, dict):
            for k, v in _vllm_leader_node_selector.items():
                set_values[f"vllm.leaderNodeSelector.{k}"] = str(v)
        if _vllm_worker_node_selector and isinstance(_vllm_worker_node_selector, dict):
            for k, v in _vllm_worker_node_selector.items():
                set_values[f"vllm.workerNodeSelector.{k}"] = str(v)

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
            # cpuHash (legacy external hasher) is auto-deployed by the chart when
            # router.hashSource=external; no explicit toggle needed here.
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
            # cpuHash (legacy external hasher) is auto-deployed by the chart when
            # router.hashSource=external; no explicit toggle needed here.

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
            boom_key_affinity_bench = bool(getattr(h, "boom_key_affinity_bench", False))
            set_values["boom.keyAffinityBench"] = boom_key_affinity_bench
            boom_max_inflight = int(getattr(h, "boom_max_inflight", 0))
            if boom_max_inflight > 0:
                set_values["boom.maxInflight"] = boom_max_inflight
            boom_upstream_timeout = int(getattr(h, "boom_upstream_timeout_seconds", 0) or 0)
            if boom_upstream_timeout > 0:
                set_values["boom.upstreamTimeoutSeconds"] = boom_upstream_timeout
            click.echo(
                f"[sweep] boom.enabled=true masterKey={set_values['boom.masterKey']!r} "
                f"claudeCodeAliases={boom_claude_aliases} routeVia={boom_route_via}"
                f" maxInflight={boom_max_inflight}"
                f" upstreamTimeoutSeconds={boom_upstream_timeout or 'chart-default'}"
                f" keyAffinityBench={boom_key_affinity_bench}"
            )
        else:
            set_values["boom.enabled"] = False

        # ---- Sidecar ----
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

        # ---- LMCache toggle ----
        # LMCache uses the LMCacheAscendConnectorV1Dynamic connector, which does
        # NOT require a Mooncake master. With mooncake_enabled=false it runs
        # "local-cache-first": a large local CPU cache + NDS/P2P for cross-engine
        # sharing and no Mooncake remote store (the direct142 design — see
        # 142/ANALYSIS_kv_cache_drop.md + disaster-recovery.md §11). The lmcache
        # config's remote mooncakestore URL simply stays inactive in that mode.
        lmcache_enabled = bool(getattr(h, "lmcache_enabled", False))
        if lmcache_enabled and not mooncake_enabled:
            click.echo(
                "[sweep] NOTE: lmcache_enabled=true with mooncake_enabled=false — "
                "local-cache-first mode (no Mooncake remote store; NDS/P2P for KV sharing)."
            )
        set_values["lmcache.enabled"] = lmcache_enabled
        lmcache_mode = str(getattr(h, "lmcache_mode", "mooncake") or "mooncake").strip().lower()
        if lmcache_enabled:
            set_values["lmcache.mode"] = lmcache_mode
            lmc_chunk = getattr(h, "lmcache_chunk_size", None)
            if lmc_chunk is not None:
                set_values["lmcache.chunkSize"] = int(lmc_chunk)
            lmc_cpu = getattr(h, "lmcache_max_local_cpu_size", None)
            if lmc_cpu is not None:
                set_values["lmcache.maxLocalCpuSize"] = int(lmc_cpu)
            click.echo(
                f"[sweep] lmcache.enabled=true mode={lmcache_mode} "
                f"chunkSize={set_values.get('lmcache.chunkSize', 'default')} "
                f"maxLocalCpuSize={set_values.get('lmcache.maxLocalCpuSize', 'default')}"
            )

            # ---- LMCache P2P / host-staging mode (142 lineage) ----
            if lmcache_mode == "p2p":
                _hs = getattr(h, "lmcache_use_host_staging", None)
                if _hs is not None:
                    set_values["lmcache.p2p.useHostStaging"] = bool(_hs)
                _osb = getattr(h, "lmcache_os_staging_bytes", None)
                if _osb is not None:
                    set_values["lmcache.p2p.osStagingBytes"] = int(_osb)
                _cpu = str(getattr(h, "lmcache_p2p_controller_pull_url", "") or "").strip()
                if _cpu:
                    set_values["lmcache.p2p.controllerPullUrl"] = _cpu
                _cru = str(getattr(h, "lmcache_p2p_controller_reply_url", "") or "").strip()
                if _cru:
                    set_values["lmcache.p2p.controllerReplyUrl"] = _cru
                set_values["deploy.lmcacheController"] = bool(
                    getattr(h, "deploy_lmcache_controller", True)
                )
                _ci = str(getattr(h, "lmcache_controller_image", "") or "").strip()
                if _ci:
                    set_values["lmcacheController.image"] = _ci
                # p2p mode never uses the Mooncake master (no remote store).
                set_values["deploy.mooncakeMaster"] = False
                if not _cpu or not _cru:
                    click.echo(
                        "[sweep] WARNING: lmcache.mode=p2p but controller pull/reply URL "
                        "unset — engines won't find the lmcache_controller. Set "
                        "lmcache_p2p_controller_pull_url / _reply_url in the config."
                    )
                click.echo(
                    f"[sweep] lmcache.mode=p2p (142 host-staging): mooncake master OFF, "
                    f"controller deploy={set_values['deploy.lmcacheController']} "
                    f"pull={_cpu or '(UNSET)'} reply={_cru or '(UNSET)'}"
                )

        # ---- NDS (NVMe Direct Storage — P2P DMA for KV cache) ----
        nds_enabled = bool(getattr(h, "lmcache_nds_enabled", False))
        if nds_enabled:
            set_values["lmcache.nds.enabled"] = True
            nds_path = str(getattr(h, "lmcache_nds_path", "") or "").strip()
            if nds_path:
                set_values["lmcache.nds.path"] = nds_path
            nds_dev = str(getattr(h, "lmcache_nds_dev", "") or "").strip()
            if nds_dev:
                set_values["lmcache.nds.dev"] = nds_dev
            nds_size = getattr(h, "lmcache_nds_size", None)
            if nds_size is not None:
                set_values["lmcache.nds.size"] = int(nds_size)
            nds_xds_leader = str(getattr(h, "lmcache_nds_xds_path_leader", "") or "").strip()
            if nds_xds_leader:
                set_values["lmcache.nds.xdsPathLeader"] = nds_xds_leader
            nds_xds_worker = str(getattr(h, "lmcache_nds_xds_path_worker", "") or "").strip()
            if nds_xds_worker:
                set_values["lmcache.nds.xdsPathWorker"] = nds_xds_worker
            click.echo(
                f"[sweep] lmcache.nds.enabled=true "
                f"path={nds_path} dev={nds_dev} size={nds_size} "
                f"xdsPathLeader={nds_xds_leader or '(default)'} "
                f"xdsPathWorker={nds_xds_worker or '(default)'}"
            )

        # ---- New vLLM fields (dtype, schedulerCls, modelLoaderExtraConfig, etc.) ----
        _dtype = str(getattr(h, "dtype", "") or "").strip()
        if _dtype and _dtype != "auto":
            set_values["vllm.dtype"] = _dtype
        _sched_cls = str(getattr(h, "scheduler_cls", "") or "").strip()
        if _sched_cls:
            set_values["vllm.schedulerCls"] = _sched_cls
        _mlec = str(getattr(h, "model_loader_extra_config", "") or "").strip()
        if _mlec:
            set_values["vllm.modelLoaderExtraConfig"] = _mlec
        _flashcomm = bool(getattr(h, "ascend_enable_flashcomm1", False))
        if _flashcomm:
            set_values["vllm.ascendEnableFlashcomm1"] = True

        # ---- Image overrides (bypass registry rewrite — used for local images) ----
        _mc_img = str(getattr(h, "mooncake_master_image", "") or "").strip()
        if _mc_img:
            set_values["images.mooncakeMasterRaw"] = _mc_img

        service_impl = str(getattr(h, "service_impl", "python")).strip().lower()
        if service_impl not in ("python", "go"):
            raise click.ClickException(
                f"Invalid service_impl '{service_impl}' in {cfg_path} (expected 'python' or 'go')"
            )
        # Single Helm-native knob: the chart's vllmkv.routerImage / vllmkv.sidecarImage
        # helpers swap router+sidecar images for their Go equivalents
        # (images.routerGo / images.sidecarGo) when serviceImpl=go. The prefix-hash
        # service stays Python (cpuHash image is never overridden): HuggingFace
        # tokenizers + vLLM block hashing are too heavy to port to Go.
        set_values["serviceImpl"] = service_impl

        set_values["autoscaling.enabled"] = bool(h.autoscaling_enabled)

        if bool(h.autoscaling_enabled):
            set_values["autoscaling.minReplicaCount"] = int(h.autoscaling_min)
            set_values["autoscaling.maxReplicaCount"] = int(h.autoscaling_max)
            set_values["autoscaling.threshold"] = str(h.autoscaling_threshold)
            set_values["autoscaling.signal"] = str(h.autoscaling_signal)
            set_values["autoscaling.vllmThreshold"] = str(h.autoscaling_vllm_threshold)
            set_values["autoscaling.prometheusServerAddress"] = str(
                h.autoscaling_prometheus_server_address
            )
            set_values["autoscaling.pollingInterval"] = int(h.autoscaling_polling_interval)
            set_values["autoscaling.cooldownPeriod"] = int(h.autoscaling_cooldown_period)

            # Optional advanced full-query override. Empty -> chart builds the
            # signal-derived per-model query (router_central_queue_length_by_model
            # or vllm:gpu_cache_usage_perc). Routed through perModel only when set
            # for a single-model sweep; otherwise left to the chart defaults.
            q = str(h.autoscaling_prometheus_query or "").strip()
            q = " ".join(q.split())
            if q:
                set_values["autoscaling.vllmQuery" if str(h.autoscaling_signal) == "vllm"
                           else "autoscaling.prometheusQuery"] = q

        # ---- Unified models[] — auto-migrate legacy flat config if needed ----
        migrate_legacy_helm_to_models(h)
        models_list = list(h.models or [])
        if not models_list:
            raise click.ClickException(
                f"No models defined in {cfg_path}. "
                f"Add helm.models[] or legacy flat vllm_*/data_parallel_* fields."
            )

        # Inject vllm_image into per-model image field (bypasses registry rewrite)
        _vllm_img = str(getattr(h, "vllm_image", "") or "").strip()
        if _vllm_img:
            for mdef in models_list:
                if not mdef.get("image"):
                    mdef["image"] = _vllm_img

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

        sidecar_prefetch = int(first_model.get("sidecarPrefetch", 0))
        if sidecar_prefetch > 0:
            set_values["sidecar.prefetch"] = sidecar_prefetch

        gen = getattr(cfg, "generation", None)
        force_eos = first_model.get("forceIgnoreEos", False)
        if not force_eos and gen is not None:
            if getattr(gen, "replay_output_lengths_from", None) or getattr(gen, "use_dataset_output_len", False):
                force_eos = True
        if force_eos:
            set_values["sidecar.forceIgnoreEos"] = True

        sidecar_streaming = first_model.get("streamingMode", False)
        if sidecar_streaming:
            set_values["sidecar.streamingMode"] = True

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

        # ---- Explicit Helm dot-path overlay ----
        # Config-local values win over generated sweep defaults. When absent,
        # legacy configs follow the exact old set_values path.
        raw_helm_values = getattr(h, "values", {}) or {}
        if raw_helm_values:
            if not isinstance(raw_helm_values, dict):
                raise click.ClickException("helm.values must be a mapping of Helm dot-paths to values")
            explicit_values = _flatten_helm_values("", raw_helm_values)
            set_values.update(explicit_values)
            click.echo(f"[sweep] applied {len(explicit_values)} explicit helm.values override(s)")

        click.echo(f"[sweep] backend={backend}  deploy_mode={deploy_mode}  skip_vllm={skip_vllm}")
        click.echo("[sweep] set values:")
        for k in sorted(set_values):
            click.echo(f"  - {k}={_coerce_set_value(set_values[k])}")

        # ---- Pre-create experiment dir + start pod-log capture BEFORE deploy ----
        # So experiments/<id>/ and vllm-logs/ exist from the start of the sweep step
        # and the collector streams the new pods' model-load phase live. This is safe
        # because the pre-deploy `helm uninstall` above waits for the previous step's
        # pods to fully terminate, so the namespace is clean before we attach. The pod
        # lister also skips any Terminating straggler as a safety net. The client reuses
        # this dir via FORCE_EXPERIMENT_DIR.
        _collect_logs = bool(getattr(cfg, "collect_vllm_logs", False))
        exp_dir: Optional[Path] = None
        _log_collectors = None
        if _collect_logs:
            try:
                exp_dir = _allocate_experiment_dir()
                (exp_dir / "vllm-logs").mkdir(parents=True, exist_ok=True)
                try:
                    shutil.copy2(cfg_path, exp_dir / "config_used.yaml")
                except Exception:
                    pass
                click.echo(f"[sweep] pre-created experiment dir: {exp_dir}")
                _log_collectors = _start_log_collectors(namespace, exp_dir / "vllm-logs")
            except Exception as e:
                click.echo(f"[logs] WARN: failed to pre-create experiment dir: {e}")
                exp_dir = None
                _log_collectors = None

        max_redeploy_attempts = 3
        redeploy_sleep_s = 10

        last_err: Optional[Exception] = None
        for attempt in range(1, max_redeploy_attempts + 1):
            click.echo(f"[deploy] attempt {attempt}/{max_redeploy_attempts}")

            if deploy_mode == "operator":
                _operator_apply(cr_name=cr_name, namespace=namespace, set_values=set_values)
                _wait_cr_phase(cr_name, namespace)
            else:
                # Debug: dump rendered templates to /tmp/helm_debug.yaml
                try:
                    rendered = _helm_template(
                        release=release,
                        chart_dir=chart_dir,
                        namespace=namespace,
                        values_file=values_file if values_file.is_file() else None,
                        set_values=set_values,
                    )
                    with open("/tmp/helm_debug.yaml", "w") as f:
                        f.write(rendered)
                    click.echo(f"[DEBUG] Rendered {len(rendered)} bytes to /tmp/helm_debug.yaml")
                    for i, doc in enumerate(rendered.split("\n---\n")):
                        stripped = doc.strip()
                        if stripped and "apiVersion" not in stripped:
                            click.echo(f"[DEBUG] Doc {i} ({len(stripped)} chars) missing apiVersion:")
                            click.echo(stripped[:300])
                except Exception as e:
                    click.echo(f"[DEBUG] helm template failed: {e}")
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
            if _log_collectors is not None:
                _stop_log_collectors(_log_collectors)
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

        # ---- Create per-pod NodePort services for direct vLLM access ----
        per_pod_endpoints = (
            _create_per_pod_services(
                namespace=namespace,
                release=release,
                chart_dir=chart_dir,
                port_offset=_port_offset,
            )
            if expose_per_pod
            else []
        )

        try:
            rendered_text = _helm_template(
                release=effective_release,
                chart_dir=chart_dir,
                namespace=namespace,
                values_file=values_file if values_file.is_file() else None,
                set_values=set_values,
            )
            (REPO_ROOT / "vllm-k8s.yaml").write_text(rendered_text, encoding="utf-8")
        except Exception as e:
            click.echo(f"[sweep] WARN: helm template for vllm-k8s.yaml failed: {e}")
            (REPO_ROOT / "vllm-k8s.yaml").write_text("# helm template failed\n", encoding="utf-8")

        # ---- Print BooM / external gateway config for maintainer ----
        _boom_config_text = _print_boom_config(cfg, h, models_list, namespace, per_pod_endpoints)

        # Write deployment info immediately (before load test) so it's
        # available even if the client run is long or gets interrupted.
        _deploy_info_path = EXPERIMENTS_ROOT / f"deployment-info-{release}.txt"
        try:
            EXPERIMENTS_ROOT.mkdir(parents=True, exist_ok=True)
            _deploy_info_path.write_text(
                _boom_config_text or "(no deployment info captured)", encoding="utf-8"
            )
            click.echo(f"[sweep] wrote {_deploy_info_path}")
        except Exception as e:
            click.echo(f"[sweep] WARN: failed to write deployment-info: {e}")

        # If the experiment dir was pre-created, drop the deploy artifacts in now
        # (before the client runs) so they're available live alongside vllm-logs/.
        if exp_dir is not None:
            try:
                (exp_dir / "vllm-k8s.yaml").write_text(rendered_text, encoding="utf-8")
                (exp_dir / "deployment-info.txt").write_text(
                    _boom_config_text or "(no deployment info captured)", encoding="utf-8"
                )
                _hv_early = REPO_ROOT / "helm-effective-values.yaml"
                if _hv_early.is_file():
                    shutil.copy2(_hv_early, exp_dir / "helm-effective-values.yaml")
            except Exception as e:
                click.echo(f"[sweep] WARN: early artifact write failed: {e}")

        tmp_cfg_path = _write_temp_job_config(cfg, method)

        # ---- Run the client ----
        # When exp_dir was pre-created, the client reuses it via FORCE_EXPERIMENT_DIR
        # (so config.json/logs.json land in the same folder that already holds
        # vllm-logs/). Otherwise the client allocates the dir and we detect it after.
        before = _snapshot_existing_experiments()
        _client_env = dict(os.environ)
        if exp_dir is not None:
            _client_env["FORCE_EXPERIMENT_DIR"] = str(exp_dir)

        client_proc = subprocess.Popen(
            [sys.executable, str(REPO_ROOT / "main.py"), "--config", str(tmp_cfg_path)],
            text=True, env=_client_env,
        )
        click.echo(
            f"[cmd] {sys.executable} main.py --config {tmp_cfg_path} (pid={client_proc.pid})"
        )
        try:
            client_proc.wait()
        finally:
            if _log_collectors is not None:
                _stop_log_collectors(_log_collectors)
            try:
                tmp_cfg_path.unlink(missing_ok=True)
            except Exception:
                pass
            if _models_values_file is not None:
                try:
                    _models_values_file.unlink(missing_ok=True)
                except Exception:
                    pass

        if client_proc.returncode not in (0, None):
            raise click.ClickException(f"client run failed (exit={client_proc.returncode})")

        if exp_dir is None:
            exp_dir = _newest_experiment_dir(before)

        if exp_dir is None:
            click.echo("[warn] could not detect new experiment dir; skipping artifact snapshot")
            continue

        (exp_dir / "vllm-k8s.yaml").write_text(rendered_text, encoding="utf-8")
        (exp_dir / "deployment-info.txt").write_text(
            _boom_config_text or "(no deployment info captured)", encoding="utf-8"
        )
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
                "autoscaling_signal": str(getattr(h, "autoscaling_signal", "queue")),
                "autoscaling_vllm_threshold": str(getattr(h, "autoscaling_vllm_threshold", "0.8")),
                "autoscaling_prometheus_server_address": str(
                    getattr(h, "autoscaling_prometheus_server_address", "")
                ),
                "router_strategy": str(getattr(h, "router_strategy", "")),
                "router_hash_source": str(getattr(h, "router_hash_source", "inline")),
                "router_owner_source": str(getattr(h, "router_owner_source", "lookup")),
                "router_lookup_max_blocks": int(getattr(h, "router_lookup_max_blocks", 512)),
                "router_kv_aware": bool(getattr(h, "router_kv_aware", True)),
                "router_len_aware": bool(getattr(h, "router_len_aware", True)),
                "router_len_policy": str(getattr(h, "router_len_policy", "short_first")),
                "router_affinity_enabled": bool(getattr(h, "router_affinity_enabled", False)),
                "router_affinity_mode": str(getattr(h, "router_affinity_mode", "soft")),
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