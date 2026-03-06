#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiohttp
import click
import datetime as dt
import json
import os
import pathlib
import re
import subprocess
import time
from typing import Dict, List, Optional, Tuple

import yaml
import matplotlib.pyplot as plt

# --- reuse your config + k8s helpers ---
from config import get_config, dump_config_dict
from utils_k8s import load_kube, discover_endpoints
from kubernetes import client as k8s_client  # type: ignore

# ---------------------------
# shell / io helpers
# ---------------------------


def sh(cmd: List[str], check=True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def now_str() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dir(p: pathlib.Path):
    p.mkdir(parents=True, exist_ok=True)


def next_experiment_dir(root: pathlib.Path) -> pathlib.Path:
    ensure_dir(root)
    nums = [int(d.name) for d in root.iterdir() if d.is_dir() and d.name.isdigit()]
    n = (max(nums) + 1) if nums else 1
    out = root / str(n)
    ensure_dir(out)
    return out


def parse_batches(batches_str: str) -> List[int]:
    parts = re.split(r"[,\s]+", batches_str.strip())
    vals = [int(p) for p in parts if p]
    if not vals:
        raise click.UsageError("No batch sizes parsed from --batches.")
    return vals


def load_prompts_file(prompts_file: str) -> List[str]:
    with open(prompts_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    if (
        isinstance(data, list)
        and data
        and isinstance(data[0], dict)
        and "prompt" in data[0]
    ):
        return [x["prompt"] for x in data]
    if isinstance(data, list) and data and isinstance(data[0], str):
        return data
    raise click.UsageError(
        "Prompts JSON must be a list of strings OR a list of objects with a 'prompt' key."
    )


# ---------------------------
# YAML patching utilities
# ---------------------------


def find_first(docs: List[dict], kind: str) -> Optional[dict]:
    for d in docs:
        if isinstance(d, dict) and d.get("kind") == kind:
            return d
    return None


def get_container_args(dep: dict) -> Optional[List[str]]:
    try:
        return dep["spec"]["template"]["spec"]["containers"][0]["args"]
    except Exception:
        return None


def set_container_args(dep: dict, args: List[str]):
    dep["spec"]["template"]["spec"]["containers"][0]["args"] = args


def upsert_max_num_seqs(args: List[str], value: int) -> List[str]:
    out = list(args) if args else []
    if "--max-num-seqs" in out:
        i = out.index("--max-num-seqs")
        if i + 1 < len(out):
            out[i + 1] = str(value)
        else:
            out.append(str(value))
    else:
        out.extend(["--max-num-seqs", str(value)])
    return out


def patch_service_to_nodeport(svc: dict, node_port: int, port_name: str = "http"):
    spec = svc.setdefault("spec", {})
    spec["type"] = "NodePort"
    ports = spec.setdefault("ports", [])
    target_idx = None
    for idx, p in enumerate(ports):
        if p.get("name") == port_name:
            target_idx = idx
            break
    if target_idx is None and ports:
        target_idx = 0
    elif target_idx is None:
        ports.append(
            {"name": port_name, "port": 8200, "targetPort": 8200, "protocol": "TCP"}
        )
        target_idx = 0
    ports[target_idx]["nodePort"] = node_port


def write_multi_yaml(docs: List[dict], path: pathlib.Path):
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump_all(docs, f, sort_keys=False)


def kubectl_apply(yaml_path: pathlib.Path):
    res = sh(["kubectl", "apply", "-f", str(yaml_path)], check=False)
    if res.returncode != 0:
        raise RuntimeError(
            f"kubectl apply failed:\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )


def rollout_status(namespace: str, deployment_name: str, timeout_s: int = 900):
    res = sh(
        [
            "kubectl",
            "-n",
            namespace,
            "rollout",
            "status",
            f"deployment/{deployment_name}",
            f"--timeout={timeout_s}s",
        ],
        check=False,
    )
    if res.returncode != 0:
        raise RuntimeError(f"Rollout status failed:\n{res.stdout}\n{res.stderr}")


def wait_for_health(url: str, timeout_s: int = 600, interval_s: float = 2.0):
    import urllib.request

    t0 = time.time()
    while time.time() - t0 < timeout_s:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    return
        except Exception:
            pass
        time.sleep(interval_s)
    raise TimeoutError(f"Timed out waiting for health: {url}")


def make_patched_yaml(
    original_yaml: pathlib.Path,
    work_dir: pathlib.Path,
    batch: int,
    node_port_override: Optional[int],
    initial_replicas: Optional[int] = None,
) -> Tuple[pathlib.Path, str, str, int]:
    """
    Returns (patched_yaml_path, namespace, deployment_name, service_node_port).
    - sets replicas to `initial_replicas` if provided, else 1
    - sets/updates --max-num-seqs
    - if node_port_override is given, makes Service NodePort with that nodePort
    """
    with open(original_yaml, "r", encoding="utf-8") as f:
        docs = list(yaml.safe_load_all(f))

    dep = find_first(docs, "Deployment")
    svc = find_first(docs, "Service")
    if dep is None or svc is None:
        raise click.UsageError("YAML must contain both a Deployment and a Service.")

    namespace = dep.get("metadata", {}).get("namespace") or "default"
    deployment_name = dep.get("metadata", {}).get("name") or "vllm-deployment"

    # replicas
    dep.setdefault("spec", {})["replicas"] = (
        int(initial_replicas) if initial_replicas is not None else 1
    )

    # args
    args = get_container_args(dep)
    if args is None:
        raise click.UsageError("Could not find containers[0].args in Deployment.")
    args = upsert_max_num_seqs(args, batch)
    set_container_args(dep, args)

    if node_port_override is not None:
        patch_service_to_nodeport(svc, node_port_override)

    patched = work_dir / f"patched_batch_{batch}.yaml"
    write_multi_yaml(docs, patched)

    # figure nodePort to use
    try:
        ports = svc["spec"]["ports"]
        http_port = next((p for p in ports if p.get("name") == "http"), ports[0])
        service_node_port = int(http_port.get("nodePort"))
    except Exception:
        raise click.UsageError(
            "Service has no nodePort; pass --node-port to force one in the patched copy."
        )

    return patched, namespace, deployment_name, service_node_port


# ---------------------------
# NPU / k8s helpers
# ---------------------------


def k_get_pods_json(namespace: str, label_selector: str) -> dict:
    res = sh(
        ["kubectl", "-n", namespace, "get", "pods", "-l", label_selector, "-o", "json"],
        check=False,
    )
    if res.returncode != 0:
        raise RuntimeError(f"kubectl get pods failed:\n{res.stdout}\n{res.stderr}")
    return json.loads(res.stdout or "{}")


def pod_is_ready(p: dict) -> bool:
    try:
        if p.get("status", {}).get("phase") != "Running":
            return False
        for cond in p.get("status", {}).get("conditions", []):
            if cond.get("type") == "Ready" and cond.get("status") == "True":
                return True
        return False
    except Exception:
        return False


def count_ready_pods(namespace: str, label_selector: str) -> int:
    pod_json = k_get_pods_json(namespace, label_selector)
    items = pod_json.get("items", [])
    return sum(1 for p in items if pod_is_ready(p))


def wait_for_min_ready_pods(
    namespace: str,
    label_selector: str,
    min_ready: int,
    timeout_s: int = 300,
    interval_s: float = 2.0,
):
    """Wait until at least `min_ready` pods are Ready (or timeout)."""
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        ready = count_ready_pods(namespace, label_selector)
        click.echo(f"[{now_str()}] Ready pods: {ready}/{min_ready} (waiting)...")
        if ready >= min_ready:
            return
        time.sleep(interval_s)
    raise TimeoutError(
        f"Timed out waiting for {min_ready} Ready pods (label={label_selector})."
    )


def scale_deployment(namespace: str, deployment_name: str, replicas: int):
    res = sh(
        [
            "kubectl",
            "-n",
            namespace,
            "scale",
            f"deployment/{deployment_name}",
            f"--replicas={replicas}",
        ],
        check=False,
    )
    if res.returncode != 0:
        raise RuntimeError(f"kubectl scale failed:\n{res.stdout}\n{res.stderr}")


def annotate_pod(namespace: str, pod_name: str, key: str, value: str):
    res = sh(
        [
            "kubectl",
            "-n",
            namespace,
            "annotate",
            "pod",
            pod_name,
            f"{key}={value}",
            "--overwrite",
        ],
        check=False,
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"kubectl annotate failed for {pod_name}:\n{res.stdout}\n{res.stderr}"
        )


def annotate_pods(namespace: str, pod_names: List[str], key: str, value: str):
    for n in pod_names:
        annotate_pod(namespace, n, key, value)


def wait_s(seconds: float):
    time.sleep(seconds)


def build_label_selector_from_cfg_or_default(cfg, deployment_name: str) -> str:
    """
    Prefer cfg.LABEL_SELECTOR. If absent, fall back to 'app=<deployment_name>'.
    Adjust if your labels differ.
    """
    sel = getattr(cfg, "LABEL_SELECTOR", None)
    if sel and isinstance(sel, str) and sel.strip():
        return sel.strip()
    return f"app={deployment_name}"


# --- discovery hardening (kept; health probes via Service below) ---


def list_ready_pod_ips_via_api(
    core_api, namespace: str, label_selector: str
) -> List[str]:
    """Return PodIPs for pods that are Ready == True."""
    ips: List[str] = []
    pods = core_api.list_namespaced_pod(
        namespace=namespace, label_selector=label_selector
    ).items
    for p in pods:
        cnd_ready = False
        for c in p.status.conditions or []:
            if c.type == "Ready" and c.status == "True":
                cnd_ready = True
                break
        ip = p.status.pod_ip
        if cnd_ready and ip:
            ips.append(ip)
    return ips


def wait_for_ready_pod_ips(
    core_api,
    namespace: str,
    label_selector: str,
    min_ips: int,
    timeout_s: int = 180,
    interval_s: float = 2.0,
) -> List[str]:
    """Wait until at least min_ips Ready podIPs are visible."""
    t0 = time.time()
    last: List[str] = []
    while time.time() - t0 < timeout_s:
        ips = list_ready_pod_ips_via_api(core_api, namespace, label_selector)
        if len(ips) >= min_ips:
            return ips
        last = ips
        time.sleep(interval_s)
    raise TimeoutError(
        f"Timed out waiting for {min_ips} Ready pod IPs; last seen: {last}"
    )


# --- drain-first helpers to switch batch cleanly ---


def deployment_exists(namespace: str, deployment_name: str) -> bool:
    res = sh(
        ["kubectl", "-n", namespace, "get", "deployment", deployment_name], check=False
    )
    return res.returncode == 0


def wait_for_pods_gone_by_selector(
    namespace: str, label_selector: str, timeout_s: int = 600, interval_s: float = 2.0
):
    """Wait until no pods exist for the given label selector."""
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        res = sh(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "pods",
                "-l",
                label_selector,
                "-o",
                "json",
            ],
            check=False,
        )
        if res.returncode == 0:
            js = json.loads(res.stdout or "{}")
            items = js.get("items", []) or []
            if not items:
                return
        time.sleep(interval_s)
    raise TimeoutError(
        f"Timed out waiting for all pods with selector '{label_selector}' to disappear."
    )


def peek_deployment_meta(yaml_path: pathlib.Path) -> Tuple[str, str, str]:
    """
    Read the deployment name/namespace from the YAML so we can drain before applying the next batch.
    Returns (namespace, name, label_selector).
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        docs = list(yaml.safe_load_all(f))
    dep = find_first(docs, "Deployment") or {}
    ns = dep.get("metadata", {}).get("namespace") or "default"
    name = dep.get("metadata", {}).get("name") or "vllm-deployment"
    label_selector = build_label_selector_from_cfg_or_default(get_config(), name)
    return ns, name, label_selector


# --- crash-prune via annotation + scale (no manual delete) ---


def run_npu_crash_prune(
    namespace: str,
    deployment_name: str,
    label_selector: str,
    initial_replicas: int,
    target_replicas: int,
    crash_wait_s: int,
    rollout_timeout: int,
):
    """
    Crash-prune procedure using annotations + scale (no manual deletes):
      1) Wait for crash discrimination window (crash_wait_s).
      2) Identify Ready vs Unready pods.
      3) Annotate pods we want to KEEP with high deletion-cost (1000),
         annotate others with low deletion-cost (-1000).
      4) Scale Deployment to target_replicas and wait for rollout.
    """
    click.echo(
        f"[{now_str()}] [NPU] Waiting {crash_wait_s}s for crash discrimination..."
    )
    wait_s(crash_wait_s)

    pod_json = k_get_pods_json(namespace, label_selector)
    items = pod_json.get("items", [])
    ready = [p for p in items if pod_is_ready(p)]
    unready = [p for p in items if not pod_is_ready(p)]

    click.echo(
        f"[{now_str()}] [NPU] Pods: ready={len(ready)} unready={len(unready)} (total={len(items)})"
    )

    # Sort unready first (CrashLoopBackOff preferred for termination)
    def crash_score(p):
        cs = p.get("status", {}).get("containerStatuses", []) or []
        if (
            cs
            and cs[0].get("state", {}).get("waiting", {}).get("reason")
            == "CrashLoopBackOff"
        ):
            return 0
        return 1

    unready_sorted = sorted(unready, key=crash_score)

    # If we have more ready pods than target, we prefer keeping the newest
    def start_ts(p):
        return p.get("status", {}).get("startTime", "9999-12-31T23:59:59Z")

    ready_sorted = sorted(ready, key=start_ts)  # keep the newest

    pods_to_keep = (
        ready_sorted[-target_replicas:]
        if len(ready_sorted) >= target_replicas
        else ready_sorted
    )
    keep_names = {p["metadata"]["name"] for p in pods_to_keep}

    # Everyone else is a candidate to remove first
    other_ready = [p for p in ready_sorted if p["metadata"]["name"] not in keep_names]
    candidates = unready_sorted + other_ready

    keep_list = list(keep_names)
    others_list = [p["metadata"]["name"] for p in candidates]

    click.echo(
        f"[{now_str()}] [NPU] Annotating keepers ({len(keep_list)}) with high deletion-cost, "
        f"candidates ({len(others_list)}) with low deletion-cost..."
    )
    try:
        if keep_list:
            annotate_pods(
                namespace,
                keep_list,
                "controller.kubernetes.io/pod-deletion-cost",
                "1000",
            )
        if others_list:
            annotate_pods(
                namespace,
                others_list,
                "controller.kubernetes.io/pod-deletion-cost",
                "-1000",
            )
    except Exception as e:
        click.echo(f"[{now_str()}] [NPU] WARNING: annotation step failed: {e}")

    # Scale down — controller terminates lowest-cost pods first
    click.echo(
        f"[{now_str()}] [NPU] Scaling {deployment_name} to target {target_replicas} and waiting for rollout..."
    )
    scale_deployment(namespace, deployment_name, target_replicas)
    rollout_status(namespace, deployment_name, timeout_s=rollout_timeout)
    click.echo(
        f"[{now_str()}] [NPU] Crash-prune complete via scale-down. Kept {target_replicas}."
    )


# ---------------------------
# traffic + logging
# ---------------------------


async def send_one(
    session: aiohttp.ClientSession,
    api_url: str,
    payload: dict,
    req_id: int,
    batch_id: int,
) -> Dict:
    """Send one request and return a record; printing happens in arrival handlers."""
    t0 = time.time()
    start_iso = dt.datetime.now().isoformat(timespec="milliseconds")
    try:
        async with session.post(api_url, json=payload, timeout=session.timeout) as resp:
            txt = await resp.text()
            t1 = time.time()
            out: Dict = {
                "request_id": req_id,
                "batch_id": batch_id,
                "status": resp.status,
                "latency_s": t1 - t0,
                "start_time": start_iso,
                "end_time": dt.datetime.now().isoformat(timespec="milliseconds"),
                "response_raw_len": len(txt),
                "output_tokens": None,
                "error": None,
            }
            if resp.status == 200:
                try:
                    js = json.loads(txt)
                    out["output_tokens"] = js.get("usage", {}).get("completion_tokens")
                except Exception:
                    pass
            else:
                out["error"] = txt[:1000]
            return out
    except Exception as e:
        t1 = time.time()
        return {
            "request_id": req_id,
            "batch_id": batch_id,
            "status": -1,
            "latency_s": t1 - t0,
            "start_time": start_iso,
            "end_time": dt.datetime.now().isoformat(timespec="milliseconds"),
            "response_raw_len": 0,
            "output_tokens": None,
            "error": repr(e),
        }


async def blast_arrivals(
    session: aiohttp.ClientSession,
    api_url: str,
    served_model_name: str,
    prompts: List[str],
    start_request_id: int,
    batch_id: int,
) -> List[Dict]:
    """Create tasks for the given prompts; print arrivals in real time; return results in arrival order."""
    tasks = []
    for i, prompt in enumerate(prompts):
        payload = {
            "model": served_model_name,
            "prompt": prompt,
            "max_tokens": 4096,
            "temperature": 0,
            "ignore_eos": True,
        }
        tasks.append(
            asyncio.create_task(
                send_one(
                    session,
                    api_url,
                    payload,
                    req_id=start_request_id + i,
                    batch_id=batch_id,
                )
            )
        )

    results: List[Dict] = []
    for fut in asyncio.as_completed(tasks):
        r = await fut
        results.append(r)
        # arrival log
        rid = r.get("request_id")
        bid = r.get("batch_id")
        status = r.get("status")
        lat = r.get("latency_s")
        end_ts = r.get("end_time")
        otoks = r.get("output_tokens")
        err = r.get("error")
        if status == 200:
            click.echo(
                f"[{end_ts}] [Batch {bid:02d}] [Req {rid:03d}] ✅ 200 in {lat:.2f}s (tokens={otoks})"
            )
        else:
            click.echo(
                f"[{end_ts}] [Batch {bid:02d}] [Req {rid:03d}] ❌ {status} in {lat:.2f}s  err={err}"
            )
    return results


async def run_burst(
    api_url: str,
    served_model_name: str,
    prompts_pool: List[str],
    request_timeout_s: int,
) -> List[Dict]:
    """Backward-compatible: send all requests at once with batch_id=1."""
    headers = {"Content-Type": "application/json"}
    timeout = aiohttp.ClientTimeout(total=request_timeout_s)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        return await blast_arrivals(
            session,
            api_url,
            served_model_name,
            prompts_pool,
            start_request_id=1,
            batch_id=1,
        )


async def run_waves(
    api_url: str,
    served_model_name: str,
    prompt_list: List[str],
    total_requests: int,
    batch_size: int,
    request_timeout_s: int,
) -> List[Dict]:
    """
    Wave mode: send 'batch_size' requests, wait for all to finish, then the next wave,
    until 'total_requests' are sent. Each wave uses batch_id = wave_number (1-based).
    """
    headers = {"Content-Type": "application/json"}
    timeout = aiohttp.ClientTimeout(total=request_timeout_s)
    results: List[Dict] = []
    sent = 0
    wave = 0
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        while sent < total_requests:
            wave += 1
            this_wave = min(batch_size, total_requests - sent)
            # round-robin prompts
            wave_prompts = [
                prompt_list[(sent + i) % len(prompt_list)] for i in range(this_wave)
            ]
            click.echo(f"[{now_str()}] Wave {wave}: sending {this_wave} requests...")
            wave_results = await blast_arrivals(
                session,
                api_url,
                served_model_name,
                wave_prompts,
                start_request_id=sent + 1,
                batch_id=wave,
            )
            results.extend(wave_results)
            click.echo(f"[{now_str()}] Wave {wave} completed.")
            sent += this_wave
    return results


def save_csv(rows: List[Dict], path: pathlib.Path):
    """Save rows sorted by arrival time (end_time ascending), keeping request_id and batch_id as columns."""
    import csv
    from datetime import datetime

    if not rows:
        return

    def to_ts(r):
        try:
            return datetime.fromisoformat(r.get("end_time", "1970-01-01T00:00:00.000"))
        except Exception:
            return datetime(1970, 1, 1)

    rows_sorted = sorted(rows, key=to_ts)

    keys = [
        "request_id",
        "batch_id",
        "status",
        "latency_s",
        "start_time",
        "end_time",
        "response_raw_len",
        "output_tokens",
        "error",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows_sorted:
            w.writerow({k: r.get(k) for k in keys})


def plot_latency_summary(all_results: Dict[int, List[Dict]], outdir: pathlib.Path):
    batches = sorted(all_results.keys())
    medians, p95s = [], []
    for b in batches:
        lats = [r["latency_s"] for r in all_results[b] if r["status"] == 200]
        if not lats:
            medians.append(float("nan"))
            p95s.append(float("nan"))
            continue
        l = sorted(lats)
        n = len(l)
        med = l[n // 2] if n % 2 else 0.5 * (l[n // 2 - 1] + l[n // 2])
        p95 = l[max(0, int(0.95 * (n - 1)))]
        medians.append(med)
        p95s.append(p95)
    plt.figure()
    plt.plot(batches, medians, marker="o", label="Median")
    plt.plot(batches, p95s, marker="o", label="P95")
    plt.xlabel("Batch size (--max-num-seqs)")
    plt.ylabel("Latency (s)")
    plt.title("Latency vs Batch Size")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(outdir / "latency_vs_batch.png", dpi=150)
    plt.close()


def plot_latency_distributions(
    all_results: Dict[int, List[Dict]], outdir: pathlib.Path
):
    batches = sorted(all_results.keys())
    data, labels = [], []
    for b in batches:
        lats = [r["latency_s"] for r in all_results[b] if r["status"] == 200]
        if lats:
            data.append(lats)
            labels.append(str(b))
    if not data:
        return
    plt.figure()
    plt.boxplot(data, labels=labels, showfliers=False)
    plt.xlabel("Batch size (--max-num-seqs)")
    plt.ylabel("Latency (s)")
    plt.title("Latency Distributions per Batch Size")
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(outdir / "latency_distributions.png", dpi=150)
    plt.close()


# ---------------------------
# CLI with defaults from config.py
# ---------------------------


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--yaml-file",
    default="/home/saeid/llm-lb/src/vllm-k8s-npu.yaml",  # default to GPU yaml
    show_default=True,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    help="Path to your vLLM Kubernetes YAML. Defaults to GPU yaml (vllm-k8s.yaml). "
    "Use vllm-k8s-npu.yaml for NPU runs.",
)
@click.option(
    "--node-ip",
    default="localhost",
    show_default=True,
    help="K8s node IP reachable from this machine (no port-forward).",
)
@click.option(
    "--node-port",
    type=int,
    default=None,
    help="NodePort to use. If omitted, uses the nodePort already in the Service "
    "(GPU yaml already has 30003). For NPU yaml (ClusterIP), pass e.g. --node-port 30005.",
)
@click.option(
    "--batches",
    default="8,16,32,64",
    show_default=True,
    help="Comma/space separated batch sizes to test.",
)
@click.option(
    "--prompts",
    default=lambda: get_config().PROMPTS_FILE_PATH,
    show_default=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Prompts JSON file (list[str] or list[{prompt: ...}]).",
)
@click.option(
    "--served-model-name",
    default=lambda: get_config().MODEL_NAME,
    show_default=True,
    help="Must match --served-model-name in Deployment.",
)
@click.option(
    "--requests-multiplier",
    default=2,
    show_default=True,
    help="Total requests per batch size = multiplier × batch.",
)
@click.option(
    "--dispatch",
    type=click.Choice(["burst", "waves"], case_sensitive=False),
    default="waves",
    show_default=True,
    help="How to send requests: 'burst' sends all at once (backward compatible). "
    "'waves' sends waves of size=batch and waits between waves.",
)
@click.option(
    "--experiments-root",
    default=lambda: os.path.join(get_config().PROJECT_PATH, "profiling"),
    show_default=True,
    help="Root folder for profiling outputs.",
)
@click.option(
    "--health-timeout",
    default=600,
    show_default=True,
    help="Seconds to wait for /health.",
)
@click.option(
    "--rollout-timeout",
    default=900,
    show_default=True,
    help="Seconds to wait for rollout.",
)
@click.option(
    "--use-discovery",
    is_flag=True,
    default=True,
    show_default=True,
    help="Use k8s discovery to sanity-check there is exactly one Running pod and probe its /health.",
)
# --- NPU crash-prune options ---
@click.option(
    "--npu-crash-prune",
    is_flag=True,
    default=True,
    show_default=True,
    help="NPU helper: roll out with initial replicas, wait for crashes on busy NPUs, "
    "preferentially remove unready pods via deletion-cost, then scale to the target replica count.",
)
@click.option(
    "--npu-initial-replicas",
    type=int,
    default=8,
    show_default=True,
    help="When --npu-crash-prune is set: replicas to start with before pruning.",
)
@click.option(
    "--npu-target-replicas",
    type=int,
    default=1,
    show_default=True,
    help="When --npu-crash-prune is set: final number of replicas to keep after pruning.",
)
@click.option(
    "--npu-crash-wait",
    type=int,
    default=40,
    show_default=True,
    help="Seconds to wait after rolling out before pruning (to let crashes surface).",
)
def main(
    yaml_file: pathlib.Path,
    node_ip: str,
    node_port: Optional[int],
    batches: str,
    prompts: str,
    served_model_name: str,
    requests_multiplier: int,
    dispatch: str,
    experiments_root: str,
    health_timeout: int,
    rollout_timeout: int,
    use_discovery: bool,
    npu_crash_prune: bool,
    npu_initial_replicas: int,
    npu_target_replicas: int,
    npu_crash_wait: int,
):
    """
    For each batch size:
      - patches YAML (replicas=<initial_replicas if crash-prune else 1>, --max-num-seqs=<batch>)
      - drain previous Deployment (scale 0, wait pods gone), then apply next batch YAML
      - kubectl apply; waits for rollout / or min Ready pods (crash-prune mode)
      - (optional) NPU crash-prune: annotate deletion-cost, then scale to target replicas
      - (optional) discovery: prefer probing Ready pod IPs to avoid stale endpoints; fall back to Service endpoints
      - service /health via NodeIP:NodePort (primary health gate)
      - sends requests according to --dispatch
      - saves CSV/JSON artifacts and plots
    """
    cfg = get_config()
    req_timeout = int(getattr(cfg, "REQUEST_TIMEOUT_S", 10000))

    # outputs
    exp_dir = next_experiment_dir(pathlib.Path(experiments_root))
    click.echo(f"[{now_str()}] Writing results to: {exp_dir}")
    work_dir = exp_dir / "patched_yamls"
    ensure_dir(work_dir)

    # Save active config snapshot
    try:
        config_snapshot = dump_config_dict()
        with open(exp_dir / "config.json", "w", encoding="utf-8") as f:
            json.dump(config_snapshot, f, indent=2)
    except Exception as e:
        click.echo(f"[{now_str()}] WARNING: could not dump config.json: {e}")

    # prompts
    prompt_list = load_prompts_file(prompts)
    click.echo(f"[{now_str()}] Loaded {len(prompt_list)} prompts.")

    batches_int = parse_batches(batches)
    all_results: Dict[int, List[Dict]] = {}

    # try to load kube config once if discovery/crash-prune enabled
    core_api = None
    if use_discovery or npu_crash_prune:
        if load_kube():
            core_api = k8s_client.CoreV1Api()
        else:
            click.echo(
                f"[{now_str()}] WARNING: could not load k8s config; skipping discovery/crash-prune k8s checks."
            )
            core_api = None

    # --- Drain any existing deployment BEFORE starting batches (first run safety) ---
    pre_ns, pre_name, pre_sel = peek_deployment_meta(yaml_file)
    if deployment_exists(pre_ns, pre_name):
        click.echo(
            f"[{now_str()}] Pre-drain: scaling {pre_name} in ns={pre_ns} to 0 before first batch..."
        )
        try:
            scale_deployment(pre_ns, pre_name, 0)
            wait_for_pods_gone_by_selector(
                pre_ns, pre_sel, timeout_s=600, interval_s=2.0
            )
            click.echo(f"[{now_str()}] Pre-drain complete. No pods remaining.")
        except Exception as e:
            click.echo(
                f"[{now_str()}] WARNING: Pre-drain encountered an issue (continuing): {e}"
            )

    for b in batches_int:
        click.echo(f"\n[{now_str()}] === Batch size {b} ===")

        # --- Drain previous batch's deployment to avoid races while switching args/replicas ---
        ns0, name0, sel0 = peek_deployment_meta(yaml_file)
        if deployment_exists(ns0, name0):
            click.echo(
                f"[{now_str()}] Draining previous batch: scaling {name0} to 0 and waiting for pods to disappear..."
            )
            try:
                scale_deployment(ns0, name0, 0)
                wait_for_pods_gone_by_selector(ns0, sel0, timeout_s=600, interval_s=2.0)
                click.echo(f"[{now_str()}] Drain complete.")
            except Exception as e:
                click.echo(
                    f"[{now_str()}] WARNING: drain step had issues (continuing): {e}"
                )

        # --- Build and apply the YAML for this batch ---
        patched_yaml, namespace, deployment_name, effective_node_port = (
            make_patched_yaml(
                yaml_file,
                work_dir,
                b,
                node_port_override=node_port,
                initial_replicas=(npu_initial_replicas if npu_crash_prune else None),
            )
        )

        kubectl_apply(patched_yaml)
        click.echo(
            f"[{now_str()}] Applied {patched_yaml.name} (ns={namespace}, deploy={deployment_name})."
        )

        # Rollout waiting:
        if npu_crash_prune and npu_initial_replicas > 1:
            # Relaxed: wait for a minimum number of Ready pods (not 8/8) to avoid progress deadline
            label_selector = build_label_selector_from_cfg_or_default(
                cfg, deployment_name
            )
            min_ready = max(1, min(npu_target_replicas, npu_initial_replicas))
            click.echo(
                f"[{now_str()}] Crash-prune mode: waiting for ≥{min_ready} Ready pods before pruning..."
            )
            wait_for_min_ready_pods(
                namespace,
                label_selector,
                min_ready=min_ready,
                timeout_s=min(rollout_timeout, max(120, npu_crash_wait)),
                interval_s=2.0,
            )
            click.echo(
                f"[{now_str()}] Minimum Ready pods reached; proceeding to crash-prune."
            )
        else:
            # Normal: require all desired replicas (here: 1) to be available
            rollout_status(namespace, deployment_name, timeout_s=rollout_timeout)
            click.echo(f"[{now_str()}] Deployment rolled out.")

        # Optional NPU crash-prune routine (only meaningful if replicas > 1)
        if npu_crash_prune and npu_initial_replicas > 1:
            if core_api is None:
                click.echo(
                    f"[{now_str()}] WARNING: crash-prune requested but Kubernetes client unavailable; skipping."
                )
            else:
                label_selector = build_label_selector_from_cfg_or_default(
                    cfg, deployment_name
                )
                run_npu_crash_prune(
                    namespace=namespace,
                    deployment_name=deployment_name,
                    label_selector=label_selector,
                    initial_replicas=npu_initial_replicas,
                    target_replicas=npu_target_replicas,
                    crash_wait_s=npu_crash_wait,
                    rollout_timeout=rollout_timeout,
                )

        # Optional: discovery helpers — prefer fresh Ready pod IPs, fall back to Service endpoints
        if use_discovery and core_api is not None:
            try:
                sel = getattr(cfg, "LABEL_SELECTOR", None) or f"app={deployment_name}"
                try:
                    min_ips = max(
                        1,
                        (
                            npu_target_replicas
                            if (npu_crash_prune and npu_initial_replicas > 1)
                            else 1
                        ),
                    )
                    _ready_ips = wait_for_ready_pod_ips(
                        core_api,
                        namespace=cfg.NAMESPACE or namespace,
                        label_selector=sel,
                        min_ips=min_ips,
                        timeout_s=min(health_timeout, 180),
                        interval_s=2.0,
                    )
                    # We deliberately probe the Service, not the podIP (to avoid stale IP races)
                    # probe_url = f"http://{_ready_ips[0]}:{cfg.VLLM_PORT}{cfg.HEALTH_PATH}"
                    # click.echo(f"[{now_str()}] (skipped) Checking pod health at {probe_url} ...")
                    # wait_for_health(probe_url, timeout_s=health_timeout)
                    # click.echo(f"[{now_str()}] Pod healthy.")
                except Exception as e:
                    click.echo(
                        f"[{now_str()}] WARNING: podIP-based readiness check failed: {e}. Falling back to Service endpoints."
                    )
                    eps = discover_endpoints(
                        core_api,
                        namespace=cfg.NAMESPACE or namespace,
                        label_selector=sel,
                        port=cfg.VLLM_PORT,
                    )
                    if eps:
                        pod_health = f"{eps[0].rstrip('/')}{cfg.HEALTH_PATH}"
                        click.echo(
                            f"[{now_str()}] Checking (fallback) endpoint health at {pod_health} ..."
                        )
                        wait_for_health(pod_health, timeout_s=health_timeout)
                        click.echo(f"[{now_str()}] Endpoint healthy.")
                    else:
                        click.echo(
                            f"[{now_str()}] WARNING: discovery found 0 endpoints; continuing anyway."
                        )
            except Exception as e:
                click.echo(f"[{now_str()}] WARNING: discovery error: {e}")

        # Service health via NodeIP:NodePort (this hits the Service, not pod IP)
        api_base = f"http://{node_ip}:{effective_node_port}"
        health_url = f"{api_base}{cfg.HEALTH_PATH}"
        click.echo(f"[{now_str()}] Waiting for service health at {health_url} ...")
        wait_for_health(health_url, timeout_s=health_timeout)
        click.echo(f"[{now_str()}] Service healthy.")

        # Total requests for this batch size
        total_reqs = max(1, int(requests_multiplier * b))
        api_url = f"{api_base}/v1/completions"

        t0 = time.time()
        if dispatch.lower() == "burst":
            # Build prompts_pool of length total_reqs
            if len(prompt_list) < total_reqs:
                mult = (total_reqs + len(prompt_list) - 1) // len(prompt_list)
                prompts_pool = (prompt_list * mult)[:total_reqs]
            else:
                prompts_pool = prompt_list[:total_reqs]
            results = asyncio.run(
                run_burst(api_url, served_model_name, prompts_pool, req_timeout)
            )
        else:
            # waves: multiplier waves of size=batch
            results = asyncio.run(
                run_waves(
                    api_url, served_model_name, prompt_list, total_reqs, b, req_timeout
                )
            )

        t1 = time.time()
        click.echo(
            f"[{now_str()}] Completed {len(results)} requests in {t1 - t0:.2f}s. "
            f"Success={sum(1 for r in results if r['status']==200)}."
        )

        # save per-batch artifacts
        batch_dir = exp_dir / f"batch_{b}"
        ensure_dir(batch_dir)
        save_csv(results, batch_dir / "latencies.csv")

        # quick summary (JSON)
        ok = [r for r in results if r["status"] == 200]
        errs = [r for r in results if r["status"] != 200]
        summary = {
            "batch_size": b,
            "requests": len(results),
            "success": len(ok),
            "errors": len(errs),
        }
        if ok:
            lats = sorted(r["latency_s"] for r in ok)
            p50 = (
                lats[len(lats) // 2]
                if len(lats) % 2
                else 0.5 * (lats[len(lats) // 2 - 1] + lats[len(lats) // 2])
            )
            p95 = lats[max(0, int(0.95 * (len(lats) - 1)))]
            summary["latency_median_s"] = round(p50, 3)
            summary["latency_p95_s"] = round(p95, 3)
        if errs:
            summary["errors_preview"] = [
                {"status": r["status"], "error": r["error"]} for r in errs[:5]
            ]
        with open(batch_dir / "summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        all_results[b] = results

    # plots across batches
    plot_latency_summary(all_results, exp_dir)
    plot_latency_distributions(all_results, exp_dir)

    # manifest
    manifest = {
        "yaml_file": str(yaml_file),
        "node_ip": node_ip,
        "node_port_arg": node_port,
        "batches": batches_int,
        "prompts_file": str(prompts),
        "served_model_name": served_model_name,
        "requests_multiplier": requests_multiplier,
        "dispatch_mode": dispatch,
        "health_timeout_s": health_timeout,
        "rollout_timeout_s": rollout_timeout,
        "npu_crash_prune": npu_crash_prune,
        "npu_initial_replicas": npu_initial_replicas,
        "npu_target_replicas": npu_target_replicas,
        "npu_crash_wait_s": npu_crash_wait,
        "generated_at": now_str(),
    }
    with open(exp_dir / "run_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    click.echo(f"\n[{now_str()}] Done. Results in: {exp_dir}")


if __name__ == "__main__":
    main()
