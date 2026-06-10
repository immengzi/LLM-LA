# k8s_time_offsets.py
#
# Compute ONE scalar per node:
#   offset_ns = remote_epoch_ns - local_midpoint_ns
#
# Uses "kubectl exec <pod> -- date ..." (NOT logs) to avoid stale timestamps.
# Robust to BusyBox date not supporting %N (falls back to seconds -> ns).
#
# Returned as:
#   { "<node_name>": offset_ns, ... }

from __future__ import annotations

import json
import subprocess
import time
from typing import Dict, List, Optional, Tuple


def _now_ns() -> int:
    return time.time_ns()


def _run(cmd: List[str], timeout_s: float) -> str:
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=timeout_s)
    return out.decode("utf-8", "replace").strip()


def _list_time_probe_pods(
    kubectl: str,
    namespace: str,
    label_selector: str,
    timeout_s: float,
) -> List[Tuple[str, str]]:
    """
    Returns list of (node_name, pod_name) for time-probe pods.
    """
    raw = _run(
        [kubectl, "-n", namespace, "get", "pods", "-l", label_selector, "-o", "json"],
        timeout_s=timeout_s,
    )
    obj = json.loads(raw)
    out: List[Tuple[str, str]] = []
    for item in obj.get("items", []):
        pod_name = item.get("metadata", {}).get("name")
        node_name = item.get("spec", {}).get("nodeName")
        if pod_name and node_name:
            out.append((str(node_name), str(pod_name)))
    return out


def _remote_now_ns_exec(
    kubectl: str,
    namespace: str,
    pod: str,
    timeout_s: float,
) -> Optional[int]:
    """
    Returns remote epoch ns by executing date inside the pod.

    Tries %s%N first; if BusyBox doesn't support %N (non-digit output),
    falls back to seconds * 1e9.
    """
    try:
        s = _run([kubectl, "-n", namespace, "exec", pod, "--", "date", "-u", "+%s%N"], timeout_s=timeout_s)
        if s.isdigit():
            return int(s)

        # Fallback: seconds -> ns
        s2 = _run([kubectl, "-n", namespace, "exec", pod, "--", "date", "-u", "+%s"], timeout_s=timeout_s)
        if s2.isdigit():
            return int(s2) * 1_000_000_000

        return None
    except Exception:
        return None


def _measure_best_offset_exec(
    kubectl: str,
    namespace: str,
    pod: str,
    samples: int,
    timeout_s: float,
) -> Optional[int]:
    """
    Take multiple samples and return offset_ns from the sample with minimum RTT.
    """
    best_offset: Optional[int] = None
    best_rtt: Optional[int] = None

    for _ in range(max(1, int(samples))):
        t0 = _now_ns()
        remote = _remote_now_ns_exec(kubectl, namespace, pod, timeout_s)
        t1 = _now_ns()

        if remote is None:
            time.sleep(0.03)
            continue

        rtt = t1 - t0
        midpoint = (t0 + t1) // 2
        offset = int(remote) - int(midpoint)

        if best_rtt is None or rtt < best_rtt:
            best_rtt = rtt
            best_offset = offset

        time.sleep(0.03)

    return best_offset


def measure_k8s_node_time_offsets(
    *,
    namespace: str = "kube-system",
    label_selector: str = "app=time-probe",
    samples: int = 15,
    kubectl: str = "kubectl",
    timeout_s: float = 5.0,
) -> Dict[str, int]:
    """
    Returns:
        { node_name: offset_ns, ... }

    where:
        offset_ns = remote_epoch_ns - local_midpoint_ns
    """
    pods = _list_time_probe_pods(kubectl, namespace, label_selector, timeout_s)
    offsets: Dict[str, int] = {}

    for node_name, pod_name in pods:
        off = _measure_best_offset_exec(
            kubectl=kubectl,
            namespace=namespace,
            pod=pod_name,
            samples=samples,
            timeout_s=timeout_s,
        )
        if off is not None:
            offsets[node_name] = int(off)

    return offsets
