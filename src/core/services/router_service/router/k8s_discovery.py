# router/k8s_discovery.py
# -*- coding: utf-8 -*-
"""Shared Kubernetes pod discovery.

Single source of truth for listing the Running vLLM pods (name -> pod IP) for a
namespace + label selector. Extracted verbatim from ``PushRouter._discover_pods``
so the sidecar push path and the sidecar-less central-push registry
(``k8s_endpoints.K8sVLLMRegistry``) discover pods identically.
"""
from __future__ import annotations

import os
from typing import Dict

from kubernetes import client as k8s_client, config as k8s_config

from .config import get_config

_cfg = get_config()


def discover_running_pods(
    namespace: str | None = None,
    label_selector: str | None = None,
    *,
    log_prefix: str = "[k8s_discovery]",
) -> Dict[str, str]:
    """Return ``{pod_name: pod_ip}`` for Running pods with an assigned IP.

    Returns an empty dict (never raises) on any k8s error, matching the prior
    ``PushRouter._discover_pods`` behavior so callers degrade gracefully.
    """
    ns = namespace if namespace is not None else _cfg.NAMESPACE
    sel = label_selector if label_selector is not None else _cfg.LABEL_SELECTOR

    running_in_cluster = os.getenv("KUBERNETES_SERVICE_HOST") is not None
    try:
        if running_in_cluster:
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config()
    except Exception as e:
        print(f"{log_prefix} failed to load K8s config: {e}")
        return {}

    v1 = k8s_client.CoreV1Api()
    try:
        pods = v1.list_namespaced_pod(
            namespace=ns,
            label_selector=sel,
        ).items
    except Exception as e:
        print(f"{log_prefix} list_namespaced_pod failed: {e}")
        return {}

    out: Dict[str, str] = {}
    for pod in pods:
        if pod.status.phase == "Running" and pod.status.pod_ip:
            out[pod.metadata.name] = pod.status.pod_ip
    return out
