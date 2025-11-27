# k8s_utils.py
from typing import List
from kubernetes import client  # type: ignore


def load_kube() -> bool:
    """Load in-cluster config, falling back to local kubeconfig."""
    try:
        from kubernetes import config
        config.load_incluster_config()
        return True
    except Exception:
        try:
            from kubernetes import config
            config.load_kube_config()
            return True
        except Exception as e:
            print("ERROR: Could not load k8s config:", e)
            return False


def discover_endpoints(
    core: client.CoreV1Api, namespace: str, label_selector: str, port: int
) -> List[str]:
    """Return list of endpoint URLs for running pods matching label."""
    eps: List[str] = []
    try:
        pods = core.list_namespaced_pod(
            namespace=namespace, label_selector=label_selector
        ).items
        for p in pods:
            if p.status.phase == "Running" and p.status.pod_ip:
                eps.append(f"http://{p.status.pod_ip}:{port}")
    except Exception as e:
        print(f"[DISCOVERY] K8s list failed: {e}")

    print(f"[DISCOVERY] endpoints -> {eps}")
    return eps
