# k8s_utils.py
from typing import List
from kubernetes import client  # type: ignore
from config import get_config

# import sim_backend


def load_kube() -> bool:
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

    cfg = get_config()

    # If sim-only, skip K8s discovery altogether
    # if cfg.SIM_ENDPOINTS and str(cfg.SIM_MODE).lower() == "only":
    #     sim_backend.configure()  # build from current cfg
    #     eps = sim_backend.endpoints()
    #     print(f"[DISCOVERY] SIM-ONLY: {len(eps)} endpoints -> {eps}")
    #     return eps

    # Otherwise: discover real pods, then optionally append sim
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

    # if cfg.SIM_ENDPOINTS:
    #     sim_backend.configure()
    #     eps.extend(sim_backend.endpoints())

    print(f"[DISCOVERY] endpoints -> {eps}")
    return eps
