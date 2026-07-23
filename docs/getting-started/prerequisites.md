# Prerequisites

What you need before deploying LA-Boom and running experiments. For full cluster bring-up (drivers, DNS, RoCE, operators), see [operations/cluster-setup.md](../operations/cluster-setup.md).

## Kubernetes cluster

- A working Kubernetes cluster (1.28+) with `kubectl` access and cluster-admin permissions. The Helm chart creates a namespace, Deployments/LeaderWorkerSets, Services, ConfigMaps, and RBAC resources.
- Accelerators available on worker nodes (the default images target Ascend NPUs; the chart and routing logic are accelerator-agnostic).
- [Prometheus](https://prometheus.io/) in the cluster if you want metrics collection or autoscaling. The chart ships `ServiceMonitor`/`PodMonitor` resources.
- [KEDA](https://keda.sh/) if you want autoscaling (`autoscaling.enabled=true`). Install via `cd infra && make keda`. See [operations/autoscaling.md](../operations/autoscaling.md).
- For data-parallel / expert-parallel deployments: the [LeaderWorkerSet](https://github.com/kubernetes-sigs/lws) operator CRD installed, and RoCE/HCCL networking configured. See [data parallel with LWS](../deployment/data-parallel-lws.md).

## Tooling

| Tool | Purpose |
|------|---------|
| `kubectl` | Kubernetes CLI |
| `helm` (>= 3.12) | Chart install / upgrade |
| `python` (>= 3.10) | Load client (`main.py`), sweep runner (`sweep_methods.py`), deploy script |
| Python deps | `pip install -r <repo-root>/src/client/requirements.txt` (includes `pyyaml`, `requests`, `click`) |

## Model storage

Model weights must be reachable from worker nodes via one of:

- **NFS** (default): an NFS export mounted by the chart's PV/PVC. Set `helm.nfs_path` per model; run the [one-time PV/PVC setup](quickstart.md#1-one-time-pvpvc-setup) once per cluster.
- **Local hostPath**: set `helm.model_host_path` to bypass the PVC and mount a local directory on each node.

Details: [Helm values reference](../configuration/helm-values.md) (`modelVolume.*`) and [cluster setup](../operations/cluster-setup.md).

## Container registry

LA-Boom images (router, sidecar, prefix-hash, vLLM, and optionally BooM/LiteLLM) are served from a private registry referenced by `global.imageRegistry`. You need:

- The registry reachable from all nodes (and configured as an insecure mirror in containerd if it serves plain HTTP).
- The LA-Boom images built and pushed. See [operations/registry.md](../operations/registry.md) and, for the gateway, [gateways/boom/build.md](../gateways/boom/build.md).

## Quick verification

```bash
kubectl get nodes
helm version
kubectl get crd | grep -i leaderworkerset   # only needed for DP/EP
kubectl get pods -A | grep -i prometheus     # only needed for metrics/autoscaling
kubectl get crd scaledobjects.keda.sh        # only needed for autoscaling (KEDA)
```

## See also

- [Quickstart](quickstart.md)
- [Cluster setup](../operations/cluster-setup.md)
- [Private registry operations](../operations/registry.md)
