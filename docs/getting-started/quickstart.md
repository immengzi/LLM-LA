# Quickstart

Deploy the LA-Boom stack on Kubernetes and run your first load experiment. This guide assumes the cluster prerequisites are already met; if not, start with [prerequisites.md](prerequisites.md).

> Conventions: replace `<node-ip>` with any cluster node IP and `<repo-root>` with your checkout path. Commands are run from `<repo-root>/src` unless noted. Cluster-specific values (registry host, NFS server, node labels) are documented under [operations/](../operations/).

## Prerequisites (short)

- A Kubernetes cluster with `kubectl` access and Helm 3.12+
- Model weights reachable from worker nodes (NFS or local path)
- A private image registry holding the LA-Boom images (router, sidecar, prefix-hash, engine, gateways)
- Python 3.10+ with `pyyaml`, `requests`, `click` (`pip install -r requirements.txt`)

Full details: [prerequisites.md](prerequisites.md).

## 1. One-time PV/PVC setup

Run once per cluster (not per experiment) to create the model `PersistentVolume`/`PersistentVolumeClaim` over your model storage. After this, `modelVolume.create` stays `false` in all later deploys.

```bash
helm upgrade --install vllm ./src/core/vllm-kv-stack -n vllm --create-namespace \
  --set modelVolume.create=true \
  --set modelVolume.modelSubPath=placeholder
```

## 2. Deploy the engine

Deploy only the engine pods (router, Redis, and prefix-hash are not deployed here) from a client config YAML.
`deploy_vllm.py` remains the entry point; set `helm.engine_type: sglang` (and usually `hardware: nvidia`) for the pinned SGLang profile — see [sglang.md](../deployment/sglang.md).

```bash
python src/client/deploy_vllm.py --config configs/router-tp8-glm.yaml
```

Flags:

- `--reinstall` — uninstall the existing release first (force a fresh pod)
- `--timeout 36000` — seconds to wait for pods Ready (default 10h)

`deploy_vllm.py` derives `modelVolume.modelSubPath` from `helm.nfs_path` in the config (e.g. `/home/models/GLM-5-w4a8-mtp-QuaRot` -> `GLM-5-w4a8-mtp-QuaRot`).

Wait for the pod to become Ready:

```bash
kubectl get pods -n vllm -w
kubectl logs -f <vllm-pod> -n vllm   # watch model load progress
```

## 3. Run a single experiment

With vLLM Ready, run one open-loop load experiment against the router:

```bash
python src/client/main.py --config router --n 500
```

- `--config <name>` resolves to `configs/<name>.yaml` (a path with a directory or `.yaml` suffix is used as-is).
- `--n N` overrides `total_requests` from the config.

Results are written to the experiments directory (see [artifacts & analysis](../benchmarking/artifacts-and-analysis.md)).

## 4. Run an automated sweep

To deploy and measure several routing methods in sequence, use the sweep runner. Use `--skip-vllm` to preserve already-running vLLM pods and only redeploy the routing stack (router + Redis + prefix-hash) between experiments:

```bash
python src/client/sweep_methods.py --config 1-master_config --skip-vllm
```

Without `--skip-vllm`, the full stack (including vLLM) is uninstalled and reinstalled before each experiment — slower, but guaranteed-clean state.

The sweep runner reads `configs/1-master_config.yaml`, which maps client configs to routing methods. See [experiment configs](../configuration/experiment-configs.md) for the master-config format, method vocabulary, and the full sweep lifecycle.

## 5. Client config essentials

Each client config (e.g. `configs/router-tp8-glm.yaml`) sets the workload plus a `helm:` section that controls vLLM and the routing stack:

```yaml
helm:
  nfs_path: "/home/models/GLM-5-w4a8-mtp-QuaRot"  # REQUIRED
  tensor_parallel_size: 8
  vllm_quantization: "ascend"          # null for dense models (e.g. Qwen3)
  vllm_enable_expert_parallel: true    # false for dense models
  router_kv_aware: true
  router_len_policy: "short_first"     # short_first | long_first
```

Model-specific cheatsheet:

| Model | `vllm_quantization` | `vllm_enable_expert_parallel` | `vllm_max_model_len` |
|---|---|---|---|
| Qwen3-8B (dense) | `null` | `false` | `null` |
| GLM-5-w4a8 (MoE W4A8) | `"ascend"` | `true` | `80000` |

The `helm:` section is documented in [experiment configs](../configuration/experiment-configs.md); the full client schema is in [client config](../configuration/client-config.md) and chart values in [Helm values](../configuration/helm-values.md).

## 6. Monitor during a run

```bash
# Pod status
kubectl get pods -n vllm -w

# Router queue depth
curl http://<node-ip>:30080/metrics | grep router_central_queue_length

# Router/backends health
curl http://<node-ip>:30080/health

# Prometheus UI (NodePort)
open http://<node-ip>:31190
```

## 7. Results

Each run is saved to its own numbered experiment directory containing per-request logs (`logs.json`), aggregate summaries (`run_summary.json`), the frozen config, and—when enabled—Prometheus samples. Sweeps add deployment snapshots.

See [artifacts & analysis](../benchmarking/artifacts-and-analysis.md) for the full directory layout, the sweep-only files, and the `logs.json` record schema.

## 8. Production gateways (optional)

For auth, virtual keys, rate limiting, and spend tracking in front of the router, deploy a gateway. Use `backend: router` for clean benchmarking; use a gateway to validate the production path.

- [BooM Gateway](../gateways/boom/overview.md) (Rust, NodePort 30401) — recommended
- LiteLLM (Python, NodePort 30400) — alternative

## Common issues

| Symptom | Fix |
|---|---|
| `modelSubPath must be set` | Add `nfs_path` to the `helm:` section of the client config |
| `NPU out of memory` | Model loaded unquantized — set `vllm_quantization: "ascend"` |
| `KV cache too small for max seq len` | Add `vllm_max_model_len: 80000` (or lower) |
| RBAC ownership conflict | All operations use release name `vllm` — never use a different release name |
| vLLM pods deleted by sweep | Run with `--skip-vllm` to preserve running pods across Helm upgrades |
| Router image stale | Router uses `imagePullPolicy: Always`; delete the cached image on the node with `crictl rmi` |
| ClusterIP `Connection error` from pods | kube-proxy iptables broken on a node — install `iptables-libs` and restart kube-proxy (see [k8s DNS runbook](../operations/k8s-dns-troubleshooting.md)) |

## See also

- [First experiment walkthrough](first-experiment.md)
- [Architecture overview](../architecture/overview.md)
- [Experiment configs and sweeps](../configuration/experiment-configs.md)
