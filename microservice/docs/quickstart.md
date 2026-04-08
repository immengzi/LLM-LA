# LLM-LB Quick Start: Deployment & Running an Experiment

## Prerequisites

- Kubernetes cluster running with `kubectl` access from master node
- NFS model storage mounted and models present under `/saeid/models/`
- PV/PVC already deployed once on the parent NFS dir (see §0 below)
- Python env `central` activated: `conda activate central`
- Working directory: `/home/saeid/llm-lb/microservice`

---

## §0 — One-time PV/PVC setup (run once per cluster, not per experiment)

```bash
helm upgrade --install vllm ./vllm-kv-stack -n vllm --create-namespace \
  --set modelVolume.create=true \
  --set modelVolume.modelSubPath=placeholder
```

After this, `modelVolume.create` is always `false` in all subsequent deploys.

---

## §1 — Deploy vLLM (run once per model change)

Deploy only the vLLM pods using a client config YAML. The router, Redis, and
cpu-hash are NOT deployed here.

```bash
python deploy_vllm.py --config configs/router-tp8-glm.yaml
```

Flags:
- `--reinstall` — force fresh pod (uninstalls existing release first)
- `--timeout 36000` — seconds to wait for pods Ready (default 10h)

The script derives `modelVolume.modelSubPath` from `helm.nfs_path` in the
config (e.g. `/saeid/models/GLM-5-w4a8-mtp-QuaRot` → `GLM-5-w4a8-mtp-QuaRot`).

Wait for the pod to become Ready. GLM-5 on local NVMe takes ~5 min.
Qwen3-8B over NFS takes longer depending on load.

```bash
kubectl get pods -n vllm -w
kubectl logs -f <pod-name> -n vllm   # watch progress
```

---

## §2 — Run a sweep (router/redis/cpu-hash redeployed per experiment)

Once vLLM is Ready, run experiments without touching it:

```bash
python sweep_methods.py --config 1-master_config.yaml --skip-vllm
```

`--skip-vllm` detects running vllm-qwen pods automatically and sets
`deploy.vllm=true` to preserve them during each Helm upgrade.

The sweep runner:
1. Reads `configs/1-master_config.yaml` → maps client configs to methods
2. For each job: upgrades Helm (router + redis + cpu-hash only), waits for
   stack Ready, runs `main.py`, saves results to `experiments/<N>/`

---

## §3 — Run without --skip-vllm (full redeploy each experiment)

When you want a clean vLLM restart between experiments:

```bash
python sweep_methods.py --config 1-master_config.yaml
```

This uninstalls and reinstalls the full stack (including vLLM) before each
experiment. Slower but guaranteed clean state.

---

## §4 — Client config reference

Each client config YAML (e.g. `configs/router-tp8-glm.yaml`) has a `helm:`
section that controls both vLLM and the router stack:

```yaml
helm:
  replicas: 1                          # vLLM pod count
  batch_size: 8                        # sidecar batch size
  tensor_parallel_size: 8              # TP degree (1 per GPU for dense models)

  nfs_path: "/saeid/models/GLM-5-w4a8-mtp-QuaRot"  # REQUIRED

  # vLLM runtime flags
  vllm_gpu_memory_utilization: 0.95
  vllm_quantization: "ascend"          # null for dense models (Qwen3)
  vllm_enable_expert_parallel: true    # false for dense models (Qwen3)
  vllm_max_model_len: 80000            # null = use model default
  vllm_compilation_config: '{"cudagraph_mode": "FULL_DECODE_ONLY"}'

  # Router toggles
  router_kv_aware: true
  router_len_aware: true
  router_len_policy: "short_first"
```

### Model-specific defaults

| Model | `vllm_quantization` | `vllm_enable_expert_parallel` | `vllm_max_model_len` |
|---|---|---|---|
| Qwen3-8B (dense) | `null` | `false` | `null` |
| GLM-5-w4a8 (MoE W4A8) | `"ascend"` | `true` | `80000` |

---

## §5 — Master config format

`configs/1-master_config.yaml` maps client configs to routing methods:

```yaml
router-tp8-glm.yaml:
  - pull
  - push-rr
  - push-random
```

Each entry produces one experiment. Methods for `backend: router` set
`router.mode`; methods for `backend: aibrix` set `aibrix.routing_strategy`;
methods for `backend: litellm` are labels only (no Helm knob — routing is
controlled by the router behind LiteLLM).

---

## §6 — Monitoring during a run

```bash
# Watch pod status
kubectl get pods -n vllm -w

# Follow vLLM logs
kubectl logs -f <vllm-pod> -n vllm

# Check router queue depth
curl http://7.216.57.215:30080/metrics | grep router_central_queue_length

# Prometheus
open http://7.216.57.215:31190
```

---

## §7 — Results

Each experiment saves to `experiments/<N>/`:

```
experiments/
  42/
    sweep_meta.json      # all Helm knobs, method, config path, timestamp
    vllm-k8s.yaml        # full rendered Helm manifests (snapshot)
    helm-effective-values.yaml
    results.json         # latency/throughput from main.py
```

---

## §8 — LiteLLM backend (production auth/spend validation)

The `backend: litellm` path routes requests through a LiteLLM proxy pod that
sits in front of the router. Use this to validate production auth, virtual key
management, and spend tracking. **Do not use for benchmarking** — use
`backend: router` for that.

### Architecture

```
Client → LiteLLM proxy (port 30400)
       → router /v1/chat/completions (port 30080)
       → sidecars → vLLM pods
```

### One-time node setup

kube-proxy requires `libxtables.so.12` to program ClusterIP iptables rules.
If missing on any node, ClusterIP traffic from pods on that node will time out.
Check and fix once per node:

```bash
# Check all nodes for kube-proxy errors
for pod in $(kubectl get pods -n kube-system -l k8s-app=kube-proxy -o name); do
  echo "=== $pod ==="
  kubectl logs -n kube-system $pod --tail=5 | grep -i "libxtables\|iptables\|error" || echo "OK"
done

# Fix any broken node (replace <node-ip> with actual IP)
ssh <node-ip> "sudo yum install -y iptables iptables-libs"

# Restart kube-proxy cluster-wide after fixing all nodes
kubectl rollout restart daemonset/kube-proxy -n kube-system
kubectl rollout status daemonset/kube-proxy -n kube-system --timeout=120s
```

### Router image

The router must be built from source (includes the `/v1/chat/completions` shim):

```bash
cd /home/saeid/llm-lb/microservice/services/router_service
bash build.sh
```

The router deployment uses `imagePullPolicy: Always` so new builds are picked
up automatically on pod restart. If the old image is cached on the node, delete
it first:

```bash
ssh <node-ip> "sudo crictl rmi reg.local:32000/kv-router:latest || true"
```

### LiteLLM image

Pull and push to private registry once:

```bash
bash services/vllm-image-litellm.sh
```

### Running a LiteLLM sweep

```bash
# Deploy vLLM first (if not already running)
python deploy_vllm.py --config configs/litellm.yaml

# Run the sweep (--skip-vllm preserves running vLLM pods)
python sweep_methods.py --config litellm_master --skip-vllm
```

Master config `configs/litellm_master.yaml`:
```yaml
configs/litellm.yaml:
  - litellm-pull
```

### Key config differences (litellm.yaml vs router.yaml)

| Field | router.yaml | litellm.yaml |
|---|---|---|
| `backend` | `router` | `litellm` |
| `transport.mode` | `async_pubsub` | `sync` |
| `transport.results_zmq` | `tcp://...` | `""` |
| `litellm.base_url` | not set | `http://<node>:30400` |
| `litellm.model` | not set | `served-model` |
| `litellm.api_key` | not set | `sk-litellm-master` |

### LiteLLM config notes

- `api_base` in `litellm_config.yaml` must be `http://router-service:8080/v1`
  (LiteLLM appends `/chat/completions` automatically for `openai/` provider)
- `router_settings.cooldown_time: 0` prevents cascading 429s if the router
  has transient errors
- The LiteLLM pod is pinned to `pin.nodeName` (node4) alongside the router

---

## Common issues

| Symptom | Fix |
|---|---|
| `modelSubPath must be set` | Add `nfs_path` to `helm:` section of client config |
| `NPU out of memory` | Model loaded unquantized — set `vllm_quantization: "ascend"` |
| `KV cache too small for max seq len` | Add `vllm_max_model_len: 80000` (or lower) |
| `glm_moe_dsa model type` tokenizer error | `prompts.py` must use `PreTrainedTokenizerFast` not `AutoTokenizer` |
| RBAC ownership conflict | All operations use release name `vllm` — never use a different release name |
| vLLM pods deleted by sweep | Run with `--skip-vllm` to preserve running pods across Helm upgrades |
| LiteLLM 429 `No deployments available` | Restart litellm-proxy pod to clear in-memory cooldown state |
| LiteLLM 404 `Not Found` | Check `api_base` in ConfigMap ends with `/v1` not bare hostname |
| LiteLLM `Connection error` | kube-proxy iptables broken on node — install `iptables-libs` and restart kube-proxy |
| Router image stale (old code running) | `imagePullPolicy: Always` on router; delete cached image on node with `crictl rmi` |
| `kubectl wait` finds no pods after helm upgrade | Normal — `time.sleep(15)` in sweep_methods.py handles this; if persisting, check chart backend guards |
