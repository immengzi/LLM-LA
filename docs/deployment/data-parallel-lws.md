# Multi-Node Data Parallel Deployment (LWS)

Deploy vLLM across multiple NPU nodes using Data Parallelism (DP) with
Expert Parallelism (EP) for MoE models. Uses the Kubernetes
[LeaderWorkerSet](https://github.com/kubernetes-sigs/lws) (LWS) CRD to
manage leader/worker pod groups as a single unit of replication.

> This is the **Helm/Kubernetes** path. The vLLM flags and environment here mirror the [bare-Docker GLM-5 reference deployment](docker-reference/glm5-dp-docker.md), which is the authoritative ("HQ") configuration.

---

## When to Use This

Use multi-node DP when a single node cannot provide enough KV cache memory
for your target context length. Each DP rank holds its own independent KV
cache, so adding nodes multiplies the available context capacity.

| Scenario | TP only (1 node) | DP=2 + EP (2 nodes) |
|----------|-------------------|----------------------|
| GLM-5-w4a8-mtp-QuaRot, TP=8, 8× Ascend 910 | ~80K context | ~160K context |
| Expert layers | TP=8 (tensor parallel) | EP=16 (expert parallel across 2 nodes) |
| Attention layers | TP=8 per node | DP=2 (independent per rank) |

For dense (non-MoE) models, standard DP is simpler — each replica is fully
independent. For MoE models like GLM-5 and DeepSeek, expert layers must
synchronise across all DP ranks every forward pass via HCCL collective
operations over the RoCE network.

---

## How It Works

### Architecture

> Resource naming: LWS objects, pods, services, and labels are templated as
> `vllm-<model.name>` (the legacy fallback model name is `qwen`, so examples
> below show `vllm-qwen`). The external NodePort service is `vllm-<name>-api`
> while the LWS headless service is `vllm-<name>`. Wherever you see `vllm-dp`
> below, read it as `vllm-<your-model-name>`.

```
                     ┌─────────────────┐
                     │  BooM / Router  │
                     │   (unchanged)   │
                     └────────┬────────┘
                              │ routes to app=vllm-qwen
                              │
                 ┌────────────▼────────────┐
                 │    vllm-qwen Service    │
                 │  (targets leader pods)  │
                 └────────────┬────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            │                                   │
     ┌──────▼───────┐                   ┌───────▼──────┐
     │  Leader Pod   │  ◄─ HCCL RoCE ─► │  Worker Pod  │
     │  (Node A)     │                   │  (Node B)    │
     │               │                   │              │
     │  vLLM :8200   │                   │  vLLM        │
     │  API server   │                   │  --headless  │
     │  DP rank 0    │                   │  DP rank 1   │
     │  TP=8         │                   │  TP=8        │
     │               │                   │              │
     │  kv-sidecar   │                   │  (no sidecar)│
     │  :9000        │                   │              │
     └──────────────-┘                   └──────────────┘
```

### What LWS Manages

LWS treats each leader + its workers as a **group**. Key behaviours:

- **Group restart**: If any pod in the group crashes, the entire group
  restarts (`RecreateGroupOnPodRestart`). This is required because all DP
  ranks must synchronise during MoE expert forward passes — a partial group
  cannot function.

- **Leader address injection**: LWS automatically sets the `LWS_LEADER_ADDRESS`
  environment variable on all pods in the group. Workers use this as
  `--data-parallel-address` to find the leader. No manual IP configuration.

- **Scaling**: `replicas` controls how many independent groups exist.
  Increase it to add more DP group copies (each group needs its own set of
  NPU nodes).

### Pod Roles

| | Leader | Worker |
|---|--------|--------|
| **vLLM entrypoint** | `python -m vllm.entrypoints.openai.api_server` | `vllm serve /model --headless` |
| **vLLM mode** | API server (port 8200) | Headless (no HTTP API) |
| **DP rank** | 0 | Computed from pod name via Downward API |
| **`--data-parallel-address`** | `${NODE_IP}` (own host IP) | `${LEADER_IP}` (resolved from `LWS_LEADER_ADDRESS`) |
| **Sidecar** | Yes (kv-sidecar :9000) | No |
| **Health probes** | HTTP `/health` on :8200 | Process liveness (`pgrep -f 'vllm.entrypoints'`) |
| **Label** | `app: vllm-<modelName>` (router-discoverable; legacy `vllm-qwen`) | `app: vllm-<modelName>-worker` (legacy `vllm-qwen-worker`; hidden from router) |
| **Network** | `hostNetwork: true` | `hostNetwork: true` |
| **Security** | `privileged: true` | `privileged: true` |

**Important**: The leader and worker use different vLLM entrypoints. The
leader uses `python -m vllm.entrypoints.openai.api_server` directly (a
single API server process managing DP coordination). The worker uses
`vllm serve /model --headless` which takes a different code path that
avoids binding the DP RPC port locally — workers only connect *to* the
leader's RPC port, they do not bind one themselves.

The worker computes its `--data-parallel-start-rank` from the pod name
obtained via the Kubernetes Downward API (`POD_NAME` env var). LWS names
worker pods as `vllm-<modelName>-<group>-<worker-idx>` (legacy fallback model
name is `qwen`, e.g. `vllm-qwen-0-1`), so the last segment is the worker index.
Start rank = worker index × `sizeLocal`.

**Note**: Do not use `HOSTNAME` for rank calculation — on `hostNetwork`
pods, `HOSTNAME` contains the node's hostname (e.g. `node3`), not the
pod name.

---

## Prerequisites

### 1. Install the LWS Operator (one-time)

**Option A — Internet-connected cluster:**

```bash
kubectl apply --server-side \
  -f https://github.com/kubernetes-sigs/lws/releases/download/v0.8.0/manifests.yaml
```

**Option B — Air-gapped cluster (local registry):**

The LWS manifest is included at `lws-manifests.yaml` with the
controller image already rewritten to `reg.local:32000/lws/lws:v0.8.0`
and `runAsNonRoot` set to `false` (required because the upstream image
runs as root).

1. Push the LWS controller image to the local registry:

```bash
# On a machine with internet access, pull & export:
sudo ctr -n k8s.io images pull \
  --skip-verify us-central1-docker.pkg.dev/k8s-staging-images/lws/lws:v0.8.0
sudo ctr -n k8s.io images export \
  --platform linux/arm64 lws.tar \
  us-central1-docker.pkg.dev/k8s-staging-images/lws/lws:v0.8.0

# Load into Docker and push to local registry:
docker load -i lws.tar
docker tag us-central1-docker.pkg.dev/k8s-staging-images/lws/lws:v0.8.0 \
  reg.local:32000/lws/lws:v0.8.0
docker push reg.local:32000/lws/lws:v0.8.0
```

2. Apply the local manifest:

```bash
kubectl apply --server-side -f lws-manifests.yaml
```

3. Verify the controller is running:

```bash
kubectl get pods -n lws-system
# NAME                                      READY   STATUS    RESTARTS
# lws-controller-manager-xxxx-xxxxx         1/1     Running   0
```

**Verify CRD (both options):**

```bash
kubectl get crd leaderworkersets.leaderworkerset.x-k8s.io
# NAME                                            CREATED AT
# leaderworkersets.leaderworkerset.x-k8s.io       2026-04-16T...
```

### 2. Verify NPU RoCE Network

All NPU nodes that will participate in DP must have their RoCE network
physically connected and configured. Run on **each node**:

```bash
# All 8 ports must show link status: UP
for i in {0..7}; do hccn_tool -i $i -link -g; done

# All must show success
for i in {0..7}; do hccn_tool -i $i -net_health -g; done

# Verify IP config exists
cat /etc/hccn.conf
```

If any port shows `DOWN` or `optical info: not present`, the physical layer
(transceivers, fibre cabling, RoCE switch) needs to be fixed first. See
[glm5_dp_docker_deployment.md](docker-reference/glm5-dp-docker.md) for
diagnostics.

### 3. Identify the NIC Name

Find the network interface that carries the management/data IP on each node:

```bash
ip addr show | grep "inet 10.50"
# example output: inet 10.50.156.65/24 brd 10.50.156.255 scope global enp189s0f0
```

The interface name (e.g. `enp189s0f0`) goes into the config as
`data_parallel_nic_name`. If all nodes use the same NIC name, set it once.
If they differ, leave it empty for auto-detection.

---

## Configuration

### Quick Start Config

Create or use `configs/llm-la-claude-glm-dp-r2-b8.yaml`. The key DP fields under
`helm:` are:

```yaml
helm:
  # ---- Data Parallel (LWS) ----
  data_parallel_enabled: true
  data_parallel_size: 2          # 2 pods per group (1 leader + 1 worker)
  data_parallel_groups: 1        # 1 DP group
  data_parallel_size_local: 1    # 1 DP rank per pod
  data_parallel_rpc_port: 13389
  data_parallel_nic_name: "enp189s0f0"   # set "" for auto-detect
  data_parallel_hccl_buff_size: 200
  data_parallel_omp_num_threads: 16

  # These must match the model requirements:
  tensor_parallel_size: 8
  vllm_enable_expert_parallel: true
  vllm_quantization: "ascend"
  vllm_trust_remote_code: true
  # ... (rest of vLLM flags)
```

### Config Field Reference

These are the flat, single-model `helm.data_parallel_*` keys used by sweep configs. For the current per-model schema (`models[].dataParallel.*`), see [multi-model deployment → per-model data parallel config](multi-model.md#per-model-data-parallel-config-inside-dataparallel) — that doc owns the field reference; the keys below map onto it one-to-one (e.g. `data_parallel_size` → `dataParallel.size`, `data_parallel_nic_name` → `dataParallel.nicName`).

| Field | Default | Description |
|-------|---------|-------------|
| `data_parallel_enabled` | `false` | Master toggle. When `false`, the standard Deployment is used. |
| `data_parallel_size` | `2` | Pods per DP group. 1 leader + (size-1) workers. |
| `data_parallel_groups` | `1` | Number of independent DP groups (LWS replicas). Each group needs `size` nodes. |
| `data_parallel_size_local` | `1` | DP ranks per pod. Usually 1 (one rank per node). |
| `data_parallel_rpc_port` | `13389` | Port for vLLM inter-rank RPC communication. |
| `data_parallel_nic_name` | `""` | NIC name for HCCL/GLOO/TP sockets. Empty = auto-detect from node IP. |
| `data_parallel_hccl_buff_size` | `200` | HCCL buffer size (MB) for RoCE transfers. |
| `data_parallel_omp_num_threads` | `16` | OpenMP thread count. |

### Relationship Between Fields

The total DP size (passed as `--data-parallel-size` to vLLM) is computed as:

```
data_parallel_size × data_parallel_size_local
```

For the typical case (1 rank per pod, 2 pods per group):
`2 × 1 = 2` → `--data-parallel-size 2`

Total NPU nodes required:

```
data_parallel_groups × data_parallel_size
```

For 1 group of 2: `1 × 2 = 2 nodes` (16 NPUs total).

---

## Deploying

### Via Sweep Runner (recommended)

Same workflow as single-node configs:

```bash
python sweep_methods.py --config llm-la-claude-glm-dp-r2-b8
```

This will:
1. Uninstall any existing Helm release
2. Deploy with `dataParallel.enabled=true` and all DP values
3. Wait for all pods to be ready
4. Run the load test

### Via Helm Directly

The GLM-5-specific values below (`seed`, `maxNumBatchedTokens`, parsers, EP, quantization) are the Helm `--set` translation of the [bare-Docker reference deployment](docker-reference/glm5-dp-docker.md) — that doc is authoritative for the exact flag values.

```bash
helm upgrade --install vllm ./vllm-kv-stack -n vllm --create-namespace \
  -f vllm-kv-stack/values.yaml \
  --set dataParallel.enabled=true \
  --set dataParallel.size=2 \
  --set dataParallel.groups=1 \
  --set dataParallel.nicName=enp189s0f0 \
  --set modelVolume.modelSubPath=GLM-5-w4a8-mtp-QuaRot \
  --set modelVolume.hostPath=/home/models \
  --set vllm.enableExpertParallel=true \
  --set vllm.quantization=ascend \
  --set vllm.trustRemoteCode=true \
  --set vllm.gpuMemoryUtilization=0.95 \
  --set vllm.seed=1924 \
  --set vllm.maxNumBatchedTokens=4096 \
  --set vllm.toolCallParser=glm47 \
  --set vllm.reasoningParser=glm45 \
  --set backend=boom \
  --set boom.enabled=true \
  --set boom.claudeCodeAliases=true
```

---

## Verifying the Deployment

### Check Pod Status

```bash
kubectl get pods -n vllm -l leaderworkerset.sigs.k8s.io/name=vllm-dp -o wide
```

Expected output (for 1 group of 2):

```
NAME          READY   STATUS    RESTARTS   NODE
vllm-dp-0    2/2     Running   0          node3    # leader (vllm + sidecar)
vllm-dp-0-1  1/1     Running   0          node4    # worker (vllm only)
```

### Check LWS Resource

```bash
kubectl get leaderworkerset -n vllm
```

Expected:

```
NAME      READY   SIZE   REPLICAS
vllm-dp   1       2      1
```

### Check Logs

Leader (should show both DP engines starting):

```bash
kubectl logs -n vllm vllm-dp-0 -c vllm -f
# Look for: "EngineCore_DP0" and vLLM startup complete
```

Worker:

```bash
kubectl logs -n vllm vllm-dp-0-1 -c vllm -f
# Look for: "EngineCore_DP1" completing startup
```

### Test Inference

```bash
curl http://<node-ip>:30034/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "served-model",
    "messages": [{"role": "user", "content": "Hello"}],
    "max_tokens": 32
  }'
```

Or through BooM Gateway (port 30401) if enabled.

---

## Scaling

### Adding More DP Groups

When more NPU node pairs become available, increase `data_parallel_groups`:

```yaml
helm:
  data_parallel_groups: 3    # 3 groups × 2 pods = 6 nodes total
```

Each group is fully independent. The router load-balances across all leader
pods. This gives you 3× the throughput of a single group.

### Larger DP Groups (3+ nodes)

For models that need more than 2-node DP:

```yaml
helm:
  data_parallel_size: 3      # 1 leader + 2 workers per group
```

The worker start-rank computation handles arbitrary group sizes
automatically.

---

## Troubleshooting

### Worker Pod Stuck in CrashLoopBackOff

Check if the worker can reach the leader:

```bash
kubectl logs -n vllm vllm-dp-0-1 -c vllm | grep -i "error\|HCCL\|connection\|zmq"
```

Common causes:
- **HCCL RoCE not connected**: Run `hccn_tool -i 0 -link -g` on both nodes.
  All ports must be UP.
- **Wrong NIC name**: If auto-detect fails, set `data_parallel_nic_name`
  explicitly.
- **Firewall**: Ensure RPC port (default 13389) is open between nodes.

### ZMQ "Cannot assign requested address" on Worker

If the worker crashes with `zmq.error.ZMQError: Cannot assign requested address`,
it means the worker is trying to *bind* to the leader's IP instead of
*connecting* to it. This happens when:
- The worker uses `python -m vllm.entrypoints.openai.api_server --headless`
  instead of `vllm serve /model --headless`
- The `--data-parallel-address` is set to a non-local IP

The correct configuration:
- **Worker**: `vllm serve /model --headless` with `--data-parallel-address`
  set to the *leader's* IP (resolved from `LWS_LEADER_ADDRESS`)
- **Leader**: `python -m vllm.entrypoints.openai.api_server` with
  `--data-parallel-address` set to its own `NODE_IP`

### Leader Shows "HELLO message from remote engine, expected local"

This `RuntimeError` means the leader was started with `vllm serve` instead
of `python -m vllm.entrypoints.openai.api_server`. The `vllm serve`
entrypoint defaults `api_server_count` to `data_parallel_size`, trying to
spawn multiple local API server processes. Use the `python -m` entrypoint
for the leader.

### Worker start_rank=0 (Wrong Rank)

If worker logs show `start_rank=0` despite being a worker, the rank
calculation is using `HOSTNAME` instead of `POD_NAME`. On `hostNetwork`
pods, `HOSTNAME` is the node name (e.g. `node3`), not the pod name
(`vllm-dp-0-1`). The template uses the Downward API `POD_NAME` env var
to extract the correct worker index.

### Leader Pod Healthy But No Inference Response

The leader waits for all workers to connect before serving. Check:

```bash
kubectl logs -n vllm vllm-dp-0 -c vllm | grep "DP"
```

Both `EngineCore_DP0` and `EngineCore_DP1` must complete startup.

### LWS_LEADER_ADDRESS Not Set

If the worker logs show an empty `LWS_LEADER_ADDRESS`:
1. Verify LWS operator is running: `kubectl get pods -n lws-system`
2. Check LWS version: the `LWS_LEADER_ADDRESS` env injection requires
   LWS v0.4.0+.

### LWS Controller Pod CrashLoopBackOff / CreateContainerConfigError

If the LWS controller itself fails:
- **"container has runAsNonRoot and image will run as root"**: The upstream
  LWS image runs as root. Set `runAsNonRoot: false` in the controller's
  `securityContext` (already done in `lws-manifests.yaml`).
- **Image size ~1.4KB / empty logs**: The image in the registry is
  corrupted. Re-push from a fresh pull (see air-gapped install steps).

### CMAKE_PREFIX_PATH: unbound variable

If the pod crashes with `set_env.sh: line 31: CMAKE_PREFIX_PATH: unbound
variable`, the Ascend toolkit setup script fails with `set -u`. The
template handles this by using `set -eo pipefail` (no `-u`) and exporting
`CMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH:-}"` before sourcing `set_env.sh`.

### Group Keeps Restarting

`RecreateGroupOnPodRestart` means if either pod crashes, both restart.
Check both leader AND worker logs to find which one crashed first:

```bash
kubectl logs -n vllm vllm-dp-0 -c vllm --previous
kubectl logs -n vllm vllm-dp-0-1 -c vllm --previous
```

### Switching Back to Single-Node

Set `data_parallel_enabled: false` (or use any existing non-DP config).
The standard `40-vllm-unified.yaml` Deployment takes over. Clean up LWS resources:

```bash
kubectl delete leaderworkerset vllm-dp -n vllm --ignore-not-found
```

---

## Comparison: Single-Node vs DP

| Aspect | Single-Node (Deployment) | Multi-Node DP (LWS) |
|--------|--------------------------|----------------------|
| K8s resource | Deployment | LeaderWorkerSet |
| Replicas meaning | Independent vLLM pods | Independent DP groups |
| Context length | Limited by 1 node's KV cache | Multiplied by node count |
| Expert parallel | EP within node (TP group) | EP across nodes (DP × TP group) |
| Network requirement | None (intra-node only) | RoCE between nodes |
| Failure handling | Per-pod restart | Per-group restart |
| Sidecar | On every pod | On leader pods only |
| Config trigger | `data_parallel_enabled: false` | `data_parallel_enabled: true` |
