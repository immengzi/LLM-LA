# Data Parallel (LWS) Multi-Node Deployment — Changelog

Track of all files added/modified to enable multi-node Data Parallel
inference via LeaderWorkerSet (LWS) on Ascend NPU clusters.

---

## Files to Copy

### New Files (create these)

| File | Description |
|------|-------------|
| `vllm-kv-stack/templates/43-vllm-lws.yaml` | LWS Helm template: leader pod (API server + DP rank 0 + sidecar) and worker pod (headless DP ranks), plus `vllm-qwen` Service |
| `configs/boom-claude-glm-dp.yaml` | Deployment config for GLM-5 with DP=2, TP=8, EP=16, BooM + Claude Code aliases |
| `docs/data_parallel_lws.md` | Full documentation for multi-node DP deployment (architecture, install, config, troubleshooting) |
| `docs/lws-manifests.yaml` | LWS controller manifest (v0.8.0) rewritten for local registry (`reg.local:32000`) |
| `docs/data_parallel_changelog.md` | This file |

### Modified Files (diff carefully)

| File | What changed |
|------|--------------|
| `config.py` | +8 fields on `HelmConfig`: `data_parallel_enabled`, `data_parallel_size`, `data_parallel_groups`, `data_parallel_size_local`, `data_parallel_rpc_port`, `data_parallel_nic_name`, `data_parallel_hccl_buff_size`, `data_parallel_omp_num_threads` |
| `sweep_methods.py` | Reads DP fields from HelmConfig, sets `dataParallel.*` in Helm values, writes DP knobs to `sweep_meta.json`. Updated `_vllm_pods_exist` to also detect `vllm-dp-worker` pods. |
| `vllm-kv-stack/values.yaml` | +1 section: `dataParallel:` with `enabled`, `size`, `groups`, `rpcPort`, `sizeLocal`, `nicName`, `hcclBuffSize`, `ompNumThreads` |
| `vllm-kv-stack/templates/40-vllm.yaml` | Line 1 guard changed from `{{- if .Values.deploy.vllm }}` to `{{- if and .Values.deploy.vllm (not .Values.dataParallel.enabled) }}` — skips Deployment when DP is active |
| `apply_patch.sh` | Added `43-vllm-lws.yaml` and `boom-claude-glm-dp.yaml` to file lists, added master config injection for `boom-claude-glm-dp:` |

---

## Detailed Change Log

### config.py

**HelmConfig dataclass** — 8 new fields after existing `vllm_*` block:

```python
# ---- Data Parallel (multi-node LWS deployment) ----
data_parallel_enabled: bool = False
data_parallel_size: int = 2          # pods per DP group (1 leader + N-1 workers)
data_parallel_groups: int = 1        # number of DP groups (LWS replicas)
data_parallel_size_local: int = 1    # --data-parallel-size-local per pod
data_parallel_rpc_port: int = 13389  # --data-parallel-rpc-port
data_parallel_nic_name: str = ""     # GLOO/TP/HCCL_SOCKET_IFNAME (empty = auto-detect)
data_parallel_hccl_buff_size: int = 200   # HCCL_BUFFSIZE
data_parallel_omp_num_threads: int = 16   # OMP_NUM_THREADS
```

All defaults match the disabled state — existing configs are unaffected.

### sweep_methods.py

**DP deployment block** — added after vLLM runtime flags, before the log line:

```python
dp_enabled = bool(getattr(h, "data_parallel_enabled", False))
set_values["dataParallel.enabled"] = dp_enabled
if dp_enabled:
    set_values["dataParallel.size"] = int(...)
    set_values["dataParallel.groups"] = int(...)
    set_values["dataParallel.sizeLocal"] = int(...)
    set_values["dataParallel.rpcPort"] = int(...)
    set_values["dataParallel.nicName"] = str(...)   # only if non-empty
    set_values["dataParallel.hcclBuffSize"] = int(...)
    set_values["dataParallel.ompNumThreads"] = int(...)
```

**sweep_meta.json** — DP knobs added to `helm_knobs_from_config`:

```python
"data_parallel_enabled": bool(...)
"data_parallel_size": int(...)
"data_parallel_groups": int(...)
"data_parallel_size_local": int(...)
"data_parallel_nic_name": str(...)
```

**`_vllm_pods_exist`** — label selector widened from `app=vllm-qwen` to
`app in (vllm-qwen,vllm-dp-worker)` so `--skip-vllm` mode detects DP pods.

### vllm-kv-stack/templates/40-vllm.yaml

**Line 1** — guard extended:

```diff
-{{- if .Values.deploy.vllm }}
+{{- if and .Values.deploy.vllm (not .Values.dataParallel.enabled) }}
```

When `dataParallel.enabled=true`, the standard Deployment is not rendered.
The LWS template (`43-vllm-lws.yaml`) takes over. When `false` (default),
everything works exactly as before.

### vllm-kv-stack/values.yaml

**New section** added between `vllm.hostNetwork` and the NFS cache warm block:

```yaml
dataParallel:
  enabled: false
  size: 2               # pods per DP group (1 leader + N-1 workers)
  groups: 1             # number of DP groups (LWS replicas)
  rpcPort: 13389        # --data-parallel-rpc-port
  sizeLocal: 1          # --data-parallel-size-local per pod
  nicName: ""           # GLOO/TP/HCCL_SOCKET_IFNAME override (empty = auto-detect)
  hcclBuffSize: 200     # HCCL_BUFFSIZE for RoCE transfers
  ompNumThreads: 16     # OMP_NUM_THREADS
```

### vllm-kv-stack/templates/43-vllm-lws.yaml

New template, only rendered when `dataParallel.enabled=true`. Contains:

**LeaderWorkerSet resource** (`leaderworkerset.x-k8s.io/v1`):
- `replicas`: number of DP groups (from `dataParallel.groups`)
- `size`: pods per group (from `dataParallel.size`)
- `restartPolicy: RecreateGroupOnPodRestart` — all-or-nothing group restart

**Leader pod** (1 per group):
- Label `app: vllm-qwen` — discoverable by router via existing label selector
- `hostNetwork: true` + `dnsPolicy: ClusterFirstWithHostNet`
- `securityContext.privileged: true` — full Ascend NPU device access for HCCL RoCE
- NIC detection: explicit `dataParallel.nicName` or auto-detect from `NODE_IP`
- HCCL env vars: `HCCL_IF_IP`, `GLOO_SOCKET_IFNAME`, `TP_SOCKET_IFNAME`, `HCCL_SOCKET_IFNAME`
- Ascend env fix: `set -eo pipefail` (no `-u`) + `CMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH:-}"`
  before sourcing `set_env.sh`
- vLLM command: `exec python -m vllm.entrypoints.openai.api_server` (foreground, single process) with:
  - `--data-parallel-address "${NODE_IP}"` — binds to its own host IP
  - `--data-parallel-size`, `--data-parallel-size-local`, `--data-parallel-rpc-port`
  - All existing vLLM flags (quantization, expert parallel, compilation, trust-remote-code, etc.)
  - `--kv-events-config` (when backend is router/litellm/boom)
  - `--kv-transfer-config` (when mooncake is enabled)
- kv-sidecar container (when backend is router/litellm/boom) — identical to `40-vllm.yaml`
- HTTP health probes on `:8200` (startup, readiness, liveness)
- Volume mounts: model, `/dev/shm`, `/root/.cache`, `/etc/hccn.conf`, Ascend driver volumes

**Worker pod** (N-1 per group):
- Label `app: vllm-dp-worker` — NOT discoverable by router (no API server)
- Same `hostNetwork`, `privileged`, NIC detection, HCCL env vars as leader
- Same Ascend env fix as leader
- vLLM command: `exec vllm serve /model --headless` (different entrypoint from leader) with:
  - `--data-parallel-address "${LEADER_IP}"` — resolved from `LWS_LEADER_ADDRESS` via `getent hosts`
  - `--data-parallel-start-rank` computed from pod name (Downward API `POD_NAME`, not `HOSTNAME`)
  - Pod name convention: `<lws-name>-<group>-<worker-idx>` → `START_RANK = WORKER_INDEX * sizeLocal`
- No sidecar, no HTTP probes
- Process-check liveness probe: `pgrep -f 'vllm'`
- Same volumes as leader

**Key design decisions** (discovered during deployment):
1. Leader uses `python -m vllm.entrypoints.openai.api_server` (not `vllm serve`) to avoid
   `api_server_count` defaulting to `data_parallel_size` and spawning multiple local processes
2. Worker uses `vllm serve --headless` (not `python -m ... --headless`) to avoid ZMQ bind
   errors — the `vllm serve --headless` code path only connects to the leader, it does not
   try to bind the RPC port
3. Worker rank uses `POD_NAME` (Downward API), not `HOSTNAME` — on `hostNetwork` pods,
   `HOSTNAME` is the node name, not the pod name

**Service** (`vllm-qwen`):
- Same NodePort 30034 as the existing Deployment Service
- Selector `app: vllm-qwen` — routes only to leader pods

**Anti-affinity**: Both leader and worker have `podAntiAffinity` on
`leaderworkerset.sigs.k8s.io/name=vllm-dp` with `topologyKey: kubernetes.io/hostname`,
ensuring all pods in the LWS land on different nodes.

### apply_patch.sh

- Added `configs/boom-claude-glm-dp.yaml` and `vllm-kv-stack/templates/43-vllm-lws.yaml` to `NEW_FILES`
- Added master config injection for `# boom-claude-glm-dp:` entry
- Added summary line for Data Parallel support

---

## Architecture

```
                    External Client
                         │
                    ┌────▼────┐
                    │  BooM   │ :30401 (NodePort)
                    │ Gateway │
                    └────┬────┘
                         │
                    ┌────▼────────┐
                    │   Router    │ :8080 (ClusterIP)
                    │   Service   │
                    └────┬────────┘
                         │ label selector: app=vllm-qwen
                         │
              ┌──────────▼──────────┐
              │  vllm-qwen Service  │ :8200 → NodePort 30034
              │  (targets leaders)  │
              └──────────┬──────────┘
                         │
         ┌───────────────┼───────────────┐
         │  LWS Group 0                  │
         │                               │
    ┌────▼────┐                   ┌──────▼──────┐
    │ Leader  │  ← HCCL RoCE →   │   Worker    │
    │ (node3) │                   │   (node4)   │
    │         │                   │             │
    │ vLLM    │                   │ vLLM        │
    │ :8200   │                   │ --headless  │
    │ DP=0    │                   │ DP=1        │
    │ TP=8    │                   │ TP=8        │
    │         │                   │             │
    │ sidecar │                   │ (no sidecar)│
    │ :9000   │                   │             │
    └─────────┘                   └─────────────┘
```

---

## Prerequisites

1. **LWS operator** must be installed in the cluster (one-time):

```bash
# Internet-connected cluster:
kubectl apply --server-side -f https://github.com/kubernetes-sigs/lws/releases/download/v0.8.0/manifests.yaml

# Air-gapped cluster (local registry):
# Push lws/lws:v0.8.0 image to reg.local:32000, then:
kubectl apply --server-side -f docs/lws-manifests.yaml
```

```bash
# Verify:
kubectl get crd leaderworkersets.leaderworkerset.x-k8s.io
kubectl get pods -n lws-system   # controller must be Running
```

2. **NPU RoCE network** must be physically connected between nodes:
   - Optical transceivers installed in all 8 NPU ports per node
   - Fiber cabling between nodes (or through an RoCE switch)
   - `/etc/hccn.conf` configured with NPU IP addresses
   - Verify with: `for i in {0..7}; do hccn_tool -i $i -link -g; done` (all must be UP)

---

## Deploying

```bash
# Via sweep runner (same workflow as single-node configs):
python sweep_methods.py --config boom-claude-glm-dp

# Or via helm directly:
helm upgrade --install vllm ./vllm-kv-stack -n vllm \
  --set dataParallel.enabled=true \
  --set dataParallel.size=2 \
  --set dataParallel.groups=1 \
  --set dataParallel.nicName=enp189s0f0 \
  --set modelVolume.modelSubPath=GLM-5-w4a8-mtp-QuaRot \
  --set modelVolume.hostPath=/home/haiting/models \
  --set vllm.enableExpertParallel=true \
  --set vllm.quantization=ascend \
  --set vllm.trustRemoteCode=true \
  ...
```

---

## Backward Compatibility

- `dataParallel.enabled` defaults to `false` in `values.yaml`
- `data_parallel_enabled` defaults to `False` in `HelmConfig`
- When disabled: `40-vllm.yaml` Deployment renders as before, `43-vllm-lws.yaml` is skipped
- All existing configs (`boom-claude-glm.yaml`, `boom-claude.yaml`, `router.yaml`, etc.) work unchanged
- Only `boom-claude-glm-dp.yaml` (or any config with `data_parallel_enabled: true`) triggers the LWS path

---

## Scaling

To add more DP groups (when more NPU node pairs become available):

```yaml
# In your config YAML:
helm:
  data_parallel_groups: 3    # 3 DP groups = 6 pods across 6 nodes
```

To increase pods per group (e.g., 3-node DP for a larger model):

```yaml
helm:
  data_parallel_size: 3      # 1 leader + 2 workers per group
```

---

## Rollback

Set `data_parallel_enabled: false` (or remove it — defaults to false).
The standard Deployment from `40-vllm.yaml` is restored. LWS resources
can be cleaned up with:

```bash
kubectl delete leaderworkerset vllm-dp -n vllm
```
