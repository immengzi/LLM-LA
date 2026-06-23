# Mooncake Integration into Helm Chart — Summary of Changes

## Overview

Mooncake manages cross-node KV cache sharing via Huawei's `AscendStoreConnector`. It is an optional enhancement **within** the router stack — when enabled, the router handles request scheduling while Mooncake handles the actual KV cache transfer between NPU nodes. Mooncake does not work independently from the router.

When `mooncake.enabled=true`, the following are added to the deployment:

1. A `mooncake-master` Deployment (metadata coordination service)
2. A ConfigMap containing `mooncake.json`
3. `--kv-transfer-config` flag on each vLLM worker
4. `hostNetwork` + HCCL NIC auto-detection on vLLM pods
5. Additional env vars, ports, and volume mounts on vLLM pods

---

## Files Changed

### 1. `values.yaml` — Modified

**What changed:**

- Added `mooncake` section with all Mooncake-specific configuration (enabled toggle, master port, master server address, global segment size, eviction parameters, Ascend buffer pool, lookup RPC port).
- Added `mooncakeMaster` under `images` (points to the same base image as vLLM since it contains the `mooncake_master` binary).
- Added `mooncakeMaster: true` under `deploy` for component deployment control.
- Added `hostNetwork: false` under `vllm` (set to `true` when Mooncake is enabled on the standard Deployment path — required for RoCE network access; the DP/LWS path already runs with hostNetwork).

> Note: PV/PVC capacity is currently hardcoded to `20Gi` in `01-model-pv.yaml`; there is no `modelVolume.storageSize` value key in the chart today.

**Why:** Centralises all Mooncake knobs in `values.yaml` so deployments can be configured via `--set` flags or override files without touching templates.

---

### 2. `templates/_helpers.tpl` — Modified

**What changed:** Appended two new template definitions at the end of the file:

- `vllmkv.ascendDriverVolumes` — outputs the 5 Ascend NPU driver hostPath volume definitions (dcmi, npu-smi, driver lib64, version.info, ascend_install.info).
- `vllmkv.ascendDriverMounts` — outputs the matching 5 volumeMount definitions.

**Why:** These 5 volumes + 5 mounts are identical across every pod that needs NPU access. Extracting them into helpers avoids duplicating ~30 lines per Deployment template. Currently used by `12-mooncake-master.yaml`; `40-vllm-unified.yaml` still uses inline definitions (can be refactored later).

---

### 3. `templates/11-mooncake-config.yaml` — New File

**What it does:** Creates a ConfigMap named `mooncake-config` containing `mooncake.json`. This JSON file configures the Mooncake protocol, metadata server mode, master server address, and global segment size. Both the mooncake-master and vLLM workers mount this file.

**Gate condition:** `mooncake.enabled=true`.

---

### 4. `templates/12-mooncake-master.yaml` — New File

**What it does:** Deploys a single-replica `mooncake-master` Deployment. This process acts as the metadata coordination service for cross-node KV cache transfers.

**Key design decisions:**

- **Node pinning:** Uses the shared `pin` mechanism (same as router and redis) rather than a separate `masterNodeName` field — keeps all control-plane components on the same node.
- **hostNetwork:** Always enabled — mooncake-master needs RoCE network access.
- **Image:** Uses `images.mooncakeMaster`, processed through `vllmkv.image` helper for registry rewriting.
- **Ascend driver mounts:** Uses `vllmkv.ascendDriverVolumes` / `vllmkv.ascendDriverMounts` helpers.
- **Logs:** Host path `/tmp/mooncake_logs` (hostPath, DirectoryOrCreate), mounted into the container at `/workspace/mooncake_logs`.
- **NIC detection:** Uses the same Python ioctl-based auto-detection as vLLM workers, compatible with different NIC names across subnets.

**Gate condition:** `mooncake.enabled=true` AND `deploy.mooncakeMaster=true`.

---

### 5. `templates/40-vllm-unified.yaml` — Modified (6 insertion points)

All additions are gated with `{{- if .Values.mooncake.enabled }}` and produce no output when `mooncake.enabled=false`.

| # | Location | What was added | Why |
|---|----------|---------------|-----|
| 1 | `spec.template.spec`, after `shareProcessNamespace` | `hostNetwork: true` + `dnsPolicy: ClusterFirstWithHostNet`. On the standard Deployment path this is gated on `mooncake.enabled` AND `vllm.hostNetwork`; the DP/LWS path always sets `hostNetwork: true` regardless. | AscendStoreConnector uses RoCE, which requires host network stack |
| 2 | Container `args` script, after `source set_env.sh` + `LD_LIBRARY_PATH` | NIC auto-detection block + HCCL/GLOO/TP env exports | hostNetwork mode requires explicit NIC configuration for HCCL; NIC names differ between subnets |
| 3 | vLLM startup command, after `--kv-events-config` | `--kv-transfer-config` with AscendStoreConnector JSON | Tells vLLM to load the Mooncake connector for cross-node KV cache sharing |
| 4 | `env` section, after `ASCEND_RT_VISIBLE_DEVICES` | `NODE_IP`, `HCCL_OP_EXPANSION_MODE`, `VLLM_USE_V1`, `PYTORCH_NPU_ALLOC_CONF`, `MOONCAKE_CONFIG_PATH`, `ASCEND_BUFFER_POOL` | Runtime env vars required by Mooncake and Ascend NPU stack |
| 5 | `ports` section, after `kvpub` port | `containerPort` for `mooncake.lookupRpcPort` (name: `kv-rpc`) | AscendStoreConnector's lookup RPC port for KV cache metadata exchange between workers |
| 6 | `volumeMounts` + `volumes` sections | `mooncake-config` ConfigMap mount at `/etc/mooncake/mooncake.json` | vLLM workers read mooncake.json to know the master address and protocol config |

---

## Files NOT Changed

| File | Reason |
|------|--------|
| `01-model-pv.yaml` | Storage size parameterisation is optional; PV already exists in cluster |
| `02-model-pvc.yaml` | Same as above |
| `10-redis.yaml` | No Mooncake-related changes needed |
| `20-cpu-hash.yaml` | No Mooncake-related changes needed |
| `30-router-rbac.yaml` | No Mooncake-related changes needed |
| `31-router.yaml` | No Mooncake-related changes needed |
| `50-podmonitors.yaml` | No Mooncake-related changes needed |

---

## Deployment Examples

```bash
# Router only (existing behaviour, unchanged)
helm upgrade --install vllm ./chart \
  --namespace vllm \
  --set backend=router \
  --set mooncake.enabled=false

# Router + Mooncake
helm upgrade --install vllm ./chart \
  --namespace vllm \
  --set backend=router \
  --set mooncake.enabled=true \
  --set vllm.hostNetwork=true \
  --set mooncake.masterServerAddress="10.50.156.65:50088" \
  --set modelVolume.modelSubPath=qwen3-32b
```

## Valid Configurations

| `backend` | `mooncake.enabled` | Result |
|-----------|-------------------|--------|
| `router` | `false` | Router + sidecar + redis + cpu-hash. No cross-node KV cache transfer. |
| `router` | `true` | All of the above + mooncake-master + AscendStoreConnector on each vLLM worker. |
