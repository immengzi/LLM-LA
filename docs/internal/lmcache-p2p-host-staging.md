# LMCache P2P + host-staging mode (the "direct142" replica)

Status: **IMPLEMENTED** and gated behind `lmcache.mode: "p2p"` (default
`"mooncake"`, so existing configs are untouched). Targets both **yz** and **bz**.

This is the **as-built reference** for the shipped feature: §0–§3 are the
background/ground-truth that motivated the work, §4–§7 describe what was actually
built (with the design decisions and their rationale), and §8–§10 are the
operations notes.

Config-authoring TL;DR — a p2p replica needs, under `helm:`:

```yaml
lmcache_enabled: true
lmcache_mode: "p2p"
lmcache_use_host_staging: true            # default; the whole point
lmcache_os_staging_bytes: 8589934592      # 8 GiB registered arena
lmcache_p2p_controller_pull_url:  "<controller-node-ip>:9800"
lmcache_p2p_controller_reply_url: "<controller-node-ip>:9900"
deploy_lmcache_controller: true
lmcache_controller_image: "reg.local:32000/lmcache-ascend:hccl-p2p"
vllm_image: "reg.local:32000/lmcache-ascend:hccl-p2p"
mooncake_enabled: false                   # 142 has no remote store
values:
  lmcacheController.nodeName: "<node hosting the controller IP above>"
```

The only per-deploy items to confirm are the **controller node + its IP** and the
**registry-host form** of the image. Everything else defaults to the 142 values.

## Backward-compatibility guarantee (acceptance criterion)

Everything is gated behind `lmcache.mode` (default `mooncake`). For any config
that does not set `lmcache_mode: p2p`, the rendered manifests MUST be byte-identical
to before. Verify with:

```bash
# render an unchanged Mooncake config on the OLD chart vs the NEW chart and diff
helm template vllm src/vllm-kv-stack -f <rendered mooncake values> > /tmp/new.yaml
git stash && helm template vllm src/vllm-kv-stack -f <same values> > /tmp/old.yaml && git stash pop
diff /tmp/old.yaml /tmp/new.yaml   # MUST be empty
```

Invariants that keep this true: (1) `lmcache.mode` defaults to `"mooncake"`;
(2) the `else` branch of `13-lmcache-config.yaml` is the historical template
verbatim; (3) the connector `else` branch keeps `LMCacheAscendConnectorV1Dynamic`;
(4) the extra `__P2P_HOST__`/`__LMCACHE_INSTANCE_ID__` sed args are no-ops when
those tokens are absent (Mooncake mode); (5) `14-lmcache-controller.yaml` renders
nothing unless `mode == "p2p"`.

## 0. Lineage (which reference is which)

- **142** (`lmcache-csi/142/142_master` + `50_worker`) = the **TARGET** to
  replicate in k8s. LMCache-native **P2P + host-staging**, standalone
  `lmcache_controller`, **Mooncake OFF**, new image
  `127.0.0.1:32000/lmcache-ascend:hccl-p2p`.
- **33** (`33-master` + `37-worker`) and **218** (`218-master` + `207-worker`) =
  what our **FORMER** config was based on: **Mooncake + vLLM (+ NDS)**,
  `P2PHANDSHAKE`. We changed a few knobs but stayed in this family. This is
  exactly what our chart renders today (the `mooncake` mode).
- Goal: add a **new** p2p/host-staging config that follows 142, **alongside**
  (not replacing) the existing Mooncake-mode configs.

## 1. Why

Under load our engines crash-loop with a "gpu size" failure. The 142 reference
LMCache config (extracted to `lmcache-csi/142/**`) shows why and how they avoid it.

The new setting is **host-staging one-sided P2P reads** in `extra_config`:

```yaml
extra_config:
  use_host_staging: True
  os_staging_bytes: 8589934592   # 8 GiB
```

Per the file's own comment: the producer registers **one bounded, pinned host
arena** and serves reads out of staged copies, *"instead of registering the full
CPU KV pool (which trips the >10GB device registration limit)."* When all slots
are busy it caps the hit (free_only) and the reader recomputes the rest — no
blocking, no crash.

Hypothesis: our worker death on first KV save is that **>10 GB device
registration ceiling** — we register the whole 90 GiB CPU KV pool with the
transfer engine. Host-staging caps the registered region at 8 GiB. This is
likely the real fix for the crash, not batch size / gpuMemoryUtilization.

> Separate crash, same mode: once host-staging is in place, a rollback under
> KV pressure can also feed a **negative prompt-token delta** into a Prometheus
> counter and kill the engine during metrics recording. That is fixed by a
> source patch baked into the image — see
> [image-patches.md](../operations/image-patches.md). Both fixes are required
> for a stable p2p deploy.

IMPORTANT CONSTRAINT: host-staging is not a standalone knob. Its comment says it
*"lives in HcclChannel, so this REQUIRES `transfer_channel: hccl` (NOT
hccl_onesided) and `p2p_delay_pull: True`."* It only works in the LMCache P2P
mode, so we cannot bolt it onto our current Mooncake remote-store setup.

## 2. Ground truth: the 142 reference design

Source files (extracted from `lmcache-csi.rar`):
- `lmcache-csi/142/142_master/lmcache_instance1_host_staging.yaml`  (DP rank 0)
- `lmcache-csi/142/50_worker/lmcache_instance2_host_staging.yaml`   (DP rank 1)
- `lmcache-csi/142/142_master/run_miniumax27_lmcache_p2p_dp0.sh`    (vLLM launch)
- `lmcache-csi/142/142_master/run_lmcache_controller.sh`            (controller)

### 2a. LMCache config (per DP instance)

```yaml
chunk_size: 512
local_cpu: True
max_local_cpu_size: 100.00
enable_async_loading: False
save_only_first_rank: False
store_async: False
# remote_url: mooncakestore://...     <-- COMMENTED OUT (no Mooncake)

internal_api_server_enabled: True
internal_api_server_host: "0.0.0.0"
internal_api_server_port_start: 6999

# P2P
enable_p2p: True
p2p_host: "<this pod's host IP>"          # 7.150.2.142 (rank0) / 7.150.2.50 (rank1)
p2p_init_ports:   [9950..9957]  (rank0) / [9960..9967] (rank1)   # per TP (8)
p2p_lookup_ports: [9970..9977]  (rank0) / [9980..9987] (rank1)   # per TP (8)
transfer_channel: "hccl"
p2p_use_npu: True
p2p_pull_mode: True
p2p_delay_pull: True
p2p_npu_buffer_size: 134217728            # 128 MB

# Controller
enable_controller: True
lmcache_instance_id: "lmcache_instance_1" # _2 on rank1
controller_pull_url:  "7.150.2.142:9800"  # points at the lmcache_controller
controller_reply_url: "7.150.2.142:9900"
lmcache_worker_ports: [9940..9947] (rank0) / [9950..9957] (rank1) # per TP (8)

extra_config:
  lookup_backoff_time: 0.001
  use_host_staging: True
  os_staging_bytes: 8589934592            # 8 GiB
  # p2p_pull_pending_ttl: 60.0            # optional lease knobs
  # p2p_pull_lease_guard_s: 15.0
```

### 2b. vLLM launch (dp0)

```
LMCACHE_CONFIG_FILE=/workspace/csy/p2p/lmcache_instance1_host_staging.yaml
--kv-transfer-config '{"kv_connector":"LMCacheAscendConnector","kv_role":"kv_both"}'
--kv-events-config   '{"enable_kv_cache_events":true,"publisher":"zmq",...}'
--gpu-memory-utilization 0.92 --tensor-parallel-size 8 --data-parallel-size 2
--data-parallel-size-local 1 --max-num-seqs 32 --max-num-batched-tokens 32768
--enable-expert-parallel --enable-prompt-tokens-details
```

Note the connector: **`LMCacheAscendConnector`** (NOT `...V1Dynamic`, no
`kv_connector_module_path`).

### 2c. LMCache controller

```
lmcache_controller --host 0.0.0.0 --port 9000 --monitor-ports '{"pull":9800,"reply":9900}'
```

One controller process; every engine's `controller_pull_url`/`controller_reply_url`
points at it (9800/9900). Replaces the Mooncake master as the coordinator.

## 3. Current chart state and the gap

`src/vllm-kv-stack/templates/13-lmcache-config.yaml` is hardwired to the
Mooncake/`P2PHANDSHAKE` mode — it always emits:

```yaml
remote_url: "mooncakestore://{{ .Values.mooncake.masterServerAddress }}/"
extra_config:
  metadata_server: "P2PHANDSHAKE"
  master_server_address: {{ .Values.mooncake.masterServerAddress }}
  global_segment_size / mooncake_prefer_local_alloc / use_ascend_direct
```

`40-vllm-unified.yaml` (3 spots: leader/worker/single) hardwires the connector to
`LMCacheAscendConnectorV1Dynamic` and sed-substitutes `__LOCAL_HOSTNAME__`.

### Gap summary

| Capability | Have | Need for 142 |
|---|---|---|
| LMCache configmap: local cache + chunk | yes | yes |
| `remote_url: mooncakestore` | always on | **must be omittable** |
| `extra_config` P2PHANDSHAKE/mooncake | always on | **must be omittable** |
| `enable_p2p` + `p2p_host` + transfer_channel + p2p_* buffers | no | **new** |
| per-TP arrays: `p2p_init_ports`, `p2p_lookup_ports`, `lmcache_worker_ports` | no | **new** |
| `enable_controller` + controller pull/reply URLs + `lmcache_instance_id` | no | **new** |
| `use_host_staging` + `os_staging_bytes` | no | **new** |
| `save_only_first_rank`, `store_async` | no | new (small) |
| connector `LMCacheAscendConnector` (non-Dynamic) | no (only Dynamic) | **new toggle** |
| `lmcache_controller` Deployment | no (only mooncake-master) | **new template** |
| per-pod instance id / `p2p_host` | no | **runtime `POD_NAME`/`NODE_IP` substitution** |

## 4. Design decisions (as built)

- **D1. New mode, not a replacement.** `lmcache.mode` (`"mooncake"` default |
  `"p2p"`) selects the backend. The Mooncake path is byte-for-byte unchanged; the
  p2p path is a new branch. See the BC guarantee at the top.
- **D2. Coordinator.** In p2p mode the sweep forces `deploy.mooncakeMaster=false`
  and deploys a standalone `lmcache_controller` (new template `14-...`). Engines
  reach it via `lmcache.p2p.controllerPullUrl` / `controllerReplyUrl` (raw
  `host:port`). The lmcache↔mooncake sweep coupling was already relaxed, so no
  Mooncake master is needed.
- **D3. Per-TP port arrays.** Rendered in `13-lmcache-config.yaml` from a base +
  `lmcache.p2p.tpSize` using Helm `until`/`add`, e.g.
  `p2p_init_ports: [9950, 9951, … 9957]`. Bases are values knobs
  (`initPortBase` / `lookupPortBase` / `workerPortBase`).
- **D4. Per-pod identity (non-obvious choice).** Rather than per-*role*
  `lmcache_instance_id` and per-role port bases, we use a **single shared
  ConfigMap** and substitute the pod-varying bits at runtime:
  `__P2P_HOST__` → `${NODE_IP}` and `__LMCACHE_INSTANCE_ID__` → `${POD_NAME}`
  (see D5). This gives every pod a unique host IP and instance id with no
  per-role templating. Port **bases are identical for all pods** — safe because
  each engine pod is host-networked on its own pinned node (leader nodes carry
  `vllm-role=leader`, workers `vllm-role=worker`, and the two DP groups are spread
  by hostname anti-affinity), so no two pods share a network namespace. This
  resolves Q1/Q2.
- **D5. Runtime substitution.** The init wrapper in `40-vllm-unified.yaml` runs
  one `sed` with three expressions:
  `s/__LOCAL_HOSTNAME__/${NODE_IP}/`, `s/__P2P_HOST__/${NODE_IP}/`,
  `s/__LMCACHE_INSTANCE_ID__/${POD_NAME}/`. The extra two are no-ops in Mooncake
  mode (those tokens don't appear there), which keeps BC.
- **D6. `os_staging_bytes` default.** 8 GiB (`lmcache.p2p.osStagingBytes`),
  matching the reference and safely below the ~10 GB registration ceiling.
- **D7. Connector.** In p2p mode all three vLLM spots emit
  `{"kv_connector":"LMCacheAscendConnector","kv_role":"kv_both"}` (non-Dynamic, no
  module path); otherwise the historical `...V1Dynamic` line.
- **D8. Controller has no Service (non-obvious choice).** It is a Deployment
  only: it runs `hostNetwork: true` and is pinned via
  `lmcacheController.nodeName`, so engines
  dial its node IP directly (exactly like the reference hardcodes
  `7.150.2.142:9800`). No ClusterIP/Service indirection.

## 5. File-by-file (as implemented)

1. `src/vllm-kv-stack/values.yaml`
   - `lmcache.mode: "mooncake"` (default; other value `"p2p"`).
   - `lmcache.p2p:` block: `tpSize: 8`, `transferChannel: "hccl"`, `useNpu`,
     `pullMode`, `delayPull`, `npuBufferSize: 134217728`, `initPortBase: 9950`,
     `lookupPortBase: 9970`, `workerPortBase: 9940`, `useHostStaging: true`,
     `osStagingBytes: 8589934592`, `saveOnlyFirstRank`, `storeAsync`,
     `lookupBackoffTime: 0.001`, `controllerPullUrl: ""`, `controllerReplyUrl: ""`.
   - `lmcacheController:` block: `image` (default
     `127.0.0.1:32000/lmcache-ascend:hccl-p2p`), `port: 9000`, `pullPort: 9800`,
     `replyPort: 9900`, `nodeName: ""`, `tolerations: []`. (No `enabled` key — the
     template gates on `mode == "p2p"` + `deploy.lmcacheController`. No `address` —
     engines use the `controllerPullUrl`/`ReplyUrl` knobs. No Service — see D8.)
   - `deploy.lmcacheController: true`.

2. `src/vllm-kv-stack/templates/13-lmcache-config.yaml`
   - `{{ if eq .Values.lmcache.mode "p2p" }}` … `{{ else }}` … `{{ end }}`.
   - **p2p branch** emits the 142 shape: no `remote_url`; `enable_p2p: True`,
     `p2p_host: "__P2P_HOST__"`, `transfer_channel`, `p2p_use_npu/pull_mode/
     delay_pull/npu_buffer_size`, the three per-TP port arrays (range over
     `tpSize`), `enable_controller: True`,
     `lmcache_instance_id: "__LMCACHE_INSTANCE_ID__"`,
     `controller_pull_url`/`controller_reply_url`, and `extra_config` with
     `lookup_backoff_time`, `use_host_staging`, `os_staging_bytes`.
   - **else branch** is the historical Mooncake template, unchanged.

3. `src/vllm-kv-stack/templates/14-lmcache-controller.yaml` (NEW)
   - Deployment **only** (no Service). `hostNetwork: true`,
     `dnsPolicy: ClusterFirstWithHostNet`, optional `nodeAffinity` from
     `lmcacheController.nodeName`, optional `tolerations`, tcpSocket
     liveness/readiness probes, modest CPU/mem requests. Command:
     `lmcache_controller --host 0.0.0.0 --port {{ port }} --monitor-ports
     '{"pull": {{ pullPort }}, "reply": {{ replyPort }}}'`. It's a lightweight
     lookup directory — **no NPU, no ascend mounts, no NIC detection** (unlike the
     mooncake master). Gated on
     `lmcache.enabled && lmcache.mode == "p2p" && deploy.lmcacheController`.

4. `src/vllm-kv-stack/templates/40-vllm-unified.yaml` (3 spots: leader/worker/single)
   - Connector: nested `{{ if eq .mode "p2p" }}` emits `LMCacheAscendConnector`
     (no module path); `{{ else }}` keeps `...V1Dynamic`.
   - `sed` line extended with `-e "s/__P2P_HOST__/${NODE_IP}/"` and
     `-e "s/__LMCACHE_INSTANCE_ID__/${POD_NAME}/"` (no-ops in Mooncake mode).

5. `src/config.py` (`HelmConfig`)
   - Added: `lmcache_mode: str = "mooncake"`, `lmcache_use_host_staging`,
     `lmcache_os_staging_bytes`, `lmcache_p2p_controller_pull_url`,
     `lmcache_p2p_controller_reply_url`, `deploy_lmcache_controller`,
     `lmcache_controller_image`. Port bases / buffer sizes / channel stay chart
     defaults (they match 142 and rarely change; override via `helm.values` if
     ever needed).

6. `src/sweep_methods.py`
   - Maps the new fields to `set_values` in the existing lmcache block. Echoes the
     active mode. In `p2p` mode: forces `deploy.mooncakeMaster=false`, sets
     `deploy.lmcacheController`, and warns if the controller URLs are unset.

7. `src/configs/prod-yz-boom-minmax-lmcache-p2p-hoststaging-affinity.yaml` (NEW)
   - Branched from `prod-yz-boom-minmax-lmcache-local-hq-affinity.yaml`: keeps
     per-role node pinning + hard affinity + `enablePromptTokensDetails`. Sets
     `lmcache_mode: p2p`, host-staging on (8 GiB), controller on (`node7` /
     `7.150.6.33`), **Mooncake off**, **NDS dropped** (Q5), `max_local_cpu_size:
     100` (matches ref; host-staging removes the registration-driven RAM driver).
     `vllm_image` + controller image → `reg.local:32000/lmcache-ascend:hccl-p2p`.

8. `src/configs/prod-bz-boom-minmax-lmcache-p2p-hoststaging-affinity.yaml` (NEW)
   - Same shape, bz paths, no NDS. Controller IP/nodeName are **placeholders**
     (`192.168.0.42` / `k8s-worker1`) — confirm real bz values before deploy.

9. `src/configs/1-master_config.yaml`
   - Both new keys added **commented out** (active deploy unchanged).

10. Docs: `docs/configuration/helm-values.md` (new "mode `mooncake` vs `p2p`"
    section + knob table) and `docs/internal/kv-cache-hit-rate-collapse.md`
    (host-staging recorded as the fix for the registration-ceiling crash).

## 6. Port scheme (as built)

TP=8. **Identical bases for every pod** (see D4 — safe because each engine pod is
host-networked on its own pinned node, so ports never collide across pods):

| Array | Base | Rendered (TP=8) |
|---|---|---|
| `p2p_init_ports`   | `lmcache.p2p.initPortBase`   = 9950 | `[9950 … 9957]` |
| `p2p_lookup_ports` | `lmcache.p2p.lookupPortBase` = 9970 | `[9970 … 9977]` |
| `lmcache_worker_ports` | `lmcache.p2p.workerPortBase` = 9940 | `[9940 … 9947]` |
| `internal_api_server_port_start` | 6999 | 6999 |
| controller listen ports | `lmcacheController.{port,pullPort,replyPort}` | 9000 / 9800 / 9900 |

We deliberately do **not** replicate the reference's per-rank base offsets (rank0
9950 / rank1 9960): those existed because the two reference instances could share
a host. Our per-role node pinning + hostname anti-affinity guarantees one engine
pod per node, so a single base set is correct and simpler. The controller ports
(9000/9800/9900) are distinct from every engine port, so a controller can even be
co-located on an engine node without collision.

## 7. Resolutions (were open questions)

- **Q1 (instance id / port uniqueness): RESOLVED via runtime substitution.**
  `lmcache_instance_id` is set to `${POD_NAME}` at container start, so all 4 pods
  (2 leaders + 2 workers) get distinct ids from one ConfigMap. Port bases are
  shared; each pod owns its node's network namespace, so no collision. See D4/D5.
- **Q2 (port collisions): RESOLVED.** With `vllm-role=leader/worker` pinning +
  hostname anti-affinity, no two engine pods land on the same node, so identical
  port bases can't collide.
- **Q3 (controller placement / HA): DECIDED.** One `lmcache_controller` per model,
  pinned via `lmcacheController.nodeName`; engines dial its node IP through
  `controllerPullUrl`/`ReplyUrl`. yz uses `node7` / `7.150.6.33`; bz uses a
  placeholder pending confirmation. It is a single point of failure (accepted for
  now) — mitigated by liveness/readiness probes; if it dies, P2P lookups fail
  until it restarts. **This is the one item to confirm per environment.**
- **Q4 (image): RESOLVED.** Both the p2p engine pods and the controller run
  `lmcache-ascend:hccl-p2p` (ships the `lmcache_controller` entrypoint + the
  non-Dynamic `LMCacheAscendConnector`). The configs use the `reg.local:32000/…`
  form to match the working pull convention; `127.0.0.1:32000` is the same local
  registry. Switch forms if a node's kubelet only resolves the other.
- **Q5 (NDS coexistence): RESOLVED — dropped.** The p2p configs set
  `lmcache_nds_enabled: false` to match 142 (pure P2P/host-staging) and remove a
  variable. (NDS remains available in Mooncake-mode configs.)
- **Q6 (kv-events-config): RESOLVED — already emitted.** The chart already renders
  `--kv-events-config` (zmq publisher) whenever the sidecar is enabled, in all
  three vLLM spots; no change needed for p2p.

## 8. Risks (and how they're handled)

- **Controller SPOF.** If `lmcache_controller` dies, P2P lookups fail until it
  restarts. Mitigated by tcpSocket liveness/readiness probes; pinned to a fixed
  node so its IP is stable. No HA yet — acceptable for the current single-model
  deploy.
- **Wrong controller IP/nodeName.** If `controllerPullUrl`/`ReplyUrl` don't match
  the node where the controller actually runs, engines can't register. The sweep
  emits a WARNING when the URLs are unset; still verify IP↔nodeName by hand
  (bz values are placeholders).
- **Registry-host form.** `reg.local:32000` vs `127.0.0.1:32000` — one-line fix in
  the config if a node's kubelet only resolves one.
- **Surface area.** Mitigated by the `lmcache.mode` gate + the BC diff check at the
  top: existing Mooncake configs render byte-identically.

## 9. Rollback

`lmcache.mode` defaults to `mooncake`, so no chart default changed. To back out a
p2p deploy, flip `1-master_config.yaml` to the Mooncake revert target
(`prod-yz-boom-minmax-lmcache-hq-affinity.yaml`, or the local-hq-affinity variant)
and redeploy. The controller Deployment stops rendering as soon as the active
config is no longer `mode: p2p`.

## 10. Deploy checklist

Build is complete; this is the per-environment rollout sequence.

1. **Confirm the controller node + IP** and set `controllerPullUrl`/`ReplyUrl` +
   `values.lmcacheController.nodeName` in the target config (yz preset to `node7`;
   bz is a placeholder).
2. **Confirm the image tag** `lmcache-ascend:hccl-p2p` pulls on the engine nodes
   (registry-host form) and that it includes the negative-counter fix — either
   the `hccl-p2p-metrics-fix` overlay tag or a rebuild with the diff baked in
   (see [image-patches.md](../operations/image-patches.md)).
3. **BC check** (see top): `helm template` an unchanged Mooncake config on this
   branch vs the previous commit → diff must be empty.
4. **Render check**: `helm template` the p2p config and eyeball
   `13-lmcache-config` output against `lmcache-csi/142/**` — connector is
   `LMCacheAscendConnector`, `use_host_staging: true`, port arrays length TP,
   `controller_*_url` populated, no `remote_url`.
5. Pre-req on each engine node: `sysctl -w vm.swappiness=0` and
   `kernel.numa_balancing=0`; label leader/worker nodes (`vllm-role=...`).
6. Deploy the controller + **one** DP group first; confirm the worker survives the
   first KV save (the host-staging crash test) and the controller shows
   registrations, then scale to both groups.
