# Helm values reference

Reference for the `vllm-kv-stack` Helm chart (`src/core/vllm-kv-stack/values.yaml`). Defaults shown are the chart defaults; cluster-specific values (registry, NFS server, node names) should be overridden for your environment.

## How values are set

There are three ways values reach the chart:

1. **Sweep runner** — `sweep_methods.py` translates the `helm:` section of a client config into `--set` flags plus a temporary `models[]` overlay. This is the primary path. See [experiment configs](experiment-configs.md).
2. **`deploy_engine.py`** (backed by the legacy `deploy_vllm.py`
   implementation) — deploys only the selected engine (`deploy.vllm=true`,
   router/redis/cpuHash off) from the same client config.
3. **Direct `helm upgrade --install ... --set ...`** — for manual/one-off deploys.

## Top-level

| Key | Default | Purpose |
|-----|---------|---------|
| `backend` | `router` | Selects the infra stack flavor. Note: gateways are **not** turned on by this — LiteLLM/BooM render only when `litellm.enabled`/`boom.enabled` are set. `litellm`/`boom` render the same router+redis+cpuHash stack as `router`. |
| `engine.type` | `vllm` | Inference engine: `vllm` or pinned `sglang`. Workload names are `<engine>-<modelName>` (for example `vllm-qwen` / `sglang-qwen`); discovery uses `component=<engine>` (default remains `component=vllm`). |
| `serviceImpl` | `python` | Router/sidecar implementation: `python` or `go`; valid for both engines and rejected otherwise |
| `hardware` | `ascend` | Accelerator backend for engine pods: `ascend` (Huawei NPU) or `nvidia` (GPU). `nvidia` requests `nvidia.com/gpu`, applies `vllm.runtimeClassName`, and drops Ascend driver mounts/toolkit. Set per cluster via `helm.values.hardware` ([GPU deployment](../deployment/gpu.md)) |
| `portOffset` | `0` | Added to the NodePorts of **redis, router, cpu-hash, and vLLM only** (not LiteLLM/BooM). For shadow deployments. |
| `replicas.router` | `1` | Router replica count |
| `replicas.vllm` | `16` | Legacy fallback replica count when `models[]` is empty |
| `batchSize` | `8` | Legacy `--max-num-seqs` (when `models[]` empty) |
| `tensorParallelSize` | `8` | Legacy TP degree (when `models[]` empty) |

## Images (`images.*`, `global.imageRegistry`)

| Key | Default | Notes |
|-----|---------|-------|
| `global.imageRegistry` | `reg.local:32000` | If set, all images are rewritten to this registry; `""` disables rewrite |
| `images.router` / `images.sidecar` | `kv-router:latest` / `kv-sidecar:latest` | Python services |
| `images.routerGo` / `images.sidecarGo` | `kv-router-go:latest` / `kv-sidecar-go:latest` | Used when `serviceImpl=go` |
| `images.vllm` | `quay.io/ascend/vllm-ascend:v0.23.0` | vLLM engine image |
| `images.sglang` | `lmsysorg/sglang:v0.5.15-cu129` | Complete official SGLang v0.5.15 image with CUDA 12.9 |
| `images.cpuHash` | `vllm-cpu-hash:latest` | Legacy external prefix-hash service (used only when `router.hashSource=external`) |
| `images.redis` | `redis:7-alpine` | Redis |
| `images.mooncakeMaster` | `quay.io/ascend/vllm-ascend:v0.23.0` | Mooncake master |

A fully-qualified per-model `image` bypasses the registry rewrite.

### Dynamic P/D (warm standby) additions

| Key | Default | Notes |
|-----|---------|-------|
| `mooncake.preferredSegment` | `true` | Rendered as `preferred_segment` in `mooncake.json`; pins store PUTs to the writing engine's own local segment |
| `vllm.ascendUseShortConnection` | `"1"` | Sets `ASCEND_USE_SHORT_CONNECTION`; keeps HIXL comms short-lived so CaMem sleep actually releases KV physical pages |
| `vllm.sleepOverlay.enabled` | `false` | Mounts the vllm-ascend sleep/wake overlay (`camem.py`, `mooncake_transfer_engine.py`) into engine pods. The overlay files ship separately; enabling without them fails the render |
| `vllm.sleepOverlay.configMapName` | `""` | `""` -> `<release>-te-unreg-sc` |
| `models[].prefillDecode.proxy.nodeSelector` | `{}` | Optional node selector for the P/D proxy pod |
| `models[].prefillDecode.proxy.retryDeadlineSeconds` | `60` | Whole-request retry deadline (seconds) for unplanned endpoint failures |
| `pdRebalancer.wakeRetries` | `3` | `/wake_up` attempts before the card is marked needs_recreate |
| `pdRebalancer.wakeBackoffSeconds` | `5` | Linear backoff between `/wake_up` attempts |
| `pdRebalancer.wakeAfterSleepSeconds` | `2` | Grace between a confirmed sleep and waking the peer |
| `pdRebalancer.kvWarmup` | `0` | `1` enables the cold-pair KV warm-up gate (only needed with long-lived transport connections) |

The explicit CUDA tag avoids the unqualified v0.5.15 image's CUDA 13 runtime,
which is incompatible with NVIDIA 570-series drivers. The complete `-cu129`
image preserves the SGLang v0.5.15 API and KV contract through CUDA
minor-version compatibility. Override `images.sglang` or a per-model `image`
when the cluster driver requires another official image.

For SGLang, `serviceImpl` selects only the router and sidecar images. The chart
keeps one shared deployment profile and emits the same engine-neutral
`INFERENCE_*` and `KV_EVENT_*` contract for both implementations. The Go
sidecar's SGLang readiness endpoint is `/ready`; liveness is `/health`.

SGLang pods default to `sglang.runtimeClassName: nvidia` so the NVIDIA container
runtime injects CUDA devices. Override only when the cluster provides GPU
visibility another way.

## Sidecar (`sidecar.*`)

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `true` | `false` drops the per-pod sidecar container. Used for BooM direct mode **and** sidecar-less push-*/central-push: the chart sources `ROUTER_SIDECAR_ENABLED` from this, so one switch flips those modes to direct-to-vLLM delivery + a router-hosted KV-events subscriber ([details](../architecture/router.md#sidecar-less-push--central-push-router_sidecar_enabledfalse)) |
| `logLevel` | `info` | `debug` \| `info` \| `warning` \| `error` |
| `prefetch` | `0` | Extra items buffered beyond `BATCH_SIZE` (pull cap = `BATCH_SIZE + PREFETCH`) |
| `forceIgnoreEos` | `false` | Force `ignore_eos=true` (replay mode: generate exactly `max_tokens`) |
| `streamingMode` | `false` | Stream from vLLM internally to capture TTFT |

## Router (`router.*`)

| Key | Default | Purpose |
|-----|---------|---------|
| `mode` | `pull` | `pull` \| `push-rr` \| `push-random` \| `push-leastq` \| `push-throughput` \| `push-p2c` \| `push-kv-cost` \| `push-least-kv` \| `push-least-latency` \| `push-least-busy` \| `central-push` (admit like pull, dispatch centrally by capacity) \| `external-push` (static external vLLM, no k8s/sidecar); [details](../architecture/router.md) and [compatibility matrix](../architecture/router.md#routing-compatibility-matrix). For sidecar-less push-*/central-push set `sidecar.enabled=false` |
| `apiKey` | `""` | Auth for `/v1/chat/completions` (empty = no auth) |
| `strategy` | `""` | Unified selector: `none` \| `prefix` \| `affinity` \| `both`. When set, overrides `kvAware`/`affinityEnabled` ([details](../architecture/key-affinity.md)) |
| `hashSource` | `inline` | KV-block hash source: `inline` (in-process/in-container `prefix_hash.py`) \| `external` (legacy `vllm-cpu-hash` pod, auto-deployed in this mode) ([details](../architecture/prefix-hash.md)) |
| `ownerSource` | `lookup` | KV block-owner source for `prefix`/`both`: `lookup` (targeted per-request Redis `HGETALL` at admit; truthful `kv_hit`) \| `watcher` (legacy background scan) ([details](../architecture/kv-cache-flow.md)) |
| `lookupMaxBlocks` | `512` | Max leading block hashes looked up per request when `ownerSource=lookup` |
| `kvAware` | `true` | KV-aware routing |
| `lenAware` | `true` | Length-aware batching |
| `lenPolicy` | `short_first` | `short_first` \| `long_first` |
| `affinityEnabled` | `false` | Conversation key-affinity (sticky chat → pod) ([details](../architecture/key-affinity.md)) |
| `affinityMode` | `soft` | `soft` (preference) \| `hard` (time-bounded pin) |
| `affinityTtlS` | `300` | Conversation → endpoint mapping lifetime (s) |
| `affinityHardTimeoutS` | `5` | Hard mode: max wait for the pinned pod before release (s) |
| `sloAware` | `false` | Enable slack-based SLO scheduling ([details](../architecture/slo-aware-routing.md)) |
| `sloWithKv` | `true` | Reward KV cache hits within SLO sort |
| `admissionThrottle` | `false` | Dynamic admission control |
| `fixedBatchSize` | `0` | Cap per-pull admit (0 = off) |
| `fairPull` | `false` | Pull-mode fairness: load-aware grant throttle; caps a pod's grant to fill up to `fairMargin × fleet-average` in-flight, trimming only movable/unpinned items (never overrides KV/affinity) ([details](../architecture/router.md)) |
| `fairMargin` | `1.25` | Overloaded threshold as a multiple of fleet-average in-flight (ceiling) |
| `fairFloor` | `1` | Min movable items an overloaded pod still gets (prevents a dead pod stalling the queue) |
| `stuckPullSeconds` | `0` | Liveness: flag a pod that hasn't pulled this long while the queue is backed up (0 = off; exposes `router_endpoint_stuck`) |
| `affinityReleaseOnStuck` | `false` | Let a stuck pod's affinity pins release to LB via the existing unavailable-target path |
| `centralPushCap` | `8` | Central-push only: per-pod concurrency ceiling; router dispatches `cap − in-flight` items per pod. Track sidecar `batchSize + prefetch` |
| `centralPushIntervalS` | `0.05` | Central-push only: periodic dispatch tick (also dispatched on every enqueue) |
| `kvCost.overlapCredit` | `1.0` | `push-kv-cost` only: credit for cached-prefix blocks in the cost function (`ROUTER_KV_OVERLAP_CREDIT`) |
| `kvCost.prefillLoadScale` | `1.0` | `push-kv-cost` only: weight on residual prefill work (`ROUTER_PREFILL_LOAD_SCALE`) |
| `kvCost.temperature` | `0.0` | `push-kv-cost` only: `0` = deterministic argmin; `>0` = softmax sample over costs (`ROUTER_TEMPERATURE`) |
| `vllmKvEventsPort` | `5557` | Sidecar-less push-*/central-push (`sidecar.enabled=false`, prefix/`both`): vLLM KV-events ZMQ port the router subscribes to per pod (`tcp://{pod_ip}:{port}`); must match the engine's `--kv-events-config` |
| `vllmKvEventsTopic` | `"kv@"` | Sidecar-less central-push only: ZMQ topic prefix the router subscribes to (vLLM publishes `kv@{POD_NAME}@{model}`; `kv@` matches all) |
| `outputLenPredictor` | `simple` | Output-length predictor |
| `batchSizeEstimate` / `fixedBatchEstimate` | `fixed` / `8` | Batch-size estimation for SLO |
| `latencyPredictor` | `linear` | Latency model |
| `latencyOnlineUpdate` | `false` | Online predictor updates |
| `latencyProfilePath` | `""` | Offline latency profile path |
| `queueWaitModel` | `none` | Queue-wait estimator |
| `chunkedPrefillAware` / `maxNumBatchedTokens` | `false` / `0` | Chunked-prefill-aware scheduling |

## Scheduling / pinning (`pin.*`, `vllm.avoidLabelValue`, `vllm.nodeSelector`)

| Key | Default | Purpose |
|-----|---------|---------|
| `pin.enabled` | `true` | Pin router, redis, hash, gateways to one node |
| `pin.nodeName` | `node3` | Hostname to pin infra pods to (cluster-specific) |
| `pin.tolerations` | control-plane/master | Allow scheduling on tainted control-plane nodes |
| `vllm.runtimeClassName` | `nvidia` | RuntimeClass applied to vLLM pods when `hardware=nvidia` (ignored on Ascend) |
| `vllm.avoidLabelValue` | `vllm` | Nodes labelled `avoid=<value>` are excluded from vLLM scheduling |
| `vllm.nodeSelector` | (template-only; no `values.yaml` default) | Optional positive node selector for vLLM pods |
| `vllm.leaderNodeSelector` | `{}` | Opt-in per-role selector for the DP LeaderWorkerSet **leader**; empty falls back to `vllm.nodeSelector` |
| `vllm.workerNodeSelector` | `{}` | Opt-in per-role selector for the DP **worker**; empty falls back to `vllm.nodeSelector` |

### Per-role node pinning (DP leader vs worker)

By default the DP leader and worker share `vllm.nodeSelector`. For the niche case
where the leader and worker must land on specific, distinct nodes (e.g. so the
NDS `file_p2p` host paths line up per role), set the two opt-in selectors. Both
default to `{}` and fall back to `vllm.nodeSelector`, so existing configs render
identically. The client-config keys are `vllm_leader_node_selector` /
`vllm_worker_node_selector` (top-level). Label the nodes first:

```bash
kubectl label node <leaderNode> vllm-role=leader --overwrite
kubectl label node <workerNode> vllm-role=worker --overwrite
```

Only the DP (LeaderWorkerSet) path honors these; the non-DP single Deployment
keeps using `vllm.nodeSelector`.

### Shadow deployments (running prod + shadow side by side)

To run a second ("shadow") vLLM stack on the same cluster without colliding with production, combine three knobs:

| Concern | Production stack | Shadow stack |
|---------|------------------|--------------|
| Exclude label | `vllm.avoidLabelValue: vllm` | `vllm.avoidLabelValue: vllm-shadow` |
| Positive target | (none) | `vllm.nodeSelector: { vllm-pool: shadow }` |
| NodePorts | `portOffset: 0` | `portOffset: 100` (avoids NodePort collisions; applies to redis/router/cpu-hash/vLLM only) |

Label the nodes so each stack lands on a disjoint set:

```bash
# Prod runs everywhere EXCEPT nodes labelled avoid=vllm
kubectl label node <prod-excluded-nodes> avoid=vllm --overwrite

# Keep shadow OFF the prod nodes, and mark the shadow target pool
kubectl label node <prod-nodes> avoid=vllm-shadow --overwrite
kubectl label node <shadow-nodes> vllm-pool=shadow --overwrite

# Inspect
kubectl get nodes -l avoid=vllm
kubectl get nodes -l avoid=vllm-shadow
kubectl get nodes -l vllm-pool=shadow
```

Net effect: prod pods avoid `avoid=vllm` nodes; shadow pods avoid `avoid=vllm-shadow` nodes and are pulled onto `vllm-pool=shadow` nodes by the nodeSelector. The shadow stack deploys into its own namespace (e.g. `vllm-shadow`) with its own PV/PVC.

## Model volume (`modelVolume.*`)

| Key | Default | Purpose |
|-----|---------|---------|
| `create` | `false` | Create PV/PVC (set `true` only on the one-time setup) |
| `nfsServer` | `nfs.local` | NFS server (cluster-specific) |
| `nfsPath` | `/home/models/` | Parent directory containing all models |
| `modelSubPath` | `""` | Per-model subfolder (set per deploy) |
| `pvcName` | `models-nfs-pvc` | PVC name |
| `hostPath` | `""` | Local path to bypass NFS PVC |

## Unified models (`models[]`)

When populated, the chart renders one Deployment (or LeaderWorkerSet) per entry plus a shared `model-registry` ConfigMap. When empty, a single-model fallback is synthesized from the top-level `tensorParallelSize`/`batchSize`/`vllm` values. The sweep runner always populates this list.

Per-model fields (camelCase):

| Field | Purpose |
|-------|---------|
| `name` | Unique identifier (used in K8s resource names) |
| `servedModelName` | vLLM `--served-model-name` (defaults to `name`) |
| `replicas` | Instance count (Deployment pods, or LWS groups in DP mode) |
| `modelSubPath` | Subfolder under the model volume |
| `tensorParallelSize` | Per-model TP |
| `batchSize` | Per-model `--max-num-seqs` |
| `image` | Optional per-model vLLM image override |
| `vllm` | Per-model runtime flags (overrides top-level `vllm`) |
| `dataParallel` | Optional LWS config (see below) |

See [multi-model](../deployment/multi-model.md) and [data parallel with LWS](../deployment/data-parallel-lws.md).

## vLLM runtime flags (`vllm.*`)

Common flags (all optional; `null` disables). Per-model `vllm:` overrides these.

| Key | Default | Purpose |
|-----|---------|---------|
| `kvCacheDtype` | `auto` | `auto` \| `fp8` \| `fp8_e4m3` (fp8 halves KV cache memory) |
| `gpuMemoryUtilization` | `null` | vLLM default if unset |
| `quantization` | `null` | e.g. `ascend` for W4A8 MoE |
| `enableExpertParallel` | `false` | Enable EP (MoE models) |
| `maxModelLen` | `null` | Lower it to save KV cache memory |
| `enablePrefixCaching` | `false` | Enable vLLM prefix caching |
| `compilationConfig.cudagraphMode` | `FULL_DECODE_ONLY` | `null` disables compilation config |
| `toolCallParser` / `reasoningParser` | `null` | Model-specific parsers |
| `dtype` | `auto` | `auto` \| `bfloat16` \| `float16` |

(See `values.yaml` for the full set including `cpuOffloadGb`, `additionalConfig`, `speculativeConfig`, `schedulerCls`, etc.)

## Data parallel (`dataParallel.*` and `models[].dataParallel`)

Legacy top-level fallback; prefer per-model `dataParallel`.

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `false` | Enable DP via LeaderWorkerSet |
| `size` | `2` | Pods per DP group (1 leader + N-1 workers) |
| `sizeLocal` | `1` | `--data-parallel-size-local` per pod |
| `rpcPort` | `13389` | DP RPC port |
| `groups` | `1` | Deprecated — LWS group count; prefer the model's `replicas` field |
| `nicName` | `""` | HCCL/GLOO/TP NIC (empty = auto-detect) |
| `hcclBuffSize` / `ompNumThreads` | `200` / `16` | HCCL buffer / OMP threads |
| `pairTopologyKey` | `""` | Node label key for RoCE-pair pinning (e.g. `roce-pair`) |

## Mooncake / LMCache (`mooncake.*`, `lmcache.*`)

Optional cross-node KV transfer. Gated on `mooncake.enabled` only. See [mooncake helm integration](../deployment/mooncake/helm-integration.md).

| Key | Default | Purpose |
|-----|---------|---------|
| `mooncake.enabled` | `false` | Deploy mooncake-master + `--kv-transfer-config` on vLLM |
| `mooncake.masterPort` | `50088` | Master port |
| `mooncake.masterServerAddress` | `10.50.156.106:50088` | Master address (override per cluster) |
| `lmcache.enabled` | `false` | Wrap Mooncake with LMCache connector (requires `mooncake.enabled`) |
| `lmcache.nds.enabled` | `false` | NVMe Direct Storage P2P DMA (needs `p2p_dev.ko`) |
| `lmcache.nds.xdsPath` | `/workspace/qyf/xds/xds/file_p2p` | Host path to the `file_p2p` xds build (mounted + on `PYTHONPATH`) |
| `lmcache.nds.xdsPathLeader` | `""` | Opt-in per-role override of `xdsPath` for the DP **leader**; empty falls back to `xdsPath` |
| `lmcache.nds.xdsPathWorker` | `""` | Opt-in per-role override of `xdsPath` for the DP **worker**; empty falls back to `xdsPath` |

The per-role `xdsPath*` knobs exist because some hosts stage the `file_p2p`
build at a different depth on the leader vs the worker (e.g. master at
`/workspace/qyf/xds/xds`, worker one level up at `/workspace/qyf/xds`). They are
opt-in via the client-config `helm:` keys `lmcache_nds_xds_path_leader` /
`lmcache_nds_xds_path_worker`; when empty both roles use the single `xdsPath`.
Only the DP path uses them; the non-DP single Deployment keeps `xdsPath`.

### LMCache backend mode: `mooncake` vs `p2p` (`lmcache.mode`)

`lmcache.mode` selects how LMCache shares KV across engines. It defaults to
`mooncake` — the historical **33/218 lineage** (LMCacheAscendConnectorV1Dynamic
+ Mooncake remote store + `P2PHANDSHAKE`). Any value other than `"p2p"` renders
the exact same config as before, so existing configs are unaffected.

`lmcache.mode: "p2p"` is the **142 lineage**: LMCacheAscendConnector (native) +
engine-to-engine HCCL P2P + **host-staging** + a standalone `lmcache_controller`,
with **no Mooncake**. The key win is `use_host_staging`: the producer registers
one bounded pinned arena (`os_staging_bytes`, default 8 GiB) instead of the full
CPU KV pool, which sidesteps the ~10 GB device-registration ceiling that
otherwise crashes the worker on first KV save.

| Key | Default | Purpose |
|-----|---------|---------|
| `lmcache.mode` | `mooncake` | `mooncake` (33/218) or `p2p` (142 host-staging) |
| `lmcache.p2p.tpSize` | `8` | Length of the per-TP port arrays |
| `lmcache.p2p.transferChannel` | `hccl` | Must be `hccl` for host-staging |
| `lmcache.p2p.useHostStaging` | `true` | Register one bounded arena, not the full pool |
| `lmcache.p2p.osStagingBytes` | `8589934592` | Arena size (8 GiB); keep below the ~10 GB reg. ceiling |
| `lmcache.p2p.npuBufferSize` | `134217728` | `p2p_npu_buffer_size` (128 MB ping-pong buffer) |
| `lmcache.p2p.initPortBase` / `lookupPortBase` / `workerPortBase` | `9950` / `9970` / `9940` | Per-TP port array bases |
| `lmcache.p2p.controllerPullUrl` / `controllerReplyUrl` | `""` | `host:port` of the `lmcache_controller` the engines dial |
| `lmcacheController.image` | `127.0.0.1:32000/lmcache-ascend:hccl-p2p` | Image for both p2p engines and the controller |
| `lmcacheController.port` / `pullPort` / `replyPort` | `9000` / `9800` / `9900` | Controller listen ports |
| `lmcacheController.nodeName` | `""` | Pin the controller to a fixed node so its IP is stable |
| `deploy.lmcacheController` | `true` | Deploy the controller when `lmcache.mode == "p2p"` |

Client-config `helm:` keys: `lmcache_mode`, `lmcache_use_host_staging`,
`lmcache_os_staging_bytes`, `lmcache_p2p_controller_pull_url` /
`lmcache_p2p_controller_reply_url`, `deploy_lmcache_controller`,
`lmcache_controller_image`. Per-pod `p2p_host` and `lmcache_instance_id` are
substituted at runtime from `NODE_IP` / `POD_NAME`, so a single ConfigMap serves
all pods. In `p2p` mode the sweep forces `deploy.mooncakeMaster=false`. See
[the P2P host-staging reference](../internal/lmcache-p2p-host-staging.md).

## Component toggles (`deploy.*`)

| Key | Default | Purpose |
|-----|---------|---------|
| `deploy.vllm` | `true` | Set `false` for stack-only deploys (sweep `--skip-vllm`) |
| `deploy.router` / `deploy.redis` | `true` | Set `false` for vLLM-only deploys (`deploy_vllm.py`) |
| `deploy.cpuHash` | `false` | No longer read by the chart; the legacy external hasher is auto-deployed when `router.hashSource=external` |
| `deploy.mooncakeMaster` | `true` | Skip mooncake-master if `false` |

## Autoscaling (`autoscaling.*`)

Per-model KEDA autoscaling that works across **all** topologies — single dense
model, multi-model, and data-parallel `LeaderWorkerSet`. When `enabled: false`
(the default) **no** autoscaling resources are rendered and the chart behaves
exactly as before, so existing releases are unaffected. See the full runbook in
[operations/autoscaling.md](../operations/autoscaling.md).

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `false` | Master switch. When false nothing is rendered (replicas stay static) |
| `signal` | `queue` | `queue` → per-model router central queue; `vllm` → vLLM KV-cache pressure; `sglang` → namespace/model-scoped fractional token usage |
| `prometheusServerAddress` | `http://kube-prometheus-stack-prometheus.monitoring.svc:9090` | Prometheus endpoint KEDA queries (matches the `monitoring` role) |
| `minReplicaCount` | `null` | Min replicas; `null` → each model's own replica count (DP: `dataParallel.groups`) |
| `maxReplicaCount` | `16` | Max replicas |
| `threshold` | `"16"` | Target central-queue depth per model (queue signal) |
| `vllmThreshold` | `"0.8"` | Target KV-cache utilisation 0..1 (vllm signal) |
| `sglangThreshold` | `"0.8"` | Target SGLang token usage 0..1 |
| `pollingInterval` | `10` | KEDA poll interval (seconds) |
| `cooldownPeriod` | `300` | KEDA scale-to-min cooldown (seconds) |
| `vllmQuery` / `sglangQuery` / `prometheusQuery` | `""` | Optional full-query overrides applied to every model |
| `perModel` | `{}` | Per-model overrides keyed by `models[].name` (`enabled`, `minReplicaCount`, `maxReplicaCount`, `threshold`, `signal`, `query`) |

One `ScaledObject` is rendered per autoscaled model, targeting `Deployment`
(dense/multi-model) or `LeaderWorkerSet` (data-parallel) automatically. The
`queue` signal uses the additive `router_central_queue_length_by_model{model="<servedModelName>"}`
metric, so the legacy global `router_central_queue_length` gauge is unchanged.
The default `sglang` query uses only `sglang:token_usage` (or its underscore
alias), filtered by Helm release namespace and served `model_name`; it never
combines fractional utilisation with request-count metrics.
KEDA must be installed first (`make keda` — see the runbook).

## Cache warm (`cacheWarm.*`)

Pre-install Job that reads model shards to warm the NFS page cache. `enabled: false` by default; set `modelSubPath` for large NFS-backed models.

## AIBrix (`aibrix.*`)

When `aibrix.enabled: true`, vLLM pods expose discovery labels for the AIBrix gateway (independent of `backend`). `enabled: false` by default.

## LiteLLM (`litellm.*`)

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `false` | Deploy LiteLLM proxy |
| `image` | `litellm:main-stable` | Proxy image |
| `masterKey` | `sk-litellm-master` | Admin key (override in production) |
| `nodePort` | `30400` | External access |
| `databaseUrl` | `""` | Optional Postgres for spend persistence |

## BooM Gateway (`boom.*`)

See [BooM overview](../gateways/boom/overview.md).

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `false` | Deploy BooM Gateway |
| `routeVia` | `router` | `router` (BooM -> router -> sidecar -> vLLM) or `direct` (BooM -> vLLM) |
| `directRoutingStrategy` | `round_robin` | `round_robin` \| `key_affinity` (direct mode only) |
| `maxInflight` | `0` | Cap concurrent upstream connections (0 = unlimited) |
| `upstreamTimeoutSeconds` | `7200` | Upstream HTTP timeout |
| `image` | `boom-gateway:v5` | Image ([build](../gateways/boom/build.md)) |
| `imagePullPolicy` | `Always` | Pull policy (so mutable tags pick up new pushes) |
| `masterKey` | `sk-boom-master` | Admin key (override in production) |
| `nodePort` | `30401` | External access |
| `databaseUrl` | `""` | Optional Postgres for spend persistence |
| `keyAffinityBench` | `false` | Deploy bench Postgres + 64 seeded keys |
| `claudeCodeAliases` | `false` | Add Claude model-name aliases ([claude-code](../gateways/boom/claude-code.md)) |
| `extraModelAliases` | `[]` | Extra `{alias, target}` entries (only when `claudeCodeAliases=true`) |

## Ports and services

NodePorts marked with `+offset` add `portOffset`; LiteLLM/BooM NodePorts are fixed (not offset).

| Service | ClusterIP | NodePort |
|---------|-----------|----------|
| Redis | `redis:6379` | 30079 +offset |
| Router HTTP | `router-service:8080` | 30080 +offset |
| Router ZMQ results | `router-service:5559` | 30559 +offset |
| Prefix-hash | `vllm-cpu-hash:9095` | 30095 +offset |
| vLLM (per model) | `vllm-<name>:8200` | 30034 +offset (legacy/DP; multi-model non-DP is ClusterIP unless `models[].nodePort` set) |
| Sidecar | `127.0.0.1:9000` (in-pod) | not exposed |
| LiteLLM | `litellm-proxy:4000` | 30400 (fixed) |
| BooM | `boom-proxy:4000` | 30401 (fixed) |
| Mooncake master | hostNetwork | 50088 |

## Routing method -> Helm mapping

| Sweep method | Helm value |
|--------------|------------|
| `pull` / `push-rr` / `push-random` / `push-leastq` / `push-throughput` / `push-p2c` / `push-kv-cost` / `push-least-kv` / `push-least-latency` / `push-least-busy` / `central-push` / `external-push` | `router.mode` |
| sidecar-less `push-*` / `central-push` | `router.mode=push-*` or `central-push` + `sidecar.enabled=false` |
| `round_robin` / `key_affinity` (BooM direct) | `boom.directRoutingStrategy` |
| AIBrix strategies | `aibrix.routing_strategy` (client-side, not Helm) |

## See also

- [Client config reference](client-config.md)
- [Experiment configs and sweeps](experiment-configs.md)
