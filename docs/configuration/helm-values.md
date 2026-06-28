# Helm values reference

Reference for the `vllm-kv-stack` Helm chart (`src/vllm-kv-stack/values.yaml`). Defaults shown are the chart defaults; cluster-specific values (registry, NFS server, node names) should be overridden for your environment.

## How values are set

There are three ways values reach the chart:

1. **Sweep runner** — `sweep_methods.py` translates the `helm:` section of a client config into `--set` flags plus a temporary `models[]` overlay. This is the primary path. See [experiment configs](experiment-configs.md).
2. **`deploy_vllm.py`** — deploys only vLLM (`deploy.vllm=true`, router/redis/cpuHash off) from the same client config.
3. **Direct `helm upgrade --install ... --set ...`** — for manual/one-off deploys.

## Top-level

| Key | Default | Purpose |
|-----|---------|---------|
| `backend` | `router` | Selects the infra stack flavor. Note: gateways are **not** turned on by this — LiteLLM/BooM render only when `litellm.enabled`/`boom.enabled` are set. `litellm`/`boom` render the same router+redis+cpuHash stack as `router`. |
| `serviceImpl` | `python` | Router/sidecar implementation: `python` or `go` (swaps images only) |
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
| `images.vllm` | `docker.io/library/vllm-ascend:v0.18.0` | vLLM engine image |
| `images.cpuHash` | `vllm-cpu-hash:latest` | Prefix-hash service |
| `images.redis` | `redis:7-alpine` | Redis |
| `images.mooncakeMaster` | `docker.io/library/vllm-ascend:v0.18.0` | Mooncake master |

A fully-qualified per-model `image` bypasses the registry rewrite.

## Sidecar (`sidecar.*`)

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `true` | Set `false` for BooM direct mode (no router/sidecar) |
| `logLevel` | `info` | `debug` \| `info` \| `warning` \| `error` |
| `prefetch` | `0` | Extra items buffered beyond `BATCH_SIZE` (pull cap = `BATCH_SIZE + PREFETCH`) |
| `forceIgnoreEos` | `false` | Force `ignore_eos=true` (replay mode: generate exactly `max_tokens`) |
| `streamingMode` | `false` | Stream from vLLM internally to capture TTFT |

## Router (`router.*`)

| Key | Default | Purpose |
|-----|---------|---------|
| `mode` | `pull` | `pull` \| `push-rr` \| `push-random` \| `push-leastq` |
| `apiKey` | `""` | Auth for `/v1/chat/completions` (empty = no auth) |
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
| `vllm.avoidLabelValue` | `vllm` | Nodes labelled `avoid=<value>` are excluded from vLLM scheduling |
| `vllm.nodeSelector` | (template-only; no `values.yaml` default) | Optional positive node selector for vLLM pods |

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

## Component toggles (`deploy.*`)

| Key | Default | Purpose |
|-----|---------|---------|
| `deploy.vllm` | `true` | Set `false` for stack-only deploys (sweep `--skip-vllm`) |
| `deploy.router` / `deploy.redis` / `deploy.cpuHash` | `true` | Set `false` for vLLM-only deploys (`deploy_vllm.py`) |
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
| `signal` | `queue` | `queue` → per-model router central queue; `vllm` → vLLM KV-cache pressure (router-less topologies) |
| `prometheusServerAddress` | `http://kube-prometheus-stack-prometheus.monitoring.svc:9090` | Prometheus endpoint KEDA queries (matches the `monitoring` role) |
| `minReplicaCount` | `null` | Min replicas; `null` → each model's own replica count (DP: `dataParallel.groups`) |
| `maxReplicaCount` | `16` | Max replicas |
| `threshold` | `"16"` | Target central-queue depth per model (queue signal) |
| `vllmThreshold` | `"0.8"` | Target KV-cache utilisation 0..1 (vllm signal) |
| `pollingInterval` | `10` | KEDA poll interval (seconds) |
| `cooldownPeriod` | `300` | KEDA scale-to-min cooldown (seconds) |
| `vllmQuery` / `prometheusQuery` | `""` | Optional full-query overrides applied to every model |
| `perModel` | `{}` | Per-model overrides keyed by `models[].name` (`enabled`, `minReplicaCount`, `maxReplicaCount`, `threshold`, `signal`, `query`) |

One `ScaledObject` is rendered per autoscaled model, targeting `Deployment`
(dense/multi-model) or `LeaderWorkerSet` (data-parallel) automatically. The
`queue` signal uses the additive `router_central_queue_length_by_model{model="<servedModelName>"}`
metric, so the legacy global `router_central_queue_length` gauge is unchanged.
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
| `image` | `boom-gateway:v4` | Image ([build](../gateways/boom/build.md)) |
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
| `pull` / `push-rr` / `push-random` / `push-leastq` | `router.mode` |
| `round_robin` / `key_affinity` (BooM direct) | `boom.directRoutingStrategy` |
| AIBrix strategies | `aibrix.routing_strategy` (client-side, not Helm) |

## See also

- [Client config reference](client-config.md)
- [Experiment configs and sweeps](experiment-configs.md)
