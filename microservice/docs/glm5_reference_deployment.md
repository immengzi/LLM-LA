# GLM-5 Reference Deployment

## Reference environment

A working GLM-5-w4a8-mtp-QuaRot deployment was observed on server
`devserver-bms-29c15fcf-1` running as a Docker container with image
`quay.io/ascend/vllm ascend:glm5 openeuler`.

The container was inspected (read-only `docker top` / `docker inspect`)
on 2026-04-16 and the full vLLM command line was extracted.

---

## Reference vLLM command line

```bash
vllm serve /workspace/models/GLM-5-w4a8-mtp-QuaRot \
  --served-model-name glm-5 \
  --host 0.0.0.0 \
  --port 8077 \
  --data-parallel-size 2 \
  --data-parallel-address 7.216.57.95 \
  --data-parallel-rpc-port 13389 \
  --tensor-parallel-size 8 \
  --quantization ascend \
  --seed 1924 \
  --enable-expert-parallel \
  --max-num-seqs 32 \
  --max-num-batched-tokens 4096 \
  --trust-remote-code \
  --gpu-memory-utilization 0.95 \
  --compilation-config '{"cudagraph_mode": "FULL_DECODE_ONLY"}' \
  --speculative-config '{"num_speculative_tokens": 3, "method": "deepseek_mtp"}' \
  --tool-call-parser glm5 \
  --reasoning-parser glm5 \
  --enable-auto-tool-choice \
  --kv-transfer-config '{"kv_role": "kv_both", "kv_connector_extra_config": {"lookup_rpc_port": "3", "backend": "mooncake"}}' \
  --api-key ZhongRuanChuangXin! \
  --additional-config '{"multistream_overlap_shared_expert": true}'
```

### Process tree (from `docker top`)

The container runs 16 vLLM worker processes (DP0_TP0..DP0_TP7 + DP1 workers),
plus an IPCoordinator, EngineCore, and two APIServer processes. This confirms
data-parallel-size=2 with tensor-parallel-size=8 per DP rank (16 GPUs total).

---

## Arg-by-arg mapping to our Helm deployment

### Fully supported — aligned in our configs

| Reference arg | Value | Our config key | Our value | Notes |
|---|---|---|---|---|
| `--tensor-parallel-size` | 8 | `helm.tensor_parallel_size` | 8 | Identical |
| `--quantization` | ascend | `helm.vllm_quantization` | `"ascend"` | Identical |
| `--enable-expert-parallel` | (flag) | `helm.vllm_enable_expert_parallel` | `true` | Identical |
| `--max-num-seqs` | 32 | `helm.batch_size` | 32 | Maps to `batchSize` in Helm |
| `--max-num-batched-tokens` | 4096 | `helm.vllm_max_num_batched_tokens` | 4096 | Identical |
| `--gpu-memory-utilization` | 0.95 | `helm.vllm_gpu_memory_utilization` | 0.95 | Identical |
| `--trust-remote-code` | (flag) | `helm.vllm_trust_remote_code` | `true` | **Was `false`, fixed** |
| `--seed` | 1924 | `helm.vllm_seed` | 1924 | **Was `null`, fixed** |
| `--compilation-config` | FULL_DECODE_ONLY | `helm.vllm_compilation_config` | `FULL_DECODE_ONLY` | Identical |
| `--speculative-config` | 3 tokens, deepseek_mtp | `helm.vllm_speculative_config` | `numSpeculativeTokens: 3, method: deepseek_mtp` | **Was `null`, fixed** |
| `--api-key` | ZhongRuanChuangXin! | `helm.router_api_key` | `ZhongRuanChuangXin!` | Applied at router level, not vLLM level |
| `--max-model-len` | **NOT SET** (auto) | `helm.vllm_max_model_len` | `null` | **Was `80000`, fixed to auto** |

### Not yet supported in Helm template — reference-only features

| Reference arg | Value | Status | Impact |
|---|---|---|---|
| `--data-parallel-size` | 2 | Not in Helm template | Reference uses DP=2 × TP=8. Our deployment uses replicas=2 with TP=8 (Kubernetes-level replication instead of vLLM-native DP). Functionally similar for serving but different scaling model. |
| `--data-parallel-address` | 7.216.57.95 | Not in Helm template | Tied to DP; not needed with K8s replicas |
| `--data-parallel-rpc-port` | 13389 | Not in Helm template | Tied to DP; not needed with K8s replicas |
| `--tool-call-parser` | glm5 | `helm.vllm_tool_call_parser` | `"glm5"` | Enables structured tool/function calling for GLM-5 |
| `--reasoning-parser` | glm5 | `helm.vllm_reasoning_parser` | `"glm5"` | Enables chain-of-thought reasoning extraction |
| `--enable-auto-tool-choice` | (flag) | Auto-added when `toolCallParser` is set | Auto tool selection. Paired with `--tool-call-parser`. |
| `--kv-transfer-config` | mooncake backend | In Helm template (mooncake section) | Supported when `mooncake.enabled: true`. Disabled by default. |
| `--additional-config` | multistream_overlap | In Helm template | Supported via `vllm_additional_config`. Set to `null` in our config (reference uses `true`). |
| `--served-model-name` | glm-5 | Hardcoded to `served-model` in template | Our deployment uses `served-model` universally; BooM/LiteLLM alias Claude model names to it. |

### Key differences explained

**Data parallelism (DP=2) vs Kubernetes replicas:**
The reference deployment uses vLLM's built-in `--data-parallel-size 2` to run
2 DP ranks within a single container, sharing memory and coordinating via RPC.
Our Kubernetes deployment uses `replicas: 2` in the Helm chart, which creates
2 independent pods each running TP=8. Both approaches serve the same purpose
(2× throughput) but:
- DP mode: shared process, lower overhead, single-node only
- K8s replicas: independent pods, can span nodes, standard K8s scaling

**multistream_overlap_shared_expert:**
The reference enables this (`--additional-config '{"multistream_overlap_shared_expert": true}'`).
This optimizes MoE expert computation by overlapping shared expert with routed
experts. To enable in our deployment, set:
```yaml
helm:
  vllm_additional_config: '{"multistreamOverlapSharedExpert": true}'
```

**Tool calling / reasoning parsers:**
The reference has `--tool-call-parser glm5 --reasoning-parser glm5 --enable-auto-tool-choice`.
These enable structured function calling and reasoning extraction for agentic
workloads. For Claude Code usage through BooM Gateway, these are not needed
since BooM handles the Anthropic-to-OpenAI conversion and Claude Code doesn't
use vLLM's native tool calling.

---

## Changes made to align with reference

The following files were modified to match the reference deployment:

### 1. `configs/boom-claude-glm.yaml` and `configs/router-tp8-glm.yaml`

| Field | Before | After | Why |
|---|---|---|---|
| `batch_size` | 8 | **32** | Reference uses `--max-num-seqs 32` |
| `vllm_trust_remote_code` | `false` | **`true`** | GLM-5 requires custom model code |
| `vllm_max_model_len` | `80000` | **`null`** (auto) | Reference doesn't set it; let vLLM auto-calculate |
| `vllm_max_num_batched_tokens` | `null` | **`4096`** | Reference limits per-step token budget |
| `vllm_seed` | `null` | **`1924`** | Matches reference |
| `vllm_speculative_config` | `null` | **MTP 3 tokens** | Enables speculative decoding (deepseek_mtp) |

### 2. `sweep_methods.py`

Fixed `vllm_speculative_config` and `vllm_additional_config` to be passed as
structured Helm values (sub-keys) instead of opaque strings. Without this fix,
`--set vllm.speculativeConfig='{...}'` would be treated as a string by Helm,
not a nested object, and the template `{{ .Values.vllm.speculativeConfig.numSpeculativeTokens }}`
would fail.

---

## Startup probe fix (pod restart during model loading)

### Problem

GLM-5-w4a8-mtp-QuaRot has 96 safetensor shards loaded from NFS storage.
On cold start (no OS page cache), NFS read throughput becomes the bottleneck:

- **Cold cache:** shards 0-10 load at ~1s each, then from shard 11 onward
  loading slows to 15-50 min/shard as NFS bandwidth saturates.
- **Warm cache:** shards 0-88 load quickly (~2s each from page cache),
  then from shard 89 onward (cache miss) loading drops to 15-20 min/shard.

Total cold-start loading time can reach **2-3.5+ hours**, plus additional
time for compilation and warmup after loading completes.

### Symptom

vLLM pods show restarts with exit code 143 (SIGTERM). No application error
in logs — kubelet kills the container because the startup probe window expires.

### Root cause

The original `startupProbe` was configured as:

```yaml
initialDelaySeconds: 60
periodSeconds: 10
failureThreshold: 1080     # 60s + 1080×10s = ~3h 1min window
timeoutSeconds: 2
```

Cold-start model loading + initialization exceeds this ~3 hour window.

### Fix applied

```yaml
startupProbe:
  initialDelaySeconds: 60
  periodSeconds: 30          # was 10 — reduce probe pressure during loading
  failureThreshold: 720      # 60s + 720×30s = ~6 hours window
  timeoutSeconds: 5          # was 2 — NFS-loaded model may be slow to respond

livenessProbe:
  initialDelaySeconds: 120
  periodSeconds: 30          # was 20
  failureThreshold: 720      # was 540 — match startup tolerance
  timeoutSeconds: 5          # was 2
```

This gives a **~6 hour startup window** — sufficient for worst-case cold NFS loading.

### Observed behavior: stall at 95% (shard 91/96)

When a previous run was killed by the startup probe after loading ~90 shards,
the OS page cache on the NFS head node retains only those shards. On the next
deployment:

- Shards 0-90: page cache hit → ~2s each (fast)
- Shards 91-96: page cache miss → 15-50 min each from NFS disk (appears stalled)

Both pods stall at the same shard because they share the same page cache
boundary. Both competing for NFS disk bandwidth on the same cold shards
makes it worse. **Do not restart** — let them finish. Once loaded, the next
restart will be fast (~11 min from fully warm cache).

### Diagnosing the stall

```bash
# On the NFS head node: confirm disk I/O is happening (not idle)
iostat -x 5 3

# On the NFS head node: watch NFS server thread activity
nfsstat -s

# From a cluster node: check if vLLM process is blocked on NFS read
kubectl -n vllm exec $(kubectl -n vllm get pod -l app=vllm-qwen \
  -o jsonpath='{.items[0].metadata.name}') -c vllm -- cat /proc/1/wchan
# Expected: "nfs_readpage" or "rpc_wait_bit_killable" = blocked on NFS
```

### Longer-term mitigations

**1. Pre-warm NFS page cache before deploying (simplest):**

```bash
# Run once on any node that mounts the NFS, before deploying vLLM
cat /mnt/nfs/saeid/models/GLM-5-w4a8-mtp-QuaRot/*.safetensors > /dev/null
```

**2. Stagger pod startup** to avoid both pods competing for the same cold shards.
Use a `StatefulSet` with `podManagementPolicy: OrderedReady` or configure
`maxUnavailable: 1` in the deployment strategy.

**3. Copy to local NVMe first** with an initContainer (highest throughput,
but uses local disk space):

```yaml
initContainers:
  - name: model-prefetch
    image: busybox
    command: ["cp", "-r", "/nfs-model/", "/local-model/"]
    volumeMounts:
      - name: nfs-models
        mountPath: /nfs-model
        subPath: GLM-5-w4a8-mtp-QuaRot
      - name: local-scratch
        mountPath: /local-model
volumes:
  - name: local-scratch
    emptyDir: {}
```

**4. Helm-integrated cache warm (recommended):**

A Helm pre-install/pre-upgrade hook Job is included in the chart
(`99-cache-warm-job.yaml`). Enable it in the config:

```yaml
helm:
  cache_warm_enabled: true
  cache_warm_pvc_name: "models-nfs-pvc"
```

This runs automatically before vLLM pods start. The Job mounts the same
NFS PVC, reads all safetensor shards sequentially (warming the NFS server's
page cache), then exits. vLLM pods then load from cached memory.

Enabled by default in `boom-claude-glm.yaml` and `router-tp8-glm.yaml`.

To run manually (without Helm hook):

```bash
kubectl -n vllm apply -f - <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: model-cache-warm
  namespace: vllm
spec:
  ttlSecondsAfterFinished: 120
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: warm
          image: busybox
          command:
            - sh
            - -c
            - |
              echo "Warming NFS cache for GLM-5..."
              total=$(ls /model/GLM-5-w4a8-mtp-QuaRot/*.safetensors | wc -l)
              i=0
              for f in /model/GLM-5-w4a8-mtp-QuaRot/*.safetensors; do
                i=$((i+1))
                echo "[$i/$total] $(basename $f)"
                cat "$f" > /dev/null
              done
              echo "Done."
          volumeMounts:
            - name: model
              mountPath: /model
              readOnly: true
      volumes:
        - name: model
          persistentVolumeClaim:
            claimName: models-nfs-pvc
EOF
```

Monitor: `kubectl -n vllm logs -f job/model-cache-warm`

**5. Switch to faster storage** (local NVMe, JuiceFS, Alluxio) for model weights.

---

## Our deployment

### Architecture

```
Claude Code (Anthropic /v1/messages)
  → BooM Gateway (:30401) — Anthropic ↔ OpenAI conversion
    → Router Service (:30079) — /v1/chat/completions with SSE streaming
      → Sidecar → vLLM pods (GLM-5-w4a8-mtp-QuaRot, TP=8, 2 replicas)
```

### Deploy

```bash
# Full deploy (vLLM + router + BooM Gateway)
python sweep_methods.py --config boom-claude-glm

# Skip vLLM if pods are already running
python sweep_methods.py --config boom-claude-glm --skip-vllm
```

### Claude Code settings (`~/.claude/settings.json`)

```json
{
  "permissions": {
    "allow": [],
    "deny": []
  },
  "env": {
    "ANTHROPIC_AUTH_TOKEN": "sk-boom-master",
    "ANTHROPIC_BASE_URL": "http://7.216.57.215:30401",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "served-model",
    "ANTHROPIC_DEFAULT_OPUS_MODEL": "served-model",
    "ANTHROPIC_DEFAULT_SONNET_MODEL": "served-model",
    "ANTHROPIC_MODEL": "served-model",
    "ANTHROPIC_REASONING_MODEL": "served-model",
    "ANTHROPIC_SMALL_FAST_MODEL": "served-model"
  }
}
```

### Sharing the model with external parties

External BooM Gateway operators can connect to our router directly:

| Field | Value |
|---|---|
| Base URL | `http://7.216.57.215:30079/v1` |
| Model | `served-model` |
| API Key | `ZhongRuanChuangXin!` |
| Streaming | Supported (SSE) |
| Protocol | OpenAI `/v1/chat/completions` |

Their BooM config entry:

```yaml
model_list:
  - model_name: served-model
    litellm_params:
      model: openai/served-model
      api_base: http://7.216.57.215:30079/v1
      api_key: "ZhongRuanChuangXin!"
```

### Verification commands

```bash
# Check router has SSE streaming + API key auth
kubectl -n vllm exec -it $(kubectl -n vllm get pod -l app=router-service \
  -o jsonpath='{.items[0].metadata.name}') -- \
  grep -n "_normalise_content\|_build_sse_chunks\|_check_api_key" /app/router/api.py

# Test streaming through BooM (full Claude Code path)
curl -v -N --noproxy '*' http://7.216.57.215:30401/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: sk-boom-master" \
  -H "anthropic-version: 2023-06-01" \
  -d '{"model":"served-model","messages":[{"role":"user","content":"say hi"}],"max_tokens":64,"stream":true}'

# Test API key auth on router directly
curl -s --noproxy '*' http://7.216.57.215:30079/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ZhongRuanChuangXin!" \
  -d '{"model":"served-model","messages":[{"role":"user","content":"hello"}]}'
```
