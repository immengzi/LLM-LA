# GLM-5 Data Parallel Docker Deployment (DP=2, TP=8, EP)

Two-node deployment of GLM-5-w4a8-mtp-QuaRot using vLLM's internal
load balancing with data parallelism + expert parallelism.

Expert layers form a DP×TP = 2×8 = 16-way expert parallel group across
both nodes. This halves the per-NPU expert weight memory, freeing
significant GPU memory for KV cache and enabling 200K+ context.

Image: `reg.local:32000/ascend/vllm-ascend:glm5-openeuler` (same as Kubernetes pods, pulled from local registry)

## Reference deployment

> **This is the reference ("HQ") vLLM deployment.** This bare-Docker GLM-5 setup (DP=2, TP=8, expert-parallel) on node3 + node4 is the canonical, ground-truth configuration of the LA-Boom serving backend. The Helm/Kubernetes path ([multi-node DP via LeaderWorkerSet](../data-parallel-lws.md)) and the [GLM-5 + Mooncake production runbook](../mooncake/glm5-production.md) are modeled to reproduce this exact set of vLLM flags, environment variables, and ports.
>
> When any GLM-5 deployment doc disagrees, the values documented **here** are authoritative.

## Cluster layout

| Role | Hostname | Internal IP | NPUs | vLLM role |
|------|----------|-------------|------|-----------|
| Head (DP rank 0) | node3 | 10.50.156.65 | 8 × Ascend | API server + engine |
| Worker (DP rank 1) | node4 | 10.50.156.106 | 8 × Ascend | Headless engine only |

Model path on both nodes: `/home/models/GLM-5-w4a8-mtp-QuaRot`

All HTTP requests go to **node3:8077** only. node4 has no API server;
it participates only in expert-layer synchronization during forward passes.

## Prerequisites

### 1. Stop Kubernetes vLLM pods

Make sure no Kubernetes vLLM pods are running on node3/node4 (they
would compete for NPU devices):

```bash
# Standard (non-DP) deployment:
kubectl -n vllm delete deploy vllm-qwen
kubectl -n vllm scale deploy vllm-qwen --replicas=0   # or scale to 0

# Data-parallel deployment uses a LeaderWorkerSet, not a Deployment:
kubectl -n vllm delete lws vllm-qwen
```

### 2. Load the Docker image from containerd

The image already exists in containerd (pulled by Kubernetes) but
Docker has a separate image store. Export from containerd and import
into Docker on **both** nodes:

```bash
# Run on BOTH node3 and node4:
ctr -n k8s.io images export /tmp/vllm-ascend.tar \
  reg.local:32000/ascend/vllm-ascend:glm5-openeuler

docker load < /tmp/vllm-ascend.tar

# Tag it with the original quay.io name for convenience:
docker tag reg.local:32000/ascend/vllm-ascend:glm5-openeuler \
  quay.io/ascend/vllm-ascend:glm5-openeuler

rm /tmp/vllm-ascend.tar

# Verify:
docker images | grep vllm-ascend
```

### 3. Verify NIC name

```bash
ip -4 addr show | grep -B2 "10.50.156"
```

The scripts below auto-detect the NIC from the node IP. If
auto-detection fails, find the correct NIC manually and hardcode it.

---

## Node 3 — Head (DP rank 0)

Run on **node3** (10.50.156.65). Start this first.

```bash
docker rm -f glm5-dp-head glm5-test 2>/dev/null

docker run -d --name glm5-dp-head \
  --network host \
  --ipc host \
  --privileged \
  -v /usr/local/dcmi:/usr/local/dcmi:ro \
  -v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi:ro \
  -v /usr/local/Ascend/driver/lib64/:/usr/local/Ascend/driver/lib64/:ro \
  -v /usr/local/Ascend/driver/version.info:/usr/local/Ascend/driver/version.info:ro \
  -v /etc/ascend_install.info:/etc/ascend_install.info:ro \
  -v /etc/hccn.conf:/etc/hccn.conf:ro \
  -v /home/models:/models:ro \
  -v /root/.cache:/root/.cache \
  -e HCCL_OP_EXPANSION_MODE=AIV \
  -e HCCL_IF_IP=10.50.156.65 \
  -e HCCL_BUFFSIZE=200 \
  -e OMP_NUM_THREADS=16 \
  -e VLLM_USE_V1=1 \
  -e PYTORCH_NPU_ALLOC_CONF=expandable_segments:True \
  -e ASCEND_BUFFER_POOL=4:8 \
  -e PYTHONHASHSEED=0 \
  quay.io/ascend/vllm-ascend:glm5-openeuler \
  bash -c '
    source /usr/local/Ascend/ascend-toolkit/set_env.sh
    export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

    export GLOO_SOCKET_IFNAME=enp189s0f0
    export TP_SOCKET_IFNAME=enp189s0f0
    export HCCL_SOCKET_IFNAME=enp189s0f0

    vllm serve /models/GLM-5-w4a8-mtp-QuaRot \
      --served-model-name served-model \
      --host 0.0.0.0 \
      --port 8077 \
      --tensor-parallel-size 8 \
      --data-parallel-size 2 \
      --data-parallel-size-local 1 \
      --data-parallel-address 10.50.156.65 \
      --data-parallel-rpc-port 13389 \
      --enable-expert-parallel \
      --quantization ascend \
      --seed 1924 \
      --max-num-seqs 32 \
      --max-num-batched-tokens 4096 \
      --trust-remote-code \
      --no-enable-prefix-caching \
      --gpu-memory-utilization 0.95 \
      --dtype auto \
      --compilation-config '"'"'{"cudagraph_mode": "FULL_DECODE_ONLY"}'"'"' \
      --additional-config '"'"'{"multistream_overlap_shared_expert":true}'"'"' \
      --tool-call-parser glm47 \
      --reasoning-parser glm45 \
      --enable-auto-tool-choice
  '

# Save full logs to file (Ctrl+C to stop following):
docker logs -f glm5-dp-head 2>&1 | tee /tmp/glm5-head.log
```

## Node 4 — Headless Worker (DP rank 1)

Run on **node4** (10.50.156.106). Start this after node3 is running.

```bash
docker rm -f glm5-dp-worker glm5-test 2>/dev/null

docker run -d --name glm5-dp-worker \
  --network host \
  --ipc host \
  --privileged \
  -v /usr/local/dcmi:/usr/local/dcmi:ro \
  -v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi:ro \
  -v /usr/local/Ascend/driver/lib64/:/usr/local/Ascend/driver/lib64/:ro \
  -v /usr/local/Ascend/driver/version.info:/usr/local/Ascend/driver/version.info:ro \
  -v /etc/ascend_install.info:/etc/ascend_install.info:ro \
  -v /etc/hccn.conf:/etc/hccn.conf:ro \
  -v /home/models:/models:ro \
  -v /root/.cache:/root/.cache \
  -e HCCL_OP_EXPANSION_MODE=AIV \
  -e HCCL_IF_IP=10.50.156.106 \
  -e HCCL_BUFFSIZE=200 \
  -e OMP_NUM_THREADS=16 \
  -e VLLM_USE_V1=1 \
  -e PYTORCH_NPU_ALLOC_CONF=expandable_segments:True \
  -e ASCEND_BUFFER_POOL=4:8 \
  -e PYTHONHASHSEED=0 \
  quay.io/ascend/vllm-ascend:glm5-openeuler \
  bash -c '
    source /usr/local/Ascend/ascend-toolkit/set_env.sh
    export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

    export GLOO_SOCKET_IFNAME=enp189s0f0
    export TP_SOCKET_IFNAME=enp189s0f0
    export HCCL_SOCKET_IFNAME=enp189s0f0

    vllm serve /models/GLM-5-w4a8-mtp-QuaRot \
      --headless \
      --served-model-name served-model \
      --host 0.0.0.0 \
      --port 8077 \
      --tensor-parallel-size 8 \
      --data-parallel-size 2 \
      --data-parallel-size-local 1 \
      --data-parallel-start-rank 1 \
      --data-parallel-address 10.50.156.65 \
      --data-parallel-rpc-port 13389 \
      --enable-expert-parallel \
      --quantization ascend \
      --seed 1924 \
      --max-num-seqs 32 \
      --max-num-batched-tokens 4096 \
      --trust-remote-code \
      --no-enable-prefix-caching \
      --gpu-memory-utilization 0.95 \
      --dtype auto \
      --compilation-config '"'"'{"cudagraph_mode": "FULL_DECODE_ONLY"}'"'"' \
      --additional-config '"'"'{"multistream_overlap_shared_expert":true}'"'"' \
      --tool-call-parser glm47 \
      --reasoning-parser glm45 \
      --enable-auto-tool-choice
  '

# Save full logs to file (Ctrl+C to stop following):
docker logs -f glm5-dp-worker 2>&1 | tee /tmp/glm5-worker.log
```

To dump logs after the fact (if you didn't use `tee`):

```bash
# On node3:
docker logs glm5-dp-head > /tmp/glm5-head.log 2>&1
# On node4:
docker logs glm5-dp-worker > /tmp/glm5-worker.log 2>&1
```

---

## Differences between head and worker

| Flag | Node 3 (head) | Node 4 (worker) |
|------|---------------|-----------------|
| `--headless` | absent | **present** |
| `--data-parallel-start-rank` | absent (defaults to 0) | **1** |
| `HCCL_IF_IP` | 10.50.156.65 | 10.50.156.106 |
| HTTP API server | **yes** (port 8077) | no |

All other flags, volumes, devices, and env vars are identical.

## Startup order

1. Start **node3 (head)** first. It will wait for workers to connect.
2. Start **node4 (worker)** second. Once connected, the head begins serving.

Both must be running before the model becomes ready.

## Monitoring

```bash
# Follow head logs (node3)
docker logs -f glm5-dp-head

# Follow worker logs (node4)
docker logs -f glm5-dp-worker
```

Look for `estimated maximum model length` in the head logs — this
confirms the actual context window achieved with DP+EP.

## Verification

After both nodes are running and model is loaded, test from any machine
that can reach node3:

```bash
curl -s http://10.50.156.65:8077/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "served-model",
    "messages": [{"role": "user", "content": "Hello"}],
    "max_tokens": 32,
    "stream": false
  }' | python3 -m json.tool
```

## Cleanup

```bash
# On node3
docker stop glm5-dp-head && docker rm glm5-dp-head

# On node4
docker stop glm5-dp-worker && docker rm glm5-dp-worker
```

## Expected context window

With DP=2 + EP, expert weights are distributed across 16 NPUs instead
of 8, roughly halving per-NPU expert memory. This frees significant
GPU memory for KV cache, potentially enabling longer context than the
single-node ~80K limit.

Note: `--kv-cache-dtype fp8` is **not supported** on this Ascend NPU
firmware (the `TransposeKvCacheByBlock` kernel only supports FP16,
BF16, and INT8). KV cache uses the model's native dtype (`auto`).

Check the vLLM startup log on node3 for the actual
`estimated maximum model length` to confirm.

## Optional: add Mooncake KV transfer

To also enable Mooncake cross-node KV cache transfer (matching the
full reference deployment), add these flags to the vllm command on
**both** nodes:

```bash
  --kv-transfer-config \
  '{"kv_connector":"AscendStoreConnector","kv_role":"kv_both","kv_connector_extra_config":{"lookup_rpc_port":"1","backend":"mooncake"}}'
```

And add `-e MOONCAKE_CONFIG_PATH=/etc/mooncake/mooncake.json` plus a
volume mount for the mooncake.json config file.

## Optional: add speculative decoding

After confirming the base model loads and the context window is
sufficient, add to the vllm command on **both** nodes:

```bash
  --speculative-config '{"num_speculative_tokens": 3, "method": "deepseek_mtp"}'
```

This uses GLM-5's built-in MTP heads (no separate drafter model).
