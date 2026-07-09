# Mooncake + GLM5 Multi-Instance Deployment Guide

This document describes how to deploy GLM5 with Mooncake KV transfer across
multiple GLM5 instances in the LLM-LA Kubernetes stack.

> The base vLLM flags/environment for GLM-5 come from the [bare-Docker GLM-5 reference deployment](../docker-reference/glm5-dp-docker.md) (the authoritative "HQ" config); this runbook layers Mooncake multi-instance production concerns on top.

## Important Note on Image

Mooncake components are already available in the GLM5 runtime image:

- `quay.io/ascend/vllm-ascend:glm5-openeuler`

You do not need to install Mooncake separately inside the vLLM pods when using
this image.

## Target Topology

For production-scale deployment:

- 4 GLM5 instances (`replicas: 4`)
- each instance uses `DP=2`, `TP=8`, `enableExpertParallel=true`
- each instance runs as one LeaderWorkerSet group (leader + worker)
- one shared `mooncake-master` coordinates metadata for all instances
- Boom Gateway -> router-service -> sidecar -> vLLM (with Mooncake KV transfer)

## Required Files

Make sure these files exist and are up to date in the target environment:

- `configs/5-1-template-boom-direct-claude-glm-mooncake.yaml`
- `configs/5-2-template-boom-claude-glm-mooncake.yaml`
- `configs/1-master_config.yaml`
- `vllm-kv-stack/templates/40-vllm-unified.yaml`
- `vllm-kv-stack/templates/11-mooncake-config.yaml`
- `vllm-kv-stack/templates/12-mooncake-master.yaml`
- `vllm-kv-stack/values.yaml`

## Configuration Requirements

### 1) Master config keys must use full `.prod.yaml` suffix

In `configs/1-master_config.yaml`, use:

```yaml
5-1-template-boom-direct-claude-glm-mooncake.yaml:
  - round_robin

5-2-template-boom-claude-glm-mooncake.yaml:
  - pull
```

Reason: `sweep_methods.py` only auto-appends `.yaml` when the key has no
suffix. Using `.prod` (without `.yaml`) will resolve to a missing file.

### 2) Mooncake must be enabled in client config

In each `*.prod.yaml` file, ensure:

- `helm.mooncake_enabled: true`
- `helm.mooncake_master_server_address: "<control-plane-ip>:50088"`
- `helm.mooncake_lookup_rpc_port: "10010"` (or your chosen port)
- `helm.mooncake_host_network: true`
- `helm.deploy_mooncake_master: true`
- `helm.model_host_path` points to model parent directory on all NPU nodes

### 3) values.yaml production defaults

In `vllm-kv-stack/values.yaml`, verify:

- `pin.enabled: true`
- `pin.nodeName: "<node-hostname-for-mooncake-master>"`
- `images.vllm` — override to a GLM5 runtime image (the chart default is `docker.io/library/vllm-ascend:v0.18.0`, not GLM5-specific)
- `images.mooncakeMaster` points to a valid image
- model volume defaults are consistent with your storage mode (hostPath or NFS)

Recommended vLLM image override:

- `quay.io/ascend/vllm-ascend:glm5-openeuler`

## Deployment Steps

### Step 1: Preflight check

Run from repo root:

```bash
python3 preflight_mooncake.py
```

Proceed only if output ends with `READY`.

### Step 2: Deploy stack

Run your sweep/deploy entrypoint (example):

```bash
python3 src/client/sweep_methods.py --config 1-master_config
```

Or run against one specific production config:

```bash
python3 src/client/sweep_methods.py --config 5-2-template-boom-claude-glm-mooncake.yaml
```

### Step 3: Wait for workload readiness

```bash
kubectl -n vllm get pods -o wide
kubectl -n vllm get leaderworkerset
kubectl -n vllm get deploy mooncake-master
```

## Runtime Verification

### Verify Mooncake control plane

```bash
kubectl -n vllm logs deploy/mooncake-master --tail=200 | \
  grep -E "Clients|Keys|Mem Storage|Get|ExistKey"
```

For 4 instances with DP2*TP8, expect total client count to scale toward 64.

### Verify vLLM is running with Mooncake transfer config

```bash
kubectl -n vllm logs <leader-pod-name> -c vllm --tail=200 | \
  grep -E "AscendStoreConnector|kv-transfer-config|MOONCAKE_CONFIG_PATH"
```

### Verify end-to-end request path

Send requests through Boom endpoint and confirm successful responses:

```bash
curl -sS -X POST http://<boom-host>:<boom-port>/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model":"glm-5",
    "messages":[{"role":"user","content":"请解释一下大语言模型中的KV Cache作用。"}],
    "max_tokens":256,
    "stream":false
  }'
```

Also run streaming and tool-call requests to validate full compatibility.

## Common Failure Modes

- `ConfigMap "mooncake-config" not found`
  - missing `11-mooncake-config.yaml` or mooncake disabled in values/overrides
- `mooncake-master` not scheduled
  - invalid `pin.nodeName`, taints not tolerated, or hostNetwork constraints
- vLLM cannot connect to Mooncake master
  - wrong `mooncake_master_server_address`, port mismatch, or network policy
- sweep cannot find prod config
  - `1-master_config.yaml` key uses `.prod` instead of `.prod.yaml`

## Operational Notes

- One `mooncake-master` is usually sufficient for multi-instance GLM5 in this
  architecture because it coordinates metadata, while KV data transfer is
  peer-to-peer between clients.
- Scale Mooncake master for HA/failover strategy, not for data-path throughput.
