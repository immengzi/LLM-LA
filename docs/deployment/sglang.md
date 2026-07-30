# Deploying SGLang

LA-Boom serves models with **vLLM by default**. SGLang is an opt-in inference
engine selected with a single switch — the same pattern as the
[`hardware`](gpu.md) accelerator switch:

| Switch | Values | Effect |
|--------|--------|--------|
| `engine.type` / `helm.engine_type` | `vllm` (default) · `sglang` | Chooses the engine image, launch command, workload name (`vllm-*` / `sglang-*`), hash backend, and sidecar/router env |
| `hardware` | `ascend` (default) · `nvidia` | Chooses the accelerator resource, RuntimeClass, and Ascend driver mounts |

Both knobs compose. Typical production profiles:

- **SGLang on NVIDIA GPUs** — `engine.type=sglang` + `hardware=nvidia`
- **SGLang on Ascend NPUs** — `engine.type=sglang` + `hardware=ascend` (set `images.sglang` to your Ascend SGLang build)
- **vLLM on either** — leave `engine.type=vllm` and set `hardware` as today

The supported NVIDIA image is pinned to `lmsysorg/sglang:v0.5.15-cu129` (CUDA 12.9).
The unqualified `v0.5.15` tag can resolve to CUDA 13 and fail on 570-series drivers.
Override `images.sglang` / `helm.sglang_image` / `models[].image` for mirrors or
Ascend builds; pin `@sha256:` digests in production.

Contract details (page size, KV events, hashing):
[`sglang-contract-v0.5.15.md`](../architecture/sglang-contract-v0.5.15.md).

## Prerequisites

- Kubernetes + Helm 3 + `kubectl` for the target context
- For **nvidia**: NVIDIA driver, device plugin, and RuntimeClass (default `nvidia`) — see [gpu.md](gpu.md)
- For **ascend**: Ascend driver / device plugin as for vLLM NPU deployments
- Model weights on a hostPath shared by engine nodes, or a model PVC + `modelSubPath`
- Pullable router, sidecar, and SGLang images (mirror + digest-pin for production)

## Cluster profile

Use the `sglang-gpu` machine profile in
[`src/client/configs/clusters.yaml`](../../src/client/configs/clusters.yaml), or
set the same values in any cluster entry:

```yaml
helm:
  engine_type: sglang
  model_host_path: /data/models
  values:
    hardware: nvidia
    engine.type: sglang
    global.imageRegistry: ""
    images.sglang: lmsysorg/sglang:v0.5.15-cu129
```

For Ascend NPUs, keep `engine.type: sglang`, set `hardware: ascend`, and point
`images.sglang` at your Ascend-capable SGLang image. The chart then requests
`huawei.com/Ascend` and keeps the Ascend driver mounts.

Deploy with the existing entry points (`deploy_vllm.py` or the thin alias
`deploy_engine.py`); both read `helm.engine_type`.

## Workload naming

| Engine | Deployment / Service / `app` label | `component` label | Container |
|--------|--------------------------------------|-------------------|-----------|
| vLLM (default) | `vllm-<modelName>` | `vllm` | `vllm` |
| SGLang | `sglang-<modelName>` | `sglang` | `sglang` |

Default deploys keep the historical `component=vllm` discovery label and
`ServiceMonitor` name `vllm`. SGLang uses `component=sglang` / monitor name
`sglang`. The KV sidecar remains `kv-sidecar`.

## Defaults (no variables set)

Leaving `engine.type` / `helm.engine_type` and `hardware` unset keeps today's
production path: **vLLM on Ascend NPUs**. SGLang and NVIDIA are opt-in only.

## Render and test locally

```bash
helm lint src/core/vllm-kv-stack

# Engine × hardware matrix (also covered by src/core/tests/helm/)
for hw in ascend nvidia; do
  for eng in vllm sglang; do
    helm template test src/core/vllm-kv-stack \
      --set global.imageRegistry= \
      --set modelVolume.modelSubPath=placeholder \
      --set hardware=$hw \
      --set engine.type=$eng \
      >/dev/null && echo "ok $hw/$eng"
  done
done

pytest src/core/tests/helm -q
```

## Limitations

### Deploy-time rejects (Helm / client validation fails)

SGLang cannot be deployed with the external CPU hash service, data-parallel
LeaderWorkerSet, Mooncake, or LMCache. `trustRemoteCode` is rejected for the
pinned v0.5.15 profile. Prefer `router_hash_source: inline` with the pinned
SGLang hash backend.

### Request-time skips (request still served)

When a request uses features the router cannot hash the same way as the engine
(cache salts / extra keys, LoRA/adapters, non-text multimodal parts,
speculative/draft/bigram inputs, or request-level tokenizer/chat-template
overrides), the router skips KV-aware scoring for that request only. It does not
reject the request.
