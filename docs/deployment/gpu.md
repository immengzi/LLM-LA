# GPU deployment (NVIDIA)

The `vllm-kv-stack` chart targets Huawei Ascend NPUs by default, but the router,
sidecar, and KV-aware logic are accelerator-agnostic. This page covers the
**only** things that differ when running on NVIDIA GPUs: the node-level device
plugin / runtime, and the single chart switch that selects the accelerator.
Everything else — routing, autoscaling, gateways, benchmarking — is identical to
the Ascend path. The `hardware` switch composes with `engine.type` (vLLM default
or pinned SGLang); see [sglang.md](sglang.md).

## What changes on GPU

| Concern | Ascend (default) | NVIDIA GPU |
|---------|------------------|------------|
| Extended resource | `huawei.com/Ascend` | `nvidia.com/gpu` |
| Device visibility | Ascend driver host mounts + `ASCEND_RT_VISIBLE_DEVICES` | injected by the NVIDIA device plugin |
| Container runtime | default | `RuntimeClass: nvidia` |
| Toolkit setup | sources the Ascend CANN toolkit | none |
| vLLM image | `vllm-ascend` | `vllm/vllm-openai` (or your mirror) |
| SGLang image | Ascend-capable build (operator-provided) | `lmsysorg/sglang:v0.5.15-cu129` |

All of this is driven by one value: `hardware: nvidia`.

## 1. Node prerequisites

On every GPU worker node:

1. **NVIDIA driver** installed and healthy (`nvidia-smi` lists the GPUs).
2. **NVIDIA container runtime** configured, with a `nvidia` RuntimeClass
   registered in the cluster:

   ```bash
   kubectl get runtimeclass nvidia
   ```

   If it is missing, create it (the NVIDIA GPU Operator does this for you):

   ```yaml
   apiVersion: node.k8s.io/v1
   kind: RuntimeClass
   metadata:
     name: nvidia
   handler: nvidia
   ```

3. **NVIDIA device plugin** running so GPUs are advertised to the scheduler.
   Either install the [device plugin](https://github.com/NVIDIA/k8s-device-plugin)
   directly or, preferably, the
   [GPU Operator](https://github.com/NVIDIA/gpu-operator) (driver + runtime +
   device plugin + DCGM in one).

   ```bash
   # Device plugin only:
   kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.15.0/deployments/static/nvidia-device-plugin.yml
   ```

### Verify GPUs are schedulable

```bash
kubectl get nodes -o custom-columns=NAME:.metadata.name,GPU:.status.allocatable.'nvidia\.com/gpu'
```

Every GPU node should report a non-zero `nvidia.com/gpu` count. This is the same
signal the chart requests one-per-`tensorParallelSize`.

## 2. Select the GPU backend

The chart exposes a single machine-level switch, `hardware`, in
[`values.yaml`](../../src/core/vllm-kv-stack/values.yaml):

```yaml
hardware: nvidia   # default: ascend
```

When set to `nvidia`, the chart:

- requests `nvidia.com/gpu` instead of `huawei.com/Ascend`,
- applies `vllm.runtimeClassName` (default `nvidia`) to the vLLM pods,
- drops all Ascend driver host mounts and the CANN toolkit sourcing,
- lets the device plugin inject GPU visibility (no `ASCEND_RT_VISIBLE_DEVICES`).

Set it **per cluster** through a profile in
[`src/client/configs/clusters.yaml`](../../src/client/configs/clusters.yaml)
rather than editing the deploy scripts — the shipped `gpu` profile does exactly
this:

```yaml
clusters:
  gpu:
    helm:
      model_host_path: "/data/models"
      values:
        hardware: nvidia
        global.imageRegistry: ""
        images.vllm: "vllm/vllm-openai:latest"
        images.router: "reg.local:32000/kv-router:latest"
        images.sidecar: "reg.local:32000/kv-sidecar:latest"
        images.cpuHash: "reg.local:32000/vllm-cpu-hash:latest"
        cpuHash.ascendDriverMounts: false
        pin.enabled: false
```

Adjust the endpoints, `model_host_path`, and images to match your cluster. See
[switch_cluster](../operations/switch_cluster.md) for how profiles merge.

## 3. Deploy

Point an experiment config at the profile with `switch_cluster: gpu` and deploy
exactly as on Ascend:

```yaml
# my-gpu-run.yaml
switch_cluster: gpu
backend: router
helm:
  models:
    - name: qwen
      servedModelName: served-model
      replicas: 1
      modelSubPath: qwen3-8b
      tensorParallelSize: 1   # 1 GPU per replica; N = N GPUs (tensor parallel)
      batchSize: 8
```

```bash
cd src/client
python deploy_vllm.py --config my-gpu-run.yaml   # vLLM only
# or a full stack sweep:
python sweep_methods.py --config configs/1-master_config.yaml
```

`deploy_vllm.py` and `sweep_methods.py` apply the profile's `helm.values`
(including `hardware: nvidia`) as Helm `--set` overrides, so no script or chart
edit is needed to move between Ascend and GPU.

## Notes

- `tensorParallelSize` maps 1:1 to GPUs per replica; a node needs at least that
  many free GPUs to schedule a pod.
- The Ascend-specific KV transfer backends (Mooncake, LMCache P2P/HCCL) remain
  NPU-only and stay disabled on GPU (`mooncake.enabled=false`,
  `lmcache.enabled=false`, the defaults).
- To confirm what the chart will render for either backend without a cluster:

  ```bash
  helm template vllm src/core/vllm-kv-stack --set hardware=nvidia -f models.yaml
  ```
