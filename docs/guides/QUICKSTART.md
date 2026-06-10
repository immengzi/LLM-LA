# llm-lb Quick Start

## Overview

This quick start walks you through deploying **llm-lb** — a KV-aware load-balancing router for vLLM — on a Kubernetes cluster using Helm. It covers the key decisions at each step, how to validate your deployment, how to send inference requests to the router, and how to tear everything down cleanly.

llm-lb sits between your clients and vLLM worker pods. Each worker runs a **sidecar** that handles push/pull dispatch and reports results back to the router. The router provides a simple, synchronous `/enqueue` API to clients while internally managing KV-aware scheduling, prefix-block reuse, and length-aware batching.

---

## Prerequisites

### Permissions

Ensure you have cluster-admin or equivalent permissions before proceeding. The Helm chart creates a `Namespace`, `Deployment`, `Service`, `ConfigMap`, and associated RBAC resources.

### Tool dependencies

You will need the following tools installed and on your `PATH`:

| Tool | Purpose |
|------|---------|
| `kubectl` | Kubernetes CLI |
| `helm` (≥ 3.12) | Chart install / upgrade |
| `python` (≥ 3.9) | Running the sweep client (`main.py`) |
| `click`, `pyyaml` | Python dependencies for `sweep_methods.py` |

Install Python dependencies with:

```bash
pip install click pyyaml
```

### Kubernetes cluster

llm-lb requires a Kubernetes cluster with:

- Sufficient compute resources for your vLLM worker replicas.
- [Prometheus](https://prometheus.io/docs/prometheus/latest/installation/) deployed in the cluster if autoscaling is enabled.
- Access to the container registry hosting the vLLM and sidecar images.

### Chart directory

Clone the repository and confirm the Helm chart is present:

```bash
git clone <llm-lb-repo-url>
cd llm-lb
ls vllm-kv-stack/   # should contain Chart.yaml, values.yaml, templates/
```

---

## Deployment

### 1. Review the default values

The chart ships with sensible defaults in `vllm-kv-stack/values.yaml`. The key knobs are:

```yaml
replicas:
  vllm: 2                  # number of vLLM worker pods

batchSize: 8               # tokens per batch passed to each worker

router:
  mode: pull               # pull | push-rr | push-random | push-leastq
  kvAware: true            # enable KV-prefix-aware routing
  lenAware: true           # enable length-aware candidate reordering
  lenPolicy: short_first   # short_first | long_first

autoscaling:
  enabled: false
```

For KV-aware routing, you will also need to point the router at your prefix-hash service and Redis instance. See the [KV-awareness configuration reference](./configuration.md#kv-awareness) for `HASH_SERVICE_URL`, `REDIS_HOST`, and `REDIS_PORT`.

### 2. Deploy with Helm

Use `helm upgrade --install` to deploy (or upgrade) the stack into the `vllm` namespace:

```bash
helm upgrade --install vllm ./vllm-kv-stack \
  -n vllm \
  --create-namespace \
  -f vllm-kv-stack/values.yaml \
  --set router.mode=pull \
  --set router.kvAware=true \
  --set replicas.vllm=2 \
  --set batchSize=8
```

> **Note:** `--create-namespace` is safe to re-run; it is a no-op if the namespace already exists.

### 3. Wait for readiness

Poll rollout status across all workload kinds:

```bash
kubectl rollout status deploy -n vllm
kubectl rollout status sts   -n vllm   # if any StatefulSets are present
kubectl wait -n vllm --for=condition=Ready pod --all --timeout=900s
```

Once all pods report `Ready`, the router and its sidecar-equipped worker pods are serving.

### 4. Understand the sidecar

Every vLLM worker pod runs a **sidecar container** alongside the main vLLM process. The sidecar:

- In **pull mode** — calls the router's `/pull` endpoint when the worker has capacity, receives a batch of jobs, forwards them to vLLM, and posts results back via `/result`.
- In **push mode** — listens on its own `/push` endpoint; the router pushes jobs directly when it selects this worker.

The sidecar also attaches timing fields to each result (e.g. `t_arrive_sidecar_pull`, `t_vllm_send`, `t_vllm_recv`) that the router merges into the final trace when `TRACE_ENABLED=true`.

No additional configuration is needed to enable the sidecar — it is included in the chart and starts automatically with the worker pod.

### 5. Using the sweep runner (optional)

For benchmarking multiple routing configurations in sequence, use the provided `sweep_methods.py` script. It reads a master config mapping client configs to routing methods, cleans the cluster between runs, redeploys via Helm, and snapshots results.

Create a master config at `configs/1-master_config.yaml`:

```yaml
# maps client config -> list of router methods to sweep
my_client_config:
  - pull
```

Then run the sweep:

```bash
python sweep_methods.py --config 1-master_config
```

The sweep runner will:

1. Uninstall any existing `vllm` release in the `vllm` namespace.
2. Deploy fresh with the Helm values derived from your client config's `helm` section.
3. Wait for all pods to be ready (with up to 3 redeploy attempts on failure).
4. Run `main.py` against the live deployment.
5. Snapshot `vllm-k8s.yaml`, `helm-effective-values.yaml`, and `sweep_meta.json` into the experiment directory.

---

## Making Inference Requests

Once the deployment is ready, expose the router service locally:

```bash
kubectl port-forward svc/vllm-router -n vllm 8080:8080
```

### Health check

```bash
curl http://localhost:8080/health
```

Expected response:

```json
{"status": "ok", "queue_length": 0}
```

### Sending a prompt via `/enqueue`

`/enqueue` is a **synchronous** endpoint — it blocks until the model result is ready and returns it in a single response.

```bash
curl -X POST http://localhost:8080/enqueue \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "Explain KV cache reuse in large language model serving.",
    "meta": {}
  }'
```

Example response:

```json
{
  "req_id": "a3f92c1d-...",
  "result": {
    "text": "KV cache reuse allows ...",
    "finish_reason": "stop",
    "latency_s": 0.84
  }
}
```

### Inspecting traces

Enable tracing by setting `TRACE_ENABLED=true` in the router's environment (via the chart values or a `--set` flag). The response will include a `trace` object with end-to-end timestamps from both the router and the sidecar:

```json
{
  "req_id": "a3f92c1d-...",
  "result": { ... },
  "trace": {
    "t_enq_router_queue":      1712300000.001,
    "t_dispatch_router":       1712300000.012,
    "t_arrive_sidecar_pull":   1712300000.015,
    "t_dequeue_sidecar":       1712300000.016,
    "t_vllm_send":             1712300000.017,
    "t_vllm_recv":             1712300000.843,
    "t_post_result_sidecar":   1712300000.845,
    "t_router_result_recv":    1712300000.847,
    "t_enqueue_response":      1712300000.848
  }
}
```

---

## Validation & Metrics

### List all resources

```bash
kubectl get all -n vllm
```

### List Helm releases

```bash
helm list -n vllm
```

### Inspect effective Helm values

After a sweep run, effective values are written to `helm-effective-values.yaml` in the repo root:

```bash
cat helm-effective-values.yaml
```

Or retrieve them live:

```bash
helm get values vllm -n vllm --all
```

### Prometheus metrics

llm-lb applies `PodMonitor` resources to expose vLLM and router metrics to Prometheus when enabled. Key metrics to watch:

| Metric | Description |
|--------|-------------|
| `vllm:cache_config_info` | KV cache block configuration |
| `vllm:gpu_prefix_cache_hit_rate` | Prefix cache hit rate per worker |
| `llm_lb_queue_depth` | Current router queue depth |
| `llm_lb_ttft_seconds` | Time to first token distribution |
| `llm_lb_tpot_seconds` | Time per output token distribution |

Enable the `PodMonitor` via the chart:

```bash
helm upgrade vllm ./vllm-kv-stack -n vllm \
  --reuse-values \
  --set monitoring.enabled=true
```

> We strongly recommend enabling monitoring. LLM inference can bottleneck in multiple places — KV cache exhaustion, queue back-pressure, and NPU/GPU saturation all manifest differently and require metric-level visibility to diagnose.

---

## Uninstall

To remove all llm-lb resources from the cluster:

```bash
helm uninstall vllm -n vllm
kubectl delete namespace vllm
```

> **Note:** Deleting the namespace removes all resources within it, including any PVCs. Ensure any experiment data you wish to retain has been copied out first.