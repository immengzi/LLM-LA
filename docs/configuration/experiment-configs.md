# Experiment configs and sweeps

How client configs, the master config, and the sweep runner fit together to deploy and measure routing strategies. For the per-field client schema see [client config](client-config.md); for Helm knobs see [Helm values](helm-values.md).

## The two config layers

| Layer | File(s) | Role |
|-------|---------|------|
| Client config | `src/configs/*.yaml` | One workload definition: prompts, RPS, generation params, backend, transport, and a `helm:` section of deployment knobs |
| Master sweep config | `src/configs/1-master_config.yaml` | Maps client configs to the list of routing methods to sweep |

A client config drives a single `main.py` run. The master config drives a `sweep_methods.py` run across many `(client_config, method)` jobs.

## Client config and the `helm:` section

Each client config carries a `helm:` block consumed by the sweep runner and `deploy_vllm.py`. It controls vLLM and the routing stack (replicas, TP, batch size, vLLM flags, router toggles, gateway settings, Mooncake/LMCache, and the unified `models[]` list). The `helm:` fields map to chart values documented in [Helm values](helm-values.md).

The same routing behaviour is expressed under three names depending on the layer — this is the mapping (the sweep/`deploy_vllm.py` translate the client `helm:` keys into chart values, which the chart renders into router env vars):

| Client `helm:` key | Chart value (`router.*`) | Router env var | Values |
|--------------------|--------------------------|----------------|--------|
| `router_mode` (or sweep method) | `router.mode` | `ROUTER_MODE` | `pull`, `push-rr`, `push-random`, `push-leastq` |
| `router_kv_aware` | `router.kvAware` | `KV_AWARE` | bool |
| `router_len_aware` | `router.lenAware` | `LEN_AWARE` | bool |
| `router_len_policy` | `router.lenPolicy` | `LEN_POLICY` | `short_first`, `long_first` |

Minimal example:

```yaml
backend: router
router_url: "http://<node-ip>:30080"
total_requests: 500
prompt_source: hf-lmsys

helm:
  nfs_path: "/home/models/Qwen3-8B"   # REQUIRED
  replicas: 2
  batch_size: 8
  tensor_parallel_size: 1
  router_kv_aware: true
  router_len_aware: true
  router_len_policy: short_first
```

## Master config format

`1-master_config.yaml` is a mapping of client-config basename to a list of methods. Each entry produces one experiment:

```yaml
router-tp8-glm:
  - pull
  - push-rr
  - push-random
```

## Method vocabulary by backend

| Backend / mode | Valid methods | Where the method goes |
|----------------|---------------|-----------------------|
| `router` (and `litellm`/`boom` with `helm.boom_route_via: router`) | `pull`, `push-rr`, `push-random`, `push-leastq` | `router.mode` (Helm). The sweep passes the method verbatim; the router service accepts the `push-least-queue` alias and normalizes it to `push-leastq`. |
| `boom` with `helm.boom_route_via: direct` | `round_robin`, `key_affinity` | `boom.directRoutingStrategy` (Helm) |
| `aibrix` | strategy names (e.g. `random`, `prefix-cache`, `least-request`) | `aibrix.routing_strategy` (client-side, not Helm) |
| `litellm` / `boom` (router mode) | any label | label only in `sweep_meta.json`; routing happens in the router behind the gateway |

In BooM direct mode (`helm.boom_route_via: direct`), the sweep also disables the router stack (`deploy.router=false`, `deploy.redis=false`, `deploy.cpuHash=false`, `sidecar.enabled=false`).

## Sweep runner lifecycle

`sweep_methods.py` runs, for each `(client_config, method)` job:

1. Loads the client config (must have a `helm:` section).
2. Cleans the cluster (`helm uninstall vllm`) unless `--skip-vllm`.
3. Builds `--set` values from `helm:` plus backend/method-specific toggles.
4. `helm upgrade --install` with a temporary `models[]` overlay.
5. Waits for the stack to be Ready (up to 3 redeploy retries).
6. Snapshots `vllm-k8s.yaml` and `helm-effective-values.yaml`.
7. Runs `python main.py` against the deployment via a temp job config.
8. Detects the new experiment directory (under `src/experiments`) and writes `sweep_meta.json`, `deployment-info.txt`, and `helm-effective-values.yaml` into it.

> Detection relies on `main.py` writing into the same root the sweep watches. By default `main.py` writes to `/home/data/saeid/experiments` while the sweep watches `src/experiments`; align these roots or sweep artifact collection won't find the run (see [artifacts & analysis](../benchmarking/artifacts-and-analysis.md)).

```bash
# Full sweep (uninstalls + redeploys the whole stack per job)
python sweep_methods.py --config 1-master_config

# Fast sweep: keep running vLLM pods, only redeploy the routing stack
python sweep_methods.py --config 1-master_config --skip-vllm
```

## Recommended flow for long-running models

Deploy vLLM once, then iterate quickly on routing methods:

```bash
python deploy_vllm.py --config configs/router-tp8-glm.yaml
python sweep_methods.py --config 1-master_config --skip-vllm
```

## Sweep artifacts

In addition to the standard run outputs (see [artifacts & analysis](../benchmarking/artifacts-and-analysis.md)), each sweep job writes:

| File | Contents |
|------|----------|
| `vllm-k8s.yaml` | Full Helm template render |
| `helm-effective-values.yaml` | `helm get values --all` |
| `sweep_meta.json` | client config, backend, method, and the Helm `--set` values used |
| `deployment-info.txt` | Router/BooM/Claude connection info |

## See also

- [Client config reference](client-config.md)
- [Helm values reference](helm-values.md)
- [Artifacts and analysis](../benchmarking/artifacts-and-analysis.md)
