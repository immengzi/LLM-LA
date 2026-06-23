# Your first experiment

A walkthrough of the anatomy of a single run: what `deploy_vllm.py` and `main.py` do, what happens end-to-end, and where the output lands. For the condensed command list, see the [quickstart](quickstart.md).

## The two entry points

| Script | Responsibility |
|--------|----------------|
| `deploy_vllm.py` | Deploys only the vLLM pods (Helm `deploy.vllm=true`, router/redis/cpuHash off) from a client config |
| `main.py` | Runs one open-loop load experiment against a deployed backend and writes artifacts |
| `sweep_methods.py` | Automates deploy + measure across multiple routing methods (see [experiment configs](../configuration/experiment-configs.md)) |

`deploy_vllm.py` and `sweep_methods.py` use the same release name (`vllm`) to avoid RBAC ownership conflicts.

## Step 1 — Deploy vLLM

```bash
cd <repo-root>/src
python deploy_vllm.py --config configs/router-tp8-qwen.yaml
```

This renders the Helm chart with only the vLLM Deployment (or LeaderWorkerSet) and its co-located sidecar, then waits for pods to be Ready. The model subpath is derived from `helm.nfs_path` in the config.

## Step 2 — Run the load test

```bash
python main.py --config router-tp8-qwen --n 200
```

What happens inside `main.py`:

1. **Load config** — parses the client YAML into the configuration schema (see [client config](../configuration/client-config.md)).
2. **Build prompts** — from a static file, LMSYS, or CodeFlowBench, depending on `prompt_source` (see [load patterns](../benchmarking/load-patterns.md)).
3. **Build the schedule** — precomputes send timestamps for the chosen RPS pattern (`det`, `poisson`, `bursty`, `steps`, `rand`, or `dump`).
4. **Create the experiment directory** — allocates the next numeric run ID and snapshots the config.
5. **Optionally start metrics + pod-map logging** — background Prometheus scraping and K8s pod placement logging.
6. **Run the open-loop load** — dispatches requests on schedule to the selected backend/transport (router `/enqueue` sync, router `/submit` + ZMQ async, or a gateway).
7. **Write summaries** — `run_summary.json`, `endpoint_tokens.json`, and the per-request `logs.json`.

## Step 3 — Inspect the results

The run writes a numbered directory containing the frozen config, the per-request `logs.json`, the aggregate `run_summary.json`, a per-endpoint token rollup, and (if enabled) Prometheus samples.

See [artifacts & analysis](../benchmarking/artifacts-and-analysis.md) for the full file list, the `logs.json` record schema, and the notebook workflow.

## Choosing a backend

| `backend` | Use case |
|-----------|----------|
| `router` | Benchmarking — clean latency, full observability (recommended) |
| `boom` | Validate the production path through [BooM Gateway](../gateways/boom/overview.md) |
| `litellm` | Validate the production path through LiteLLM |
| `aibrix` | Baseline routing-strategy comparison |

## Next steps

- Tune the workload: [client config reference](../configuration/client-config.md)
- Sweep multiple routing methods: [experiment configs](../configuration/experiment-configs.md)
- Understand the routing: [architecture overview](../architecture/overview.md)
