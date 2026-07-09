# Benchmark harness (client)

The benchmark harness is the **client** side of the repo. It is a standalone load
generator and experiment orchestrator that drives the deployed serving platform,
measures it, and archives the results. It lives entirely under
[`src/client/`](../../src/client) and does not run inside the cluster — it talks to
the platform over HTTP (directly to the router or through a gateway such as BooM).

The deployed system it exercises (router, sidecars, vLLM, Helm chart) lives under
[`src/core/`](../../src/core); see the [architecture overview](../architecture/overview.md).

## Layout

```
src/client/
  main.py                    # load generator entry point (single run)
  sweep_methods.py           # sweep runner: deploy → run → collect, per method
  deploy_vllm.py             # vLLM-only deploy helper (no router/redis)
  prod_latency_collector.py  # external observer for live/production deployments
  config.py                  # client + Helm config schema and cluster profiles
  load_runner.py scheduler.py http_client.py prompts.py prompts.json
  trace_utils.py experiment_io.py metrics_prom.py router_log_collector.py
  redis_watch.py redis_verify.py k8s_event_podmap.py k8s_time_offsets.py
  rolling_restart_vllm.sh requirements.txt
  configs/                   # experiment/client YAMLs + clusters.yaml + master sweep
  multiturn-generation/      # Claude Code-style injection templates
  experiments -> <shared>    # gitignored per-machine symlink to results storage
```

## Components

| Piece | What it does |
|-------|--------------|
| Load generator ([`main.py`](../../src/client/main.py)) | Runs one experiment from a client config: builds prompts/conversations, drives an open-loop schedule, records per-request timings, and writes the run directory. |
| Sweep runner ([`sweep_methods.py`](../../src/client/sweep_methods.py)) | Orchestrates a matrix of routing methods: renders/applies the Helm chart, waits for readiness, launches `main.py`, then snapshots `vllm-k8s.yaml`, `helm-effective-values.yaml`, and `deployment-info.txt` into the experiment dir. |
| Deploy helper ([`deploy_vllm.py`](../../src/client/deploy_vllm.py)) | Deploys **only** the vLLM pods (and co-located sidecar) from a client config — no router/Redis/prefix-hash — for iterating on the engine independently. |
| External observer ([`prod_latency_collector.py`](../../src/client/prod_latency_collector.py)) | Polls a live router's `/latency_log` and Prometheus endpoints and streams them to `logs.json` (mirrored to `router_logs.json`), so production deployments can be observed without a client-driven sweep. |
| Config ([`config.py`](../../src/client/config.py)) | Defines the client + `helm:` schema and merges per-cluster profiles from `configs/clusters.yaml` via [`switch_cluster`](../operations/switch_cluster.md). |

## Running it

All commands run from the repository root:

```bash
# single experiment
python src/client/main.py --config router --n 500

# deploy vLLM only
python src/client/deploy_vllm.py --config configs/router-tp8-glm.yaml

# sweep a set of routing methods
python src/client/sweep_methods.py --config 1-master_config --skip-vllm
```

`--config <name>` resolves to `src/client/configs/<name>.yaml` (a value containing a
directory or a `.yaml` suffix is used as given). Relative `template_dir` and config
paths resolve against `src/client/`, so runs behave the same regardless of the
current working directory.

## Experiments directory

`src/client/experiments` is a **gitignored per-machine symlink** to shared results
storage (bz → `/data/experiments`, yz → `/home/data/saeid/experiments`). Recreate it
on each box after cloning. Each run lands in `experiments/<N>/`; see
[artifacts & analysis](artifacts-and-analysis.md) for the directory layout and the
analysis notebooks.

## Related

- [Load patterns](load-patterns.md) — RPS schedules and the open-loop model
- [Client config reference](../configuration/client-config.md) — the client + `helm:` YAML
- [Experiment configs](../configuration/experiment-configs.md) — naming and the sweep matrix
- [Artifacts & analysis](artifacts-and-analysis.md) — `experiments/<N>/` layout and notebooks
