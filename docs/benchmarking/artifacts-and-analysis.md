# Artifacts and analysis

What each experiment produces and how to analyze it. For how runs are launched, see [load patterns](load-patterns.md) and [experiment configs](../configuration/experiment-configs.md).

## Experiment directory

Each run is written to a numbered directory under the experiments root. The run ID is the highest existing numeric subdirectory plus one.

> Roots differ between entry points: `main.py`/`experiment_io.init_experiment()` default to `/home/data/saeid/experiments`, while `sweep_methods.py` detects new run directories under `src/experiments` (`REPO_ROOT/experiments`). Out of the box these don't match, so a sweep may not auto-detect the run unless you align the roots (e.g. symlink). Adjust before relying on sweep artifact collection.

```
<experiments-root>/<N>/
├── config.json                    # frozen ClientConfig + node time offsets
├── config_used.yaml               # exact input YAML
├── vllm-k8s.yaml                  # Helm snapshot (real on sweeps; placeholder stub on standalone runs)
├── logs.json                      # per-request NDJSON (the primary data)
├── router_logs.json               # router /latency_log truth (if collect_router_log)
├── run_summary.json               # aggregate throughput/latency
├── endpoint_tokens.json           # per-endpoint token rollup
├── metrics.jsonl                  # Prometheus samples per tick (if enabled)
├── metrics_summary.json           # Prometheus rollups
├── pod_node_mapping_events.jsonl  # K8s pod placement events
├── sweep_meta.json                # sweep only: method + Helm set values
├── deployment-info.txt            # sweep only: connection info
└── helm-effective-values.yaml     # sweep only: resolved Helm values
```

## `logs.json` record schema

`logs.json` is newline-delimited JSON, one object per request. Typical success fields:

| Field | Meaning |
|-------|---------|
| `idx`, `req_id` | Request index and ID |
| `prompt` | Prompt text |
| `planned_ts_mono`, `actual_send_ts_mono` | Scheduled vs actual send time |
| `t0_wall`, `t1_wall` | Wall-clock start/end |
| `end_to_end_s` | Client-observed latency |
| `model_latency_s` | Backend model latency |
| `finish_reason` | `stop` / `length` / ... |
| `prompt_tokens`, `completion_tokens` | Token counts |
| `endpoint_id` | Serving pod/replica |
| `kv_hits_len`, `total_blocks`, `matched_tokens`, `kv_hit` | Router prefix/KV-hit decision (when `collect_router_log` is on) |
| `affinity_key` | Conversation key the router pinned on (when affinity is active) |
| `trace`, `trace_metrics` | Stage timestamps + derived stage latencies (when tracing is on) |
| `output` | Generated text (when logged) |

Multi-turn runs add `conversation_id`, `turn_idx`, and streaming runs add `ttft_s` / `tpot_avg_s`. Failed requests carry `error`, `send_failed`, or `lost (...)` markers.

## Router request log (`collect_router_log`)

Set `collect_router_log: true` in the client config to capture the router's own
per-request routing decision independent of the response body (so it survives the
BooM hop). During the run, [`router_log_collector.py`](../../src/router_log_collector.py)
polls the router's `/latency_log` ring and writes `router_logs.json` (the router
truth), while live-enriching `logs.json` records with `endpoint_id` and the
prefix/KV fields above. At shutdown an authoritative join rewrites `logs.json`
from `router_logs.json` (covering any records the live path missed) and adds a
`routing` rollup to `run_summary.json` (per-endpoint request/KV counts plus
conversation stickiness grouped by `conversation_id`, else `affinity_key`).

The same module powers the external observer `prod_latency_collector.py`, so a
prod capture emits the identical `router_logs.json` + enriched `logs.json`. Set
`ROUTER_LOG_BLOCK_HASHES=true` on the router to also include the raw prefix
`block_hashes` list per request (bulky; off by default).

## Trace metrics

When `TRACE_ENABLED=true` on the router and sidecar, each result carries timestamps that the client converts into stage latencies (router queue wait, sidecar queue, vLLM compute, TTFT, post-result handling). See [request tracing](../architecture/trace.md).

## Prometheus samples

If `metrics.enabled` is set, a background collector scrapes Prometheus during the run and writes `metrics.jsonl` (per-tick samples) and `metrics_summary.json` (rollups such as queue depth and cache hit rate). These samples also drive the async-transport fleet-idle termination logic.

## Analysis notebooks

Post-experiment analysis lives in `src/jupyters/`. The notebooks read `logs.json` / `run_summary.json` / `metrics.jsonl` across runs to produce latency distributions, throughput curves, and routing-method comparisons. Figures are written under `src/jupyters/figures/`.

## See also

- [Load patterns](load-patterns.md)
- [Request tracing](../architecture/trace.md)
- [Experiment configs and sweeps](../configuration/experiment-configs.md)
