# vLLM Metrics Monitor

A lightweight, zero-dependency background monitor for vLLM's `/metrics` endpoint.
Collects, logs, and visualises inference performance metrics including prefix cache hit rate,
KV cache usage, latency distributions, throughput, and request context length distribution.
Supports monitoring multiple vLLM instances in parallel from a single machine.

---

## Files

| File | Description |
|---|---|
| `vllm_monitor.py` | Background daemon — polls `/metrics` every N seconds, writes `metrics.jsonl` |
| `vllm_analyze.py` | Offline analysis — reads `metrics.jsonl`, saves one PNG per chart |
| `launch_monitors.py` | Multi-instance launcher — starts/stops/monitors one `vllm_monitor` process per instance |
| `vllm_log_viewer.py` | Real-time web dashboard — serves a live-updating UI at `http://localhost:9999` |
| `instances.yaml` | Configuration file — defines all instances, intervals, and log directory |

---

## Architecture

### Data flow (single instance)

```
vLLM /metrics endpoint  (Prometheus exposition format)
        |
        |  poll every --interval seconds
        v
vllm_monitor.py
        |
        |  parses raw text, computes derived fields, writes one JSON line per poll
        v
vllm_logs/<instance_name>/metrics.jsonl        (primary data store)
vllm_logs/<instance_name>/vllm_monitor.log     (operational log)
        |
        |  offline
        v
vllm_analyze.py
        |
        |  reads jsonl, plots charts per scope (all data + one folder per day)
        v
vllm_logs/experiment_<EXPERIMENT_ID>/
    all/
        01_requests.png
        02_prefix_hit_rate.png
        03_external_prefix_hit_rate.png
        04_context_length_distribution.png
        05_token_throughput.png
        06_latency.png
    2026-03-20/
        ...
```

### Data flow (multi-instance)

```
instances.yaml
        |
        v
launch_monitors.py  ──── forks one vllm_monitor.py process per instance
        |
        ├── vllm_monitor.py  (instance_a, PID stored in /tmp/vllm_monitor_pids/instance_a.pid)
        ├── vllm_monitor.py  (instance_b)
        └── ...  (up to N instances)

vllm_logs/
    launcher.log
    instance_a/
        metrics.jsonl
        vllm_monitor.log
    instance_b/
        metrics.jsonl
        vllm_monitor.log
    ...

vllm_log_viewer.py  ──── reads latest record from each metrics.jsonl every 3s
        |
        v
http://localhost:9999  (live dashboard, no external dependencies)
```

### JSONL record format

Every poll writes one line to `metrics.jsonl`:

```json
{
  "ts": "2026-03-20T15:59:53.914813+00:00",
  "mode": "instance_a",
  "samples": [{
    "instance": "http://192.168.1.101:8000",
    "requests_running": 3.0,
    "requests_waiting": 1.0,
    "gpu_kv_cache_usage_frac": 0.42,
    "prefix_cache_hit_rate": 0.98,
    "ttft_seconds_avg": 0.31,
    "ttft_seconds_avg__p99": 0.51,
    "ttft_seconds__buckets": {"0.001": 0, "0.01": 12, "0.1": 180, "+Inf": 200},
    "queue_time_seconds_avg": 0.012,
    "queue_time_seconds_avg__p99": 0.045,
    "queue_time_seconds__buckets": {"0.001": 5, "0.005": 23, "+Inf": 200},
    "gen_tokens_per_sec": 185.4,
    "request_prompt_tokens__buckets": {"128": 10, "256": 45, "512": 120, "+Inf": 200}
  }]
}
```

Error ticks (fetch failed) are also written:
```json
{"ts": "...", "mode": "instance_a", "samples": [], "error": "timed out", "consecutive_failures": 3}
```

---

## Configuration

### instances.yaml

```yaml
log_dir: /mnt/nvme1/haiting_jd/llm-lb/vllm_logs
interval: 5        # poll interval in seconds
timeout: 10        # HTTP request timeout in seconds

instances:
  - name: GLM-5-direct15
    host: http://192.168.1.101:8000
    model: GLM-5

  - name: Qwen3.5-direct90
    host: http://192.168.1.102:8000
    model: Qwen3.5

  # ... additional instances
```

`name` is used as both the subdirectory name under `log_dir` and the `mode` label in each JSONL record.

---

## Multi-instance usage

### Start all monitors

```bash
# Foreground — Ctrl-C stops all; crashed children are auto-restarted every 5s
python launch_monitors.py

# Background — detaches immediately, no auto-restart
python launch_monitors.py --daemon
```

### Check status

```bash
python launch_monitors.py --status

# NAME                       PID  STATUS        HOST
# ──────────────────────────────────────────────────────────────────────
# GLM-5-direct15           12301  running       http://192.168.1.101:8000
# Qwen3.5-direct90         12302  running       http://192.168.1.102:8000
# MiniMax-M2.5-direct27       —   not started   http://192.168.1.103:8000
```

Status values:

| Status | Meaning |
|---|---|
| `running` | process alive, PID file present |
| `not started` | no PID file found |
| `dead (stale)` | PID file exists but process is gone — re-run start to recover |

### Stop all monitors

```bash
python launch_monitors.py --stop
```

### Restart a dead instance

Re-running start skips already-running instances and only launches those that are not running:

```bash
python launch_monitors.py --daemon
```

### Manual single-instance start (without launcher)

```bash
nohup python3 vllm_monitor.py \
    --host http://YOUR_HOST_IP:PORT \
    --interval 5 \
    --log-dir ./vllm_logs \
    --mode my_instance \
    --no-stdout &

echo "PID: $!"
```

### Stop a single monitor

```bash
pkill -f vllm_monitor.py           # stop all monitor processes
kill <PID>                          # stop a specific one
```

---

## Live dashboard

### Start the viewer

```bash
python log_viewer.py
# or
python log_viewer.py --config instances.yaml --port 9999
```

Then open `http://localhost:9999` in a browser. The dashboard polls `/api/metrics` every
3 seconds and updates all cards without a full page reload.

### Dashboard cards

Each instance gets one card showing:

| Section | Fields |
|---|---|
| Status | dot colour (green = live, orange = stale >30s, grey = no data), age of last record |
| Queue | `requests_running`, `requests_waiting` |
| Latency | TTFT, E2E, queue time, TPOT — each as `avg / p99` in ms |
| KV cache | GPU usage bar (blue), CPU usage bar (purple) |
| Prefix cache | hit rate (interval); external hit rate if KV Connector is active |
| Throughput | gen tok/s, prefill tok/s, success req/s, preemptions/s |

The viewer reads directly from JSONL files — it does not interact with vllm_monitor processes
and can be started and stopped independently at any time.

---

## Metrics collected

### Field types

| Type | Storage | Example |
|---|---|---|
| `gauge` | plain float | `requests_running = 3.0` |
| `counter_rate` | float/s (delta ÷ dt) | `gen_tokens_per_sec = 185.4` |
| `histogram _avg` | plain float (cumulative mean since vLLM start) | `ttft_seconds_avg = 0.31` |
| `histogram __p99` | plain float (companion p99 estimate) | `ttft_seconds_avg__p99 = 0.51` |
| `histogram __buckets` | `dict[str, float]` — cumulative bucket counts | `ttft_seconds__buckets = {"0.01": 12, ...}` |
| `counter_cumulative` | raw cumulative float | `prefix_cache_queries = 10500.0` |
| `derived` | computed each poll | `prefix_cache_hit_rate = 0.98` |

### Histogram bucket fields

All latency and token-count histograms persist their raw cumulative `_bucket` data every poll.
This allows downstream analysis to recover the **full request distribution** for any time window,
not just the scalar mean or p99.

| Bucket field | Source histogram |
|---|---|
| `e2e_latency_seconds__buckets` | `vllm:e2e_request_latency_seconds` |
| `ttft_seconds__buckets` | `vllm:time_to_first_token_seconds` |
| `tpot_seconds__buckets` | `vllm:time_per_output_token_seconds` |
| `queue_time_seconds__buckets` | `vllm:request_queue_time_seconds` |
| `prefill_time_seconds__buckets` | `vllm:request_prefill_time_seconds` |
| `decode_time_seconds__buckets` | `vllm:request_decode_time_seconds` |
| `inference_time_seconds__buckets` | `vllm:request_inference_time_seconds` |
| `request_prompt_tokens__buckets` | `vllm:request_prompt_tokens` |
| `request_generation_tokens__buckets` | `vllm:request_generation_tokens` |
| `request_max_generation_tokens__buckets` | `vllm:request_max_num_generation_tokens` |

Bucket values are **cumulative counters** (monotonically increasing). To recover a distribution:

```python
# Full-experiment distribution: diff last snapshot against first
first = records[0]["samples"][0]["queue_time_seconds__buckets"]
last  = records[-1]["samples"][0]["queue_time_seconds__buckets"]
delta = {le: last[le] - first.get(le, 0) for le in last}

# Per-interval distribution: diff consecutive snapshots
delta = {le: cur[le] - prev.get(le, 0) for le in cur}
```

The p99 companion fields (e.g. `ttft_seconds_avg__p99`) are estimated via linear interpolation
across bucket boundaries. Accuracy depends on bucket density — they are approximations, not
exact per-request values.

### Complete field list

#### KV cache
| Field | Source metric | Notes |
|---|---|---|
| `gpu_kv_cache_usage_frac` | `vllm:kv_cache_usage_perc` | 0–1 fraction |
| `gpu_kv_cache_free_frac` | derived | `1 - usage` |
| `cpu_kv_cache_usage_frac` | `vllm:cpu_cache_usage_perc` | CPU offload usage |

#### Prefix cache (local GPU HBM)
| Field | Source metric | Notes |
|---|---|---|
| `prefix_cache_queries` | `vllm:prefix_cache_queries` | cumulative block queries |
| `prefix_cache_hits` | `vllm:prefix_cache_hits` | cumulative block hits |
| `prefix_cache_hit_rate` | derived | `dh/dq` over last interval; `null` when no requests |
| `prefix_cache_hit_rate_cumulative` | derived | lifetime `h/q`; only present on first poll |

#### External prefix cache (KV Connector — OffloadingConnector / LMCache)
| Field | Source metric | Notes |
|---|---|---|
| `external_prefix_cache_queries` | `vllm:external_prefix_cache_queries_total` | cumulative |
| `external_prefix_cache_hits` | `vllm:external_prefix_cache_hits_total` | cumulative |
| `external_prefix_cache_hit_rate` | derived | `dh/dq` over last interval |
| `external_prefix_cache_hit_rate_cumulative` | derived | lifetime `h/q`; only on first poll |

#### Request queue
| Field | Source metric |
|---|---|
| `requests_running` | `vllm:num_requests_running` |
| `requests_waiting` | `vllm:num_requests_waiting` |
| `preemptions_per_sec` | `vllm:num_preemptions_total` |

#### Latency (cumulative mean since vLLM start)
| Field | Source metric | Notes |
|---|---|---|
| `e2e_latency_seconds_avg` | `vllm:e2e_request_latency_seconds` | client submit → last token |
| `ttft_seconds_avg` | `vllm:time_to_first_token_seconds` | queue + prefill |
| `tpot_seconds_avg` | `vllm:time_per_output_token_seconds` | decode ÷ (output_tokens−1) |
| `queue_time_seconds_avg` | `vllm:request_queue_time_seconds` | scheduler wait only |
| `prefill_time_seconds_avg` | `vllm:request_prefill_time_seconds` | prompt processing |
| `decode_time_seconds_avg` | `vllm:request_decode_time_seconds` | total decode phase |
| `inference_time_seconds_avg` | `vllm:request_inference_time_seconds` | prefill + decode |

Latency decomposition:
```
E2E latency = queue_time + prefill_time + decode_time
TTFT        = queue_time + prefill_time
TPOT        = decode_time / (output_tokens - 1)
```

Note: `e2e_latency` is measured from when the request enters the vLLM engine to when the
last token is written out. It does not include client-side network round-trip time.

#### Token throughput
| Field | Source metric |
|---|---|
| `prefill_tokens_per_sec` | `vllm:prompt_tokens_total` (delta ÷ dt) |
| `gen_tokens_per_sec` | `vllm:generation_tokens_total` (delta ÷ dt) |

#### Request outcomes
| Field | Source metric |
|---|---|
| `request_success_per_sec` | `vllm:request_success_total` |
| `request_failure_per_sec` | `vllm:request_failure_total` |

#### Context and output length
| Field | Notes |
|---|---|
| `request_prompt_tokens_avg` | cumulative mean prompt length (tokens) |
| `request_generation_tokens_avg` | cumulative mean generation length (tokens) |
| `request_max_generation_tokens_avg` | cumulative mean of max generation budget |
| `request_prompt_tokens__buckets` | cumulative histogram buckets — used by analyzer for true distribution |

#### Speculative decoding (absent when SD not enabled)
| Field | Source metric |
|---|---|
| `spec_tokens_accepted_per_sec` | `vllm:spec_decode_num_accepted_tokens_total` |
| `spec_tokens_draft_per_sec` | `vllm:spec_decode_num_draft_tokens_total` |
| `spec_tokens_emitted_per_sec` | `vllm:spec_decode_num_emitted_tokens_total` |
| `spec_decode_acceptance_rate` | derived: `accepted_per_sec / draft_per_sec` |

---

## How prefix cache hit rate is calculated

`vllm:prefix_cache_queries` and `vllm:prefix_cache_hits` are Prometheus **counters**
(monotonically increasing, unit = KV cache blocks, not requests).

Each poll:
```
dq = prefix_cache_queries(now) - prefix_cache_queries(prev)   # new block queries this interval
dh = prefix_cache_hits(now)    - prefix_cache_hits(prev)      # new block hits this interval
hit_rate = dh / dq                                            # fraction, 0–1
```

- `dq = 0` (no requests this interval) → `hit_rate = null`, plot shows a gap
- This is equivalent to `rate(hits[interval]) / rate(queries[interval])` in PromQL
- The external prefix cache hit rate is computed identically from the `external_*` counters

---

## How histogram distributions are recovered from bucket data

`_bucket` series in Prometheus store **cumulative counts** per boundary since vLLM start:

```
vllm:request_queue_time_seconds_bucket{le="0.01"}  = 89    # requests with queue_time ≤ 0.01s, all time
vllm:request_queue_time_seconds_bucket{le="0.05"}  = 130
vllm:request_queue_time_seconds_bucket{le="+Inf"}  = 200   # total requests, all time
```

The monitor stores this full bucket dict every poll. Analysis options:

**Full-experiment distribution** (most common):
```python
# diff last snapshot against first non-zero snapshot
delta = {le: last[le] - first.get(le, 0) for le in last}
# delta gives request counts per bucket for the entire monitored window
```

**Per-interval distribution** (time-resolved):
```python
# diff consecutive snapshots to get counts for that poll interval
delta = {le: cur[le] - prev.get(le, 0) for le in cur}
```

**CDF plot** (to read off p50/p90/p99):
```python
total = delta["+Inf"]  # or last le value
cdf   = {le: cnt / total for le, cnt in sorted_delta.items()}
```

The p99 values stored in `*__p99` fields are computed from the same buckets via linear
interpolation and are approximations. Accuracy depends on bucket boundary density.

---

## How context length distribution is calculated

`vllm:request_prompt_tokens_bucket` stores cumulative counts per bucket since vLLM start:
```
bucket{le="512"}  = 120   # requests with prompt ≤ 512 tokens, all time
bucket{le="1024"} = 280   # requests with prompt ≤ 1024 tokens, all time
```

The monitor stores this full bucket dict every poll. The analyzer then computes:
```
window_count(bin) = bucket_latest(le) - bucket_earliest(le)
                  - (bucket_latest(prev_le) - bucket_earliest(prev_le))
```

This gives the number of requests in each token range **during the monitored window**,
not the full lifetime history. If `--last-minutes 60` is used, the distribution reflects
only that hour.

---

## Requirements

- Python 3.9+
- `vllm_monitor.py`: zero dependencies (stdlib only — `urllib`, `json`, `re`, `math`)
- `vllm_analyze.py`: `matplotlib` only (`pip install matplotlib`)
- `launch_monitors.py` / `vllm_log_viewer.py`: `pyyaml` (`pip install pyyaml`)

---

## Operational checks

### Check all instances are running

```bash
python launch_monitors.py --status
```

### Check a specific instance is writing data

```bash
# File growing?
ls -la vllm_logs/GLM-5-direct15/metrics.jsonl

# Latest timestamp
tail -1 vllm_logs/GLM-5-direct15/metrics.jsonl | python3 -c \
    "import json,sys; print(json.loads(sys.stdin.read())['ts'])"

# Operational log (fetch errors, restarts)
tail -f vllm_logs/GLM-5-direct15/vllm_monitor.log
```

### Watch raw JSONL with formatting

```bash
tail -f vllm_logs/GLM-5-direct15/metrics.jsonl \
  | jq '.samples[0] | {queue: .queue_time_seconds_avg, ttft: .ttft_seconds_avg, e2e: .e2e_latency_seconds_avg}'
```

### Verify metrics endpoint before starting

```bash
curl -s http://YOUR_HOST_IP:PORT/metrics | grep -E \
    "^vllm:num_requests_running|^vllm:kv_cache_usage_perc|\
^vllm:prefix_cache_queries|^vllm:request_prompt_tokens"

# Confirm bucket data is available
curl -s http://YOUR_HOST_IP:PORT/metrics | grep "^vllm:request_queue_time_seconds_bucket"
```

---

## Offline analysis

### Generate charts

```bash
# All data + per-day breakdown
python3 vllm_analyze.py vllm_logs/GLM-5-direct15/metrics.jsonl

# Last hour only
python3 vllm_analyze.py vllm_logs/GLM-5-direct15/metrics.jsonl --last-minutes 60

# Custom output directory
python3 vllm_analyze.py vllm_logs/GLM-5-direct15/metrics.jsonl --out-dir ./plots

# Skip per-day breakdown, only write the 'all' folder
python3 vllm_analyze.py vllm_logs/GLM-5-direct15/metrics.jsonl --no-daily
```

### Output structure

```
experiment_1/
├── all/
│   ├── 01_requests.png
│   ├── 02_prefix_hit_rate.png
│   ├── 03_external_prefix_hit_rate.png
│   ├── 04_context_length_distribution.png
│   ├── 05_token_throughput.png
│   └── 06_latency.png
├── 2026-03-20/
│   └── ...
└── 2026-03-21/
    └── ...
```

Change `EXPERIMENT_ID` at the top of `vllm_analyze.py` before each run to avoid
overwriting previous results.

### Chart descriptions

| File | Content |
|---|---|
| `01_requests.png` | Three stacked subplots: running + waiting overlaid, running only, waiting only |
| `02_prefix_hit_rate.png` | Interval prefix cache hit rate (local GPU HBM) — filled area, avg/peak annotated |
| `03_external_prefix_hit_rate.png` | Interval external prefix cache hit rate (KV Connector) |
| `04_context_length_distribution.png` | Prompt token length distribution (from histogram buckets) |
| `05_token_throughput.png` | Gen tok/s and prefill tok/s over time |
| `06_latency.png` | TTFT and TPOT (ms) over time |

### Transfer charts to local machine

```bash
scp -r user@server:~/path/to/vllm_logs/experiment_1/ ~/Desktop/
```

Or serve directly from the server:
```bash
cd vllm_logs && python3 -m http.server 8888
# open http://SERVER_IP:8888 in local browser
```

---

## CLI reference

### vllm_monitor.py

| Argument | Env var | Default | Description |
|---|---|---|---|
| `--host` | `VLLM_HOST` | `http://localhost:8000` | vLLM server base URL |
| `--interval` | `VLLM_INTERVAL` | `15` | Poll interval (seconds) |
| `--log-dir` | `VLLM_LOG_DIR` | `./vllm_logs` | Output directory |
| `--mode` | — | `monitor` | Mode label in JSONL records (set to instance name by launcher) |
| `--timeout` | — | `10` | HTTP request timeout (seconds) |
| `--no-stdout` | — | off | Suppress live summary (used by launcher when daemonising) |
| `--summary-every` | — | `1` | Print summary every N polls |

### launch_monitors.py

| Argument | Default | Description |
|---|---|---|
| `--config` | `instances.yaml` | Path to instances config file |
| `--daemon` | off | Detach all child processes and return immediately |
| `--stop` | — | Send SIGTERM to all running monitor processes |
| `--status` | — | Print status table for all configured instances |

### vllm_log_viewer.py

| Argument | Default | Description |
|---|---|---|
| `--config` | `instances.yaml` | Path to instances config file |
| `--port` | `9999` | Port to serve the dashboard on |

### vllm_analyze.py

| Argument | Default | Description |
|---|---|---|
| `log` | (required) | Path to `metrics.jsonl` |
| `--out-dir` | same dir as jsonl | Base output directory |
| `--last-minutes` | all data | Only analyse last N minutes |
| `--no-daily` | off | Skip per-day folders; only write the `all/` folder |
