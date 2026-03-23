# vLLM Metrics Monitor

A lightweight, zero-dependency background monitor for vLLM's `/metrics` endpoint.
Collects, logs, and visualises inference performance metrics including prefix cache hit rate,
KV cache usage, latency, throughput, and request context length distribution.

---

## Files

| File | Description |
|---|---|
| `vllm_monitor.py` | Background daemon — polls `/metrics` every N seconds, writes `metrics.jsonl` |
| `vllm_analyze.py` | Offline analysis — reads `metrics.jsonl`, saves one PNG per chart |

---

## Architecture

### Data flow

```
vLLM /metrics endpoint  (Prometheus exposition format)
        |
        |  curl every --interval seconds
        v
vllm_monitor.py
        |
        |  parses raw text, computes derived fields, writes one JSON line per poll
        v
vllm_logs/metrics.jsonl        (primary data store)
vllm_logs/vllm_monitor.log     (operational log)
        |
        |  offline
        v
vllm_analyze.py
        |
        |  reads jsonl, plots 6 charts per scope (all data + one folder per day)
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
        01_requests.png
        ...
    2026-03-21/
        01_requests.png
        ...
```

### JSONL record format

Every poll writes one line to `metrics.jsonl`. Format matches `prom_utils.py` and
`metrics_prom.py` exactly so records can be consumed by the same downstream tooling:

```json
{
  "ts": "2026-03-20T15:59:53.914813+00:00",
  "mode": "monitor",
  "samples": [{
    "instance": "http://7.216.57.75:8077",
    "requests_running": 3.0,
    "gpu_kv_cache_usage_frac": 0.42,
    "prefix_cache_hit_rate": 0.98,
    "external_prefix_cache_hit_rate": 0.72,
    "ttft_seconds_avg": 0.31,
    "gen_tokens_per_sec": 185.4,
    "request_prompt_tokens__buckets": {
      "128": 10.0, "256": 45.0, "512": 120.0, "...": "..."
    }
  }]
}
```

Error ticks (fetch failed) are also written:
```json
{"ts": "...", "mode": "monitor", "samples": [], "error": "timed out", "consecutive_failures": 3}
```

---

## Metrics collected

### Field types

| Type | Storage | Example |
|---|---|---|
| `gauge` | plain float | `requests_running = 3.0` |
| `counter_rate` | float/s (delta ÷ dt) | `gen_tokens_per_sec = 185.4` |
| `histogram` | plain float (cumulative mean) | `ttft_seconds_avg = 0.31` |
| `histogram p99` | plain float (companion field) | `ttft_seconds_avg__p99 = 0.51` |
| `counter_cumulative` | raw cumulative float | `prefix_cache_queries = 10500.0` |
| `derived` | computed each poll | `prefix_cache_hit_rate = 0.98` |

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
| `prefix_cache_hit_rate_cumulative` | derived | lifetime `h/q`; only on first poll |

#### External prefix cache (KV Connector)
| Field | Source metric | Notes |
|---|---|---|
| `external_prefix_cache_queries` | `vllm:external_prefix_cache_queries_total` | cumulative |
| `external_prefix_cache_hits` | `vllm:external_prefix_cache_hits_total` | cumulative |
| `external_prefix_cache_hit_rate` | derived | `dh/dq` over last interval |

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

#### Context length
| Field | Notes |
|---|---|
| `request_prompt_tokens_avg` | cumulative mean prompt length (tokens) |
| `request_prompt_tokens__buckets` | full histogram bucket dict — used by analyze for true distribution |

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

## How context length distribution is calculated

`vllm:request_prompt_tokens_bucket` stores cumulative counts per bucket since vLLM start:
```
bucket{le="512"}  = 120   # requests with prompt <= 512 tokens, all time
bucket{le="1024"} = 280   # requests with prompt <= 1024 tokens, all time
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

---

## Usage

### Start monitoring (background, survives SSH logout)

```bash
nohup python3 vllm_monitor.py \
    --host http://YOUR_HOST_IP:PORT \
    --interval 5 \
    --log-dir ./vllm_logs \
    --no-stdout &

echo "PID: $!"
```

### Check it is running

```bash
# Process still alive?
pgrep -f vllm_monitor.py

# Data being written? (file size should grow)
ls -la vllm_logs/metrics.jsonl

# Latest timestamp in data
tail -1 vllm_logs/metrics.jsonl | python3 -c \
    "import json,sys; print(json.loads(sys.stdin.read())['ts'])"

# Live operational log
tail -f vllm_logs/vllm_monitor.log
```

### Stop

```bash
kill $(pgrep -f vllm_monitor.py)
```

### Generate charts

```bash
# All data + per-day breakdown (default)
python3 vllm_analyze.py vllm_logs/metrics.jsonl

# Last hour only
python3 vllm_analyze.py vllm_logs/metrics.jsonl --last-minutes 60

# Custom output directory
python3 vllm_analyze.py vllm_logs/metrics.jsonl --out-dir ./plots

# Skip per-day breakdown, only write the 'all' folder
python3 vllm_analyze.py vllm_logs/metrics.jsonl --no-daily
```

### Output structure

Charts are grouped under `experiment_<EXPERIMENT_ID>/` (set `EXPERIMENT_ID` at the top of
`vllm_analyze.py`). Two levels of output are always produced:

```
experiment_1/
├── all/                        ← full dataset across all time
│   ├── 01_requests.png
│   ├── 02_prefix_hit_rate.png
│   ├── 03_external_prefix_hit_rate.png
│   ├── 04_context_length_distribution.png
│   ├── 05_token_throughput.png
│   └── 06_latency.png
├── 2026-03-20/                 ← one folder per calendar day found in the data
│   ├── 01_requests.png         ← title includes [2026-03-20] label
│   └── ...
└── 2026-03-21/
    └── ...
```

The day is determined from the local time of the `ts` field in each record
(e.g. `"ts": "2026-03-20T15:59:53.914813+00:00"` → folder `2026-03-20`).

### Chart descriptions

| File | Content |
|---|---|
| `01_requests.png` | Three stacked subplots: (1) running + waiting overlaid, (2) running only, (3) waiting only — all sharing the same time axis |
| `02_prefix_hit_rate.png` | Interval prefix cache hit rate (local GPU HBM) — filled area, with avg/peak annotation |
| `03_external_prefix_hit_rate.png` | Interval external prefix cache hit rate (KV Connector) |
| `04_context_length_distribution.png` | Prompt token length distribution (from histogram buckets) |
| `05_token_throughput.png` | Gen tok/s and prefill tok/s over time |
| `06_latency.png` | TTFT and TPOT (ms) over time |

### Switch experiment

Change `EXPERIMENT_ID` at the top of `vllm_analyze.py` before running:

```python
EXPERIMENT_ID = "2"   # output goes to experiment_2/
```

Each value produces an independent output tree, so results from different runs are never
overwritten.

### Verify metrics endpoint before starting

```bash
# Confirm key metrics are present
curl -s http://YOUR_HOST_IP:PORT/metrics | grep -E \
    "^vllm:num_requests_running|^vllm:kv_cache_usage_perc|\
^vllm:prefix_cache_queries|^vllm:request_prompt_tokens"

# Confirm bucket data is available (needed for context length distribution)
curl -s http://YOUR_HOST_IP:PORT/metrics | grep "^vllm:request_prompt_tokens"
```

### Transfer charts to local machine

```bash
scp -r user@server:~/path/to/vllm_logs/experiment_1/ ~/Desktop/
```

Or serve directly from the server:
```bash
cd vllm_logs && python3 -m http.server 8888
# then open http://SERVER_IP:8888 in local browser
```

---

## CLI reference

### vllm_monitor.py

| Argument | Env var | Default | Description |
|---|---|---|---|
| `--host` | `VLLM_HOST` | `http://localhost:8000` | vLLM server base URL |
| `--interval` | `VLLM_INTERVAL` | `15` | Poll interval (seconds) |
| `--log-dir` | `VLLM_LOG_DIR` | `./vllm_logs` | Output directory |
| `--mode` | — | `monitor` | Mode label in JSONL records |
| `--timeout` | — | `10` | HTTP request timeout (seconds) |
| `--no-stdout` | — | off | Suppress live summary (use when daemonising) |
| `--summary-every` | — | `1` | Print summary every N polls |

### vllm_analyze.py

| Argument | Default | Description |
|---|---|---|
| `log` | (required) | Path to `metrics.jsonl` |
| `--out-dir` | same dir as jsonl | Base output directory; charts go into `<out-dir>/experiment_<EXPERIMENT_ID>/` |
| `--last-minutes` | all data | Only analyse last N minutes |
| `--no-daily` | off | Skip per-day folders; only write the `all/` folder |
