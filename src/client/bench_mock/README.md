# Gateway overhead benchmark (LiteLLM-style)

Clone of the methodology on [LiteLLM Benchmarks](https://docs.litellm.ai/docs/benchmarks):
flood each gateway with Locust against a **fake OpenAI** backend, report median /
p95 / p99 / avg / RPS and a **Gateway Overhead** custom metric, at **2 vs 4**
instances.

## Paths

| Path | Compose override | Scale |
|------|------------------|-------|
| `litellm` | `configs/litellm/` → fake endpoint | `--scale litellm=N` |
| `litellm_network_mock` | same + `proxy.network_mock.yaml` | `--scale litellm=N` |
| `llmla_sidecarless` | `configs/llmla_sidecarless/` Python `external-push` | `--scale router=N` |
| `llmla_sidecar` | `configs/llmla_sidecar/` Python pull + sidecar | `--scale router=1 --scale sidecar=N` |
| `llmla_go_sidecarless` | `configs/llmla_go_sidecarless/` Go `external-push` | `--scale router=N` |
| `llmla_go_sidecar` | `configs/llmla_go_sidecar/` Go pull + sidecar | `--scale router=1 --scale sidecar=N` |
| `boom_direct` | `configs/boom_direct/` (needs `boom-gateway` image) | `--scale boom=N` |

Go images build from `src/core/services/go` via `docker/Dockerfile.*-go` (first run compiles Go; set `GOPROXY` if needed). `run_all.sh` includes Go cells by default (`RUN_GO=0` to skip).

Locust always targets `http://127.0.0.1:14000` (nginx LB).

## Quick start

```bash
cd src/client/bench_mock

# One cell (LiteLLM defaults: 1000 users, 500 spawn/s, 5m)
./scripts/run_path.sh litellm 2

# LLM-LA sidecarless / with sidecar
./scripts/run_path.sh llmla_sidecarless 2
./scripts/run_path.sh llmla_sidecar 4

# Full matrix (skips Boom unless RUN_BOOM=1)
./scripts/run_all.sh

# Boom (image must exist locally or via BOOM_IMAGE=...)
RUN_BOOM=1 ./scripts/run_path.sh boom_direct 2
```

Smoke without a long Locust run:

```bash
SKIP_LOCUST=1 SKIP_DOWN=1 ./scripts/run_path.sh llmla_sidecarless 2
curl -s http://127.0.0.1:14000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"served-model","messages":[{"role":"user","content":"hi"}],"max_tokens":8}'
```

## Locust knobs

| Env | Default | LiteLLM doc equivalent |
|-----|---------|------------------------|
| `USERS` | `1000` | 1000 users |
| `SPAWN_RATE` | `500` | 500 ramp-up |
| `RUN_TIME` | `5m` | (their Portkey section used 5 minutes) |
| `BENCH_MODEL` | path-specific | model name in payload |
| `MOCK_LATENCY_MS` | `0` | fake backend sleep |
| `PROMPT_REPEAT` | `40` (via `run_path.sh`) | prompt size / no-cache filler |
| `CLEAR_RESULTS` | `1` (`run_all.sh`) | wipe `results/` before matrix |

Locust is started with `--exit-code-on-error 0`: a few failed requests are
recorded in `locust_failures.csv` / the summary, but they do **not** abort the
matrix (same spirit as LiteLLM publishing tables despite occasional errors).
`run_all.sh` continues to the next cell on script failure. `run_path.sh` always
tears down compose (unless `SKIP_DOWN=1`).

**Pull-mode scaling:** for `llmla_sidecar` / `llmla_go_sidecar`, “N instances”
means N sidecars behind **one** router. Scaling routers in pull mode splits the
queue (Locust → router A, sidecar pulls router B) and produces mass `status=0`
timeouts.

## Layout

```text
bench_mock/
├── docker-compose.yml          # mock-vllm + redis + gateway-lb
├── configs/<path>/             # one folder per gateway arm
├── locust/                     # locustfile + requirements
├── scripts/                    # run_path / run_locust / summarize / run_all
├── results/<path>_<N>inst/     # CSV, HTML, summary.md (gitignored)
└── fake_openai/                # pointer to e2e mock_vllm
```

## Publishing results

Copy `results/*/summary.md` into
`analysis-notebooks/results-log/06-gateway-overhead-mock/` and fill the tables in
[`docs/benchmarking/gateway-overhead-benchmark.md`](../../../docs/benchmarking/gateway-overhead-benchmark.md).
