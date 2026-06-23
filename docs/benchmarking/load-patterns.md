# Load patterns and the open-loop model

How the load generator schedules and sends traffic. For the full client YAML schema see [client config](../configuration/client-config.md); for what a run produces see [artifacts & analysis](artifacts-and-analysis.md).

## Open-loop model

LA-Boom's load generator is **open-loop**: it precomputes a schedule of send timestamps and dispatches each request at its planned time regardless of whether previous requests have completed. This decouples offered load from system latency, so a backlog under saturation shows up as growing queue depth rather than throttled arrivals.

Key behaviors:

- **Precomputed schedule** — `scheduler.build_schedule()` builds a list of monotonic send timestamps.
- **Dispatch model** — sync paths (router `sync`, and the `aibrix`/`litellm`/`boom` gateways) use one worker thread per request; the `async_pubsub` path uses a single main-thread submit loop plus one ZMQ SUB listener thread that matches results back.
- **Warmup** — `warmup_reqs` requests are sent before the timed window to prime caches and connections (using the configured `backend`, not always `/enqueue`).
- **Schedule alignment** — in `load_runner`, planned times are re-aligned **after warmup** so the first timed request fires immediately at load start.
- **Reproducibility** — `loadgen_seed` (default 12345) seeds the stochastic patterns (`poisson`, `rand`).

## RPS patterns

Set via `load_pattern` in the client config. This table summarizes behavior; for each parameter's type and default see the [`load_pattern` section of the client config](../configuration/client-config.md#load_pattern-section).

| Pattern | Behavior | Key params |
|---------|----------|------------|
| `dump` | All requests at t=0 | — |
| `det` | Even spacing within each 1s window | `rate_rps` |
| `poisson` | Exponential inter-arrivals | `rate_rps` |
| `bursty` | Alternating on/off windows | `burst_rps_on`, `burst_rps_off`, `burst_on_s`, `burst_off_s` |
| `steps` | Piecewise RPS schedule | `step_schedule` (e.g. `"0:3,30:5,60:1"`) |
| `rand` | Random integer RPS per epoch | `rand_epoch_s` |

## Backends and transports

The backend (`backend`) and, for the router, the transport (`transport.mode`) determine the request path.

| Backend | Transport | Path |
|---------|-----------|------|
| `router` | `sync` | `POST /enqueue` (blocks until result) |
| `router` | `async_pubsub` | `POST /submit` + subscribe to ZMQ results (`tcp://<router>:5559` / NodePort 30559) |
| `aibrix` | (HTTP) | OpenAI `/v1/chat/completions` with a `routing-strategy` header |
| `litellm` / `boom` | (HTTP) | OpenAI chat via the gateway (Bearer auth), optional SSE streaming |

> Use `backend: router` for clean latency measurement. Gateways (`litellm`/`boom`) add auth/spend/rate-limit layers that affect timing.

For `async_pubsub`, the run terminates when Prometheus shows all vLLM replicas idle for `idle_zero_running_s`, with a backstop of no completion for `idle_timeout_s`.

## Generation parameters

Token limits and decode behavior are set under `generation.*` (including replay of per-request output lengths from a prior run). See the [`generation` section of the client config](../configuration/client-config.md#generation-section) for the field list and defaults.

## Prompt sources

Set via `prompt_source`:

- `file` — static JSON prompts (`{short, medium, long}`)
- `hf-lmsys` — LMSYS Chat 1M from HuggingFace (single-turn or [multi-turn](multi-turn.md))
- `codeflow` — [CodeFlowBench](codeflowbench.md) competitive programming dataset

## Multi-model client routing

For `litellm`/`boom`, `multi_model` distributes traffic across models either by `fraction` (weighted pick) or `mirror` (clone each request to all targets).

## SLO annotations

`slo.mix` injects per-request SLO fields (TTFT/TPOT/E2E targets) into router payloads, exercising [SLO-aware routing](../architecture/slo-aware-routing.md).

## See also

- [Client config reference](../configuration/client-config.md)
- [Multi-turn benchmarking](multi-turn.md)
- [Artifacts and analysis](artifacts-and-analysis.md)
