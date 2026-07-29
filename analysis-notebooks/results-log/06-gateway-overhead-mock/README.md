# 06 — Gateway overhead mock (LiteLLM-style)

Campaign harness: [`src/client/bench_mock/`](../../../src/client/bench_mock/).

Spec / tables: [`docs/benchmarking/gateway-overhead-benchmark.md`](../../../docs/benchmarking/gateway-overhead-benchmark.md).

## Entries

Copy each `src/client/bench_mock/results/<path>_<N>inst/summary.md` here after a run,
named like:

| File | Cell |
|------|------|
| `litellm_2inst.md` | LiteLLM × 2 |
| `litellm_4inst.md` | LiteLLM × 4 |
| `llmla_sidecarless_2inst.md` | LLM-LA sidecarless × 2 |
| `llmla_sidecarless_4inst.md` | LLM-LA sidecarless × 4 |
| `llmla_sidecar_2inst.md` | LLM-LA Python + sidecar × 2 |
| `llmla_sidecar_4inst.md` | LLM-LA Python + sidecar × 4 |
| `llmla_go_sidecarless_2inst.md` | LLM-LA Go sidecarless × 2 |
| `llmla_go_sidecarless_4inst.md` | LLM-LA Go sidecarless × 4 |
| `llmla_go_sidecar_2inst.md` | LLM-LA Go + sidecar × 2 |
| `llmla_go_sidecar_4inst.md` | LLM-LA Go + sidecar × 4 |
| `boom_direct_2inst.md` | BooM direct × 2 |
| `boom_direct_4inst.md` | BooM direct × 4 |

## Load recipe (fixed across arms)

- Locust: 1000 users, 500 spawn rate
- Fake OpenAI: e2e `mock-vllm`
- Instance counts: 2 and 4
