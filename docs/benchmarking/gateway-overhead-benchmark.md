# Gateway Overhead Benchmarks (LiteLLM-style)

Benchmarks for **LiteLLM** and **LLM-LA** gateways against a **fake OpenAI
endpoint** — same methodology as
[LiteLLM’s Benchmarks page](https://docs.litellm.ai/docs/benchmarks).

Harness: [`src/client/bench_mock/`](../../src/client/bench_mock/).
Load: Locust **1000 users**, **500** ramp-up, **5 minutes**. Backend mock latency ≈ 0 ms.

> **Go (4×): 350 ms median · ~760 RPS.**
> LiteLLM true overhead (4×): **6 ms** (`x-litellm-overhead-duration-ms`).

Only clean cells are shown below (near-zero request errors). The Go pull+sidecar
arm was dropped from this report (nginx ephemeral-port exhaustion under load).

## Machine Spec used for testing

Target class (LiteLLM published):

- 4 CPU
- 8 GB RAM

## Configuration

| Approach | Gateway | Topology | What “N instances” means |
|----------|---------|----------|---------------------------|
| **LiteLLM** | LiteLLM proxy | proxy → mock | scale `litellm=N` |
| **Python sidecarless** | Python router | `external-push` → mock | scale `router=N` |
| **Python + sidecar** | Python router + sidecar | `pull` → sidecar → mock | **1 router**, scale `sidecar=N` |
| **Go** | Go gateway | `external-push` → mock | scale `router=N` |

- **Database:** not used (overhead-focused).
- **Redis:** LLM-LA only (bookkeeping; not a request-path cache).

```text
Locust → nginx LB (:14000) → gateway path → mock-vllm (fake OpenAI)
```

### How overhead is measured

| Gateway | Overhead source |
|---------|-----------------|
| LiteLLM | `x-litellm-overhead-duration-ms` (**true proxy tax**) |
| Python / Go | Custom metric ≈ e2e latency (mock ~0 ms; no LiteLLM-style header) |

Ignore Locust **Aggregated RPS** (double-counts the Custom overhead event).

---

## 2 Instance results

### LiteLLM

| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| POST | /chat/completions | 620 | 960 | 1500 | 646.66 | 675.66 |
| Custom | Gateway Overhead Duration (ms) | 13 | 23 | 30 | 13.14 | 675.66 |

### Python sidecarless

| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| POST | /chat/completions | 2600 | 8300 | 8700 | 2548.76 | 297.48 |
| Custom | Gateway Overhead Duration (ms) | 2600 | 8300 | 8700 | 2548.76 | 297.48 |

### Python + sidecar

| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| POST | /chat/completions | 1800 | 4300 | 4600 | 2142.00 | 343.27 |
| Custom | Gateway Overhead Duration (ms) | 1800 | 4300 | 4600 | 2142.00 | 343.27 |

### Go

| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| POST | /chat/completions | 580 | 1000 | 1300 | 580.62 | 741.63 |
| Custom | Gateway Overhead Duration (ms) | 580 | 1000 | 1300 | 578.47 | 741.62 |

---

## 4 Instances

### LiteLLM

| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| POST | /chat/completions | 590 | 760 | 860 | 605.71 | 678.74 |
| Custom | Gateway Overhead Duration (ms) | 6 | 13 | 18 | 6.80 | 678.74 |

### Python sidecarless

| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| POST | /chat/completions | 100 | 5100 | 7100 | 964.44 | 578.44 |
| Custom | Gateway Overhead Duration (ms) | 100 | 5100 | 7100 | 964.44 | 578.44 |

### Python + sidecar

| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| POST | /chat/completions | 1600 | 1900 | 2300 | 1677.62 | 410.04 |
| Custom | Gateway Overhead Duration (ms) | 1600 | 1900 | 2300 | 1677.62 | 410.04 |

### Go

| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| POST | /chat/completions | 350 | 1300 | 1700 | 532.72 | 760.16 |
| Custom | Gateway Overhead Duration (ms) | 350 | 1300 | 1700 | 530.09 | 760.14 |

---

## Key Findings

- **Go** leads on e2e median and RPS at both scales (580→350 ms, ~742→760 RPS).
- **LiteLLM** is the stable baseline; only arm with a true measured proxy tax (13→6 ms).
- **Python sidecarless** scales hard at 4× (median 2600→100 ms) but keeps a heavy tail (p95 ~5 s).
- **Python + sidecar** is the slowest clean arm (~1.6–1.8 s median); 2→4 mainly shrinks the tail.

---

## Multi-path comparison (4×)

| Metric | Go | LiteLLM | Python sidecarless | Python + sidecar |
| --- | ---: | ---: | ---: | ---: |
| Median Latency (ms) | **350** | 590 | 100† | 1600 |
| p95 Latency (ms) | 1300 | **760** | 5100 | 1900 |
| p99 Latency (ms) | 1700 | **860** | 7100 | 2300 |
| Average Latency (ms) | **533** | 606 | 964 | 1678 |
| Current RPS | **760** | 679 | 578 | 410 |
| Overhead median (ms) | ≈ e2e 350 | **6** (header) | ≈ e2e 100 | ≈ e2e 1600 |

† Uneven distribution (very low median, multi-second p95).

Lower is better for latency; higher is better for RPS.

---

## Ranking (4×)

Sorted for **predictable end-to-end latency + throughput** under this Locust load
(primary: median, then p95, then RPS). LiteLLM’s header overhead is noted but not
used as the e2e rank key (other arms cannot report the same metric).

| Rank | Approach | Median | p95 | RPS | Why |
| ---: | -------- | -----: | --: | --: | --- |
| 1 | **Go** | 350 | 1300 | 760 | Best balanced e2e + highest RPS |
| 2 | **LiteLLM** | 590 | 760 | 679 | Best tail among stable arms; true 6 ms tax |
| 3 | **Python sidecarless** | 100 | 5100 | 578 | Fast median / RPS, poor predictability |
| 4 | **Python + sidecar** | 1600 | 1900 | 410 | Clean but slowest (pull hop) |

---

## Are these benchmarks completely consistent?

**Mostly yes on harness, not fully apples-to-apples on semantics.**

| Dimension | Consistent? | Detail |
|-----------|:-----------:|--------|
| Load generator | Yes | Same Locust file, 1000 / 500 / 5m, same prompt style |
| Backend | Yes | Shared `mock-vllm`, ~0 ms injected latency |
| Entry point | Yes | All via nginx `:14000` |
| Host / run | Yes | Same `run_all` matrix on one machine |
| “N instances” meaning | No | Proxies vs routers vs sidecars (pull keeps 1 router) |
| Request path | No | `external-push` (LiteLLM / Python sidecarless / Go) vs `pull` (+sidecar) |
| Overhead metric | No | LiteLLM = header tax; Python/Go custom ≈ full e2e |
| Failure filtering | Yes (this page) | Only clean cells; Go+sidecar omitted |

So: fair for **comparative gateway e2e under identical Locust load**; do **not**
treat the Custom “overhead” row as the same quantity across LiteLLM vs LLM-LA,
and do not treat “4 instances” as identical hardware topology across arms.

---

## Locust settings

- 1000 Users
- 500 user Ramp Up
- Run time: 5 minutes

```bash
cd src/client/bench_mock
./scripts/run_all.sh
./scripts/run_path.sh litellm 4
```

## See also

- Harness README: [`src/client/bench_mock/README.md`](../../src/client/bench_mock/README.md)
- LiteLLM upstream page: https://docs.litellm.ai/docs/benchmarks
