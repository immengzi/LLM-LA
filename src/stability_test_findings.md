# Stability Test Findings: Experiment #1

## Executive Summary

A 24-hour stability test terminated after **~2 hours** because the vLLM inference pods were **severely under-provisioned** for the workload.

**What happened:** Despite configuring `max_tokens: 8192`, the flag `use_dataset_output_len: true` caused the system to use actual dataset reply lengths (up to 9,048 tokens). Each 9,048-token request requires **181 seconds** of inference time, but at 0.3 rps, **54 new requests arrive during that window**. The queue grew faster than it drained until the client timed out after 1,000 seconds.

**Key numbers:**
- Sent 1,819 requests but only 857 (47%) completed
- 962 requests (53%) were lost to timeout
- Per-pod throughput: 50 tok/sec | P95 output: 2,167 tokens (needs 325 tok/sec)
- **6.5x capacity shortfall** for P95 workload

**Fix:** Set `max_tokens: 512` and `use_dataset_output_len: false` so all requests complete in ~10 seconds, within the 3.3-second arrival interval.

---

## Experiment Setup

| Parameter | Value |
|-----------|-------|
| **Config** | `4-2-template-boom-claude-glm.yaml` |
| **Duration Setting** | 24 hours (86,400 seconds) |
| **Load Pattern** | Deterministic at 0.3 rps |
| **Total Requests Configured** | 5,000 |
| **Max Tokens** | 8,192 (hard cap) |
| **Use Dataset Output Lengths** | Yes (CodeFlowBench actual reply lengths) |
| **Backend** | Boom → vLLM Router (pull mode) |
| **vLLM Pods** | 2 replicas (glm5-chat-0, glm5-chat-1) |
| **Per-Pod Throughput** | ~50 tok/sec |

---

## Results

| Metric | Value |
|--------|-------|
| **Requests Sent** | 1,819 |
| **Requests Completed** | 857 (47%) |
| **Requests Lost (timeout)** | 962 (53%) |
| **Wall Clock Time** | 7,072s (~2 hours) |
| **Expected Runtime** | ~4.6 hours (at 0.3 rps, 5,000 requests) |
| **Actual vs Expected** | Terminated 2.6 hours early |

### Why 1,819 Instead of 5,000?

At 0.3 rps, 5,000 requests would take **16,667 seconds (4.6 hours)** to send. The experiment only sent 1,819 requests before the client-side timeout triggered.

### Why Only 857 Completed?

The 962 remaining requests were marked **LOST** after waiting 1,000 seconds for a response. The server was overwhelmed by queue buildup.

---

## Output Length Distribution (Dataset Reality)

| Percentile | Completion Tokens | Inference Time at 50 tok/sec | Arrivals During Inference |
|------------|-------------------|------------------------------|---------------------------|
| **Avg** | 652 | 13.0s | ~4 requests |
| **P95** | 2,167 | 43.3s | ~13 requests |
| **Max** | 9,048 | **181.0s** | ~54 requests |

Despite `max_tokens: 8192` being configured, **`use_dataset_output_len: true`** instructed the system to use actual dataset reply lengths, which ranged from 9 to 9,048 tokens.

---

## Root Cause Analysis

### The Problem: Capacity vs Load Mismatch

| Metric | Capacity | Requirement at 0.3 rps |
|--------|----------|------------------------|
| **Per-pod throughput** | 50 tok/sec | — |
| **Avg output (652 tokens)** | 50 tok/sec | 98 tok/sec (1.9x shortfall) |
| **P95 output (2,167 tokens)** | 50 tok/sec | **325 tok/sec (6.5x shortfall)** |

The system could not keep up with the arrival rate of long-output requests.

### Timeline of Failure

1. **T+0s**: Experiment starts at 0.3 rps
2. **T+23s**: First 422-second request arrives (4,554 tokens)
3. **T+660s**: First 679-second request (9,048 tokens - maximum)
4. **T+1,000s**: Client timeout triggers, 962 threads marked LOST
5. **T+7,070s**: Experiment terminates

### Why the Timeout?

- A single 9,048-token request requires **181 seconds** of inference
- During that time, **54 new requests** arrive (0.3 rps × 181s)
- The queue grows faster than it drains
- After 1,000 seconds of waiting, the client gives up

---

## Key Takeaways

1. **Not a bug** — The system behaved correctly; it was simply under-provisioned for the workload.

2. **`use_dataset_output_len` bypasses `max_tokens`** — The dataset's actual reply lengths (up to 9,048 tokens) were used, not the 8,192 cap.

3. **Duration = 24h, but total_requests = 5,000** — At 0.3 rps, 5,000 requests complete in 4.6 hours. The 24-hour duration was never the limiting factor.

4. **The fix**: Cap output tokens to keep inference times manageable:

```yaml
generation:
  max_tokens: 512
  use_dataset_output_len: false
```

With `max_tokens: 512`, all requests complete in ~10 seconds, well under the 3.3-second arrival interval, preventing queue buildup.

---

## Recommendations for Future Stability Tests

| Goal | Recommended Setting |
|------|---------------------|
| 24h stability test | `max_tokens: 512`, `use_dataset_output_len: false` |
| Test with real lengths | Reduce `rate_rps` to 0.05 or lower |
| Measure throughput limit | Increase `max_tokens` to 128, test at various rps |



Test Three
 Here's the full analysis:

  ---
  What Happened

  Timeline:
  - Experiment started: 06:18:40 UTC, first request c0t0 sent
  - First 15 min: Normal operation, ~9 requests/30s (0.3 RPS)
  - Around 06:48: Submission rate drops from 9→3 per 30s
  - 06:50:23: Last request c571t0 sent
  - 07:05:31: Last completion c571t0 received (907s roundtrip!)
  - After 07:05:31: boom proxy silently stops — no more ZMQ completions
  - ~07:21: idle_timeout 1000s fires, 1267 threads marked LOST
  - Experiment total wall time: 7072s

  Key fact: only 552 of 5000 planned requests succeeded. Boom stalled and HTTP
  submit failures started cascading.

  ---
  Configuration Issues Found

  1. boom_upstream_timeout_seconds: 0 — CRITICAL

  boom_upstream_timeout_seconds: 0

  Set to 0 means no timeout — boom will wait indefinitely on a hung vLLM
  connection. c571t0 took 907s to complete, showing boom was already struggling
  to reach vLLM. Set this to a concrete value like 300 or 600.

  2. step_schedule: "0:5,30:15,60:3" — IGNORED

  pattern: "det"        # ← step_schedule is ONLY used by "steps" pattern
  step_schedule: "0:5,30:15,60:3"  # ← ignored in "det" mode

  If you want the step schedule applied, change pattern: "det" → pattern:
  "steps". Otherwise remove the field to avoid confusion.

  3. boom_max_inflight: 1000 — Possibly too low

  With boom_route_via: router, requests go through the router sidecar before
  reaching vLLM. The router queue was building up (390→368). If boom was waiting
   for router slots and the router was backlogged, boom could have hit its 1000
  inflight limit. Consider raising to 2000-5000 or adding backpressure
  boom_upstream_timeout_seconds: 0

  Set to 0 means no timeout — boom will wait indefinitely on a hung vLLM connection. c571t0 took 907s to complete, showing boom was already struggling to reach vLLM. Set this to a concrete
  value like 300 or 600.

  2. step_schedule: "0:5,30:15,60:3" — IGNORED

  pattern: "det"        # ← step_schedule is ONLY used by "steps" pattern
  step_schedule: "0:5,30:15,60:3"  # ← ignored in "det" mode

  If you want the step schedule applied, change pattern: "det" → pattern: "steps". Otherwise remove the field to avoid confusion.


  Set to 0 means no timeout — boom will wait indefinitely on a hung vLLM connection. c571t0 took 907s to complete, showing boom was already struggling to reach vLLM. Set this to a concrete
  value like 300 or 600.

  2. step_schedule: "0:5,30:15,60:3" — IGNORED

  pattern: "det"        # ← step_schedule is ONLY used by "steps" pattern
  step_schedule: "0:5,30:15,60:3"  # ← ignored in "det" mode

  If you want the step schedule applied, change pattern: "det" → pattern: "steps". Otherwise remove the field to avoid confusion.

  3. boom_max_inflight: 1000 — Possibly too low

  With boom_route_via: router, requests go through the router sidecar before reaching vLLM. The router queue was building up (390→368). If boom was waiting for router slots and the router

  If you want the step schedule applied, change pattern: "det" → pattern: "steps". Otherwise remove the field to avoid confusion.

  3. boom_max_inflight: 1000 — Possibly too low

  With boom_route_via: router, requests go through the router sidecar before reaching vLLM. The router queue was building up (390→368). If boom was waiting for router slots and the router
  was backlogged, boom could have hit its 1000 inflight limit. Consider raising to 2000-5000 or adding backpressure monitoring.


  With boom_route_via: router, requests go through the router sidecar before reaching vLLM. The router queue was building up (390→368). If boom was waiting for router slots and the router
  was backlogged, boom could have hit its 1000 inflight limit. Consider raising to 2000-5000 or adding backpressure monitoring.

  4. idle_timeout_s: 1000 — Reasonable but aggressive for slow requests

  Average request took 90-910s. A 1000s timeout means almost any slow request triggers the drain. If your SLO allows, consider 1500-1800 to avoid false positives on legitimately slow
  With boom_route_via: router, requests go through the router sidecar before reaching vLLM. The router queue was building up (390→368). If boom was waiting for router slots and the router
  was backlogged, boom could have hit its 1000 inflight limit. Consider raising to 2000-5000 or adding backpressure monitoring.

  4. idle_timeout_s: 1000 — Reasonable but aggressive for slow requests

  Average request took 90-910s. A 1000s timeout means almost any slow request triggers the drain. If your SLO allows, consider 1500-1800 to avoid false positives on legitimately slow
  requests.

  was backlogged, boom could have hit its 1000 inflight limit. Consider raising to 2000-5000 or adding backpressure monitoring.

  4. idle_timeout_s: 1000 — Reasonable but aggressive for slow requests

  Average request took 90-910s. A 1000s timeout means almost any slow request triggers the drain. If your SLO allows, consider 1500-1800 to avoid false positives on legitimately slow
  requests.

  5. Missing boom health check / restart on stuck


  4. idle_timeout_s: 1000 — Reasonable but aggressive for slow requests

  Average request took 90-910s. A 1000s timeout means almost any slow request triggers the drain. If your SLO allows, consider 1500-1800 to avoid false positives on legitimately slow
  requests.

  5. Missing boom health check / restart on stuck

  There's no boom-side circuit breaker. If boom's connection to vLLM hangs, it stays stuck indefinitely (as seen here for ~16 minutes). The boom_upstream_timeout_seconds fix above addresses
   the root cause.


  Average request took 90-910s. A 1000s timeout means almost any slow request triggers the drain. If your SLO allows, consider 1500-1800 to avoid false positives on legitimately slow
  requests.

  5. Missing boom health check / restart on stuck

  There's no boom-side circuit breaker. If boom's connection to vLLM hangs, it stays stuck indefinitely (as seen here for ~16 minutes). The boom_upstream_timeout_seconds fix above addresses
   the root cause.

  ---
  What's NOT a problem
  requests.

  5. Missing boom health check / restart on stuck

  There's no boom-side circuit breaker. If boom's connection to vLLM hangs, it stays stuck indefinitely (as seen here for ~16 minutes). The boom_upstream_timeout_seconds fix above addresses
   the root cause.

  ---
  What's NOT a problem
  There's no boom-side circuit breaker. If boom's connection to vLLM hangs, it stays stuck indefinitely (as seen here for ~16 minutes). The boom_upstream_timeout_seconds fix above addresses
   the root cause.

  ---
  What's NOT a problem

  What's NOT a problem

  - rate_rps: 0.3 — correct, submission rate was steady
  - rate_rps: 0.3 — correct, submission rate was steady
  step_schedule: "0:5,30:15,60:3"  # ← ignored in "det" mode

  If you want the step schedule applied, change pattern: "det" → pattern: "steps". Otherwise remove the field to avoid confusion.

  3. boom_max_inflight: 1000 — Possibly too low

  With boom_route_via: router, requests go through the router sidecar before reaching vLLM. The router queue was building up (390→368). If boom was waiting for router slots and the router
  was backlogged, boom could have hit its 1000 inflight limit. Consider raising to 2000-5000 or adding backpressure monitoring.

  4. idle_timeout_s: 1000 — Reasonable but aggressive for slow requests

  Average request took 90-910s. A 1000s timeout means almost any slow request triggers the drain. If your SLO allows, consider 1500-1800 to avoid false positives on legitimately slow
  requests.

  5. Missing boom health check / restart on stuck

  There's no boom-side circuit breaker. If boom's connection to vLLM hangs, it stays stuck indefinitely (as seen here for ~16 minutes). The boom_upstream_timeout_seconds fix above addresses
   the root cause.

  ---
  What's NOT a problem

  - rate_rps: 0.3 — correct, submission rate was steady
  - boom_route_via: router — correct for comparing router vs direct
  - router_kv_aware: true, router_len_aware: true — correct settings
  - boom_claude_aliases: true — correct for Claude Code format

✻ Worked for 18m 4s