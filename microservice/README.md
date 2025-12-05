# Microservice Load Client & Router Ecosystem

This repository contains two main layers:

1. A **microservice-based router system** for distributed vLLM serving.
2. A standalone **open-loop load client**, used for benchmarking
   and validating routing, batching, and KV-aware execution.

The load client interacts only with the router's synchronous `/enqueue`
endpoint and does not require any knowledge of internal router or sidecar
mechanisms.

For configuration documentation, see:  
`docs/config_knobs.md`

---

# 1. High-Level Architecture

The serving system follows a microservice design:

- **Router Service**  
  Receives `/enqueue` requests, performs queueing, batching, routing
  decisions, and returns final responses.

- **Sidecar**  
  Runs alongside each model worker. It handles KV synchronization, forwards
  tasks to vLLM, and reports completions back to the router service.

- **Prefix-Hash / KV Awareness Pipeline**  
  Optional module that computes or manages prefix signatures for block reuse.

- **Load Client**  
  Generates controlled request patterns, measures latencies, and stresses the
  router under different batching, KV-aware, and length-aware conditions.

This design allows independent scaling of router logic, model workers,
sidecars, and load generators.

```
                 +-------------------------+
 Client --------> |       Router Svc       | --------+
                  +-------------------------+        |
                              ^                      |
                              |                      |
                       routing decisions             |
                              |                      v
                 +-------------------------+     Final Response
                 |        Sidecar          |
                 | local queue + workers   |
                 +-------------------------+
                              |
                              v
                        Local vLLM Pod
                              |
                              v
                             Output
```

---

# 2. Routing Algorithms (Background)

The router uses several algorithmic layers to optimize batching efficiency,
latency, and reuse of cached key/value blocks.

## 2.1 Push-Mode Routing

Push routing assigns work from a central queue *proactively* to workers.

Shared behavior:

- All new requests are placed into a global queue.
- Router determines which worker should receive the next batch.
- Batches are formed using length and KV awareness.

Strategies:

- **Round-Robin**  
  Simple cyclic worker selection.

- **Random**  
  Uniform worker selection.

- **Least-Queue**  
  Chooses the worker with the fewest active requests.

```
(1) /enqueue
Client -------------------------------------------> Router

Router chooses worker (RR / random / least-q)
        |
        | (2) /push(req_id, prompt)
        v

Sidecar ---------------------------> vLLM
                                    |
                                    | output
                                    v

(3) /result(req_id, output)
Sidecar ------------------------------------------> Router
                                                    |
                                                    v
                                                  Client
```

---

## 2.2 Pull-Mode Routing

In pull-mode, workers request tasks when they are ready:

```bash
worker → router: pull(want = N)
router → worker: batch of size <= N
```


Characteristics:

- Workers self-regulate load.
- Stabilizes GPU utilization.
- Queueing is predictable.

```
                 (1) /enqueue
Client ------------------------------------------>

                    +-------------------------+
                    |       Router Svc        |
                    |   global request queue  |
                    +-------------------------+
                               ^
                               |
                (2) /pull (want = N)
Sidecar ------------------------------------------>
                               |
                               v
                    +-------------------------+
                    |    batch (≤ N items)    |
                    +-------------------------+
                               |
                               v
                       Local vLLM Server
                               |
                 (3) /result(req_id, output)
                               |
                               v
                            Router
                               |
                               v
                            Client
```

---

## 2.3 Length-Aware Batching

The router uses estimated output lengths to group similar-length requests for
better GPU efficiency.

Policies:

- **short_first**
- **long_first**
- **even_short_long**

Heuristics avoid starvation by occasionally forcing long-sequence inclusion.

---

## 2.4 KV-Aware Routing

KV-awareness enables reuse of cached transformer key/value blocks.

The router maintains:

- prefix_hash → worker mappings  
- real-time updates from sidecars about KV creation/eviction  

If a prefix hash matches an existing block, the request is pinned to that
worker.

```
 Client
    |
    v
 +------------------+
 |    Router Svc    |
 | queue, batch,    |
 | KV-aware routing |
 +------------------+
    |        ^
    |        |
    v        |
 +------------------+      KV Events      +--------+
 |     Sidecar      | <------------------ | vLLM   |
 | local queue,     |                     | Pod    |
 | workers, pull    | ------------------> |        |
 +------------------+     /v1/chat        +--------+
    |
    v
 Redis (KV-block ownership map)
```

---

# 3. Microservice Architecture (High-Level Summary)

Conceptual pipeline:

1. **Client → Router**  
   Sends `/enqueue`.

2. **Router Decision Layer**  
   Applies push/pull, batching, KV-awareness, length-awareness.

3. **Router → Sidecar → vLLM**  
   Sidecar executes request, forwards to vLLM, captures output.

4. **Sidecar → Router → Client**  
   Router returns result.

---

# 4. Load Client

The load client generates reproducible synthetic load and measures precise
latencies under different routing and batching configurations.

---

## 4.1 Behavior Summary

- Builds planned timestamps from a load pattern.
- For each request:
  - sleeps,
  - sends `/enqueue`,
  - waits for result,
  - logs `[SEND]`/`[RECV]`.
- Supports warmup sequences.

---

## 4.2 Load Patterns (Scheduler)

All patterns output sorted timestamps, then re-anchor to “now”.

Patterns:

- dump  
- det  
- poisson  
- bursty  
- steps  
- rand  

---

# 5. Warmup

Sends synchronous warmup requests to initialize:

- router queues,
- KV maps,
- model load,
- networking paths.

---

# 6. Execution Model

For each request:

1. Sleep until timestamp.
2. Log `[SEC]` when entering a new second.
3. Log `[SEND]`.
4. Send `/enqueue` and block.
5. Log `[RECV]` with details.

Workers do not block each other.

---

# 7. Client Behavior (Load Runner)

The client does not need configuration changes. If the server returns a trace
object inside `result.trace`, the load runner prints it.

There is no client flag for tracing; it is fully controlled by the server-side
environment variable:

    TRACE_ENABLED=true

---

# 8. Running the Client

```bash
python main.py --config example_config.yaml
```


Ensure the correct router URL.

---

# 9. Related Documentation

- `docs/config_knobs.md`  
- `services/router_service/`  
- `services/sidecar/`  
- `services/prefix_hash/`
