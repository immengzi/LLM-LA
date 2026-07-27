# Tracing & Latency Breakdown

Technical reference for the end-to-end tracing path:

- Client (load_runner / http_client)
- Router service
- Sidecar
- vLLM server

Tracing is enabled/disabled purely via an environment variable on the router + sidecars.

    TRACE_ENABLED=true

When tracing is enabled, each successful `/enqueue` response includes a trace object under:

    result.trace

The router and sidecar populate raw timestamps + queue metadata; the client converts them into derived latency metrics.

## 1. End-to-End Timeline Diagram

```bash
          t_enq_client                          t_enqueue_response
Client  |--------------------------------------------*---------|
        |                                            ^         |
        |                                            |         |
        |                  t_arrive_router           t_router_result_recv
        |                *-----------*--------------------* |
        |                |           |                    |    |
        v                v           |                    v    v

Router  |   /enqueue     |  queue    |  dispatch   /result     |
        |---------------->-----------|------------<------------|
                         ^           |
                         |           v
                         |    t_enq_router_queue
                         |    t_dispatch_router

                                             t_arrive_sidecar_(push|pull)
Sidecar |                                 *-----------*---------------------*
        |                                 |           |                     |
        |                                 v           |                     |
        |       /push or /pull            |    local dequeue                |
        |<------------------------|-----------*---------------------|
                                    t_dequeue_sidecar   t_post_result_sidecar
                                                    |             ^
                                                    v             |
vLLM    |                                     t_vllm_send   t_vllm_recv
        |-------------------------------------> [compute] --------|

```

The client never sees these raw timestamps directly; it only sees derived metrics printed by `load_runner.py`.

## 2. Raw Trace Fields (Server-Side)

### 2.1 Router timestamps
These fields are attached by the router when `TRACE_ENABLED=true`:

**t_arrive_router**
Time when the router’s `/enqueue` handler receives the request.

**t_enq_router_queue**
Time when the request is put into the router’s internal queue (pull mode).
In push modes this may be absent.

**t_dispatch_router**
Time when the router chooses a sidecar and dispatches the request:
* via `/pull` response in pull mode, or
* via sidecar `/push` in push modes.

**t_router_result_recv**
Time when the router receives `/result` from the sidecar.

**t_enqueue_response**
Time just before the router writes the final HTTP response back to the client.

Router may also add:
* `router_mode` (e.g. `"pull"`, `"push-rr"`, `"push-random"`, `"push-leastq"`, `"push-throughput"`, `"push-p2c"`, `"push-kv-cost"`, `"push-least-kv"`, `"push-least-latency"`, `"push-least-busy"`, `"central-push"`, `"external-push"`) — the trace emits the normalized mode name (aliases like `push-least-queue` → `push-leastq`)
* `router_queue_len_at_arrive`
* `router_queue_len_at_dispatch`

### 2.2 Sidecar timestamps
These fields are attached by the sidecar and propagated via `/result`:

**t_arrive_sidecar_push**
Time when the sidecar receives a `/push` from the router (push modes).

**t_arrive_sidecar_pull**
Time when the sidecar receives work as a result of its `/pull` call (pull mode).

**t_dequeue_sidecar**
Time when a worker thread pops the item from the local queue.

**t_vllm_send**
Time when the sidecar issues the HTTP request to the local vLLM server.

**t_vllm_recv**
Time when the sidecar receives the vLLM response.

**t_post_result_sidecar**
Time when the sidecar completes the `/result` POST back to the router.

Sidecar may also add queue/capacity snapshots:
* `sidecar_queue_len_before`, `sidecar_queue_len_after`
* `sidecar_inflight_before`, `sidecar_inflight_at_dequeue`, `sidecar_inflight_at_result`
* `sidecar_logical_*` counters

### 2.3 Client timestamp
The client adds its own timestamp and the router echoes it into the trace:

**t_enq_client**
Time when `http_client.send_one()` builds the `/enqueue` payload.

## 3. Client-Derived Latency Metrics

The function `_compute_trace_metrics(trace)` in `load_runner.py` converts the raw timestamps into human-readable latencies (seconds).

The following metrics are derived by the client:

| Metric Name | Calculation Formula | Description |
| :--- | :--- | :--- |
| **end_to_end_s** | `t_enqueue_response` - `t_enq_client` | **Total Roundtrip.** The full time elapsed from the client's perspective. |
| **client_to_router_s** | `t_arrive_router` - `t_enq_client` | **Network Ingress.** Time from client construction to router arrival (network + front-end handling). |
| **router_queue_s** | `t_dispatch_router` - `t_enq_router_queue` | **Router Wait.** Time spent waiting in the router’s internal queue. *(Falls back to `t_arrive_router` if queue timestamp is missing).* |
| **router_to_sidecar_s** | `t_arrive_sidecar_*` - `t_dispatch_router` | **Dispatch Latency.** Time from router dispatch to sidecar receipt. *(Uses either `push` or `pull` arrival timestamp).* |
| **sidecar_queue_s** | `t_dequeue_sidecar` - `t_arrive_sidecar_*` | **Sidecar Wait.** Time waiting in the sidecar’s local queue before a worker picks it up. |
| **vllm_compute_s** | `t_vllm_recv` - `t_vllm_send` | **GPU Compute.** Pure model runtime latency on the vLLM server. |
| **sidecar_post_s** | `t_post_result_sidecar` - `t_vllm_recv` | **Sidecar Overhead.** Time spent extracting the response, constructing the payload, and sending `/result`. |
| **sidecar_to_router_s** | `t_router_result_recv` - `t_post_result_sidecar` | **Callback Network.** Network + router ingress time for the `/result` callback. |
| **router_post_result_s** | `t_enqueue_response` - `t_router_result_recv` | **Router Overhead.** Time from router receiving the result to writing the HTTP response. |
| **server_roundtrip_s** | `t_enqueue_response` - `t_arrive_router` | **Server Total.** "Server-side" latency excluding client-side network. |

*Note: All metrics are optional: each is only emitted if both endpoints of the interval are present and numeric.*

## 4. Client Logging Behavior

When `result.trace` is present:

The client prints the endpoint and router mode:

```bash
    [client][T120]   trace_endpoint=vllm-qwen-5b7d457949-tcq6t
    [client][T120]   router_mode=pull
```

The client calls `_compute_trace_metrics(trace)` and prints each derived metric:

```bash
    [client][T120]   end_to_end_s=38.330556s
    [client][T120]   client_to_router_s=2.684757s
    [client][T120]   router_queue_s=30.396861s
    [client][T120]   router_to_sidecar_s=0.000640s
    [client][T120]   sidecar_queue_s=0.000041s
    [client][T120]   vllm_compute_s=7.930408s
    [client][T120]   sidecar_post_s=0.000049s
    [client][T120]   sidecar_to_router_s=0.002196s
    [client][T120]   router_post_result_s=0.000355s
    [client][T120]   server_roundtrip_s=35.645799s
```

The client then prints any non-timestamp extras from trace (queue lengths, inflight counts, etc.), skipping all keys starting with `t_`:

```bash
    [client][T120]   router_queue_len_at_arrive=103
    [client][T120]   router_queue_len_at_dispatch=0
    [client][T120]   sidecar_queue_len_before_pull=0
    [client][T120]   sidecar_inflight_before_pull=7
    [client][T120]   sidecar_logical_before_pull=7
    [client][T120]   sidecar_queue_len_after_pull=1
    [client][T120]   sidecar_logical_after_pull=8
    [client][T120]   sidecar_queue_len_at_dequeue=0
    [client][T120]   sidecar_inflight_at_dequeue=8
    [client][T120]   sidecar_logical_at_dequeue=8
    [client][T120]   sidecar_queue_len_at_result=0
    [client][T120]   sidecar_inflight_at_result=1
    [client][T120]   sidecar_logical_at_result=1
```

Raw timestamps (`t_*`) are not printed in the client logs; they only exist inside `result.trace` for post-processing if needed.

## 5. Routing Mode Differences

The same trace schema is used for all router modes:

**ROUTER_MODE=pull**
* `t_enq_router_queue` and `t_arrive_sidecar_pull` are present.
* `t_arrive_sidecar_push` is typically absent.

**ROUTER_MODE=push-\*** (`push-rr`, `push-random`, `push-leastq`, `push-throughput`, `push-p2c`, `push-kv-cost`, `push-least-kv`, `push-least-latency`, `push-least-busy`)
* `t_enq_router_queue` may be absent.
* `t_arrive_sidecar_push` is present.
* `t_arrive_sidecar_pull` is absent.

`_compute_trace_metrics()` automatically chooses whichever sidecar-arrival timestamp exists (`t_arrive_sidecar_pull` or `t_arrive_sidecar_push`) and computes the same `router_to_sidecar_s` and `sidecar_queue_s` metrics for both styles.
