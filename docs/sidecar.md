# Sidecar Service

The sidecar runs next to each vLLM pod and turns that pod into a well-behaved
worker in the cluster. It handles local queuing, talks to the router-service
and vLLM, and keeps track of the pod’s KV-cache blocks.

---

## Role in the System

- Receives work from the router and feeds it to the pod’s vLLM endpoint.
- Limits how many requests run on the pod at once.
- Reports completions back to the router so client calls can finish.
- Listens to KV events from vLLM and records which KV blocks live on this pod.

This lets the router focus on global decisions while each sidecar manages its
own pod.

---

## Local Queue and Capacity

Each sidecar keeps a small in-memory queue holding `(req_id, prompt, meta)`.

- It tracks two counters: pending items in the queue and inflight items being
  processed by workers.
- A configuration value `BATCH_SIZE` defines the maximum of
  `pending + inflight` allowed.
- When the limit is reached, the sidecar temporarily stops asking the router
  for more work.

This ensures each pod only runs as many vLLM requests as it can handle.

---

## Getting Work from the Router

A pull helper checks the local queue state and, when there is spare capacity,
asks the router for more items with a single `/pull` call that includes:

- the pod identity (used as the endpoint name), and  
- how many new requests it can accept.

The helper is triggered by events:

- after each completed request to immediately top up, and  
- occasionally while idle to see if new work is available.

There is no tight polling loop; traffic is proportional to actual activity.

---

## Talking to vLLM and Returning Results

Worker threads repeatedly:

1. Take a request from the local queue.  
2. Call the pod’s vLLM `/v1/chat/completions` endpoint using the stored
   prompt and meta.  
3. Extract the generated text from the response.  
4. Send the result back to the router via a `/result` call that includes the
   original `req_id`.

On any error, the sidecar logs the issue and still marks the request complete
in the local queue so the capacity accounting stays correct.

The number of workers is usually equal to `BATCH_SIZE`, giving that many
concurrent vLLM requests per pod.

---

## KV-Cache Awareness

A background subscriber connects to vLLM over ZMQ and receives KV-cache events
such as blocks being stored, removed, or fully cleared. For each event, it
updates Redis so that other components know:

- which block hashes belong to this pod, and  
- which pods are associated with each block hash.

The router can then use this information to send requests to pods that already
hold matching KV blocks, enabling cache reuse.

---

## Sidecar HTTP API

The sidecar exposes a small internal API:

- `GET /health` — reports basic status, queue length, and inflight count.  
- `POST /push` — optional way to inject work directly into the local queue
  (useful for testing or alternative routing modes).

---

## Startup and Shutdown

On startup the sidecar:

1. Loads configuration from environment variables.  
2. Creates the local queue and binds it to the HTTP API.  
3. Starts the pull helper (in pull mode).  
4. Launches vLLM worker threads.  
5. Starts the KV subscriber.  
6. Runs the FastAPI server with Uvicorn.

On shutdown it stops workers, the pull helper, and the subscriber cleanly.

Together, this makes each vLLM pod a self-contained worker with clear capacity,
KV visibility, and a simple integration point for the central router-service.

# Tracing (Sidecar)

When `TRACE_ENABLED=true`, the sidecar records high-resolution timing for:

- arrival (pull or push mode)
- dequeue
- sending request to vLLM
- receiving response from vLLM
- sending result back to router

These fields are stored in:

    meta["__trace__"]

The sidecar forwards this object to the router’s `/result` endpoint, where the
router merges it with its own timestamps.
