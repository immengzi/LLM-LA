# v0.1 Production Runtime Implementation Plan

Production-ready serving runtime derived from the current Python implementation
Go-based runtime for LLM serving, scheduling, worker coordination, and cluster integration


======================================================================
## 1. Scope of v0.1
======================================================================

v0.1 establishes the first production-capable runtime for the serving
framework. The current Python implementation already proves the main
serving concepts:

- centralized request ownership
- batching-aware routing
- pull-based scheduling behavior
- worker discovery and refresh
- metrics collection
- trace-based latency decomposition
- KV-aware and prefix-aware scheduling direction

The purpose of v0.1 is to harden these capabilities into a production
runtime implemented in Go.

The Go runtime becomes the source of truth for:

- request admission
- centralized queue ownership
- worker coordination
- pull scheduling
- runtime metrics
- autoscaling signals
- pod-local sidecar state export
- backend abstraction
- accelerator metric normalization
- Kubernetes deployment integration

The Python codebase remains useful for:

- behavior reference
- old experiments and comparisons
- load generation and offline analysis
- temporary fallback during migration validation


### Current Python baseline
-----------------------

The current serving path is spread across a few runtime-oriented Python
modules and a few experiment-oriented modules.

Most important serving-related Python modules already known in the stack:

- current router/API entrypoint module
- router_core.py
- router_modes.py
- utils_prom.py
- utils_k8s.py
- utils.py
- sidecar/config.py
- backend-facing request code
- http_client.py

Most important experiment / analysis oriented modules already known:

- loadgen.py
- analysis notebooks and scripts
- simulation-only branches
- experiment runner CLI flow


### What to add in v0.1
-------------------

The production version should add the things that are weak or implicit
in the current Python system:

- stronger typed request and worker models
- explicit internal APIs between gateway and sidecar
- explicit failure and retry boundaries
- clear queue ownership
- explicit inflight assignment tracking
- production health/readiness endpoints
- structured JSON logging
- OpenTelemetry support
- controller-based deployment shape
- clearer separation between runtime path and experiment path


======================================================================
## 2. Core Technology Stack
======================================================================

Language
- Go 1.22+

HTTP serving
- net/http
- chi router

Metrics
- Prometheus client_golang

Tracing
- OpenTelemetry SDK and OTLP exporter

Logging
- zap

Configuration
- environment variables
- optional Viper for layered config

Kubernetes integration
- client-go
- controller-runtime / Kubebuilder

Packaging
- Docker / OCI images
- Helm

Accelerator telemetry
- NVML for NVIDIA GPU
- Ascend / NPU-specific collector through a dedicated adapter
- normalized generic accelerator interface

Primary backend in v0.1
- vLLM OpenAI-compatible API

Future optional internal transport
- gRPC for internal control APIs if REST becomes limiting


### Current Python baseline
-----------------------

The Python stack currently relies on:

- FastAPI / HTTP runtime behavior for router-style service layers
- requests-based backend and client submission paths
- Prometheus helper code in utils_prom.py
- Kubernetes endpoint discovery in utils_k8s.py
- plain Python dict / tuple-based state passing
- ad hoc runtime composition across router_core.py, router_modes.py,
  and helper modules


### What to add in v0.1
-------------------

- replace implicit dict-based contracts with typed Go structs
- unify runtime and metrics serving around one HTTP stack
- introduce explicit internal API contracts for pull and completion
- normalize accelerator telemetry behind one collector interface
- use structured logging and tracing from the start
- avoid mixing experiment CLI logic into the production runtime binary


======================================================================
## 3. Target Go Repository Structure
======================================================================

```bash
cmd/
  gateway/
    main.go
  sidecar/
    main.go
  controller/
    main.go

internal/
  gateway/
    handlers.go
    middleware.go
    models.go
    queue.go
    scheduler.go
    registry.go
    service.go
    internal_api.go

  sidecar/
    worker.go
    backend.go
    metrics.go
    accel.go
    kv.go
    state.go
    heartbeat.go

  autoscaling/
    signals.go
    aggregator.go

  controller/
    reconciler.go
    resources.go

  common/
    config.go
    logging.go
    tracing.go
    health.go
    prefix.go
    time.go

pkg/
  api/
    v1alpha1/
      modeldeployment_types.go

deploy/
  crds/
  helm/
  manifests/
```

### Current Python baseline
-----------------------

Current Python module grouping reflects history and experimentation
more than long-term runtime ownership.

Known patterns in the Python layout:
- router entrypoint and router runtime logic are close together
- router_core.py contains core dispatch behavior
- router_modes.py mixes mode selection and serving concepts
- utils_prom.py and utils_k8s.py provide cross-cutting support
- sidecar/config.py contains pod-local settings
- utils.py contains mixed responsibilities including logging/results


### What to add in v0.1
-------------------

- keep runtime packages organized by operational ownership
- keep v0.1 package layout flat enough to move quickly
- split later only when files become too large
- keep experiment code out of cmd/gateway and cmd/sidecar
- keep analysis and loadgen outside the production binaries


======================================================================
## 4. System Architecture
======================================================================

```bash
Client
  |
  v
Go Gateway
  |
  v
Centralized Request Queue
  |
  v
Pull Scheduler
  |
  +------------------------------------+
  |                 |                  |
  v                 v                  v
Worker Pod A     Worker Pod B      Worker Pod C
  |                 |                  |
Go Sidecar       Go Sidecar        Go Sidecar
  |                 |                  |
Backend           Backend            Backend
(vLLM)            (vLLM)             (vLLM)
```

Architecture rules

- gateway owns request admission and queue mutation
- scheduler runs in gateway
- sidecar is the worker agent
- sidecar is the source of pod-local runtime state
- controller manages deployment shape only
- Kubernetes API is not in the hot scheduling path
- autoscaling signal aggregation is outside the request execution hot path


### Current Python baseline
-----------------------

The current Python implementation already demonstrates:
- centralized scheduling semantics
- pull-based batching direction
- endpoint refresh and cluster discovery
- backend-facing request submission
- runtime metrics collection
- queue-wait and latency decomposition

These behaviors are spread across:
- router_core.py
- router_modes.py
- utils_k8s.py
- utils_prom.py
- backend request helpers
- sidecar-related helpers


### What to add in v0.1
-------------------

- explicit internal API boundaries
- explicit queue / inflight / completion state ownership
- explicit sidecar heartbeat model
- clear controller/runtime split
- clear worker registry with TTL and staleness handling


======================================================================
## 5. Gateway: Request Admission and North-South API
======================================================================

### Gateway responsibilities
------------------------

- accept OpenAI-compatible requests
- validate and normalize input
- assign RequestID
- compute prompt metadata
- compute PrefixHash
- enqueue request
- expose internal worker APIs
- expose framework metrics
- expose autoscaling signals
- expose health and readiness endpoints

### Suggested endpoints
-------------------

Public
- `POST /v1/chat/completions`
- `POST /v1/completions`

Internal
- `POST /internal/pull`
- `POST /internal/complete`
- `POST /internal/worker/register   (optional in v0.1)`
- `GET  /metrics`
- `GET  /healthz`
- `GET  /readyz`

### Request model
-------------

```go
type InferenceRequest struct {
    RequestID          string
    Model              string
    Messages           []map[string]any
    MaxTokens          int
    Temperature        float64
    Stream             bool
    ArrivalTime        time.Time
    ArrivalMonoNanos   int64
    PromptChars        int
    PromptBytes        int
    PrefixHash         string
    PrefixHashVersion  string
    Metadata           map[string]any
}
```

### Admission invariants
--------------------

- every request must have RequestID
- every enqueued request must have arrival timestamps
- every request must be normalized before queue insert
- request payload is immutable after enqueue except trace/result fields


### Current Python baseline
-----------------------

Modules to map here:
- current router/API entrypoint module
- router_core.py
- possibly existing request parsing helpers
- http_client.py as compatibility reference for current wire format

What the Python code currently does here:
- accepts OpenAI-style requests
- forwards generation controls
- creates request metadata in a looser dict-based way
- may combine admission and dispatch more tightly than desired


### Migration and changes
---------------------

Existing Python module affected:
- current router/API entrypoint module
- `router_core.py`

Go replacement:
- `cmd/gateway/main.go`
- `internal/gateway/handlers.go`
- `internal/gateway/middleware.go`
- `internal/gateway/models.go`
- `internal/gateway/service.go`

What is good to add:
- request validation middleware
- request size limits
- overload rejection before queue insertion
- request normalization step
- backward-compatible support for fields currently sent by http_client.py
- explicit correlation ID propagation
- request timeout and cancellation plumbing


======================================================================
## 6. Centralized Queue
======================================================================

### Queue responsibilities
----------------------

- own all queued requests
- preserve FIFO by default
- support bounded prefix-aware scans
- track oldest waiting request
- support inflight assignment bookkeeping
- expose queue metrics
- provide hooks for expiration and retry logic

### Queue structure
---------------

```go
type QueueEntry struct {
    Request    InferenceRequest
    EnqueuedAt time.Time
}

type InflightAssignment struct {
    RequestID   string
    WorkerID    string
    AssignedAt  time.Time
}

type CentralQueue struct {
    mu         sync.Mutex
    queued     []QueueEntry
    inflight   map[string]InflightAssignment
    enqueued   int64
    assigned   int64
    completed  int64
    failed     int64
    expired    int64
}
```

### Required queue operations
-------------------------

- Enqueue
- Dequeue
- RemoveFirstMatchBounded
- MarkAssigned
- MarkCompleted
- MarkFailed
- Requeue
- Size
- OldestAgeSeconds

### Implementation notes
--------------------

- use mutex protection first; optimize only if needed
- bounded scan for prefix matching should be configurable
- avoid O(n) full-queue scans in v0.1
- keep inflight map keyed by RequestID


### Current Python baseline
-----------------------

Modules to map here:
- `router_core.py`
- `router_modes.py` indirectly
- possibly `utils.py` for `result/log` handling

What the Python code currently does here:
- holds queue-like state in router runtime
- uses tuple/list structures in some paths
- supports dispatch-later semantics
- measures queue-related timings


### Migration and changes
---------------------

Existing Python module affected:
- `router_core.py`

Go replacement:
- `internal/gateway/queue.go`

What is good to add:
- explicit inflight map
- explicit requeue support for failed assignment cases
- oldest request age metric
- queue pressure thresholds
- queue admission cap and backpressure policy
- request expiration handling for abandoned items


======================================================================
## 7. Pull Scheduler
======================================================================

### Scheduler responsibilities
--------------------------

- decide whether a worker may pull now
- validate worker health and staleness
- validate compatibility
- prefer local prefix hit when cheap
- fall back to FIFO compatible request
- export scheduler reason metrics

### Worker eligibility
------------------

```go
func WorkerCanPull(w WorkerState) bool {
    if !w.Healthy {
        return false
    }
    if w.RunningRequests >= MaxRunningRequests {
        return false
    }
    if w.AcceleratorMemoryUsage > MemoryThreshold {
        return false
    }
    return true
}
```

### Selection policy for v0.1
-------------------------

1. reject stale or unhealthy worker
2. reject saturated worker
3. bounded-scan prefix hit
4. FIFO compatible fallback
5. no_work result if nothing available

### Compatibility dimensions
------------------------

- model served
- backend type
- accelerator type
- optional memory class / capacity class
- health and last-seen freshness


### Current Python baseline
-----------------------

Modules to map here:
- `router_core.py`
- `router_modes.py`

What the Python code currently does here:
- pull-batching behavior
- inflight admission limits
- endpoint ordering / dispatch heuristics
- util-aware or cap-aware admission concepts
- some experimental variants beyond production needs


### Migration and changes
---------------------

Existing Python module affected:
- `router_core.py`
- `router_modes.py`

Go replacement:
- `internal/gateway/scheduler.go`

What is good to add:
- explicit SchedulerReason values
- explicit compatibility functions
- starvation guard so locality preference does not starve old requests
- configurable scan limit
- separate hard reject vs soft preference logic
- scheduling metrics by reason and worker
- remove all experiment-only strategies from the production runtime path


======================================================================
## 8. Standardized request/response format plan
======================================================================

The new Go framework should standardize all northbound inference traffic on a single request/response contract in v0.1.

Recommended v0.1 standard:
- OpenAI-compatible API as the primary external request format
- KServe compatibility as a deployment/runtime integration layer, not the main northbound schema in v0.1

Reason:
- KServe’s generative inference path emphasizes OpenAI-compatible endpoints for LLM workloads
- vLLM already exposes OpenAI-compatible chat/completions APIs
- this minimizes adapter work across clients, SDKs, gateways, and backends
- this keeps the first version simpler while remaining compatible with KServe-based deployments later

Relevant standard API surface for v0.1:
- /v1/chat/completions
- /v1/completions
- optional later:
  - /v1/embeddings
  - /v1/models

The framework should therefore define:
- one canonical internal request model
- one OpenAI-compatible ingress layer
- one backend adapter mapping layer
- optional future KServe protocol adapter

----------------------------------------------------------------------
### A. Standardization rule for v0.1
----------------------------------------------------------------------

External client protocol:
- OpenAI-compatible JSON schema

Internal canonical request model:
- Go struct used throughout gateway, scheduler, sidecar, and backend adapter

Backend-facing protocol:
- adapter-specific translation from canonical request model to backend-native payload

This means the framework should not pass raw OpenAI JSON everywhere internally.
Instead:
- parse OpenAI request once at ingress
- convert to canonical internal request struct
- pass canonical struct through queue/scheduler/sidecar
- convert to backend-native payload only at adapter boundary

----------------------------------------------------------------------
### B. Current Python file -> target Go file migration checklist for request standardization
----------------------------------------------------------------------

Current Python request handling code
------------------------------------
Current role:
- parses incoming request payloads
- likely forwards dict/json directly to backend or router
- may keep request fields loosely typed

Target Go files:
- internal/gateway/api/openai.go
- internal/gateway/models/request.go
- internal/sidecar/backend/interface.go
- internal/sidecar/backend/vllm.go

Migration goal:
- replace ad hoc JSON/dict propagation with typed canonical Go models
- make OpenAI-compatible request parsing the official ingress contract

What should change from existing Python code:
- remove tuple-only or prompt-only queue entries
- stop passing backend-shaped payloads through the queue
- queue canonical request objects instead

----------------------------------------------------------------------
### C. Canonical internal request model
----------------------------------------------------------------------

The canonical request model should be richer than the external API format because it must carry scheduling and runtime metadata.

Example target Go model:

```go
package models

import "time"

type ChatMessage struct {
    Role    string `json:"role"`
    Content string `json:"content"`
}

type InferenceRequest struct {
    RequestID       string                 `json:"request_id"`
    APIFormat       string                 `json:"api_format"`   // "openai-chat", "openai-completions"
    Model           string                 `json:"model"`
    Messages        []ChatMessage          `json:"messages,omitempty"`
    Prompt          string                 `json:"prompt,omitempty"`
    MaxTokens       int                    `json:"max_tokens"`
    Temperature     float64                `json:"temperature"`
    Stream          bool                   `json:"stream"`
    Stop            []string               `json:"stop,omitempty"`

    ArrivalTime     time.Time              `json:"arrival_time"`
    PromptChars     int                    `json:"prompt_chars"`
    PrefixHash      string                 `json:"prefix_hash,omitempty"`

    Metadata        map[string]any         `json:"metadata,omitempty"`
}
```

This canonical model becomes the only object stored in:
- centralized queue
- scheduler
- dispatch logic
- trace records

----------------------------------------------------------------------
### D. OpenAI-compatible ingress layer
----------------------------------------------------------------------

Implement in:
- internal/gateway/api/openai.go

Responsibilities:
- expose OpenAI-compatible endpoints
- validate request schema
- normalize into canonical internal model
- enqueue canonical request object

Endpoints to implement in v0.1:
- POST /v1/chat/completions
- optional: POST /v1/completions

Suggested minimal request structs:

```go
type OpenAIChatCompletionRequest struct {
    Model       string        `json:"model"`
    Messages    []ChatMessage `json:"messages"`
    MaxTokens   int           `json:"max_tokens,omitempty"`
    Temperature float64       `json:"temperature,omitempty"`
    Stream      bool          `json:"stream,omitempty"`
    Stop        []string      `json:"stop,omitempty"`
}
```

Normalization example:

```go
func toCanonicalChatRequest(in OpenAIChatCompletionRequest) models.InferenceRequest {
    return models.InferenceRequest{
        RequestID:   uuid.NewString(),
        APIFormat:   "openai-chat",
        Model:       in.Model,
        Messages:    in.Messages,
        MaxTokens:   in.MaxTokens,
        Temperature: in.Temperature,
        Stream:      in.Stream,
        Stop:        in.Stop,
        ArrivalTime: time.Now(),
        PromptChars: countChatChars(in.Messages),
        PrefixHash:  prefix.ComputePrefixHashFromMessages(in.Messages, 2048),
    }
}
```

This is the core replacement for any current Python logic that directly forwards raw request JSON.

----------------------------------------------------------------------
### E. Backend adapter translation layer
----------------------------------------------------------------------

Implement in:
- internal/sidecar/backend/interface.go
- internal/sidecar/backend/vllm.go

Responsibilities:
- accept canonical request model
- translate canonical request to backend-native payload
- translate backend response back into OpenAI-compatible result shape for gateway/client

Example backend interface:

```go
type Adapter interface {
    SubmitRequest(ctx context.Context, req models.InferenceRequest) (BackendResult, error)
    Health(ctx context.Context) error
    RuntimeStats(ctx context.Context) (RuntimeStats, error)
    Capabilities() Capabilities
}
```

Example vLLM translation:

```go
func buildVLLMPayload(req models.InferenceRequest) map[string]any {
    if req.APIFormat == "openai-chat" {
        return map[string]any{
            "model":       req.Model,
            "messages":    req.Messages,
            "max_tokens":  req.MaxTokens,
            "temperature": req.Temperature,
            "stream":      req.Stream,
            "stop":        req.Stop,
        }
    }

    return map[string]any{
        "model":       req.Model,
        "prompt":      req.Prompt,
        "max_tokens":  req.MaxTokens,
        "temperature": req.Temperature,
        "stream":      req.Stream,
        "stop":        req.Stop,
    }
}
```

This means the queue/scheduler never sees backend-specific JSON.
Only adapters do.

----------------------------------------------------------------------
### F. Response standardization
----------------------------------------------------------------------

Gateway responses should also be standardized in OpenAI-compatible form in v0.1.

That means:
- gateway owns the northbound response schema
- backend adapters return normalized backend results
- gateway serializes OpenAI-style response JSON

For non-streaming responses, define a canonical backend result model:

```go
type BackendResult struct {
    RequestID          string
    Model              string
    FinishReason       string
    OutputText         string
    PromptTokens       int
    CompletionTokens   int
    TotalTokens        int
}
```

Gateway then maps that to OpenAI-style response output.

For streaming:
- use OpenAI-style chunked event stream format
- adapter returns incremental token chunks in normalized form
- gateway serializes them

----------------------------------------------------------------------
### G. KServe compatibility plan
----------------------------------------------------------------------

KServe should be treated as:
- deployment/runtime environment
- control-plane compatibility target
- future protocol adapter target

not as the primary v0.1 northbound request schema.

In practice:
- v0.1 should deploy on Kubernetes in a KServe-friendly way if needed
- v0.1 request format should remain OpenAI-compatible
- later version can add:
  - KServe data-plane adapter
  - KServe CRD integration layer
  - dual protocol support if needed

This is simpler and better aligned with current generative serving practice around KServe and vLLM. :contentReference[oaicite:1]{index=1}

----------------------------------------------------------------------
### H. Concrete migration from current Python request handling
----------------------------------------------------------------------

If current Python code effectively does something like:

```python
payload = request.json()
router.q.put((prompt, t_enq_client, req_id))
```

or forwards raw dicts:

backend.send(payload)

that should become:

1. parse OpenAI-compatible request in Go
2. normalize into InferenceRequest
3. enqueue InferenceRequest
4. scheduler dispatches InferenceRequest
5. backend adapter converts InferenceRequest to backend payload
6. backend result normalized
7. gateway returns OpenAI-compatible response

This is one of the biggest design cleanups in the rewrite.

----------------------------------------------------------------------
### I. v0.1 acceptance criteria for API standardization
----------------------------------------------------------------------

- OpenAI-compatible request schema is the official external API
- all queued work items use one canonical internal Go request struct
- backend adapters translate from canonical request to backend-native format
- gateway returns OpenAI-compatible responses
- no raw backend-specific payloads flow through the scheduler or queue
- KServe remains a deployment/control-plane compatibility target for later expansion


======================================================================
## 8. Worker Registry
======================================================================

### Registry responsibilities
-------------------------

- maintain live worker state
- update from heartbeats
- mark stale workers
- support aggregate metrics
- provide scheduler lookup and iteration
- keep runtime truth independent of K8s watch latency

### Worker state
------------

```go
type WorkerState struct {
    WorkerID               string
    PodName                string
    Backend                string
    BackendVersion         string
    Model                  string
    AcceleratorType        string
    AcceleratorName        string
    AcceleratorMemoryBytes uint64
    RunningRequests        int
    BatchSize              int
    TokensPerSecond        float64
    KVCacheUsage           float64
    AcceleratorUtilization float64
    AcceleratorMemoryUsage float64
    KnownPrefixHashes      []string
    Healthy                bool
    LastSeen               time.Time
    LastBackendError       string
}
```

### Implementation notes
--------------------

- use in-memory map keyed by WorkerID
- update atomically on heartbeat
- prune or mark stale after WorkerTTL
- expose aggregate helpers for ready workers, total running requests,
  mean batch size, mean util, etc.


### Current Python baseline
-----------------------

Modules to map here:
- utils_k8s.py
- router_core.py
- utils_prom.py for util/TPS/state-related inputs

What the Python code currently does here:
- endpoint discovery through Kubernetes
- health and util probing
- dynamic endpoint refresh
- identity caching and possibly TPS caching


### Migration and changes
---------------------

Existing Python module affected:
- `utils_k8s.py`
- `router_core.py`
- `utils_prom.py`

Go replacement:
- `internal/gateway/registry.go`

What is good to add:
- TTL-based liveness
- staleness metrics
- explicit worker registration / heartbeat update path
- separation between deployment-discovered worker identity and
  sidecar-reported runtime truth
- aggregate helper methods for autoscaling signals


======================================================================
## 9. Sidecar Runtime
======================================================================

### Sidecar responsibilities
------------------------

- act as worker agent
- collect local backend runtime stats
- collect accelerator metrics
- compute/report prefix and KV summary
- heartbeat to gateway
- pull work from gateway
- execute request on local backend
- report completion
- expose pod-local health, metrics, and state

### Main loops
----------

stats loop
- query backend runtime stats
- query accelerator stats
- query prefix/KV state
- update local snapshot
- export Prometheus metrics

pull loop
- send heartbeat snapshot
- request work
- if assigned, submit to executor

completion/report loop
- report result and trace back to gateway

### Suggested sidecar endpoints
---------------------------

- `GET /metrics`
- `GET /healthz`
- `GET /readyz`
- `GET /state`

### Concurrency model
-----------------

- bounded executor semaphore
- no unbounded goroutine fan-out
- local concurrency cap tied to backend characteristics


### Current Python baseline
-----------------------

Modules to map here:
- sidecar/config.py
- current sidecar-like helper scripts
- backend metric probing helpers
- runtime monitoring helpers
- backend-facing request helpers

What the Python code currently does here:
- pod-local configuration
- monitoring and state export concepts
- local backend integration
- possibly local queue/batching visibility


### Migration and changes
---------------------

Existing Python module affected:
- `sidecar/config.py`
- existing sidecar-like monitoring scripts
- backend-facing helper code

Go replacement:
- `cmd/sidecar/main.go`
- `internal/sidecar/worker.go`
- `internal/sidecar/state.go`
- `internal/sidecar/metrics.go`
- `internal/sidecar/heartbeat.go`

What is good to add:
- one consolidated worker agent instead of scattered helpers
- local snapshot model
- local health/readiness endpoints
- sidecar version/build info metrics
- graceful shutdown behavior
- explicit retry policy for gateway communication
- bounded local executor concurrency


======================================================================
## 10. Backend Adapter Layer
======================================================================

### Backend adapter responsibilities
--------------------------------

- hide backend-specific transport details
- normalize health checks
- normalize runtime stats
- normalize request submission
- normalize response parsing

### Primary backend for v0.1
------------------------

- vLLM OpenAI-compatible API

### Adapter interface
-----------------

```go
type Adapter interface {
    SubmitRequest(ctx context.Context, req InferenceRequest) (BackendResult, error)
    Health(ctx context.Context) error
    RuntimeStats(ctx context.Context) (RuntimeStats, error)
    PrefixState(ctx context.Context) (PrefixState, error)
    Capabilities() Capabilities
}

type RuntimeStats struct {
    RunningRequests int
    BatchSize int
    TokensPerSecond float64
    QueueDepth int
}

type PrefixState struct {
    KnownPrefixHashes []string
    KVCacheUsage float64
    PrefixEntries int
}

type BackendResult struct {
    Output string
    FinishReason string
    PromptTokens int
    CompletionTokens int
    TotalTokens int
    TTFTMillis int64
    BackendLatencyMs int64
    Raw map[string]any
}
```

### Current Python baseline
-----------------------

Modules to map here:
- `http_client.py`
- backend request submission helpers
- router-side direct backend request code
- AIBrix/vLLM request shaping code if still part of serving path

What the Python code currently does here:
- submits requests to router or backend
- shapes payload
- handles timeout/error cases
- parses completion response
- forwards extra generation fields


### Migration and changes
---------------------

Existing Python module affected:
- `http_client.py`
- backend request code

Go replacement:
- `i`nternal/sidecar/backend.go`

What is good to add:
- adapter abstraction
- explicit error classification
- per-backend timeout policy
- capability reporting
- future support for non-vLLM backends without changing scheduler logic
- richer typed backend results for tracing and metrics


======================================================================
## 11. Continuous Batching Awareness
======================================================================

### Responsibilities
----------------

- surface current backend batch size
- surface running request count
- surface tokens/sec if available
- provide standardized metrics for autoscaling and debugging

### Metrics
-------

- framework_batch_size
- framework_running_requests
- framework_tokens_per_second

### Implementation notes
--------------------

- sidecar polls backend runtime stats periodically
- metrics should be per-worker
- freshness should be visible or implicit through heartbeat interval


### Current Python baseline
-----------------------

Modules to map here:
- utils_prom.py
- router_core.py
- sidecar-related stat collectors

What the Python code currently does here:
- collects runtime stats
- may use util/TPS identity caching
- uses batching visibility for scheduling or status output


### Migration and changes
---------------------

Existing Python module affected:
- utils_prom.py
- router_core.py

Go replacement:
- internal/sidecar/metrics.go
- internal/sidecar/state.go
- internal/gateway/registry.go for aggregated view

What is good to add:
- normalized stat polling contract
- freshness-aware metrics
- batch-size aggregation helpers
- separate pod-local vs framework-global metric surfaces


======================================================================
## 12. Prefix Cache Awareness
======================================================================

### Responsibilities
----------------

- compute deterministic prefix identity at admission
- report known reusable prefixes from workers
- prefer prefix-local worker assignment when available
- keep locality as a soft hint, not a correctness rule

### Prefix hash helper
------------------

```go
func ComputePrefixHash(messages []map[string]any, maxBytes int) string {
    b, _ := json.Marshal(messages)
    if len(b) > maxBytes {
        b = b[:maxBytes]
    }
    sum := sha256.Sum256(b)
    return hex.EncodeToString(sum[:])
}
```

### Implementation notes
--------------------

- hash normalized prompt/message content
- version the hashing scheme
- limit bytes hashed to a configurable threshold
- keep worker-reported known prefixes bounded


### Current Python baseline
-----------------------

Modules to map here:
- router_core.py
- current KV-aware / prefix-aware scheduling logic
- any helper logic around block hashes or prefix tracking

What the Python code currently does here:
- uses prefix or block locality concepts in routing experiments
- does not yet expose a single simple production-safe prefix identity
  surface everywhere


### Migration and changes
---------------------

Existing Python module affected:
- `router_core.py`
- prefix/KV helper code if present

Go replacement:
- `internal/common/prefix.go`
- `internal/gateway/models.go`
- `internal/gateway/scheduler.go`
- `internal/sidecar/kv.go`

What is good to add:
- one canonical PrefixHash definition
- one clear worker-reported prefix list surface
- metrics for prefix-hit rate
- bounded list sizes and bounded scheduler scan sizes


======================================================================
## 13. KV Cache Awareness
======================================================================

### Responsibilities
----------------

- surface normalized KV usage per worker
- report known reusable KV/prefix state
- allow scheduler to prefer workers likely to reuse cache
- provide observability into KV pressure

### KV-related worker fields
------------------------

- KVCacheUsage
- KnownPrefixHashes
- PrefixEntries
- optional local cache pressure indicators

### Implementation notes
--------------------

- keep engine-specific KV details hidden behind sidecar/backend layer
- export only normalized scheduling-relevant summary fields
- do not couple scheduler directly to backend internals


### Current Python baseline
-----------------------

Modules to map here:
- `router_core.py`
- KV tracking and trace fields already present in the current stack
- sidecar/runtime collectors related to KV state

What the Python code currently does here:
- tracks block hashes
- uses KV-related trace data
- explores KV-aware routing direction


### Migration and changes
---------------------

Existing Python module affected:
- `router_core.py`
- KV collectors/helpers

Go replacement:
- `internal/sidecar/kv.go`
- `internal/gateway/registry.go`
- `internal/gateway/scheduler.go`

What is good to add:
- normalized KV usage schema
- per-worker KV pressure metrics
- scheduler reason codes for cache-local assignment
- clean separation between trace-only KV info and scheduling-relevant KV info


======================================================================
## 14. KV Cache Offloading
======================================================================

### Responsibilities
----------------

- provide initial pressure-relief mechanism under high KV usage
- define interface for local KV offload store
- emit metrics and traces for offload events

### Interface
---------

type OffloadStore interface {
    Put(key string, value []byte) error
    Get(key string) ([]byte, error)
    Delete(key string) error
}

### Initial implementation
----------------------

- local filesystem store
- optional PVC-backed store

### Initial trigger policy
----------------------

- soft threshold, for example KVCacheUsage >= 0.85
- hard protection threshold above that
- local-only operation in v0.1


### Current Python baseline
-----------------------

Modules to map here:
- current research logic and design ideas around KV pressure
- sidecar/runtime state collection
- not necessarily a fully productionized offload implementation yet

What the Python code currently does here:
- establishes that KV footprint matters for scheduling and memory pressure
- may not yet provide a mature offload subsystem


### Migration and changes
---------------------

Existing Python module affected:
- none as a strong direct 1:1 runtime module if offload is still mostly
  conceptual
- KV and sidecar state helpers provide the baseline

Go replacement:
- internal/sidecar/kv.go

What is good to add:
- explicit OffloadStore interface now
- local-only safe implementation
- offload metrics
- offload event logging
- defer distributed restore and remote coordination


======================================================================
## 15. Accelerator Metrics Integration
======================================================================

### Responsibilities
----------------

- collect per-worker accelerator utilization
- collect memory used and memory total
- normalize GPU and NPU data into one schema
- expose data to sidecar metrics and gateway scheduling

### Metrics schema
--------------

```go
type AcceleratorMetrics struct {
    AcceleratorType string
    AcceleratorName string
    Utilization float64
    MemoryUsedBytes uint64
    MemoryTotalBytes uint64
}
```

### Implementation notes
--------------------

- keep collector interface generic
- cache last successful sample briefly if needed
- track collector errors without crashing sidecar
- do not allow stale metrics to appear indefinitely fresh


### Current Python baseline
-----------------------

Modules to map here:
- utils_prom.py
- router_core.py util/TPS logic
- current accelerator probing helpers

What the Python code currently does here:
- probes GPU utilization
- caches endpoint identity and TPS-like metadata
- feeds scheduling and observability with util-like signals


### Migration and changes
---------------------

Existing Python module affected:
- utils_prom.py
- router_core.py

Go replacement:
- internal/sidecar/accel.go
- internal/sidecar/metrics.go
- internal/gateway/registry.go

What is good to add:
- one normalized collector interface
- separate GPU and NPU collector implementations
- metric freshness handling
- error counters for failed telemetry collection
- scheduler guards on normalized memory pressure rather than backend-
  specific raw formats


======================================================================
## 16. Heterogeneous Hardware Awareness
======================================================================

### Responsibilities
----------------

- avoid assuming workers are homogeneous
- track worker hardware identity
- filter incompatible workers
- expose enough metadata for future scheduling expansion

### Compatibility dimensions
------------------------

- model served
- backend type
- accelerator type
- memory class
- optional performance class

### Implementation notes
--------------------

- keep compatibility filtering simple and safe in v0.1
- do not attempt complex cost-model optimization yet
- expose hardware identity in traces and metrics


### Current Python baseline
-----------------------

Modules to map here:
- router_core.py
- utils_k8s.py
- util/identity caching helpers
- research logic around heterogeneous environments

What the Python code currently does here:
- already treats endpoints as non-identical in practice
- tracks endpoint identity and util
- supports research direction toward heterogeneous clusters


### Migration and changes
---------------------

Existing Python module affected:
- router_core.py
- utils_k8s.py
- utils_prom.py

Go replacement:
- internal/gateway/models.go
- internal/gateway/registry.go
- internal/gateway/scheduler.go

What is good to add:
- explicit model/backend/accelerator compatibility checks
- worker capability metadata
- scheduler reason codes for incompatibility
- metrics for rejected assignments due to incompatibility


======================================================================
## 17. Autoscaling Signals
======================================================================

### Responsibilities
----------------

- aggregate framework-global state
- export autoscaling-relevant metrics
- keep signal aggregation outside the request hot path as much as practical

### Signals
-------

- framework_queue_length
- framework_oldest_request_age_seconds
- framework_running_requests_total
- framework_ready_workers
- framework_mean_batch_size
- framework_mean_kv_cache_usage
- framework_mean_accelerator_utilization

### Signal model
------------

```go
type Signals struct {
    QueueLength             int
    OldestRequestAgeS       float64
    RunningRequests         int
    ReadyWorkers            int
    MeanBatchSize           float64
    MeanKVCacheUsage        float64
    MeanAcceleratorUtil     float64
}
```

### Current Python baseline
-----------------------

Modules to map here:
- utils_prom.py
- router_core.py
- endpoint refresh and runtime stat logic

What the Python code currently does here:
- already surfaces queue-related and util-related information
- supports experiment-time observability into capacity and waiting


### Migration and changes
---------------------

Existing Python module affected:
- utils_prom.py
- router_core.py

Go replacement:
- internal/autoscaling/signals.go
- internal/autoscaling/aggregator.go
- internal/gateway/service.go
- internal/gateway/registry.go
- internal/gateway/queue.go

What is good to add:
- one explicit framework-global signal surface
- separation between pod-local sidecar metrics and gateway-level
  autoscaling signals
- compatibility with HPA/KEDA metric consumption
- aggregation helpers on registry and queue


======================================================================
## 18. Kubernetes Controller and Deployment Shape
======================================================================

### Responsibilities
----------------

- reconcile a ModelDeployment CRD
- deploy backend pods
- inject sidecar
- create Services
- create monitoring-related objects
- create autoscaling objects if configured

### CRD
---

type ModelDeploymentSpec struct {
    Backend string
    Model string
    Replicas int32
    AcceleratorType string
    SidecarEnabled bool
    MetricsEnabled bool
}

### Implementation stack
--------------------

- controller-runtime
- Kubebuilder
- Helm and manifests retained during early migration

### Important rule
--------------

- controller manages deployment shape
- controller does not participate in live request scheduling


### Current Python baseline
-----------------------

Modules to map here:
- utils_k8s.py
- existing Helm chart / manifests
- current deployment scripts and templates

What the Python code currently does here:
- depends on Kubernetes-native deployment and discovery
- uses service / endpoint discovery at runtime
- is deployed through Helm/manifests rather than a Go operator


### Migration and changes
---------------------

Existing Python module affected:
- utils_k8s.py
- deployment/config templates around the current runtime

Go replacement:
- cmd/controller/main.go
- internal/controller/reconciler.go
- internal/controller/resources.go
- pkg/api/v1alpha1/modeldeployment_types.go

What is good to add:
- thin operator first
- preserve current deployment shape before inventing a new one
- operator-driven sidecar injection
- label and annotation standards for gateway/sidecar discovery
- explicit ServiceMonitor/PodMonitor creation if needed


======================================================================
## 19. Observability, Tracing, and Logging
======================================================================

### Responsibilities
----------------

- provide enough information to reconstruct request lifecycle
- preserve current latency semantics
- expose framework and pod-local metrics
- provide structured logs for production debugging

### Logging
-------

- zap JSON logs
- request-scoped correlation IDs
- worker-scoped runtime logs
- startup/config logs
- scheduler decision logs at controlled verbosity

### Tracing
-------

- OpenTelemetry spans
- gateway admission span
- queue wait span or derived timing
- sidecar execution span
- backend submission span

### Important trace fields
----------------------

- request_id
- worker_id
- pod_name
- backend
- accelerator_type
- scheduler_reason
- kv_prefix_hit
- kv_cache_usage_at_dispatch
- queue_length_at_dispatch
- t_arrival_gateway
- t_dispatch_gateway
- t_response_gateway
- queue_wait_seconds
- backend_roundtrip_seconds
- end_to_end_seconds


### Current Python baseline
-----------------------

Modules to map here:
- utils.py
- trace/logging helpers
- router_core.py request timing fields
- utils_prom.py metric export logic

What the Python code currently does here:
- preserves SSOT-like timestamps
- supports queue-wait, roundtrip, and end-to-end timing derivations
- emits experiment logs and result records


### Migration and changes
---------------------

Existing Python module affected:
- utils.py
- router_core.py
- utils_prom.py

Go replacement:
- internal/common/logging.go
- internal/common/tracing.go
- internal/gateway/service.go
- internal/sidecar/metrics.go

What is good to add:
- keep current timing semantics stable
- move to structured JSON logs
- introduce span-based tracing
- preserve analysis compatibility where possible
- add explicit scheduler decision reason fields


======================================================================
## 20. Failure Handling, Reliability, and Safety
======================================================================

### Responsibilities
----------------

- detect stale or failed workers
- avoid duplicate assignment
- tolerate duplicate completion reports
- recover from transient gateway/sidecar transport failures
- protect against backend timeout and overload
- support graceful shutdown

### Failure scenarios
-----------------

- worker crash
- sidecar crash
- gateway restart
- backend timeout
- backend unhealthy
- Kubernetes rescheduling
- network failure between gateway and sidecar

### Reliability mechanisms
----------------------

- worker heartbeat TTL
- explicit inflight assignment map
- bounded requeue policy
- idempotent completion handling
- health checks
- readiness checks
- overload rejection
- graceful drain on shutdown


### Current Python baseline
-----------------------

Modules to map here:
- router_core.py
- backend request helpers
- utils.py
- current health/util probing code

What the Python code currently does here:
- handles a subset of runtime failures implicitly
- uses health checks and timeouts
- is not yet structured around production-grade reliability boundaries


### Migration and changes
---------------------

Existing Python module affected:
- router_core.py
- backend request code
- utils.py

Go replacement:
- internal/gateway/queue.go
- internal/gateway/service.go
- internal/sidecar/worker.go
- internal/common/health.go

What is good to add:
- explicit timeout matrix
- explicit retry rules
- graceful shutdown sequencing
- overload rejection before queueing
- duplicate completion tolerance
- abandoned inflight recovery policy


======================================================================
## 21. Testing and Validation
======================================================================

### Testing layers
--------------

Unit tests
- queue operations
- scheduler selection rules
- registry update and expiry
- prefix hashing
- accelerator metric normalization
- backend adapter parsing

Integration tests
- gateway + mock sidecar
- sidecar + mock backend
- full request lifecycle
- worker stale/rejoin
- failure and retry handling

### Migration regression tests
--------------------------

- compare Go runtime against Python baseline
- same prompts
- same backend
- compare completion rate
- compare queue-wait distribution
- compare scheduler behavior at a coarse level
- compare trace field coverage

### Kubernetes tests
----------------

- deploy controller-managed runtime
- verify sidecar injection
- verify metrics surfaces
- verify autoscaling signal availability


### Current Python baseline
-----------------------

Modules to map here:
- loadgen.py
- analysis scripts and notebooks
- router_core.py output traces
- utils.py result persistence

What the Python code currently does here:
- provides the reference workloads, logs, and metrics needed for
  migration validation


### Migration and changes
---------------------

Existing Python module affected:
- loadgen.py
- analysis scripts
- utils.py

Go replacement:
- production runtime has its own tests
- Python loadgen and analysis may remain for comparison during v0.1

What is good to add:
- baseline comparison test suite
- schema adapters so existing analysis can read Go-generated traces
- golden request/response fixtures
- integration tests for failure conditions, not only happy path


======================================================================
## 22. Acceptance Criteria for v0.1
======================================================================

The system is considered production-ready for v0.1 when the following
conditions are met.

### Gateway/runtime
---------------

- Go gateway implements public inference APIs
- request admission is performed in Go
- centralized queue exists and is authoritative
- pull scheduling is the only production assignment path

### Worker/runtime
--------------

- Go sidecar runs in each worker pod
- sidecar exports health, metrics, and state
- sidecar pulls work and submits to backend
- sidecar reports completion back to gateway

### Scheduling/runtime awareness
----------------------------

- worker health and capacity guards are enforced
- prefix-local assignment is supported as a soft preference
- KV usage is visible and usable by scheduler
- heterogeneous worker compatibility filtering is implemented

### Observability/autoscaling
-------------------------

- framework-global metrics exist
- pod-local worker metrics exist
- autoscaling signals exist
- trace/logging coverage is sufficient to reconstruct request lifecycle

### Platform/deployment
-------------------

- controller can deploy runtime shape on Kubernetes
- sidecar injection is automated or controller-managed
- Helm/manifests can deploy the runtime during the transition phase

### Migration completeness
----------------------

- all production-relevant capabilities from the current Python serving
  path are represented in the v0.1 runtime plan
- each capability is either implemented directly in Go for v0.1 or
  explicitly retained through transitional compatibility where needed
- experiment-only Python code is not required for the live serving path


======================================================================
## 23. Appendix: Current Python Module to Go v0.1 Mapping
======================================================================

current router/API entrypoint
-> cmd/gateway/main.go
-> internal/gateway/handlers.go
-> internal/gateway/service.go

router_core.py
-> internal/gateway/queue.go
-> internal/gateway/scheduler.go
-> internal/gateway/registry.go

router_modes.py
-> only policy/config concepts retained
-> not ported as a runtime module

utils_prom.py
-> internal/sidecar/metrics.go
-> internal/sidecar/accel.go
-> internal/autoscaling/aggregator.go
-> internal/gateway/service.go

utils_k8s.py
-> internal/gateway/registry.go for runtime-facing worker view
-> internal/controller/reconciler.go and resources.go for deployment shape

utils.py
-> internal/common/logging.go
-> internal/common/tracing.go
-> gateway/sidecar result handling paths as needed

sidecar/config.py
-> internal/common/config.go

backend request code
-> internal/sidecar/backend.go

http_client.py
-> external client compatibility reference
-> wire-format compatibility target for gateway APIs

loadgen.py
-> remains outside production runtime
-> reused for validation and comparison during migration

analysis scripts and notebooks
-> remain outside production runtime
-> reused for behavioral comparison during migration

current Python request parsing / payload forwarding
-> internal/gateway/api/openai.go
-> internal/gateway/models/request.go
-> internal/sidecar/backend/interface.go
-> internal/sidecar/backend/vllm.go