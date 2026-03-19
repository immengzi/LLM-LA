# LLM Inference Framework Plan

## Background

Serving large generative models requires balancing user-facing latency
metrics such as time-to-first-token and time-between-tokens with system
goals such as throughput, accelerator utilization, and cost efficiency.
Traditional serving systems scale entire model replicas using generic
infrastructure signals such as CPU or accelerator utilization. This
approach performs poorly for LLM workloads because requests vary widely
in prompt length, decode length, KV cache footprint, and service time.

LLM inference systems also exhibit additional complexities. Continuous
batching behavior strongly affects throughput and latency. KV cache
reuse can significantly reduce prefill costs but introduces placement
and memory management challenges. In addition, modern inference clusters
increasingly contain heterogeneous accelerator types such as GPUs and
NPUs with different memory capacities and compute characteristics.

This framework proposes a Kubernetes-native architecture for LLM
inference that provides deeper control over request admission,
scheduling, autoscaling, and memory management. The system combines
centralized admission control, pull-based scheduling, LLM-aware
autoscaling, KV-aware runtime behavior, heterogeneous accelerator
awareness, and extensible backend integration. The goal is to provide
both a production-ready serving platform and a flexible research
framework for exploring advanced scheduling and memory-management
strategies.

------------------------------------------------------------------------

# Full Feature Set

## Queueing and Scheduling

Centralized Queue\
A global admission queue receives all incoming requests before they are
assigned to replicas.

Pull-Based Scheduling\
Workers pull requests from the centralized queue when they have
available capacity.

Token-Length--Aware Queue Ordering\
Queue ordering may incorporate token-length predictions to reduce
head-of-line blocking.

Adaptive Batch Size Control\
Batch sizes can dynamically adapt to latency targets and memory
constraints.

Multi-Queue Scheduling\
Multiple queues may be used to separate workloads or priority levels.

Hierarchical Queueing\
Queues may exist at cluster, node, and worker levels.

Token-Level Capacity Scheduling\
Scheduling can consider token workload instead of request counts.

Preemption-Aware Scheduling\
Scheduling may consider backend memory pressure and preemption behavior.

------------------------------------------------------------------------

## Autoscaling and Capacity Management

LLM-Aware Autoscaling\
Scaling decisions use LLM workload signals such as queue pressure and
token workload.

Operator-Level Autoscaling\
Future versions may scale model operators independently rather than
scaling entire replicas.

Joint Load Balancing and Autoscaling Control\
Routing and scaling loops may be coordinated.

KV-Aware Scale-Down\
Replicas containing valuable KV states may be preserved during
scale-down.

Cross Engine p2p KV cache reuse\
Similar to the Clyde project being able to locate the relevant KV cache block

KV Offloading\
KV offloading for increased kv pooling capacity. Similar projects https://aibrix.readthedocs.io/latest/designs/aibrix-kvcache-offloading-framework.html#aibrix-kvcache-offloading-framework

Fast Replica Startup\
Replica startup latency may be reduced using optimized container images
and layered model loading.

------------------------------------------------------------------------

## KV Cache and Memory Infrastructure

KV Cache Awareness\
The scheduler tracks KV cache usage and reuse opportunities.

Prefix Caching\
Requests with identical prefixes may reuse cached KV states.

CacheBlend-Based KV Reuse\
KV states may be reused even when cached segments are not strict
prefixes.

KV Cache Offloading\
KV states may move from accelerator memory to CPU memory or remote
storage.

Distributed KV Cache Transfer\
KV states may be transferred across replicas.

KV Prefix Location Map\
Cluster metadata may track reusable KV prefix locations.

KV Cache Hierarchy and Eviction Policies\
KV storage may span multiple tiers with eviction policies.

Predictive KV Caching\
The system may predict which KV states are likely to be reused.

------------------------------------------------------------------------

## Routing Intelligence

SLO-Based Routing\
Requests may be routed based on latency targets.

Agent-Aware Routing\
Agent workflows may be routed to preserve context locality.

Semantic and Task-Aware Routing\
Routing decisions may depend on task type or reasoning complexity.

Multi-Level Routing\
Routing logic may operate at cluster, node, and worker levels.

------------------------------------------------------------------------

## Runtime Optimization

Token Length Prediction\
Prompt and decode lengths may be predicted before execution.

Continuous Batching Awareness\
The framework observes continuous batching behavior.

Prefill and Decode Disaggregation\
Prefill and decode stages may run independently.

Speculative Decoding\
Draft models may accelerate token generation.

Layered Model Loading\
Model weights may load incrementally to reduce startup time.

------------------------------------------------------------------------

## Backend and Infrastructure Abstraction

Envoy Proxy Support\
The backend should support routing through the envoy as the gateway instead of nodeported router

Multi-Backend Support\
The framework supports multiple inference engines.

Multiple Accelerator Type Support\
Clusters may contain GPUs and NPUs.

GPU/NPU Metrics Integration\
Normalized runtime metrics are collected from accelerators.

Heterogeneous Hardware Scheduling\
Scheduling accounts for accelerator differences.

GPU/NPU Time-Sharing\
Accelerators may be partitioned or time-shared.

Multi Cloud Support\
Support Multi Cloud Platform like Huawei Cloud, Ali Cloud, GCP etc

------------------------------------------------------------------------

# Implementation Plan

## v0.1 --- Core System (1 Month)

Core features implemented:

-   centralized queue
-   pull-based scheduling
-   continuous batching awareness
-   LLM-aware autoscaling
-   KV cache awareness
-   prefix caching
-   initial KV cache offloading
-   GPU/NPU metrics integration
-   heterogeneous hardware awareness

### High-Level Architecture

                    +-----------------------+
                    |        Client         |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |    Gateway / Router   |
                    |  (API + Admission)    |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |    Centralized Queue  |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |  Pull-Based Scheduler |
                    +-----------+-----------+
                                |
                -------------------------------------
                |                  |                |
                v                  v                v

            +--------+        +--------+        +--------+
            | Pod A  |        | Pod B  |        | Pod C  |
            +---+----+        +---+----+        +---+----+
                |                 |                 |
       +--------+------+  +------+--------+  +------+--------+
       | Backend Runtime|  | Backend Runtime| | Backend Runtime|
       | (GPU / NPU)    |  | (GPU / NPU)    | | (GPU / NPU)    |
       +--------+------+  +------+--------+  +------+--------+
                |                 |                 |
       +--------+------+  +------+--------+  +------+--------+
       | Framework     |  | Framework     |  | Framework     |
       | Sidecar       |  | Sidecar       |  | Sidecar       |
       | metrics/KV    |  | metrics/KV    |  | metrics/KV    |
       +---------------+  +---------------+  +---------------+

------------------------------------------------------------------------

### Pod Architecture

    +--------------------------------------------------+
    | Kubernetes Pod                                   |
    |--------------------------------------------------|
    | Backend Runtime                                  |
    |  - model execution                               |
    |  - batching engine                               |
    |  - KV cache                                      |
    |  - GPU/NPU execution                             |
    |--------------------------------------------------|
    | Framework Sidecar                                |
    |  - normalized metrics                            |
    |  - KV cache summary                              |
    |  - batching statistics                           |
    |  - accelerator metrics                           |
    |  - health endpoints                              |
    +--------------------------------------------------+

------------------------------------------------------------------------

### Request Flow

    Client Request
          |
          v
    Gateway Admission
          |
          v
    Centralized Queue
          |
          v
    Worker Pulls Request
          |
          v
    Backend Runtime Execution
          |
          v
    Response Streamed to Client

------------------------------------------------------------------------

### Autoscaling Loop

    Queue Pressure Increases
            |
            v
    Autoscaler Observes Backlog
            |
            v
    Desired Replica Count Increases
            |
            v
    Kubernetes Launches New Pods
            |
            v
    New Pods Register and Pull Work

------------------------------------------------------------------------

### KV Cache Flow

    Incoming Request
           |
           v
    Check Prefix Cache
           |
       +---+---+
       |       |
    Hit       Miss
     |         |
     v         v
    Reuse KV  Full Prefill

    Memory Pressure
           |
           v
    KV Offloaded to CPU / Storage Tier

------------------------------------------------------------------------

### GPU/NPU Metrics Normalization

    Raw GPU Metrics
    Raw NPU Metrics
    Backend Runtime Metrics
            |
            v
    Sidecar Normalization Layer
            |
            v
    Unified Metrics Format

    accelerator_type
    accelerator_utilization
    accelerator_memory_usage
    running_requests
    batch_size
    kv_cache_usage

------------------------------------------------------------------------

## v0.2 --- Scheduling and Routing Expansion

Scheduler Improvements - token-length aware queue ordering - adaptive
batch size control - multi-queue scheduling - token-level capacity
scheduling

Routing Layer - SLO-based routing - agent-aware routing - semantic
routing - multi-level routing

Architecture Extension

    Incoming Request
            |
            v
    Routing Classifier
            |
       +----+----+
       |    |    |
     Short Long Priority
     Queue Queue Queue

------------------------------------------------------------------------

## v0.3 --- KV Infrastructure

Distributed KV System

            +-------------------------+
            | KV Metadata / Prefix DB |
            +-----------+-------------+
                        |
           --------------------------------
           |              |               |
           v              v               v
         Pod A          Pod B           Pod C
        Local KV       Local KV        Local KV

Features - distributed KV cache transfer - KV prefix location index - KV
hierarchy and eviction policies - predictive KV caching - CacheBlend KV
reuse

------------------------------------------------------------------------

## v1.0 --- Production Platform

Advanced Scaling - operator-level autoscaling - joint autoscaling and
load balancing

Runtime Optimization - prefill/decode disaggregation - speculative
decoding

Infrastructure - accelerator partitioning - heterogeneous accelerator
scheduling

Production Platform - security and authentication - multi-tenancy -
model lifecycle management - observability dashboards - reliability and
recovery

------------------------------------------------------------------------

## Timeline

  Version   Time           Focus
  --------- -------------- ----------------------
  v0.1      1 month        core framework
  v0.2      2--3 months    scheduling & routing
  v0.3      3--5 months    KV infrastructure
  v1.0      6--12 months   production platform


## Roadmap
 
1. Energy-Aware Routing based on GPUs and NPUs\
The scheduler is extended to incorporate per-accelerator power draw,
thermal state, and power headroom as routing signals alongside existing
utilization and memory metrics. A thermally throttled or power-capped
GPU incurs higher latency per token than a cooler device even at similar
utilization, so routing without energy visibility produces unpredictable
tail latency under sustained load. The sidecar collects and normalizes
these signals across GPU and NPU device classes, the scheduler uses power
headroom as a soft preference when latency SLOs are tight, and aggregate
energy metrics are surfaced for cluster-level cost reporting and carbon
accounting.
 
2. Operator and Fine-Grained Scaling\
Current autoscaling operates at replica granularity, which is too coarse
for LLM inference because prefill and decode stages have different
resource profiles and different sensitivity to scale events. Fine-grained
scaling tracks prefill workers, decode workers, and KV cache managers as
independent scaling targets, each with its own queue pressure signals and
autoscaling policy. Scale-up decisions are driven by token-level backlog
rather than request count, scale-down defers termination of replicas
holding reusable KV states until those states expire or transfer, and
fast startup using pre-warmed images reduces the latency cost of
scale-out events.
 
3. Agentic-Aware Routing and Scaling\
Multi-stage agentic workloads produce many dependent inference calls
within a single session, each extending the prior prompt with tool
results or observations. The KV cache from stage N is almost entirely
reusable in stage N+1, but a scheduler unaware of session identity routes
successive calls to different workers and forces redundant full prefill
on every stage. The framework introduces session identity as a first-class
metadata field, uses session-affinity scheduling to prefer the worker
already holding the relevant KV state, gives blocking tool-use calls
stage-aware priority, and accounts for expected future requests from
active sessions in queue pressure and autoscaling signals.
 
4. LoRA Adapter Support\
Serving a base model with many fine-tuned LoRA adapters is a common
multi-tenant pattern, but without adapter awareness the scheduler cannot
avoid redundant adapter loading or missed batching opportunities. Adapter
identity is propagated as a first-class field through admission, queue,
and scheduling layers. The worker registry tracks currently loaded
adapters per worker, the scheduler routes requests toward workers with
the required adapter already resident, and requests sharing an adapter
are grouped into the same batch window where possible. The prefix
location map is keyed by model, adapter, and prefix hash combined, since
KV states are adapter-specific and cross-adapter reuse is invalid.
 
5. Heterogeneity-Aware Autoscaling\
Clusters increasingly mix GPU generations, NPUs, and devices with
different memory and compute characteristics, but standard autoscalers
treat all replicas as equivalent and scale on aggregate utilization,
which is not comparable across device classes. The framework maintains a
capacity model per accelerator class tracking token throughput, maximum
batch size, and KV memory headroom, and scales each class as an
independent pool. Requests are matched to device classes at admission
based on prompt length and expected KV footprint, cost-weighted
scale-out prefers lower-cost classes when latency constraints allow, and
capacity models are periodically recalibrated against observed sidecar
throughput rather than relying on static vendor specifications.
