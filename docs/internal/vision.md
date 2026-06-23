# Vision: load-aware LLM serving on Kubernetes

> Design vision and aspirational feature set. For what is implemented today, see the [architecture docs](../architecture/overview.md). This is a planning record, not a user guide.

Serving large generative models requires balancing user-facing latency metrics such as time-to-first-token and time-between-tokens with system goals such as throughput, accelerator utilization, and cost efficiency.

Traditional serving systems scale entire model replicas using generic infrastructure signals such as CPU or accelerator utilization. This approach performs poorly for LLM workloads because requests vary widely in prompt length, decode length, KV cache footprint, and service time. LLM inference systems also exhibit additional complexities: continuous batching behavior strongly affects throughput and latency; KV cache reuse can significantly reduce prefill costs but introduces placement and memory management challenges; and modern inference clusters increasingly contain heterogeneous accelerator types (GPUs and NPUs) with different memory capacities and compute characteristics.

LA-Boom proposes a Kubernetes-native architecture for LLM inference that provides deeper control over request admission, scheduling, autoscaling, and memory management. The system combines centralized admission control, pull-based scheduling, LLM-aware autoscaling, KV-aware runtime behavior, heterogeneous accelerator awareness, and extensible backend integration. The goal is to provide both a production-ready serving platform and a flexible research framework for exploring advanced scheduling and memory-management strategies.

## Feature areas

### Queueing and scheduling

- **Centralized request orchestration** — a central queue receives all incoming requests before they are assigned to inference engine replicas such as vLLM.
- **Pull-based scheduling** — workers pull requests from the central queue when they have available capacity.
- **Token-length-aware queue ordering** — queue ordering incorporates token-length predictions to reduce head-of-line blocking.
- **Adaptive batch size control** — batch sizes adapt to latency targets and memory constraints.
- **Multi-queue scheduling** — multiple queues separate workloads or priority levels.
- **Hierarchical queueing** — queues at cluster, node, and worker levels.
- **Token-level capacity scheduling** — scheduling considers token workload instead of request counts.
- **Preemption-aware scheduling** — scheduling considers backend memory pressure and preemption behavior.

### Autoscaling and capacity management

- **LLM-aware autoscaling** — scaling decisions use LLM workload signals such as queue pressure and token workload.
- **Operator-level autoscaling** — scale model operators independently rather than scaling entire replicas.
- **Joint load balancing and autoscaling control** — coordinate routing and scaling loops.
- **KV-aware scale-down** — preserve replicas containing valuable KV state during scale-down.
- **Fast replica startup** — reduce startup latency via optimized container images and layered model loading.
