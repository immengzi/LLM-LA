# LLM Load-Aware Load Balancing framework on Kubernetes with LLM-LA #
Serving large generative models requires balancing user-facing latency metrics such as time-to-first-token and time-between-tokens with system goals such as throughput, accelerator utilization, and cost efficiency.
Traditional serving systems scale entire model replicas using generic infrastructure signals such as CPU or accelerator utilization. This approach performs poorly for LLM workloads because requests vary widelyin prompt length, decode length, KV cache footprint, and service time. LLM inference systems also exhibit additional complexities. Continuous batching behavior strongly affects throughput and latency. KV cache reuse can significantly reduce prefill costs but introduces placement and memory management challenges. In addition, modern inference clusters increasingly contain heterogeneous accelerator types such as GPUs and NPUs with different memory capacities and compute characteristics.
This framework proposes a Kubernetes-native architecture for LLM inference that provides deeper control over request admission, scheduling, autoscaling, and memory management. The system combines centralized admission control, pull-based scheduling, LLM-aware autoscaling, KV-aware runtime behavior, heterogeneous accelerator awareness, and extensible backend integration. The goal is to provide both a production-ready serving platform and a flexible research framework for exploring advanced scheduling and memory-management strategies.



## Feature Guide ##
We provide LLM serving capabilities that are fully designed, documented, tested, and benchmarked to minimise the integration complexity and reduce your operational overhead. The following features are provided with deployment and testing examples:

### Queueing and Scheduling ###
* **Centralized Request Orchestration**– Deploy a **Centralized Queue** to receives all incoming requests before they are assigned to inference engine replicas like **vLLM**. 
* **Pull-Based Scheduling**– Workers pull requests from the centralized queue when they have available capacity.
* **Token-Length--Aware Queue Ordering**– Queue ordering may incorporate token-length predictions to reduce head-of-line blocking.
* **Adaptive Batch Size Control**– Batch sizes can dynamically adapt to latency targets and memory constraints.
* **Multi-Queue Scheduling**– Multiple queues may be used to separate workloads or priority levels.
* **Hierarchical Queueing**– Queues may exist at cluster, node, and worker levels.
* **Token-Level Capacity Scheduling**– Scheduling can consider token workload instead of request counts.
* **Preemption-Aware Scheduling**– Scheduling may consider backend memory pressure and preemption behavior.

### Autoscaling and Capacity Management ###
* **LLM-Aware Autoscaling**– Scaling decisions use LLM workload signals such as queue pressure and token workload.
* **Operator-Level Autoscaling**– Future versions may scale model operators independently rather than scaling entire replicas.
* **Joint Load Balancing and Autoscaling Control**– Routing and scaling loops may be coordinated.
* **KV-Aware Scale-Down**– Replicas containing valuable KV states may be preserved during scale-down.
* **Fast Replica Startup**– Replica startup latency may be reduced using optimized container images and layered model loading.


