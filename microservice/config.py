#!/usr/bin/env python3
# config.py

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
import yaml
from urllib.parse import urlparse


# =========================
# Prompt sources
# =========================

@dataclass
class FilePromptsConfig:
    path: str = "prompts.json"
    variant: str = "medium"  # short | medium | long


@dataclass
class HFLmsysConfig:
    dataset_name: str = "/mnt/nvme1/saeid/datasets/lmsys_chat_1m"
    split: str = "train"
    tokenizer_name: str = "/mnt/nvme1/saeid/models/qwen3-8b"
    streaming: bool = False
    min_input_tokens: Optional[int] = None
    max_input_tokens: Optional[int] = None
    repeat_each: int = 1


# =========================
# Load generation
# =========================

@dataclass
class LoadPatternConfig:
    pattern: str = "dump"
    rate_rps: float = 5.0
    duration_s: float = 60.0
    warmup_reqs: int = 0

    burst_on_s: float = 2.0
    burst_off_s: float = 2.0
    burst_rps_on: float = 10.0
    burst_rps_off: float = 0.0

    step_schedule: str = "0:3,30:5,60:1"

    rand_rps_min: Optional[float] = None
    rand_rps_max: Optional[float] = None
    rand_epoch_s: float = 5.0
    loadgen_seed: int = 12345


# =========================
# Generation parameters
# =========================

@dataclass
class GenerationConfig:
    max_tokens: int = 256
    temperature: float = 0.0
    length_mode: str = "legacy"
    target_output_tokens: Optional[int] = None
    target_total_tokens: Optional[int] = None
    think: bool = False


# =========================
# Metrics / observability
# =========================

@dataclass
class PrometheusMetricsConfig:
    enabled: bool = True
    prometheus_base_url: str = "http://7.216.57.215:31190"
    scrape_interval_s: float = 2.0
    window_s: float = 10.0
    include_debug_metrics: bool = False
    model_name: Optional[str] = None
    max_instances: Optional[int] = None


# =========================
# Transport (router backend)
# =========================

@dataclass
class TransportConfig:
    """
    Transport selection for the router backend.

    - "sync": existing behavior (POST /enqueue, block until completion).
    - "async_pubsub": submit+ack (POST /submit) + receive completions via one ZMQ SUB socket.
      End-of-run termination:
        (A) If Prometheus shows vllm:num_requests_running == 0 for ALL vLLM instances
            continuously for idle_zero_running_s -> declare remaining pending LOST.
        (B) Backstop: if no completion is received for idle_timeout_s -> declare pending LOST.
    """
    mode: str = "sync"  # sync | async_pubsub

    # Router endpoint for submit in async_pubsub mode.
    submit_path: str = "/submit"

    # ZMQ subscriber endpoint where router publishes completions.
    # If empty in YAML, we auto-derive a default from router_url.
    results_zmq: str = ""

    # Topic prefix; router publishes as f"{topic}.{run_id_or_default}"
    # Client SUBSCRIBE uses prefix matching; subscribing to "results" will also receive "results.default".
    topic: str = "results"

    # Optional run identifier used for pubsub topic isolation.
    #
    # IMPORTANT:
    # - Router only includes a run_id in published payload if the client supplies meta["__run_id"].
    # - If you set this in YAML but do NOT also inject __run_id into meta, the client will FILTER OUT results.
    # - So default is None (no filtering).
    run_id: Optional[str] = None

    # ZMQ SUB receive high-water-mark (client-side queue). (Router has its own HWM too.)
    results_hwm: int = 10000

    # Backstop: after the last RECEIVED completion (last RECV), wait at most this many seconds.
    # If no new completion arrives in that idle window, mark remaining pending as LOST and terminate.
    idle_timeout_s: float = 60.0

    # Prometheus fleet-idle detector (preferred).
    # If vllm:num_requests_running is ZERO across ALL vLLM instances continuously
    # for this many seconds while the experiment still has pending requests, we treat
    # those pending requests as LOST and terminate.
    #
    # Set <=0 to disable and rely only on idle_timeout_s.
    idle_zero_running_s: float = 10.0

    # Buffer completions that arrive BEFORE the submit thread records pending[rid].
    # TTL bounds memory; size cap bounds worst-case.
    orphan_ttl_s: float = 300.0
    orphan_max: int = 100000


# =========================
# AIBrix backend
# =========================

@dataclass
class AIBrixConfig:
    """
    Runtime config for AIBrix gateway requests.

    This is separate from Helm knobs. Helm only controls whether the deployed
    vLLM pods expose the labels AIBrix needs for discovery.
    """
    base_url: str = "http://127.0.0.1:31639"
    chat_path: str = "/v1/chat/completions"
    model: str = "served-model"
    routing_strategy: str = "least-request"
    timeout_s: float = 1000.0

    # Keep HTTP responses open; threaded open-loop client will issue concurrent requests.
    stream: bool = False

    # Forward non-standard generation fields to AIBrix so its requests stay aligned
    # with the router path. This is especially important for disabling thinking mode.
    forward_extra_generation_fields: bool = True


# =========================
# NEW: LiteLLM backend
# =========================

@dataclass
class LiteLLMConfig:
    """
    Runtime config for LiteLLM proxy requests.

    LiteLLM proxy speaks OpenAI /v1/chat/completions format, identical to
    AIBrix. This config is used when backend="litellm" in the client YAML.

    The LiteLLM proxy sits in front of the router and handles:
      - Virtual key auth (Bearer sk-...)
      - Per-key/team spend tracking
      - Rate limiting

    For benchmarking, always use backend="router" to bypass LiteLLM entirely.
    Use backend="litellm" only for production/demo validation runs.
    """
    base_url: str = "http://127.0.0.1:30400"   # NodePort exposed by litellm-proxy Service
    chat_path: str = "/v1/chat/completions"
    model: str = "served-model"                  # must match model_name in litellm_config.yaml
    api_key: str = "sk-litellm-master"           # virtual key or master key
    timeout_s: float = 1000.0
    stream: bool = False


# =========================
# BooM Gateway backend
# =========================

@dataclass
class BooMConfig:
    """
    Runtime config for BooM Gateway requests.

    BooM Gateway speaks the same OpenAI /v1/chat/completions protocol as
    LiteLLM. This config is used when backend="boom" in the client YAML.

    BooM Gateway is a Rust replacement for LiteLLM that provides:
      - Virtual key auth (Bearer sk-...)
      - Per-key/team spend tracking
      - Rate limiting / plans
      - Multi-provider routing

    For benchmarking, always use backend="router" to bypass BooM entirely.
    Use backend="boom" only for production/demo validation runs.
    """
    base_url: str = "http://127.0.0.1:30401"   # NodePort exposed by boom-proxy Service
    chat_path: str = "/v1/chat/completions"
    model: str = "served-model"                  # must match model_name in boom config
    api_key: str = "sk-boom-master"              # virtual key or master key
    timeout_s: float = 1000.0
    stream: bool = False


# =========================
# SLO annotations (client → router)
# =========================

@dataclass
class SLOConfig:
    """
    Controls SLO annotations injected into each request by the load client.

    When enabled=true, the client attaches per-request SLO fields to the
    /enqueue or /submit payload so the router can do slack-based scheduling.

    The `mix` section defines the traffic composition: each entry maps a
    task_type to its SLO targets and the fraction of requests that receive
    those targets.  Fractions must sum to <= 1.0; the remainder gets no SLO.

    Example YAML:
        slo:
          enabled: false
          default_slo_type: "ttft+tpot"
          mix:
            - task_type: "chat"
              fraction: 0.5
              slo_type: "ttft+tpot"
              slo_ttft_ms: 500.0
              slo_tpot_ms: 50.0
            - task_type: "summarize"
              fraction: 0.3
              slo_type: "e2e"
              slo_e2e_ms: 10000.0
            - task_type: "code"
              fraction: 0.2
              slo_type: "tpot"
              slo_tpot_ms: 30.0
    """
    enabled: bool = False
    default_slo_type: str = "ttft+tpot"
    mix: Optional[list] = None


# =========================
# Helm knobs for sweeps
# =========================

@dataclass
class HelmConfig:
    """
    Helm knobs used by the sweep runner.

    These are NOT used by main.py directly.
    They allow each client config YAML to control:
      - initial vLLM replicas
      - autoscaling enabled + key autoscaling fields
      - sidecar batch size
      - router KV-aware and LEN-aware toggles + policy
      - whether the deployed vLLM pods expose AIBrix discovery labels
      - vLLM runtime flags (gpu memory, quantization, expert parallel, etc.)

    vLLM flag defaults are intentionally conservative (no quantization, no expert
    parallel) so that configs that don't explicitly set these fields are safe for
    dense non-quantized models like Qwen3. MoE/quantized models (e.g. GLM-5 W4A8)
    must explicitly set vllm_quantization and vllm_enable_expert_parallel in their
    client config YAML.
    """
    # initial replicas for vLLM deployment (even when autoscaling is enabled)
    replicas: int = 4

    # sidecar batch size
    batch_size: int = 8

    # tensor parallelism size for vLLM (1 = no parallelism, 2 = 2-way TP, etc.)
    tensor_parallel_size: int = 1

    # autoscaling toggle + core knobs
    autoscaling_enabled: bool = False
    autoscaling_min: int = 1
    autoscaling_max: int = 16
    autoscaling_threshold: str = "16"

    # Prometheus query used by the autoscaler (string; can be multi-line in YAML)
    autoscaling_prometheus_query: str = 'max(router_central_queue_length{namespace="vllm"})'

    # ---- router feature toggles (maps to Helm chart values.router.*) ----
    router_kv_aware: bool = True
    router_len_aware: bool = True
    router_len_policy: str = "short_first"  # short_first | long_first | even_short_long

    # ---- AIBrix exposure knobs (maps to Helm chart values.aibrix.*) ----
    aibrix_enabled: bool = False
    aibrix_model_name: str = "served-model"
    aibrix_port: int = 8200
    # --------------------------------------------------------------------

<<<<<<< HEAD
    # ---- SLO-aware routing knobs (maps to Helm chart values.router.slo*) ----
    router_slo_aware: bool = False
    router_slo_with_kv: bool = True
    router_admission_throttle: bool = False
    router_fixed_batch_size: int = 0
    router_output_len_predictor: str = "simple"
    router_batch_size_estimate: str = "fixed"
    router_fixed_batch_estimate: int = 8
    router_latency_predictor: str = "linear"
    router_latency_online_update: bool = False
    router_latency_profile_path: str = ""
    router_queue_wait_model: str = "none"
    router_chunked_prefill_aware: bool = False
    router_max_num_batched_tokens: int = 0
=======
    # ---- Mooncake KV cache transfer knobs (maps to Helm chart values.mooncake.*) ----
    mooncake_enabled: bool = False
    mooncake_host_network: bool = True
    mooncake_master_server_address: Optional[str] = None
    mooncake_master_port: Optional[int] = None
    mooncake_global_segment_size: Optional[int] = None
    mooncake_ascend_buffer_pool: Optional[str] = None
    mooncake_lookup_rpc_port: Optional[str] = None
>>>>>>> 557a338 (add mooncake related config to vllm-kv-stack, config.py and helm chart template)
    # --------------------------------------------------------------------

    # ---- Deploy mode: "helm" (direct Helm CLI) or "operator" (VllmKvStack CR) ----
    deploy_mode: str = "helm"  # helm | operator
    operator_cr_name: str = "vllm"  # metadata.name for the VllmKvStack CR
    # --------------------------------------------------------------------

    # ---- Service implementation: "python" (default) or "go" (operator-go images) ----
    service_impl: str = "python"  # python | go
    # --------------------------------------------------------------------

    # ---- vLLM model config (maps to Helm chart values.modelVolume.*) ----
    model_name: str = "served-model"  # vLLM served model name (--served-model-name)
    nfs_path: str = ""  # NFS path to model (e.g., /saeid/models/GLM-5-w4a8-mtp-QuaRot)
    # --------------------------------------------------------------------

    # ---- vLLM runtime flags (maps to Helm chart values.vllm.*) ----
    vllm_gpu_memory_utilization: Optional[float] = 0.95
    vllm_quantization: Optional[str] = None
    vllm_enable_expert_parallel: bool = False
    vllm_max_model_len: Optional[int] = None
    vllm_compilation_config: Optional[str] = '{"cudagraph_mode": "FULL_DECODE_ONLY"}'
    vllm_trust_remote_code: bool = False
    vllm_max_num_batched_tokens: Optional[int] = None
    vllm_seed: Optional[int] = None
    vllm_additional_config: Optional[str] = None
    vllm_node_selector: Optional[str] = None           # JSON string, e.g. '{"kubernetes.io/hostname": "node4"}'
    vllm_speculative_config: Optional[str] = None
    # --------------------------------------------------------------------


# =========================
# Top-level client config
# =========================

@dataclass
class ClientConfig:
    router_url: str = "http://127.0.0.1:30080"
    total_requests: int = 50
    prompt_source: str = "file"

    # Chooses how requests are executed and how methods are interpreted in sweeps:
    # - router   -> methods are router modes (pull, push-rr, ...)
    # - aibrix   -> methods are AIBrix routing strategies (least-request, prefix-cache, ...)
    # - litellm  -> routes through LiteLLM proxy (production auth/spend validation only,
    #               NOT for benchmarking — use router directly for clean measurements)
    # - boom     -> routes through BooM Gateway (Rust LiteLLM replacement, same protocol)
    backend: str = "router"  # router | aibrix | litellm | boom

    file_prompts: FilePromptsConfig = field(default_factory=FilePromptsConfig)
    hf_lmsys: HFLmsysConfig = field(default_factory=HFLmsysConfig)
    load_pattern: LoadPatternConfig = field(default_factory=LoadPatternConfig)
    generation: GenerationConfig = field(default_factory=GenerationConfig)

    # Output / tracing
    output_log_mode: str = "full"
    print_trace: bool = False

    # Metrics
    metrics: PrometheusMetricsConfig = field(default_factory=PrometheusMetricsConfig)

    # Router transport config
    transport: TransportConfig = field(default_factory=TransportConfig)

    # AIBrix runtime config
    aibrix: AIBrixConfig = field(default_factory=AIBrixConfig)

    # LiteLLM runtime config
    litellm: LiteLLMConfig = field(default_factory=LiteLLMConfig)

    # BooM Gateway runtime config
    boom: BooMConfig = field(default_factory=BooMConfig)

    # SLO annotation config
    slo: SLOConfig = field(default_factory=SLOConfig)

    # Helm knobs
    helm: HelmConfig = field(default_factory=HelmConfig)


# =========================
# Helpers
# =========================

def _merge_dataclass(dc_cls, data_dict: dict):
    kwargs = {}
    for field_name in dc_cls.__dataclass_fields__.keys():
        if field_name in data_dict:
            kwargs[field_name] = data_dict[field_name]
    base = dc_cls()
    for k, v in kwargs.items():
        setattr(base, k, v)
    return base


def _derive_results_zmq_from_router_url(router_url: str) -> str:
    """
    Derive a sensible default ZMQ endpoint from router_url.

    Original assumption: you expose router's ZMQ PUB via a NodePort 30559:
      http://<host>:<anything> -> tcp://<host>:30559
    """
    try:
        u = urlparse(router_url)
        host = u.hostname or "127.0.0.1"
        return f"tcp://{host}:30559"
    except Exception:
        return "tcp://127.0.0.1:30559"


# =========================
# Config loader
# =========================

def load_config(path: str) -> ClientConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}

    router_url = raw.get("router_url", ClientConfig.router_url)
    total_requests = int(raw.get("total_requests", ClientConfig.total_requests))
    prompt_source = raw.get("prompt_source", ClientConfig.prompt_source)

    backend = str(raw.get("backend", ClientConfig.backend) or ClientConfig.backend).strip().lower()
    if backend not in ("router", "aibrix", "litellm", "boom"):
        raise ValueError(f"Invalid backend '{backend}'. Expected 'router', 'aibrix', 'litellm', or 'boom'.")

    file_prompts = _merge_dataclass(FilePromptsConfig, raw.get("file_prompts", {}))
    hf_lmsys = _merge_dataclass(HFLmsysConfig, raw.get("hf_lmsys", {}))
    load_pattern = _merge_dataclass(LoadPatternConfig, raw.get("load_pattern", {}))
    generation = _merge_dataclass(GenerationConfig, raw.get("generation", {}))
    metrics = _merge_dataclass(PrometheusMetricsConfig, raw.get("metrics", {}))

    # backend-specific config
    transport = _merge_dataclass(TransportConfig, raw.get("transport", {}))
    aibrix = _merge_dataclass(AIBrixConfig, raw.get("aibrix", {}))
    litellm = _merge_dataclass(LiteLLMConfig, raw.get("litellm", {}))
    boom = _merge_dataclass(BooMConfig, raw.get("boom", {}))

    # SLO config
    slo = _merge_dataclass(SLOConfig, raw.get("slo", {}))

    # helm config
    helm = _merge_dataclass(HelmConfig, raw.get("helm", {}))

    output_log_mode = raw.get("output_log_mode", ClientConfig.output_log_mode)
    print_trace = raw.get("print_trace", ClientConfig.print_trace)

    # -----------------------------
    # Normalize router transport
    # -----------------------------
    if backend == "router" and str(transport.mode).lower() == "async_pubsub":
        if not str(transport.results_zmq or "").strip():
            transport.results_zmq = _derive_results_zmq_from_router_url(router_url)

        if transport.run_id is not None and not str(transport.run_id).strip():
            transport.run_id = None

        try:
            transport.results_hwm = int(transport.results_hwm)
        except Exception:
            transport.results_hwm = 10000

        try:
            transport.idle_timeout_s = float(transport.idle_timeout_s)
        except Exception:
            transport.idle_timeout_s = 60.0
        transport.idle_timeout_s = max(1.0, transport.idle_timeout_s)

        try:
            transport.idle_zero_running_s = float(getattr(transport, "idle_zero_running_s", 10.0))
        except Exception:
            transport.idle_zero_running_s = 10.0
        if transport.idle_zero_running_s < 0.0:
            transport.idle_zero_running_s = 0.0

        try:
            transport.orphan_ttl_s = float(transport.orphan_ttl_s)
        except Exception:
            transport.orphan_ttl_s = 300.0
        transport.orphan_ttl_s = max(1.0, transport.orphan_ttl_s)

        try:
            transport.orphan_max = int(transport.orphan_max)
        except Exception:
            transport.orphan_max = 100000
        transport.orphan_max = max(1000, transport.orphan_max)

    # -----------------------------
    # Normalize AIBrix config
    # -----------------------------
    if backend == "aibrix":
        aibrix.base_url = str(aibrix.base_url or AIBrixConfig.base_url).rstrip("/")
        aibrix.chat_path = str(aibrix.chat_path or AIBrixConfig.chat_path)
        if not aibrix.chat_path.startswith("/"):
            aibrix.chat_path = "/" + aibrix.chat_path

        try:
            aibrix.timeout_s = float(aibrix.timeout_s)
        except Exception:
            aibrix.timeout_s = 1000.0
        aibrix.timeout_s = max(1.0, aibrix.timeout_s)

        aibrix.forward_extra_generation_fields = bool(
            getattr(aibrix, "forward_extra_generation_fields", True)
        )

        chart_vllm_port = 8200

        if "helm" not in raw or "aibrix_enabled" not in raw.get("helm", {}):
            helm.aibrix_enabled = True
        if "helm" not in raw or "aibrix_model_name" not in raw.get("helm", {}):
            helm.aibrix_model_name = str(aibrix.model)
        if "helm" not in raw or "aibrix_port" not in raw.get("helm", {}):
            helm.aibrix_port = chart_vllm_port

    # -----------------------------
    # Normalize LiteLLM config
    # -----------------------------
    if backend == "litellm":
        litellm.base_url = str(litellm.base_url or LiteLLMConfig.base_url).rstrip("/")
        litellm.chat_path = str(litellm.chat_path or LiteLLMConfig.chat_path)
        if not litellm.chat_path.startswith("/"):
            litellm.chat_path = "/" + litellm.chat_path

        try:
            litellm.timeout_s = float(litellm.timeout_s)
        except Exception:
            litellm.timeout_s = 1000.0
        litellm.timeout_s = max(1.0, litellm.timeout_s)

    # -----------------------------
    # Normalize BooM Gateway config
    # -----------------------------
    if backend == "boom":
        boom.base_url = str(boom.base_url or BooMConfig.base_url).rstrip("/")
        boom.chat_path = str(boom.chat_path or BooMConfig.chat_path)
        if not boom.chat_path.startswith("/"):
            boom.chat_path = "/" + boom.chat_path

        try:
            boom.timeout_s = float(boom.timeout_s)
        except Exception:
            boom.timeout_s = 1000.0
        boom.timeout_s = max(1.0, boom.timeout_s)

    return ClientConfig(
        router_url=router_url,
        total_requests=total_requests,
        prompt_source=prompt_source,
        backend=backend,
        file_prompts=file_prompts,
        hf_lmsys=hf_lmsys,
        load_pattern=load_pattern,
        generation=generation,
        output_log_mode=output_log_mode,
        print_trace=print_trace,
        metrics=metrics,
        transport=transport,
        aibrix=aibrix,
        litellm=litellm,
        boom=boom,
        slo=slo,
        helm=helm,
    )