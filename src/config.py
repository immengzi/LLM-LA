#!/usr/bin/env python3
# config.py

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, List, Optional
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
    dataset_profile: Optional[str] = None
    dataset_name: str = "/home/data/saeid/datasets/lmsys_chat_1m"
    split: str = "train"
    tokenizer_name: str = "/home/models/qwen3-8b"
    streaming: bool = False
    min_input_tokens: Optional[int] = None
    max_input_tokens: Optional[int] = None
    min_output_tokens: Optional[int] = None
    max_output_tokens: Optional[int] = None
    repeat_each: int = 1
    seed: Optional[int] = None
    multi_turn: bool = False
    min_user_turns: Optional[int] = None
    max_pool_size: Optional[int] = None


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


@dataclass
class UsersLoadConfig:
    """Closed-loop "users" load model (used when transport.mode == "claude").

    Instead of an open-loop arrival rate (LoadPatternConfig), this models N
    concurrent *users*, each running ``convs_per_user`` conversations
    sequentially with ``interval_between_convs_s`` seconds between them. Each
    conversation is a distinct multi-turn chat; turns within a conversation fire
    back-to-back (no intra-conversation delay). Total conversations = num_users *
    convs_per_user.
    """
    num_users: int = 10
    convs_per_user: int = 2
    interval_between_convs_s: float = 5.0
    # Stagger each user's start by user_index * ramp_s (0 = all start together).
    ramp_s: float = 0.0


# =========================
# Generation parameters
# =========================

@dataclass
class GenerationConfig:
    max_tokens: int = 256
    min_tokens: Optional[int] = None
    temperature: float = 0.0
    length_mode: str = "legacy"
    target_output_tokens: Optional[int] = None
    target_total_tokens: Optional[int] = None
    think: bool = False
    use_dataset_output_len: bool = False
    # When None: enable ignore_eos automatically iff use_dataset_output_len (vLLM fixed-length).
    ignore_eos: Optional[bool] = None

    # Minimum floor for output tokens when replaying dataset lengths.
    # _replace_gen_cfg clamps output_tokens to at least this value.
    min_output_tokens_floor: int = 1024

    # Replay output lengths from a previous experiment's logs.json.
    # Path to a logs.json (JSONL) from a previous run. Each line must have
    # "idx" and "completion_tokens".  When set, the completion_tokens from
    # that file are used as per-request max_tokens/min_tokens (same as
    # use_dataset_output_len but sourced from a prior run instead of the
    # LMSYS dataset).  Implies use_dataset_output_len=True + ignore_eos.
    replay_output_lengths_from: Optional[str] = None


def generation_effective_ignore_eos(gen_cfg: GenerationConfig) -> bool:
    """True → request vLLM/OpenAI-compat ignore_eos so EOS does not end decoding early."""
    if gen_cfg.ignore_eos is not None:
        return bool(gen_cfg.ignore_eos)
    return bool(gen_cfg.use_dataset_output_len)


# =========================
# Metrics / observability
# =========================

@dataclass
class PrometheusMetricsConfig:
    enabled: bool = True
    prometheus_base_url: str = "http://10.50.156.65:31190"
    scrape_interval_s: float = 2.0
    window_s: float = 10.0
    include_debug_metrics: bool = False
    model_name: Optional[str] = None
    max_instances: Optional[int] = None


@dataclass
class RedisWatchConfig:
    """Live Redis KV-block ownership watcher.

    When enabled, a background thread periodically SCANs ``<model>:kvblock:*`` in
    Redis and appends a per-tick summary (key count, unique owners, shared-block
    distribution, top owners) to ``redis_kv_watch.jsonl`` in the experiment dir,
    in parallel with the load run (wired into main.py like the Prometheus
    collector). Off by default.

    ``node_ip`` empty -> derive the host from ``router_url``. ``model`` empty ->
    fall back to boom.model / the served model prefix.
    """
    enabled: bool = False
    node_ip: str = ""          # Redis host (k8s node IP for NodePort); empty -> router_url host
    port: int = 30079
    db: int = 0
    password: Optional[str] = None
    interval_s: float = 2.0
    model: str = ""            # key prefix <model>:kvblock:* ; empty -> boom.model
    max_keys: int = 5000
    scan_count: int = 500


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


@dataclass
class ClaudeTransportConfig:
    """Runtime config for the integrated real-``claude``-CLI transport.

    Active when ``transport.mode == "claude"``. Each conversation turn shells out
    to the ``claude`` binary (``claude -p <turn> --output-format json
    [--resume <session_id>]``) so the real Claude Code request envelope (its own
    system prompt + growing history) hits the BooM Anthropic-compatible gateway.

    ``base_url`` / ``model`` / ``api_key`` default from the ``boom`` block when
    left empty, so auth/endpoint work exactly like current BooM access:
      - ANTHROPIC_BASE_URL = base_url (no ``/v1``)
      - ANTHROPIC_AUTH_TOKEN = api_key (e.g. ``sk-boom-master``)
    Tools are OFF by default (text-only, safe).
    """
    claude_bin: str = "claude"
    base_url: str = ""          # empty -> boom.base_url
    model: str = ""             # empty -> boom.model
    api_key: str = ""           # empty -> boom.api_key
    bare: bool = False
    enable_tools: bool = False
    timeout_s: float = 300.0


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
    # Must be >= longest expected generation; align with boom.upstreamTimeoutSeconds / litellm_params.timeout (often 7200).
    timeout_s: float = 7200.0
    stream: bool = False

    # Key-affinity benchmarking: number of pre-seeded virtual keys.
    # 0 = disabled (all requests use api_key above).
    # >0 = per-conversation key rotation using sk-bench-{conv_id % N}.
    # Requires BooM deployed with keyAffinityBench=true + directRoutingStrategy=key_affinity.
    key_affinity_keys: int = 0


# =========================
# Multi-model client routing
# =========================

@dataclass
class MultiModelTarget:
    """One target model for multi-model load generation."""
    model: str = "served-model"
    weight: float = 1.0
    prompt_source: Optional[str] = None
    hf_lmsys: Optional[dict] = None


@dataclass
class MultiModelConfig:
    """Client-side multi-model routing configuration.

    strategy:
      - "fraction": each request picks one model by weighted probability.
      - "mirror":   every request is cloned and sent to ALL target models.
    """
    strategy: str = "fraction"
    targets: List[MultiModelTarget] = field(default_factory=list)


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

    # Scaling signal: "queue" (router central queue, per model) or "vllm"
    # (vLLM KV-cache pressure; works for router-less topologies).
    autoscaling_signal: str = "queue"
    # Threshold for the vllm signal (fraction of KV cache utilised, 0..1).
    autoscaling_vllm_threshold: str = "0.8"
    # Prometheus endpoint KEDA queries (kube-prometheus-stack service by default).
    autoscaling_prometheus_server_address: str = (
        "http://kube-prometheus-stack-prometheus.monitoring.svc:9090"
    )
    # KEDA polling / cooldown.
    autoscaling_polling_interval: int = 10
    autoscaling_cooldown_period: int = 300

    # Optional full-query override (advanced). Empty -> use the signal-derived
    # per-model query built by the chart. Kept for backward compatibility with
    # older configs that set a global query string.
    autoscaling_prometheus_query: str = ""

    # ---- unified routing strategy (maps to values.router.strategy) ----
    # "" = use router_kv_aware / router_affinity_enabled below directly.
    # One of: none | prefix | affinity | both -> overrides those flags.
    router_strategy: str = ""

    # ---- KV-block hash source (maps to values.router.hashSource) ----
    # "inline" = in-process / in-container hasher (default); "external" = legacy
    # vllm-cpu-hash service (the external pod is auto-deployed when external).
    router_hash_source: str = "inline"

    # ---- KV block-owner source for prefix routing (maps to values.router.ownerSource) ----
    # "lookup"  = targeted per-request Redis HGETALL of the request's own block
    #             hashes at ingress (default). Makes prefix/both routing and the
    #             kv_hit metric truthful; inert under affinity/none strategies.
    # "watcher" = legacy background scan that populated a shared block-owner map
    #             (add-only, could starve under load -> under-counted kv_hit).
    router_owner_source: str = "lookup"
    # Cap on how many leading block hashes are looked up per request (owner
    # source = lookup). Maps to values.router.lookupMaxBlocks.
    router_lookup_max_blocks: int = 512

    # ---- Claude Code attribution stripping (maps to values.router.stripCch) ----
    # "1" strips the whole x-anthropic-billing-header attribution block (rotating
    # cc_version + per-request cch counter) from system messages router-side so
    # KV-prefix matching is byte-stable across requests. "0" leaves requests
    # untouched. Default OFF.
    router_strip_cch: str = "0"

    # ---- full prefix block-hash logging (maps to values.router.logBlockHashes) ----
    # When true, the router emits each request's full block-hash list into
    # /latency_log; with collect_router_log on, it lands in router_logs.json and
    # logs.json. Bulky; off by default. Hit counts are always logged regardless.
    router_log_block_hashes: bool = False

    # ---- full request-body logging (maps to values.router.logRequestBody) ----
    # When true, the router stores each request's full body (messages + sampling
    # params) into /latency_log; with collect_router_log on it lands in
    # router_logs.json / logs.json. Opt-in and off by default. Bodies ride the
    # bounded /latency_log ring (evict automatically); router_log_request_body_max_bytes
    # caps each body (0 = unlimited).
    router_log_request_body: bool = False
    router_log_request_body_max_bytes: int = 16384

    # ---- prefix measurement-only (maps to values.router.measurePrefix) ----
    # When true, the router computes per-request prefix blocks + the chosen
    # endpoint's hit count for LOGGING even when KV routing is off (affinity-only
    # / none). Makes kv_hits_len/total_blocks/kv_hit comparable across the four
    # strategies. Routing is unaffected.
    router_measure_prefix: bool = False

    # ---- router feature toggles (maps to Helm chart values.router.*) ----
    router_kv_aware: bool = True
    router_len_aware: bool = True
    router_len_policy: str = "short_first"  # short_first | long_first | even_short_long
    router_api_key: str = ""  # API key for /v1/chat/completions (empty = no auth)

    # ---- conversation key-affinity knobs (maps to Helm chart values.router.affinity*) ----
    router_affinity_enabled: bool = False
    router_affinity_mode: str = "soft"  # soft | hard
    router_affinity_ttl_s: float = 86400.0
    router_affinity_hard_timeout_s: float = 300.0

    # ---- persistent affinity map (Redis-backed) knobs ----
    # OFF by default; when on, affinity_key->pod is written through to Redis and
    # reloaded on startup so it survives router restarts + full redeploys.
    router_affinity_persist_enabled: bool = False
    router_affinity_redis_ttl_seconds: int = 0        # 0 = no expiry
    router_affinity_redis_key_prefix: str = "affinity"
    router_affinity_cache_max: int = 100000           # in-memory front-cache bound (0 = unbounded)
    router_affinity_cache_refresh_s: float = 0.0      # periodic re-warm (0 = startup only)
    router_affinity_endpoint_stale_s: float = 1800.0  # pod "available"/ready-heartbeat window for stale-pin fallback
    router_affinity_cluster: str = ""                 # key namespace cluster (empty => k8s namespace)

    # ---- AIBrix exposure knobs (maps to Helm chart values.aibrix.*) ----
    aibrix_enabled: bool = False
    aibrix_model_name: str = "served-model"
    aibrix_port: int = 8200
    # --------------------------------------------------------------------

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
    # --------------------------------------------------------------------

    # ---- BooM Gateway: Claude Code alias toggle ----
    # When true and backend=boom, deploy BooM with model aliases so Claude Code's
    # default model names (claude-sonnet-4-20250514, etc.) map to served-model.
    # This is a deployment-only knob — it does not affect the load test client.
    boom_claude_aliases: bool = False

    # BooM routing mode: "router" (default) or "direct" (bypass our router).
    boom_route_via: str = "router"

    # BooM routing strategy when boom_route_via=direct.
    # Options: round_robin (default), key_affinity
    boom_direct_routing_strategy: str = "round_robin"

    # Deploy ephemeral Postgres with 64 pre-seeded virtual keys for
    # key_affinity benchmarking. Only useful with boom_direct_routing_strategy=key_affinity.
    boom_key_affinity_bench: bool = False

    # BooM flow control: max concurrent upstream connections per deployment.
    # 0 = unlimited (default). Set >0 to queue excess requests inside BooM.
    boom_max_inflight: int = 0

    # BooM upstream HTTP timeout (seconds) per model (litellm_params.timeout).
    # 0 = use chart default (values.yaml boom.upstreamTimeoutSeconds, typically 7200).
    boom_upstream_timeout_seconds: int = 0
    # --------------------------------------------------------------------

    # ---- Sidecar ----
    sidecar_log_level: str = "info"   # debug | info | warning | error
    # sidecarPrefetch is per-model (in models[].sidecarPrefetch), not here.
    # --------------------------------------------------------------------

    # ---- NFS cache warm (pre-reads model shards before vLLM starts) ----
    cache_warm_enabled: bool = False
    cache_warm_pvc_name: str = "models-nfs-pvc"
    cache_warm_model_sub_path: str = "GLM-5-w4a8-mtp-QuaRot"
    # --------------------------------------------------------------------

    # ---- Mooncake KV cache transfer (requires backend=router) ----
    mooncake_enabled: bool = False
    mooncake_master_port: int = 50088
    mooncake_master_server_address: str = "10.50.156.65:50088"
    mooncake_global_segment_size: int = 140000000000
    mooncake_eviction_high_watermark: float = 0.9
    mooncake_eviction_ratio: float = 0.1
    mooncake_ascend_buffer_pool: str = "4:8"
    mooncake_lookup_rpc_port: str = "10010"
    mooncake_host_network: bool = False
    deploy_mooncake_master: bool = True
    # --------------------------------------------------------------------

    # ---- LMCache (wraps Mooncake for cross-replica KV coordination) ----
    lmcache_enabled: bool = False
    lmcache_chunk_size: Optional[int] = None
    lmcache_max_local_cpu_size: Optional[int] = None
    # LMCache backend mode: "mooncake" (default, 33/218 lineage) or "p2p"
    # (142 lineage: engine-to-engine HCCL P2P + host-staging + lmcache_controller,
    # no Mooncake). Any value != "p2p" keeps the historical Mooncake behavior.
    lmcache_mode: str = "mooncake"
    # P2P / host-staging knobs (only used when lmcache_mode == "p2p"). None => use
    # the chart's values.yaml default (which matches the 142 reference).
    lmcache_use_host_staging: Optional[bool] = None
    lmcache_os_staging_bytes: Optional[int] = None
    lmcache_p2p_controller_pull_url: Optional[str] = None
    lmcache_p2p_controller_reply_url: Optional[str] = None
    # lmcache_controller deployment (p2p mode). Image defaults to the chart's.
    deploy_lmcache_controller: bool = True
    lmcache_controller_image: Optional[str] = None
    # --------------------------------------------------------------------

    # ---- NDS (NVMe Direct Storage — P2P DMA for KV cache) ----
    lmcache_nds_enabled: bool = False
    lmcache_nds_path: str = "/workspace/nds_kvcache"
    lmcache_nds_dev: str = "/dev/md0"
    lmcache_nds_size: int = 2048
    # Optional per-role overrides for the xds/file_p2p binary path (some hosts
    # stage the build at a different depth on leader vs worker). None => fall
    # back to the chart's single nds xdsPath.
    lmcache_nds_xds_path_leader: Optional[str] = None
    lmcache_nds_xds_path_worker: Optional[str] = None
    # --------------------------------------------------------------------

    # ---- Image overrides (bypass registry rewrite — for local images) ----
    vllm_image: Optional[str] = None               # injected into per-model image field (e.g. "minimax27:selfcontained")
    mooncake_master_image: Optional[str] = None     # sets images.mooncakeMasterRaw (e.g. "minimax27:selfcontained")
    # --------------------------------------------------------------------

    # ---- New vLLM flags (dtype, schedulerCls, modelLoaderExtraConfig, etc.) ----
    dtype: str = "auto"                              # "auto", "bfloat16", "float16"
    scheduler_cls: Optional[str] = None              # e.g. "lsched.lsched_vllm.LSchedVLLMAdapter"
    model_loader_extra_config: Optional[str] = None  # JSON string
    ascend_enable_flashcomm1: bool = False
    # --------------------------------------------------------------------

    # ---- Deploy mode: "helm" (direct Helm CLI) or "operator" (VllmKvStack CR) ----
    deploy_mode: str = "helm"  # helm | operator
    operator_cr_name: str = "vllm"  # metadata.name for the VllmKvStack CR
    # --------------------------------------------------------------------

    # ---- Per-pod vLLM NodePort services ----
    # When true, sweep_methods creates a NodePort Service per vLLM pod so each
    # instance is individually accessible from outside the cluster.
    expose_per_pod: bool = False
    # --------------------------------------------------------------------

    # ---- Service implementation: "python" (default) or "go" (operator-go images) ----
    service_impl: str = "python"  # python | go
    # --------------------------------------------------------------------

    # ---- Shadow deployment overrides ----
    # Override release name and namespace for parallel deployments (e.g. shadow).
    # Default "" means use the global constants (release="vllm", namespace="vllm").
    release: str = ""
    namespace: str = ""
    port_offset: int = 0
    pin_node_name: str = ""

    # ---- Raw Helm values overlay ----
    # Optional dot-path map passed through to sweep_methods.py set_values.
    # Empty by default so legacy configs produce identical effective values.
    values: dict = field(default_factory=dict)
    # --------------------------------------------------------------------

    # ---- vLLM model config (maps to Helm chart values.modelVolume.*) ----
    model_name: str = "served-model"  # vLLM served model name (--served-model-name)
    nfs_path: str = ""  # NFS path to model (e.g., /saeid/models/GLM-5-w4a8-mtp-QuaRot)
    model_host_path: str = ""  # local path on each node (e.g. /home/haiting/models) — empty = NFS PVC
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
    vllm_speculative_config: Optional[str] = None
    vllm_kv_cache_dtype: str = "auto"                   # "auto", "fp8", "fp8_e4m3" — fp8 halves KV memory
    vllm_cpu_offload_gb: Optional[float] = None         # offload N GiB of model weights to CPU for more KV cache
    vllm_enable_prefix_caching: bool = False           # true = enable prefix caching
    vllm_tool_call_parser: Optional[str] = None      # e.g. "qwen3_coder", "glm47"
    vllm_reasoning_parser: Optional[str] = None       # e.g. "qwen3", "glm45"
    # --------------------------------------------------------------------

    # ---- Multi-model support ----
    # When non-empty, Helm generates per-model vLLM Deployments and a shared
    # model-registry ConfigMap consumed by both the router and BooM.
    # Each entry is a dict with keys: name, servedModelName, replicas,
    # modelSubPath, tensorParallelSize, batchSize, vllm (nested dict).
    models: list = field(default_factory=list)
    # --------------------------------------------------------------------

    # ---- Data Parallel (multi-node LWS deployment) ----
    # When enabled, deploys a LeaderWorkerSet instead of the standard Deployment.
    # Requires the LWS CRD/controller to be installed in the cluster.
    data_parallel_enabled: bool = False
    data_parallel_size: int = 2          # pods per DP group (1 leader + N-1 workers)
    data_parallel_groups: int = 1        # number of DP groups (LWS replicas)
    data_parallel_size_local: int = 1    # --data-parallel-size-local per pod
    data_parallel_rpc_port: int = 13389  # --data-parallel-rpc-port
    data_parallel_nic_name: str = ""     # GLOO/TP/HCCL_SOCKET_IFNAME (empty = auto-detect)
    data_parallel_hccl_buff_size: int = 200   # HCCL_BUFFSIZE
    data_parallel_omp_num_threads: int = 16   # OMP_NUM_THREADS
    # --------------------------------------------------------------------


def migrate_legacy_helm_to_models(h: HelmConfig) -> None:
    """Auto-convert legacy flat vllm_*/data_parallel_* fields into a models[] entry.

    If ``h.models`` is already populated the function is a no-op.
    Otherwise it builds a single model entry from the flat fields and assigns
    it to ``h.models``, printing a deprecation warning to stderr.
    """
    if h.models:
        return

    vllm: dict = {}
    if h.vllm_gpu_memory_utilization is not None:
        vllm["gpuMemoryUtilization"] = h.vllm_gpu_memory_utilization
    if h.vllm_quantization is not None:
        vllm["quantization"] = h.vllm_quantization
    if h.vllm_enable_expert_parallel:
        vllm["enableExpertParallel"] = True
    if h.vllm_max_model_len is not None:
        vllm["maxModelLen"] = h.vllm_max_model_len
    if h.vllm_compilation_config is not None:
        try:
            cc = json.loads(h.vllm_compilation_config)
            vllm["compilationConfig"] = {"cudagraphMode": cc.get("cudagraph_mode", "FULL_DECODE_ONLY")}
        except Exception:
            vllm["compilationConfig"] = {"cudagraphMode": "FULL_DECODE_ONLY"}
    if h.vllm_trust_remote_code:
        vllm["trustRemoteCode"] = True
    if h.vllm_max_num_batched_tokens is not None:
        vllm["maxNumBatchedTokens"] = h.vllm_max_num_batched_tokens
    if h.vllm_seed is not None:
        vllm["seed"] = h.vllm_seed
    if h.vllm_additional_config is not None:
        ac = h.vllm_additional_config
        if isinstance(ac, str):
            try:
                ac = json.loads(ac)
            except Exception:
                ac = None
        if isinstance(ac, dict):
            vllm["additionalConfig"] = ac
    if h.vllm_speculative_config is not None:
        sc = h.vllm_speculative_config
        if isinstance(sc, str):
            try:
                sc = json.loads(sc)
            except Exception:
                sc = None
        if isinstance(sc, dict):
            vllm["speculativeConfig"] = sc
    kv_dtype = str(h.vllm_kv_cache_dtype or "auto").strip()
    if kv_dtype and kv_dtype != "auto":
        vllm["kvCacheDtype"] = kv_dtype
    if h.vllm_cpu_offload_gb is not None:
        vllm["cpuOffloadGb"] = h.vllm_cpu_offload_gb
    if h.vllm_enable_prefix_caching:
        vllm["enablePrefixCaching"] = True
    if h.vllm_tool_call_parser is not None:
        vllm["toolCallParser"] = h.vllm_tool_call_parser
    if h.vllm_reasoning_parser is not None:
        vllm["reasoningParser"] = h.vllm_reasoning_parser

    model_entry: dict = {
        "name": "qwen",
        "servedModelName": h.model_name or "served-model",
        "replicas": h.replicas,
        "tensorParallelSize": h.tensor_parallel_size,
        "batchSize": h.batch_size,
    }

    nfs_path = str(h.nfs_path or "").strip()
    if nfs_path:
        model_entry["modelSubPath"] = PurePosixPath(nfs_path.rstrip("/")).name

    if vllm:
        model_entry["vllm"] = vllm

    if h.data_parallel_enabled:
        model_entry["replicas"] = h.data_parallel_groups
        model_entry["dataParallel"] = {
            "enabled": True,
            "size": h.data_parallel_size,
            "sizeLocal": h.data_parallel_size_local,
            "rpcPort": h.data_parallel_rpc_port,
            "nicName": h.data_parallel_nic_name,
            "hcclBuffSize": h.data_parallel_hccl_buff_size,
            "ompNumThreads": h.data_parallel_omp_num_threads,
        }

    h.models = [model_entry]
    print(
        "[config] Legacy flat vllm_*/data_parallel_* config auto-converted to models[] format. "
        "Consider migrating your YAML config to the unified models[] format.",
        file=sys.stderr,
    )


# =========================
# Top-level client config
# =========================

@dataclass
class ClientConfig:
    switch_cluster: Optional[str] = None
    experiments_root: str = "/home/data/saeid/experiments"

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

    multi_turn: bool = False

    file_prompts: FilePromptsConfig = field(default_factory=FilePromptsConfig)
    hf_lmsys: HFLmsysConfig = field(default_factory=HFLmsysConfig)
    load_pattern: LoadPatternConfig = field(default_factory=LoadPatternConfig)
    # Closed-loop "users" load model (used when transport.mode == "claude").
    users: UsersLoadConfig = field(default_factory=UsersLoadConfig)
    generation: GenerationConfig = field(default_factory=GenerationConfig)

    # Output / tracing
    output_log_mode: str = "full"
    print_trace: bool = False

    # When true, persist the full request body (messages + sampling params, i.e.
    # the exact wire payload) into each logs.json record under "request_body".
    # Opt-in (off by default); request_body_max_bytes bounds each logged body
    # (0 = unlimited), truncating oversized bodies to a bounded marker.
    log_request_body: bool = False
    request_body_max_bytes: int = 16384

    # When true (sweep_methods only): stream `kubectl logs` of every pod/container
    # in the deploy namespace into <exp_dir>/vllm-logs/<pod>/<container>.log.
    collect_vllm_logs: bool = False

    # When true: poll the router's /latency_log ring during the run, persist it to
    # <exp_dir>/router_logs.json, live-enrich logs.json with the serving endpoint +
    # prefix/KV-hit fields, and run an authoritative join at shutdown. BooM-proof
    # (reads the router directly, not the response body). router_log_url is the
    # router base URL for /latency_log; empty -> derived from router_url.
    collect_router_log: bool = False
    router_log_url: str = ""

    # Metrics
    metrics: PrometheusMetricsConfig = field(default_factory=PrometheusMetricsConfig)

    # Live Redis KV-block ownership watcher (opt-in; writes redis_kv_watch.jsonl)
    redis_watch: RedisWatchConfig = field(default_factory=RedisWatchConfig)

    # Router transport config
    transport: TransportConfig = field(default_factory=TransportConfig)

    # Integrated claude-CLI transport (active when transport.mode == "claude")
    claude: ClaudeTransportConfig = field(default_factory=ClaudeTransportConfig)

    # AIBrix runtime config
    aibrix: AIBrixConfig = field(default_factory=AIBrixConfig)

    # LiteLLM runtime config
    litellm: LiteLLMConfig = field(default_factory=LiteLLMConfig)

    # BooM Gateway runtime config
    boom: BooMConfig = field(default_factory=BooMConfig)

    # SLO annotation config
    slo: SLOConfig = field(default_factory=SLOConfig)

    # Multi-model routing (optional; when None, single boom.model / litellm.model is used)
    multi_model: Optional[MultiModelConfig] = None

    # Helm knobs
    helm: HelmConfig = field(default_factory=HelmConfig)

    # Shadow deployment: node selector and anti-affinity label for vLLM pods.
    # These are top-level so sweep_methods can pass them as --set to Helm.
    vllm_node_selector: Optional[dict] = None
    vllm_avoid_label: str = ""

    # Optional per-role node pinning for the DP LeaderWorkerSet. None => fall
    # back to vllm_node_selector (same selector for both roles).
    vllm_leader_node_selector: Optional[dict] = None
    vllm_worker_node_selector: Optional[dict] = None

    # Optional Claude-Code-style request injection (kept as a raw dict and read
    # by http_client._maybe_inject_claude_code_template). None/absent => disabled.
    claude_code_injection: Optional[dict] = None


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


def _deep_merge_missing(profile: dict, inline: dict) -> dict:
    """Return profile overlaid by inline values; inline explicit values win."""
    merged = dict(profile)
    for key, inline_value in inline.items():
        profile_value = merged.get(key)
        if isinstance(profile_value, dict) and isinstance(inline_value, dict):
            merged[key] = _deep_merge_missing(profile_value, inline_value)
        else:
            merged[key] = inline_value
    return merged


def _load_cluster_profile(config_path: str, cluster_name: str) -> dict:
    cfg_path = Path(config_path).resolve()
    clusters_path = cfg_path.parent / "clusters.yaml"
    if not clusters_path.is_file():
        raise FileNotFoundError(f"switch_cluster={cluster_name!r} requires {clusters_path}")

    with open(clusters_path, "r") as f:
        raw_profiles = yaml.safe_load(f) or {}

    profiles = raw_profiles.get("clusters", raw_profiles)
    if not isinstance(profiles, dict):
        raise ValueError(f"Invalid cluster profiles in {clusters_path}: expected mapping")

    profile = profiles.get(cluster_name)
    if profile is None:
        known = ", ".join(sorted(str(k) for k in profiles.keys()))
        raise ValueError(f"Unknown switch_cluster {cluster_name!r}; known clusters: {known}")
    if not isinstance(profile, dict):
        raise ValueError(f"Invalid profile for switch_cluster={cluster_name!r}: expected mapping")
    return profile


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

    switch_cluster = raw.get("switch_cluster")
    if switch_cluster is not None:
        switch_cluster = str(switch_cluster).strip()
        if not switch_cluster:
            switch_cluster = None
    if switch_cluster:
        profile = _load_cluster_profile(path, switch_cluster)
        raw = _deep_merge_missing(profile, raw)

    router_url = raw.get("router_url", ClientConfig.router_url)
    experiments_root = raw.get("experiments_root") or ClientConfig.experiments_root
    total_requests = int(raw.get("total_requests", ClientConfig.total_requests))
    prompt_source = raw.get("prompt_source", ClientConfig.prompt_source)

    backend = str(raw.get("backend", ClientConfig.backend) or ClientConfig.backend).strip().lower()
    if backend not in ("router", "aibrix", "litellm", "boom"):
        raise ValueError(f"Invalid backend '{backend}'. Expected 'router', 'aibrix', 'litellm', or 'boom'.")

    multi_turn = bool(raw.get("multi_turn", False))

    # Optional Claude-Code-style injection block (passed through as a raw dict).
    claude_code_injection = raw.get("claude_code_injection")
    if claude_code_injection is not None and not isinstance(claude_code_injection, dict):
        raise ValueError("claude_code_injection must be a mapping if provided")

    file_prompts = _merge_dataclass(FilePromptsConfig, raw.get("file_prompts", {}))
    hf_lmsys_raw = raw.get("hf_lmsys", {}) or {}
    if not isinstance(hf_lmsys_raw, dict):
        raise ValueError("hf_lmsys must be a mapping if provided")
    dataset_profile = hf_lmsys_raw.get("dataset_profile")
    if dataset_profile is not None:
        dataset_profile = str(dataset_profile).strip()
        if not dataset_profile:
            raise ValueError("hf_lmsys.dataset_profile must be non-empty if provided")
        datasets = raw.get("datasets", {}) or {}
        if not isinstance(datasets, dict):
            raise ValueError("datasets must be a mapping if provided")
        dataset_entry = datasets.get(dataset_profile)
        if not isinstance(dataset_entry, dict):
            known = ", ".join(sorted(str(k) for k in datasets.keys()))
            raise ValueError(
                f"Unknown hf_lmsys.dataset_profile {dataset_profile!r}; "
                f"known profiles: {known or '<none>'}"
            )
        for field_name in ("dataset_name", "tokenizer_name"):
            value = dataset_entry.get(field_name)
            if not value:
                raise ValueError(
                    f"datasets.{dataset_profile}.{field_name} is required "
                    f"when hf_lmsys.dataset_profile is used"
                )
            hf_lmsys_raw[field_name] = str(value)
        # Resolution is idempotent: once dataset_name/tokenizer_name are filled
        # from the profile, drop dataset_profile so re-loading an already-resolved
        # config (e.g. sweep_methods writes a temp config without switch_cluster /
        # datasets) does not try to re-resolve a now-absent profile.
        hf_lmsys_raw["dataset_profile"] = None
    hf_lmsys = _merge_dataclass(HFLmsysConfig, hf_lmsys_raw)
    load_pattern = _merge_dataclass(LoadPatternConfig, raw.get("load_pattern", {}))
    users = _merge_dataclass(UsersLoadConfig, raw.get("users", {}))
    generation = _merge_dataclass(GenerationConfig, raw.get("generation", {}))
    metrics = _merge_dataclass(PrometheusMetricsConfig, raw.get("metrics", {}))
    redis_watch = _merge_dataclass(RedisWatchConfig, raw.get("redis_watch", {}))

    # backend-specific config
    transport = _merge_dataclass(TransportConfig, raw.get("transport", {}))
    claude = _merge_dataclass(ClaudeTransportConfig, raw.get("claude", {}))
    aibrix = _merge_dataclass(AIBrixConfig, raw.get("aibrix", {}))
    litellm = _merge_dataclass(LiteLLMConfig, raw.get("litellm", {}))
    boom = _merge_dataclass(BooMConfig, raw.get("boom", {}))

    # SLO config
    slo = _merge_dataclass(SLOConfig, raw.get("slo", {}))

    # multi-model config
    multi_model: Optional[MultiModelConfig] = None
    raw_mm = raw.get("multi_model")
    if isinstance(raw_mm, dict):
        strategy = str(raw_mm.get("strategy", "fraction")).strip().lower()
        if strategy not in ("fraction", "mirror"):
            raise ValueError(f"Invalid multi_model.strategy '{strategy}'. Expected 'fraction' or 'mirror'.")
        targets: List[MultiModelTarget] = []
        for t in raw_mm.get("targets", []):
            if not isinstance(t, dict):
                continue
            targets.append(MultiModelTarget(
                model=str(t.get("model", "served-model")),
                weight=float(t.get("weight", 1.0)),
                prompt_source=t.get("prompt_source"),
                hf_lmsys=t.get("hf_lmsys"),
            ))
        if len(targets) < 2:
            raise ValueError("multi_model.targets must have at least 2 entries.")
        multi_model = MultiModelConfig(strategy=strategy, targets=targets)

    # helm config
    helm = _merge_dataclass(HelmConfig, raw.get("helm", {}))

    output_log_mode = raw.get("output_log_mode", ClientConfig.output_log_mode)
    print_trace = raw.get("print_trace", ClientConfig.print_trace)
    log_request_body = bool(raw.get("log_request_body", ClientConfig.log_request_body))
    request_body_max_bytes = int(raw.get("request_body_max_bytes", ClientConfig.request_body_max_bytes))
    collect_vllm_logs = bool(raw.get("collect_vllm_logs", ClientConfig.collect_vllm_logs))
    collect_router_log = bool(raw.get("collect_router_log", ClientConfig.collect_router_log))
    router_log_url = str(raw.get("router_log_url", ClientConfig.router_log_url) or "")

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
            boom.timeout_s = 7200.0
        boom.timeout_s = max(1.0, boom.timeout_s)

    # -----------------------------
    # Normalize claude transport + closed-loop "users" load model
    # -----------------------------
    if str(transport.mode).lower() == "claude":
        # Endpoint/model/auth default from the boom block so the claude CLI talks
        # to the same BooM Anthropic-compatible gateway with the same credentials.
        if not str(claude.base_url or "").strip():
            claude.base_url = boom.base_url
        claude.base_url = str(claude.base_url or "").rstrip("/")
        if not str(claude.model or "").strip():
            claude.model = boom.model
        if not str(claude.api_key or "").strip():
            claude.api_key = boom.api_key
        claude.claude_bin = str(claude.claude_bin or "claude")
        claude.bare = bool(claude.bare)
        claude.enable_tools = bool(claude.enable_tools)
        try:
            claude.timeout_s = float(claude.timeout_s)
        except Exception:
            claude.timeout_s = 300.0
        claude.timeout_s = max(1.0, claude.timeout_s)

        # Closed-loop users model bounds.
        try:
            users.num_users = int(users.num_users)
        except Exception:
            users.num_users = 10
        users.num_users = max(1, users.num_users)
        try:
            users.convs_per_user = int(users.convs_per_user)
        except Exception:
            users.convs_per_user = 2
        users.convs_per_user = max(1, users.convs_per_user)
        try:
            users.interval_between_convs_s = float(users.interval_between_convs_s)
        except Exception:
            users.interval_between_convs_s = 5.0
        users.interval_between_convs_s = max(0.0, users.interval_between_convs_s)
        try:
            users.ramp_s = float(users.ramp_s)
        except Exception:
            users.ramp_s = 0.0
        users.ramp_s = max(0.0, users.ramp_s)

    # -----------------------------
    # Normalize Redis watcher
    # -----------------------------
    if redis_watch.enabled:
        if not str(redis_watch.node_ip or "").strip():
            try:
                redis_watch.node_ip = urlparse(router_url).hostname or "127.0.0.1"
            except Exception:
                redis_watch.node_ip = "127.0.0.1"
        if not str(redis_watch.model or "").strip():
            # Prefer the claude/boom served model; else the metrics model name.
            redis_watch.model = (
                str(claude.model or "").strip()
                or str(boom.model or "").strip()
                or str(metrics.model_name or "").strip()
            )
        try:
            redis_watch.port = int(redis_watch.port)
        except Exception:
            redis_watch.port = 30079
        try:
            redis_watch.interval_s = float(redis_watch.interval_s)
        except Exception:
            redis_watch.interval_s = 2.0
        redis_watch.interval_s = max(0.5, redis_watch.interval_s)
        try:
            redis_watch.max_keys = int(redis_watch.max_keys)
        except Exception:
            redis_watch.max_keys = 5000
        try:
            redis_watch.scan_count = int(redis_watch.scan_count)
        except Exception:
            redis_watch.scan_count = 500

    # Shadow deployment overrides (top-level keys)
    vllm_node_selector = raw.get("vllm_node_selector", None)
    vllm_avoid_label = str(raw.get("vllm_avoid_label", "") or "").strip()

    # Optional per-role node pinning for the DP LeaderWorkerSet (top-level keys)
    vllm_leader_node_selector = raw.get("vllm_leader_node_selector", None)
    vllm_worker_node_selector = raw.get("vllm_worker_node_selector", None)

    return ClientConfig(
        switch_cluster=switch_cluster,
        experiments_root=experiments_root,
        router_url=router_url,
        total_requests=total_requests,
        prompt_source=prompt_source,
        backend=backend,
        multi_turn=multi_turn,
        file_prompts=file_prompts,
        hf_lmsys=hf_lmsys,
        load_pattern=load_pattern,
        users=users,
        generation=generation,
        output_log_mode=output_log_mode,
        print_trace=print_trace,
        log_request_body=log_request_body,
        request_body_max_bytes=request_body_max_bytes,
        collect_vllm_logs=collect_vllm_logs,
        collect_router_log=collect_router_log,
        router_log_url=router_log_url,
        metrics=metrics,
        redis_watch=redis_watch,
        transport=transport,
        claude=claude,
        aibrix=aibrix,
        litellm=litellm,
        boom=boom,
        slo=slo,
        multi_model=multi_model,
        helm=helm,
        vllm_node_selector=vllm_node_selector,
        vllm_avoid_label=vllm_avoid_label,
        vllm_leader_node_selector=vllm_leader_node_selector,
        vllm_worker_node_selector=vllm_worker_node_selector,
        claude_code_injection=claude_code_injection,
    )