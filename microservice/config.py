# config.py  (FULL MODIFIED: old config + minimal Helm section)

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
    prometheus_base_url: str = "http://localhost:31190"
    scrape_interval_s: float = 2.0
    window_s: float = 10.0
    include_debug_metrics: bool = False
    model_name: Optional[str] = None
    max_instances: Optional[int] = None


# =========================
# Transport (sync vs async_pubsub)
# =========================

@dataclass
class TransportConfig:
    """
    Transport selection for the client.

    - "sync": existing behavior (POST /enqueue, block until completion).
    - "async_pubsub": submit+ack (POST /submit) + receive completions via one ZMQ SUB socket.
      End-of-run termination: idle timeout AFTER LAST received completion.
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

    # after the last RECEIVED completion (last RECV), wait at most this many seconds.
    # If no new completion arrives in that idle window, mark remaining pending as LOST and terminate.
    idle_timeout_s: float = 60.0

    # Buffer completions that arrive BEFORE the submit thread records pending[rid].
    # TTL bounds memory; size cap bounds worst-case.
    orphan_ttl_s: float = 300.0
    orphan_max: int = 100000


# =========================
# Helm knobs for sweeps (MINIMAL, only what you asked)
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
    """
    # initial replicas for vLLM deployment (even when autoscaling is enabled)
    replicas: int = 8

    # sidecar batch size
    batch_size: int = 8

    # autoscaling toggle + core knobs
    autoscaling_enabled: bool = False
    autoscaling_min: int = 1
    autoscaling_max: int = 16
    autoscaling_threshold: str = "16"

    # Prometheus query used by the autoscaler (string; can be multi-line in YAML)
    autoscaling_prometheus_query: str = 'max(router_central_queue_length{namespace="vllm"})'


# =========================
# Top-level client config
# =========================

@dataclass
class ClientConfig:
    router_url: str = "http://127.0.0.1:30080"
    total_requests: int = 50
    prompt_source: str = "file"

    file_prompts: FilePromptsConfig = field(default_factory=FilePromptsConfig)
    hf_lmsys: HFLmsysConfig = field(default_factory=HFLmsysConfig)
    load_pattern: LoadPatternConfig = field(default_factory=LoadPatternConfig)
    generation: GenerationConfig = field(default_factory=GenerationConfig)

    # Output / tracing
    output_log_mode: str = "full"
    print_trace: bool = False

    # Metrics
    metrics: PrometheusMetricsConfig = field(default_factory=PrometheusMetricsConfig)

    # Transport (new; backward-compatible default is sync)
    transport: TransportConfig = field(default_factory=TransportConfig)

    # Helm knobs (new; optional in YAML; safe defaults)
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

    file_prompts = _merge_dataclass(FilePromptsConfig, raw.get("file_prompts", {}))
    hf_lmsys = _merge_dataclass(HFLmsysConfig, raw.get("hf_lmsys", {}))
    load_pattern = _merge_dataclass(LoadPatternConfig, raw.get("load_pattern", {}))
    generation = _merge_dataclass(GenerationConfig, raw.get("generation", {}))
    metrics = _merge_dataclass(PrometheusMetricsConfig, raw.get("metrics", {}))

    # transport config (fully optional in YAML)
    transport = _merge_dataclass(TransportConfig, raw.get("transport", {}))

    # helm config (fully optional in YAML)
    helm = _merge_dataclass(HelmConfig, raw.get("helm", {}))

    # Only special-case: output_log_mode. Use the dataclass default if not in YAML.
    output_log_mode = raw.get("output_log_mode", ClientConfig.output_log_mode)
    print_trace = raw.get("print_trace", ClientConfig.print_trace)

    # -----------------------------
    # Transport defaults/fixes
    # -----------------------------
    if str(transport.mode).lower() == "async_pubsub":
        # If user forgot results_zmq, derive a sane default.
        if not str(transport.results_zmq or "").strip():
            transport.results_zmq = _derive_results_zmq_from_router_url(router_url)

        # IMPORTANT: do NOT auto-force run_id="default".
        if transport.run_id is not None and not str(transport.run_id).strip():
            transport.run_id = None

        # Sanity: numeric fields
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
            transport.orphan_ttl_s = float(transport.orphan_ttl_s)
        except Exception:
            transport.orphan_ttl_s = 300.0
        transport.orphan_ttl_s = max(1.0, transport.orphan_ttl_s)

        try:
            transport.orphan_max = int(transport.orphan_max)
        except Exception:
            transport.orphan_max = 100000
        transport.orphan_max = max(1000, transport.orphan_max)

    return ClientConfig(
        router_url=router_url,
        total_requests=total_requests,
        prompt_source=prompt_source,
        file_prompts=file_prompts,
        hf_lmsys=hf_lmsys,
        load_pattern=load_pattern,
        generation=generation,
        output_log_mode=output_log_mode,
        print_trace=print_trace,
        metrics=metrics,
        transport=transport,
        helm=helm,
    )
