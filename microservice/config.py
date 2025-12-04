# config.py
# Simple YAML-backed config loader for the microservice load client.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
import yaml


@dataclass
class FilePromptsConfig:
    path: str = "prompts.json"
    variant: str = "medium"  # short | medium | long


@dataclass
class HFLmsysConfig:
    dataset_name: str = "lmsys/lmsys-chat-1m"
    split: str = "train"
    tokenizer_name: str = "gpt2"
    streaming: bool = False
    min_input_tokens: Optional[int] = None
    max_input_tokens: Optional[int] = None
    repeat_each: int = 1


@dataclass
class LoadPatternConfig:
    pattern: str = "dump"  # dump | det | poisson | bursty | steps | rand
    rate_rps: float = 5.0
    duration_s: float = 60.0

    # Number of dummy warmup requests to send before timed load
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
class GenerationConfig:
    max_tokens: int = 256
    temperature: float = 0.0
    # legacy | target-output | target-total
    length_mode: str = "legacy"
    target_output_tokens: Optional[int] = None
    target_total_tokens: Optional[int] = None


@dataclass
class ClientConfig:
    router_url: str = "http://127.0.0.1:30080"
    total_requests: int = 50
    # "file" or "hf-lmsys"
    prompt_source: str = "file"

    # nested sections
    file_prompts: FilePromptsConfig = field(default_factory=FilePromptsConfig)
    hf_lmsys: HFLmsysConfig = field(default_factory=HFLmsysConfig)
    load_pattern: LoadPatternConfig = field(default_factory=LoadPatternConfig)
    generation: GenerationConfig = field(default_factory=GenerationConfig)


def _merge_dataclass(dc_cls, data_dict: dict):
    """Helper to merge a dict into a dataclass (with defaults)."""
    kwargs = {}
    for field_name in dc_cls.__dataclass_fields__.keys():
        if field_name in data_dict:
            kwargs[field_name] = data_dict[field_name]
    base = dc_cls()
    for k, v in kwargs.items():
        setattr(base, k, v)
    return base


def load_config(path: str) -> ClientConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}

    # Top-level fields
    router_url = raw.get("router_url", "http://127.0.0.1:30080")
    total_requests = int(raw.get("total_requests", 50))
    prompt_source = raw.get("prompt_source", "file")

    # Nested sections
    file_prompts = _merge_dataclass(
        FilePromptsConfig, raw.get("file_prompts", {})
    )
    hf_lmsys = _merge_dataclass(
        HFLmsysConfig, raw.get("hf_lmsys", {})
    )
    load_pattern = _merge_dataclass(
        LoadPatternConfig, raw.get("load_pattern", {})
    )
    generation = _merge_dataclass(
        GenerationConfig, raw.get("generation", {})
    )

    return ClientConfig(
        router_url=router_url,
        total_requests=total_requests,
        prompt_source=prompt_source,
        file_prompts=file_prompts,
        hf_lmsys=hf_lmsys,
        load_pattern=load_pattern,
        generation=generation,
    )
