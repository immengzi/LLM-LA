# -*- coding: utf-8 -*-
import os, json
from dataclasses import dataclass, asdict, field
from typing import Optional, Any, Dict, List, Literal

# Optional YAML support (preferred)
try:
    import yaml  # type: ignore
except Exception:
    yaml = None


@dataclass
class RouterConfig:
    # =========================
    # vLLM HTTP API + model defaults
    # =========================
    VLLM_CHAT_PATH: str = "/v1/chat/completions"
    HEALTH_PATH: str = "/health"

    MODEL_NAME: str = "served-model"
    MAX_TOKENS: int = 4096
    TEMPERATURE: float = 0.0
    TOP_P: float = 1.0
    TOP_K: Optional[int] = None
    DO_SAMPLE: bool = False
    N: int = 1
    STREAM: bool = False
    PRESENCE_PENALTY: float = 0.0
    FREQUENCY_PENALTY: float = 0.0
    SEED: int = 42
    LOGPROBS: int = 0
    STOP: List[str] = field(default_factory=list)
    LOGIT_BIAS: Dict[str, float] = field(default_factory=dict)
    VLLM_AUTH_BEARER: str = ""

    # HTTP timeouts
    REQUEST_TIMEOUT_S: int = 1000
    HEALTH_TIMEOUT_S: float = 3.0

    # =========================
    # Prometheus + Local GPU Util (for prom_utils.py)
    # =========================
    PROMETHEUS_URL: str = "http://localhost:31190"
    PROM_TIMEOUT_S: float = 10.0
    RATE_INTERVAL: str = "2s"
    USE_LOCAL_GPU_UTIL: bool = True
    LOCAL_GPU_REFRESH_S: float = 2.0
    LOCAL_GPU_POD_CACHE_TTL_S: float = 10.0

    # =========================
    # Length planning / SIM knobs
    # =========================
    # Global mode
    LENGTH_MODE: Literal["legacy", "target-output", "target-total",
                         "dist-output", "replay-output"] = "dist-output"
    TARGET_OUTPUT_TOKENS: Optional[int] = 54
    TARGET_TOTAL_TOKENS: Optional[int] = None
    IGNORE_EOS: bool = True

    # SIM input tokens (used for estimation in length_backend)
    SIM_VARY_IN_TOKENS: bool = True
    SIM_IN_TOKENS: int = 64
    SIM_CHARS_PER_TOKEN: float = 4.0
    SIM_IN_MIN: int = 1
    SIM_IN_MAX: int = 8192

    # SIM output tokens
    SIM_VARY_OUT_TOKENS: bool = False
    SIM_OUT_TOKENS: int = 100
    SIM_OUT_DIST: Dict[str, Any] = field(
        default_factory=lambda: {
            "kind": "lognormal",
            "mu": 4.8,
            "sigma": 0.8,
            "min": 8,
            "max": 2048,
        }
    )
    LENGTH_DIST_SEED: Optional[int] = None
    LENGTH_DIST_BY_PROMPT: bool = True
    LENGTH_DIST_STRICT_HIST: bool = True
    LENGTH_HIST_SERIES_LABEL: str = "default"

    # =========================
    # Simulator endpoint wiring (HTTP sim support)
    # =========================
    SIM_ENDPOINTS: Dict[str, Any] = field(default_factory=dict)
    SIM_MODE: Literal["append", "only", "off"] = "off"
    SIM_HTTP_HOST: str = "127.0.0.1"
    SIM_HTTP_PORT_BASE: int = 9101

    # =========================
    # Project paths / prompts / results
    # =========================
    PROJECT_PATH: str = "/home/saeid/llm-lb"

    # Prompts from file
    PROMPTS: str = "prompts"
    BASE_CONFIGS_PATH: str = os.path.join(PROJECT_PATH, "configs")
    PROMPTS_FOLDER_PATH: str = os.path.join(PROJECT_PATH, "prompts")
    PROMPTS_FILE_PATH: str = os.path.join(PROMPTS_FOLDER_PATH, f"{PROMPTS}.json")
    PROMPTS_LIMIT: Optional[int] = None
    PROMPTS_SHUFFLE: bool = False
    PROMPTS_SEED: int = 123

    # Results / logging
    RESULTS_DIR: str = os.path.join(PROJECT_PATH, "results")
    QUEUE_LOG_FILENAME: str = "queue.json"

    # Payload logging controls (used by utils.log_result)
    LOG_PAYLOAD_MODE: Literal["off", "head", "full"] = "head"
    LOG_HEAD_CHARS: int = 512

    # =========================
    # Load generation knobs
    # =========================
    LOAD_PATTERN: Literal["dump", "det", "poisson", "bursty",
                          "steps", "rand"] = "dump"

    # Common knobs for det/poisson/steps default rate
    LOAD_RATE_RPS: float = 2.0
    LOAD_WARMUP_S: float = 0.0
    LOAD_DURATION_S: float = 999999.0

    # Steps & bursty specifics
    BURST_ON_S: float = 2.0
    BURST_OFF_S: float = 2.0
    BURST_RPS_ON: float = 10.0
    BURST_RPS_OFF: float = 0.0
    STEP_SCHEDULE: str = ""

    # Random-range specifics
    RAND_RPS_MIN: Optional[float] = None
    RAND_RPS_MAX: Optional[float] = None
    RAND_EPOCH_S: float = 5.0
    RAND_KIND: Literal["poisson", "det"] = "poisson"

    # Repeatability controls
    LOADGEN_SEED: int = 12345

    # Load logging controls
    VERBOSE_LOAD: bool = False
    LOAD_LOG_EVERY: int = 1

    # =========================
    # Prompt source (file vs LMSYS HF dataset)
    # =========================
    PROMPTS_SOURCE: Literal["file", "hf-lmsys"] = "file"
    HF_DATASET_NAME: str = "lmsys/lmsys-chat-1m"
    HF_DATASET_SPLIT: str = "train"
    HF_TOKENIZER_NAME: str = "/mnt/nvme1/saeid/models/qwen3-8b"
    HF_STREAMING: bool = False
    LMSYS_MIN_INPUT_TOKENS: Optional[int] = 256
    LMSYS_MAX_INPUT_TOKENS: Optional[int] = None
    LMSYS_REPEAT_EACH: int = 32

    # =========================
    # Misc
    # =========================
    PREDICTOR_NAME: str = "oracle"
    THINK: bool = False


# --- global holder ---
_CONFIG: Optional[RouterConfig] = None


def _coerce(v: Any, typ):
    if typ is bool:
        return str(v).lower() in {"1", "true", "t", "yes", "y", "on"}
    for target in (int, float, str):
        if typ is target:
            return target(v)
    try:
        return json.loads(v)
    except Exception:
        return v


def _env_or(current: Any, key: str, typ):
    ev = os.getenv(key)
    if ev is None:
        return current
    try:
        return _coerce(ev, typ)
    except Exception:
        return current


def _load_mapping_file(path: str, base_configs_path: str) -> Dict[str, Any]:
    path = os.path.join(base_configs_path, f"{path}.yaml")
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    ext = os.path.splitext(path)[1].lower()
    if ext in (".yaml", ".yml"):
        if yaml is None:
            raise RuntimeError(
            f"PyYAML is not installed but a YAML file was provided: {path}"
        )
        data = yaml.safe_load(text) or {}
    else:
        data = json.loads(text or "{}")
    if not isinstance(data, dict):
        raise ValueError("Config file must contain a top-level mapping/object.")
    return data


def load_config(path: Optional[str]) -> RouterConfig:
    cfg = RouterConfig()
    base_configs_path = cfg.BASE_CONFIGS_PATH
    if path:
        raw = _load_mapping_file(path, base_configs_path)
        for k, v in raw.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
    for field_desc in RouterConfig.__dataclass_fields__.values():
        name, typ = field_desc.name, field_desc.type
        setattr(cfg, name, _env_or(getattr(cfg, name), name, typ))
    return cfg


def set_config(c: RouterConfig):
    global _CONFIG
    _CONFIG = c


def get_config() -> RouterConfig:
    return _CONFIG or RouterConfig()


def dump_config_dict() -> Dict[str, Any]:
    return asdict(get_config())


def __getattr__(name: str):
    cfg = get_config()
    if hasattr(cfg, name):
        return getattr(cfg, name)
    raise AttributeError(name)
