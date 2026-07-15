from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import time


# -------------------------
# Client → Router
# -------------------------

class EnqueueRequest(BaseModel):
    prompt: str
    req_id: Optional[str] = None
    t_enq_client: Optional[float] = None
    meta: Dict[str, Any] = {}
    model: str = ""           # target model queue (empty = default MODEL_NAME)

    # SLO annotations (all optional; existing clients unaffected)
    slo_type: Optional[str] = None          # "ttft" | "tpot" | "ttft+tpot" | "e2e"
    slo_ttft_ms: Optional[float] = None     # TTFT budget in milliseconds
    slo_tpot_ms: Optional[float] = None     # per-token decode budget in milliseconds
    slo_e2e_ms: Optional[float] = None      # end-to-end budget in milliseconds
    task_type: Optional[str] = None         # e.g. "chat", "summarize", "code"
    output_len_hint: Optional[int] = None   # client-supplied expected output length


class EnqueueResponse(BaseModel):
    """
    In the new synchronous design, the router blocks until vLLM has
    produced an answer and sidecar pushed it back.

    So this response includes the model output.
    """
    req_id: str
    output: Optional[str] = None             # text from vLLM
    finish_reason: Optional[str] = None      # e.g. "stop"
    latency_s: Optional[float] = None        # model roundtrip
    raw: Optional[Dict[str, Any]] = None     # full vLLM response if desired


# -------------------------
# Sidecar → Router (pull mode)
# -------------------------

class PullRequest(BaseModel):
    # endpoint identity (e.g. pod name) – must match KVWatcher register_block_owners()
    endpoint: str
    want: int                 # sidecar-computed capacity
    model: str = ""           # target model queue (empty = default MODEL_NAME)
    # Optional GPU KV fill fraction [0,1] reported by the sidecar (soft divert).
    # Omitted / null => router treats as unknown (no divert for missing samples).
    kv_usage: Optional[float] = None


class JobItem(BaseModel):
    req_id: str
    prompt: str
    t_enq_client: float
    meta: Dict[str, Any] = {}


class PullResponse(BaseModel):
    items: List[JobItem]


# -------------------------
# Sidecar → Router (result callback)
# -------------------------

class ResultRequest(BaseModel):
    """
    Internal API: sidecar posts vLLM output back to router.

    Example payload:
    {
        "req_id": 17,
        "result": {
            "output": "hello ...",
            "finish_reason": "stop",
            "latency_s": 0.342,
            "raw": {... full vLLM JSON ...}
        }
    }
    """
    req_id: str
    result: Dict[str, Any]


# -------------------------
# Helpers
# -------------------------

def now_s() -> float:
    return time.time()
