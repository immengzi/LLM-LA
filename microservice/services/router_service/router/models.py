from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import time


# -------------------------
# Client → Router
# -------------------------

class EnqueueRequest(BaseModel):
    prompt: str
    req_id: Optional[int] = None
    t_enq_client: Optional[float] = None
    meta: Dict[str, Any] = {}


class EnqueueResponse(BaseModel):
    """
    In the new synchronous design, the router blocks until vLLM has
    produced an answer and sidecar pushed it back.

    So this response includes the model output.
    """
    req_id: int
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


class JobItem(BaseModel):
    req_id: int
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
    req_id: int
    result: Dict[str, Any]


# -------------------------
# Helpers
# -------------------------

def now_s() -> float:
    return time.time()
