from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import time


class EnqueueRequest(BaseModel):
    prompt: str
    req_id: Optional[int] = None
    t_enq_client: Optional[float] = None
    meta: Dict[str, Any] = {}


class EnqueueResponse(BaseModel):
    req_id: int


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


def now_s() -> float:
    return time.time()
