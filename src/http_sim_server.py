# http_sim_server.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, time, queue, asyncio, logging
from typing import Optional, List, Dict, Any, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

from config import load_config, get_config, set_config
from length_backend import (
    estimate_in_tokens_from_chars,
    rng_for_prompt,
    sample_out_tokens_from_cfg,
)

# ---- quiet /health logs ----
class HealthFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(HealthFilter())

# =========================
# Pydantic models
# =========================

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    model: Optional[str] = None
    messages: List[ChatMessage]
    max_tokens: Optional[int] = None
    forced_output_tokens: Optional[int] = None
    forced_total_tokens: Optional[int] = None

    class Config:
        extra = "ignore"  # ignore other OpenAI fields


# =========================
# Internal shared types
# =========================

class _Req:
    def __init__(
        self,
        prompt: str,
        req_max: Optional[int],
        forced_out: Optional[int],
        forced_total: Optional[int],
    ):
        self.prompt = prompt
        self.req_max = req_max
        self.forced_out = forced_out
        self.forced_total = forced_total

        # completion signal (FastAPI handler blocks on this)
        self.done = asyncio.Event()
        self.result: Dict[str, Any] | None = None
        self.error: Optional[str] = None

        # wall timestamps (derived from virtual times via SIM_TIMESCALE)
        self.ts_enq: float = 0.0
        self.ts_deq: Optional[float] = None
        self.ts_start: Optional[float] = None
        self.ts_end: Optional[float] = None

        # engine bookkeeping
        self._i_tok: int = 0
        self._o_cap: int = 0
        self._o_done: int = 0

        # virtual enqueue base (seconds)
        self._virt_enq: float = 0.0
        self._virt_deq: Optional[float] = None


# =========================
# Helpers
# =========================

def _estimate_in(prompt: str) -> int:
    return int(estimate_in_tokens_from_chars(prompt))

def _rng_for(cfg, prompt: str):
    seed_base = cfg.LENGTH_DIST_SEED if (cfg.LENGTH_DIST_SEED is not None) else cfg.SEED
    return rng_for_prompt(
        int(seed_base),
        prompt if bool(cfg.LENGTH_DIST_BY_PROMPT) else None,
        by_prompt=bool(cfg.LENGTH_DIST_BY_PROMPT),
    )

def _sample_out(cfg, prompt: str) -> int:
    rng = _rng_for(cfg, prompt)
    return int(sample_out_tokens_from_cfg(rng))

def _effective_cap(cfg, req_max: Optional[int]) -> int:
    g = int(cfg.MAX_TOKENS or 0)
    r = int(req_max or 0)
    g = g if g > 0 else 0
    r = r if r > 0 else 0
    return min(g, r) if (g and r) else (g or r or 0)

def _cap_out(cfg, i_tok: int, r: _Req) -> int:
    if r.forced_out is not None:
        o_raw = max(0, int(r.forced_out))
    elif r.forced_total is not None:
        o_raw = max(0, int(r.forced_total) - i_tok)
    else:
        o_raw = _sample_out(cfg, r.prompt)
    cap = _effective_cap(cfg, r.req_max)
    return max(0, min(o_raw, max(0, cap - i_tok)) if cap > 0 else o_raw)

def _result_dict(
    cfg,
    name: str,
    r: _Req,
    i_tok: int,
    o_tok: int,
    sim_nominal: float,
    sim_actual: float,
    batch_size: int,
) -> Dict[str, Any]:
    timescale = float(cfg.SIM_TIMESCALE)
    total_wall = (r.ts_end - r.ts_enq) if r.ts_enq else None
    lat = (r.ts_end - r.ts_start) if (r.ts_end and r.ts_start) else None
    queue_only = (r.ts_deq - r.ts_enq) if (r.ts_deq and r.ts_enq) else None
    batch_wait = (r.ts_start - r.ts_deq) if (r.ts_start and r.ts_deq) else None
    queue_full = (r.ts_start - r.ts_enq) if (r.ts_start and r.ts_enq) else None
    return {
        "choices": [
            {
                "message": {"role": "assistant", "content": f"[SIM:{name}] OK ({i_tok}+{o_tok})"},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": int(i_tok),
            "completion_tokens": int(o_tok),
            "total_tokens": int(i_tok + o_tok),
        },
        # virtual-time diagnostics (derived wall timestamps)
        "_sim_latency_s": lat,                 # start -> end
        "_sim_queue_wait_s": queue_full,       # enq -> start
        "_sim_service_s": lat,
        "_sim_total_wall_s": total_wall,       # enq -> end
        "_sim_service_nominal_s": sim_nominal, # pure virtual seconds
        "_sim_timescale": timescale,
        "_sim_queue_only_s": queue_only,
        "_sim_batch_wait_s": batch_wait,
        "_sim_batch_size": int(batch_size),
        "_sim_in_tokens": int(i_tok),
        "_sim_out_tokens": int(o_tok),
        "_sim_ts_enq": r.ts_enq,
        "_sim_ts_deq": r.ts_deq,
        "_sim_ts_start": r.ts_start,
        "_sim_ts_end": r.ts_end,
    }


# =========================
# VTServer (asyncio virtual-time engine)
# =========================

class VTServer:
    """
    Continuous batching only.
    Timing model uses seconds-per-token (SEC_PER_TOKEN):
      - Prefill cost = in_tokens * SEC_PER_TOKEN
      - Decode: one token per active unfinished request per tick
                virtual time advance per tick = active_count * SEC_PER_TOKEN
    """

    def __init__(self, *, name: str, sec_per_token: float, cfg):
        self.name = name
        self.spt = float(sec_per_token)  # seconds per token
        self.cfg = cfg

        # inbound from HTTP
        self._in_q: "queue.Queue[_Req]" = queue.Queue()

        # scheduler state
        self.vnow = 0.0  # virtual seconds
        self.active: List[_Req] = []
        self.store: List[_Req] = []

        # wake-up event (async)
        self._wake: Optional[asyncio.Event] = None
        self._task: Optional[asyncio.Task] = None

    # API used by FastAPI
    def get_active_count(self) -> int:
        return len(self.active)

    def enqueue(self, r: _Req) -> None:
        self._in_q.put(r)
        if self._wake is not None:
            self._wake.set()

    async def start(self):
        if self._task is None:
            self._wake = asyncio.Event()
            self._task = asyncio.create_task(self._run())

    # ----- core scheduler (continuous batching only) -----

    async def _run(self):
        cfg = self.cfg
        spt = max(self.spt, 1e-9)  # guard against zero/negative
        timescale = float(cfg.SIM_TIMESCALE)

        max_batch = int(getattr(cfg, "SIM_MAX_BATCH", 8) or 8)

        while True:
            # 1) move inbound to store (virtual enqueue baseline)
            while True:
                try:
                    r = self._in_q.get_nowait()
                    r._virt_enq = float(self.vnow)
                    self.store.append(r)
                except queue.Empty:
                    break

            # 2) no work? wait briefly
            if not self.active and not self.store:
                if self._wake is None:
                    self._wake = asyncio.Event()
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=0.05)
                except asyncio.TimeoutError:
                    pass
                await asyncio.sleep(0)
                continue

            # admit up to capacity
            while len(self.active) < max_batch and self.store:
                nxt = self.store.pop(0)
                nxt._virt_deq = float(self.vnow)
                i_tok = _estimate_in(nxt.prompt)
                o_cap = _cap_out(cfg, i_tok, nxt)
                nxt._i_tok, nxt._o_cap, nxt._o_done = i_tok, o_cap, 0
                # prefill (virtual): linear in input tokens
                if i_tok > 0:
                    self.vnow += i_tok * spt
                # stamp wall times
                nxt.ts_deq = nxt.ts_enq + ((getattr(nxt, "_virt_deq", self.vnow) - nxt._virt_enq) * timescale)
                nxt.ts_start = nxt.ts_enq + ((self.vnow - nxt._virt_enq) * timescale)
                self.active.append(nxt)

            # one decode tick: 1 token per unfinished request
            tokens_this_tick = 0
            for r in self.active:
                if r._o_done < r._o_cap:
                    r._o_done += 1
                    tokens_this_tick += 1

            if tokens_this_tick > 0:
                # cost scales with number of unfinished (tokens_this_tick) * sec_per_token
                self.vnow += (tokens_this_tick * spt)
            else:
                self.vnow += 0.0005  # idle virtual nudge

            # retire finished
            remaining: List[_Req] = []
            for r in self.active:
                if r._o_done >= r._o_cap:
                    req_nominal = (r._i_tok * spt) + (r._o_done * spt)
                    r.ts_end = (r.ts_start or r.ts_enq) + req_nominal * timescale
                    r.result = _result_dict(
                        cfg, self.name, r,
                        int(r._i_tok), int(r._o_done),
                        sim_nominal=req_nominal,
                        sim_actual=req_nominal * timescale,
                        batch_size=len(self.active),
                    )
                    r.done.set()
                else:
                    remaining.append(r)
            self.active = remaining

            await asyncio.sleep(0)


# =========================
# FastAPI app factory
# =========================

def build_app(
    name: str,
    sec_per_token: float,
    engine: Literal["vt"] = "vt",  # fixed to 'vt'
    config_path: Optional[str] = None,
) -> FastAPI:
    """
    Build a FastAPI app for one simulated endpoint.
    Continuous batching only. Timing uses seconds-per-token.
    """
    if config_path:
        set_config(load_config(config_path))
    cfg = get_config()

    srv = VTServer(name=name, sec_per_token=sec_per_token, cfg=cfg)
    app = FastAPI()

    @app.on_event("startup")
    async def _start_scheduler():
        await srv.start()

    @app.get(cfg.HEALTH_PATH)
    def health():
        return {"ok": True, "name": name, "sec_per_token": sec_per_token, "engine": "vt"}

    @app.get(cfg.METRICS_PATH)
    def metrics():
        return {
            "endpoint": name,
            "active_count": int(srv.get_active_count()),
            "engine": "vt",
            "sec_per_token": sec_per_token,
        }

    # watchdog so client never hangs forever
    SIM_HTTP_WAIT_S = float(getattr(cfg, "SIM_HTTP_WAIT_S", 30.0))

    @app.post(cfg.VLLM_CHAT_PATH)
    async def chat(req: ChatRequest):
        # extract last user content
        prompt = ""
        for m in reversed(req.messages or []):
            if (m.role or "").lower() == "user":
                prompt = m.content or ""
                break

        item = _Req(
            prompt=prompt,
            req_max=req.max_tokens,
            forced_out=req.forced_output_tokens,
            forced_total=req.forced_total_tokens,
        )
        item.ts_enq = time.time()

        # enqueue and await completion
        srv.enqueue(item)
        try:
            await asyncio.wait_for(item.done.wait(), timeout=SIM_HTTP_WAIT_S)
        except asyncio.TimeoutError:
            item.error = f"Sim endpoint timed out after {SIM_HTTP_WAIT_S:.1f}s"
            raise HTTPException(status_code=503, detail={"error": item.error})

        return item.result or {
            "choices": [{"message": {"role": "assistant", "content": "[SIM] empty"}}]
        }

    return app


# =========================
# Module-level `app` (for uvicorn discovery)
# =========================

def _env_default_app() -> FastAPI:
    cfg_path = os.getenv("CONFIG_PATH") or None
    name = os.getenv("NAME", "sim-ep")
    sec_per_token = float(os.getenv("SEC_PER_TOKEN", "0.01"))  # default 100 tok/s
    if cfg_path:
        set_config(load_config(cfg_path))
    return build_app(
        name=name,
        sec_per_token=sec_per_token,
        engine="vt",
        config_path=None,
    )

# Used by: `python -m uvicorn http_sim_server:app --host ... --port ...`
app = _env_default_app()


# =========================
# Debug-friendly single-server main
# =========================

def main():
    import argparse
    parser = argparse.ArgumentParser("http_sim_server (VT asyncio)")
    parser.add_argument("--config", dest="config_path", type=str, default=None)
    parser.add_argument("--name", type=str, default=os.getenv("NAME", "sim-ep"))
    parser.add_argument("--sec-per-token", type=float, default=float(os.getenv("SEC_PER_TOKEN", "0.01")))
    parser.add_argument("--host", type=str, default=os.getenv("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "9101")))
    args = parser.parse_args()

    if args.config_path:
        set_config(load_config(args.config_path))

    app = build_app(name=args.name, sec_per_token=args.sec_per_token, engine="vt", config_path=None)
    uvicorn.run(app, host=args.host, port=args.port, reload=False, workers=1)

if __name__ == "__main__":
    main()
