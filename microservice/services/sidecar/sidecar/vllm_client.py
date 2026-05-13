# sidecar/vllm_client.py
# -*- coding: utf-8 -*-
import time
import threading
from typing import Dict, Any, Optional

import requests

from .config import get_config
from .local_queue import LocalQueue
from .router_client import RouterPullWorker
from .result_poster import ResultPoster
from .metrics import inc_completed, set_sidecar_workers_busy

_cfg = get_config()

# ------------------------------------------------------------
# process-wide busy counter for sidecar workers
# ------------------------------------------------------------
_BUSY_LOCK = threading.Lock()
_BUSY_N = 0


def _busy_inc() -> int:
    global _BUSY_N
    with _BUSY_LOCK:
        _BUSY_N += 1
        return _BUSY_N


def _busy_dec() -> int:
    global _BUSY_N
    with _BUSY_LOCK:
        _BUSY_N = max(0, _BUSY_N - 1)
        return _BUSY_N


class VLLMWorker:
    """
    Worker loop:
      - Pop from local queue
      - POST to vLLM /v1/chat/completions
      - Enqueue result for async posting back to router /result (ResultPoster)

    Trace fields added (if TRACE_ENABLED):
      * t_dequeue_sidecar
      * t_vllm_send
      * t_vllm_recv
      * t_post_result_sidecar

    Plus sidecar queue snapshots:
      * sidecar_queue_len_at_dequeue
      * sidecar_inflight_at_dequeue
      * sidecar_logical_at_dequeue
      * sidecar_queue_len_at_result
      * sidecar_inflight_at_result
      * sidecar_logical_at_result
    """

    def __init__(
        self,
        local_q: LocalQueue,
        pull_worker: Optional[RouterPullWorker] = None,
        result_poster: Optional[ResultPoster] = None,
    ):
        self.local_q = local_q
        self._pull_worker = pull_worker
        self._result_poster = result_poster

        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------

    def start(self):
        if self._thread is not None:
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print("[sidecar] VLLMWorker started")

    def stop(self):
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        print("[sidecar] VLLMWorker stopped")

    # ------------------------------------------------------------

    def _loop(self):
        session = requests.Session()
        vllm_url = f"{_cfg.VLLM_URL}/v1/chat/completions"

        # Keep router_result_url only for fallback mode (if poster not provided)
        router_result_url = f"{_cfg.ROUTER_URL}/result"

        # Idle sleep: short enough that workers pick up buffered items fast,
        # but not a pure spin. Background poller handles queue top-up.
        idle_sleep_s = 0.005

        try:
            while not self._stop_evt.is_set():

                # --------------------------------------------------------
                # Attempt to dequeue work
                # --------------------------------------------------------
                item = self.local_q.get_nowait()
                if not item:
                    time.sleep(idle_sleep_s)
                    continue

                req_id, prompt, meta = item

                # --------------------------------------------------------
                # Mark this worker as busy (process-wide counter)
                # --------------------------------------------------------
                try:
                    busy_now = _busy_inc()
                    set_sidecar_workers_busy(_cfg.CONTAINER_NAME, busy_now)
                except Exception:
                    pass

                # --------------------------------------------------------
                # Trace: dequeue timestamp + queue snapshot
                # --------------------------------------------------------
                if getattr(_cfg, "TRACE_ENABLED", False):
                    pending_dq, inflight_dq = self.local_q.state()
                    logical_dq = pending_dq + inflight_dq

                    tr = dict(meta.get("__trace__") or {})
                    tr["t_dequeue_sidecar"] = time.time()
                    tr["sidecar_queue_len_at_dequeue"] = pending_dq
                    tr["sidecar_inflight_at_dequeue"] = inflight_dq
                    tr["sidecar_logical_at_dequeue"] = logical_dq
                    meta["__trace__"] = tr

                try:
                    # ----------------------------------------------------
                    # Build vLLM request
                    # ----------------------------------------------------
                    chat_req = meta.get("__chat_request__")
                    if chat_req and isinstance(chat_req, dict):
                        payload = dict(chat_req)
                        payload["model"] = _cfg.MODEL_NAME
                        payload["stream"] = False
                    else:
                        max_tokens = meta.get("max_tokens", 128)
                        temperature = meta.get("temperature", 0.0)
                        enable_thinking = bool(meta.get("enable_thinking", False))

                        payload: Dict[str, Any] = {
                            "model": _cfg.MODEL_NAME,
                            "messages": [{"role": "user", "content": prompt}],
                            "max_tokens": max_tokens,
                            "temperature": temperature,
                            "chat_template_kwargs": {
                                "enable_thinking": enable_thinking,
                            },
                        }
                        if "min_tokens" in meta:
                            payload["min_tokens"] = int(meta["min_tokens"])

                    if meta.get("ignore_eos"):
                        payload["ignore_eos"] = bool(meta["ignore_eos"])

                    if _cfg.FORCE_IGNORE_EOS:
                        payload["ignore_eos"] = True

                    # ----------------------------------------------------
                    # Trace: vLLM send timestamp
                    # ----------------------------------------------------
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        tr = dict(meta.get("__trace__") or {})
                        tr["t_vllm_send"] = time.time()
                        meta["__trace__"] = tr

                    # ----------------------------------------------------
                    # Call vLLM
                    # ----------------------------------------------------
                    use_stream = _cfg.STREAMING_MODE
                    if use_stream:
                        payload["stream"] = True
                        payload["stream_options"] = {"include_usage": True}

                    if _cfg.LOG_LEVEL == "debug":
                        _eos = payload.get("ignore_eos", "MISSING")
                        _mt = payload.get("min_tokens", "MISSING")
                        _mx = payload.get("max_tokens", "MISSING")
                        print(
                            f"[sidecar] vLLM send req_id={req_id} model={_cfg.MODEL_NAME} "
                            f"ignore_eos={_eos} min_tokens={_mt} max_tokens={_mx} "
                            f"stream={use_stream}"
                        )

                    output_text: str
                    finish_reason: Optional[str] = None
                    usage: Optional[Dict[str, Any]] = None
                    raw_vllm: Optional[Dict[str, Any]] = None
                    latency_s: Optional[float] = None
                    ttft_s: Optional[float] = None

                    if use_stream:
                        # --------------------------------------------------
                        # Streaming path: SSE from vLLM, accumulate locally
                        # and optionally forward chunks to router
                        # --------------------------------------------------
                        router_chunk_url = f"{_cfg.ROUTER_URL}/result_chunk"
                        forward_stream = bool(meta.get("__chat_request__", {}).get("stream"))

                        t_vllm_send = time.time()
                        resp = session.post(
                            vllm_url,
                            json=payload,
                            timeout=_cfg.VLLM_TIMEOUT_S,
                            stream=True,
                        )

                        if not resp.ok:
                            print(f"[sidecar] vLLM stream error: {resp.status_code} {resp.text}")
                            output_text = f"[vLLM error {resp.status_code}]"
                            resp.close()
                        else:
                            import json as _json
                            parts: list = []
                            t_first_token: Optional[float] = None
                            chunk_idx = 0
                            try:
                                for raw_line in resp.iter_lines(decode_unicode=True):
                                    if raw_line is None:
                                        continue
                                    line = raw_line.rstrip("\r\n")
                                    if not line.startswith("data: "):
                                        continue
                                    data_str = line[6:]
                                    if data_str.strip() == "[DONE]":
                                        break
                                    try:
                                        chunk = _json.loads(data_str)
                                    except Exception:
                                        continue

                                    delta_content = None
                                    chunk_fr = None
                                    choices = chunk.get("choices")
                                    if isinstance(choices, list) and choices:
                                        c0 = choices[0]
                                        delta = c0.get("delta")
                                        if isinstance(delta, dict):
                                            content = delta.get("content")
                                            if isinstance(content, str) and content:
                                                if t_first_token is None:
                                                    t_first_token = time.time()
                                                parts.append(content)
                                                delta_content = content

                                        fr = c0.get("finish_reason")
                                        if isinstance(fr, str):
                                            finish_reason = fr
                                            chunk_fr = fr

                                    u = chunk.get("usage")
                                    if isinstance(u, dict) and u:
                                        usage = u

                                    if forward_stream and (delta_content or chunk_fr):
                                        is_final = chunk_fr is not None
                                        chunk_payload = {
                                            "req_id": req_id,
                                            "chunk_idx": chunk_idx,
                                            "delta": delta_content or "",
                                            "is_final": is_final,
                                        }
                                        if chunk_fr:
                                            chunk_payload["finish_reason"] = chunk_fr
                                        if is_final and usage:
                                            chunk_payload["usage"] = usage
                                        try:
                                            session.post(
                                                router_chunk_url,
                                                json=chunk_payload,
                                                timeout=5.0,
                                            )
                                        except Exception as ce:
                                            if _cfg.LOG_LEVEL == "debug":
                                                print(f"[sidecar] chunk forward failed: {ce}")
                                        chunk_idx += 1
                            finally:
                                resp.close()

                            t_vllm_recv = time.time()
                            output_text = "".join(parts) if parts else ""
                            latency_s = t_vllm_recv - t_vllm_send

                            if t_first_token is not None:
                                ttft_s = t_first_token - t_vllm_send

                    else:
                        # --------------------------------------------------
                        # Non-streaming path (original)
                        # --------------------------------------------------
                        resp = session.post(
                            vllm_url,
                            json=payload,
                            timeout=_cfg.VLLM_TIMEOUT_S,
                        )

                        try:
                            latency_s = float(resp.elapsed.total_seconds())
                        except Exception:
                            latency_s = None

                        if not resp.ok:
                            print(f"[sidecar] vLLM error: {resp.status_code} {resp.text}")
                            output_text = f"[vLLM error {resp.status_code}]"
                        else:
                            try:
                                data = resp.json()
                                raw_vllm = data

                                choices = data.get("choices") or []
                                if choices:
                                    first = choices[0]
                                    msg = first.get("message") or {}
                                    output_text = msg.get("content") or str(first)
                                    finish_reason = (
                                        first.get("finish_reason")
                                        or data.get("finish_reason")
                                    )
                                else:
                                    output_text = str(data)

                                if isinstance(data.get("usage"), dict):
                                    usage = data["usage"]
                            except Exception as e:
                                print(f"[sidecar] parse error for req_id={req_id}: {e}")
                                output_text = "[parse error in vLLM response]"
                                raw_vllm = None
                                finish_reason = None
                                usage = None

                    # ----------------------------------------------------
                    # Trace: vLLM recv timestamp
                    # ----------------------------------------------------
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        tr = dict(meta.get("__trace__") or {})
                        tr["t_vllm_recv"] = time.time()
                        if ttft_s is not None:
                            tr["ttft_sidecar_s"] = ttft_s
                        meta["__trace__"] = tr

                    # ----------------------------------------------------
                    # Trace: queue snapshot at result time
                    # ----------------------------------------------------
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        pending_res, inflight_res = self.local_q.state()
                        logical_res = pending_res + inflight_res

                        tr = dict(meta.get("__trace__") or {})
                        tr["sidecar_queue_len_at_result"] = pending_res
                        tr["sidecar_inflight_at_result"] = inflight_res
                        tr["sidecar_logical_at_result"] = logical_res
                        tr["t_post_result_sidecar"] = time.time()
                        meta["__trace__"] = tr

                    # ----------------------------------------------------
                    # Build result object (FULL vLLM info preserved)
                    # ----------------------------------------------------
                    result_obj: Dict[str, Any] = {
                        "output": output_text,
                        "endpoint_id": _cfg.CONTAINER_NAME,
                    }

                    if finish_reason is not None:
                        result_obj["finish_reason"] = finish_reason

                    # HTTP-level latency as seen by sidecar -> vLLM
                    if latency_s is not None:
                        result_obj["latency_s"] = latency_s

                    if ttft_s is not None:
                        result_obj["ttft_sidecar_s"] = ttft_s

                    # Full raw OpenAI-compatible JSON from vLLM
                    if raw_vllm is not None:
                        result_obj["raw"] = raw_vllm

                    # Token usage from vLLM (prompt/completion/total)
                    if usage is not None:
                        result_obj["usage"] = usage

                    # Attach trace dictionary into result, if enabled
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        result_obj["trace"] = dict(meta.get("__trace__") or {})

                    # ----------------------------------------------------
                    # Send result back to router (ASYNC via ResultPoster)
                    # ----------------------------------------------------
                    result_payload = {
                        "req_id": req_id,
                        "result": result_obj,
                    }

                    # Prom: completed (sidecar produced a result payload)
                    inc_completed(_cfg.CONTAINER_NAME)

                    if _cfg.LOG_LEVEL == "debug":
                        _tok = usage.get("completion_tokens", "?") if usage else "?"
                        print(f"[sidecar] vLLM done req_id={req_id} latency={latency_s:.2f}s tokens={_tok}")

                    if self._result_poster is not None:
                        # Non-blocking: avoids tying up vLLM workers on router backpressure / TCP resets
                        self._result_poster.submit(result_payload)
                    else:
                        # Fallback: original synchronous behavior (kept for safety)
                        try:
                            r2 = session.post(
                                router_result_url,
                                json=result_payload,
                                timeout=_cfg.ROUTER_RESULT_TIMEOUT_S,
                            )
                            if not r2.ok:
                                print(
                                    f"[sidecar] router /result error for req_id={req_id}: "
                                    f"{r2.status_code} {r2.text}"
                                )
                        except Exception as e:
                            print(f"[sidecar] router /result failed for req_id={req_id}: {e}")

                except Exception as e:
                    print(f"[sidecar] vLLM request failed for req_id={req_id}: {e}")
                    error_result = {
                        "req_id": req_id,
                        "result": {
                            "output": f"[sidecar error: {e}]",
                            "finish_reason": "error",
                            "error": str(e),
                        },
                    }
                    if self._result_poster is not None:
                        try:
                            self._result_poster.submit(error_result)
                        except Exception:
                            pass
                    else:
                        try:
                            session.post(
                                f"{_cfg.ROUTER_URL}/result",
                                json=error_result,
                                timeout=10.0,
                            )
                        except Exception:
                            pass

                finally:
                    # Mark job done
                    self.local_q.task_done()

                    # Decrement busy counter and update metric
                    try:
                        busy_now = _busy_dec()
                        set_sidecar_workers_busy(_cfg.CONTAINER_NAME, busy_now)
                    except Exception:
                        pass

                    # Busy-path capacity top-up
                    if self._pull_worker is not None:
                        try:
                            self._pull_worker.pull_if_capacity()
                        except Exception as e:
                            print(f"[sidecar] post-completion pull_if_capacity error: {e}")

        finally:
            session.close()
