# sidecar/vllm_client.py
# -*- coding: utf-8 -*-
_VLLM_CLIENT_VERSION = "2026-05-13-streaming-endpoint-id"

import time
import threading
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

import requests

from .config import get_config
from .local_queue import LocalQueue
from .router_client import RouterPullWorker
from .result_poster import ResultPoster
from .metrics import inc_completed, set_sidecar_workers_busy

_cfg = get_config()
print(f"[sidecar] inference client version={_VLLM_CLIENT_VERSION}")


@dataclass(frozen=True)
class InferenceEngineProfile:
    """Engine-specific request policy for the shared OpenAI chat API."""

    name: str
    unsupported_fields: frozenset[str] = frozenset()

    def adapt_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.unsupported_fields:
            return payload
        return {
            key: value
            for key, value in payload.items()
            if key not in self.unsupported_fields
        }


_ENGINE_PROFILES = {
    # No filtering: this keeps the legacy vLLM payload exactly unchanged.
    "vllm": InferenceEngineProfile(name="vllm"),
    # SGLang v0.5.15 accepts the OpenAI chat fields used by the sidecar,
    # including its documented min_tokens extension.
    "sglang": InferenceEngineProfile(name="sglang"),
}


def get_engine_profile(engine: str) -> InferenceEngineProfile:
    normalized = str(engine).strip().lower()
    try:
        return _ENGINE_PROFILES[normalized]
    except KeyError as exc:
        supported = ", ".join(sorted(_ENGINE_PROFILES))
        raise ValueError(
            f"Unsupported inference engine {engine!r}; expected one of: {supported}"
        ) from exc

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


def _merge_tool_call_delta(
    acc: Dict[int, Dict[str, Any]],
    tool_calls: Any,
) -> None:
    """Accumulate OpenAI streaming tool_call deltas by index.

    The sidecar forwards per-chunk deltas unchanged to the router for streaming
    clients, but it also needs a complete tool_calls list for non-streaming
    clients when STREAMING_MODE=true and the engine is always queried via SSE.
    """
    if not isinstance(tool_calls, list):
        return

    for i, tc in enumerate(tool_calls):
        if not isinstance(tc, dict):
            continue
        idx_raw = tc.get("index", i)
        try:
            idx = int(idx_raw)
        except Exception:
            idx = i

        cur = acc.setdefault(
            idx,
            {
                "id": tc.get("id") or f"call_{idx}",
                "type": tc.get("type") or "function",
                "function": {"name": "", "arguments": ""},
            },
        )

        if tc.get("id"):
            cur["id"] = tc["id"]
        if tc.get("type"):
            cur["type"] = tc["type"]

        func = tc.get("function")
        if isinstance(func, dict):
            cur_func = cur.setdefault("function", {"name": "", "arguments": ""})
            if func.get("name"):
                cur_func["name"] = func["name"]
            if isinstance(func.get("arguments"), str):
                cur_func["arguments"] = cur_func.get("arguments", "") + func["arguments"]


def _complete_tool_calls(acc: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return completed OpenAI tool_calls ordered by streaming index."""
    out: List[Dict[str, Any]] = []
    for idx in sorted(acc):
        tc = acc[idx]
        func = tc.get("function")
        if not isinstance(func, dict):
            continue
        if not func.get("name"):
            continue
        out.append(
            {
                "id": tc.get("id") or f"call_{idx}",
                "type": tc.get("type") or "function",
                "function": {
                    "name": func.get("name", ""),
                    "arguments": func.get("arguments", ""),
                },
            }
        )
    return out


class InferenceWorker:
    """
    Worker loop:
      - Pop from local queue
      - POST to the inference engine's /v1/chat/completions endpoint
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
        self._profile = get_engine_profile(_cfg.INFERENCE_ENGINE)

        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------

    def start(self):
        if self._thread is not None:
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print("[sidecar] InferenceWorker started")

    def stop(self):
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        print("[sidecar] InferenceWorker stopped")

    def _build_payload(
        self,
        prompt: str,
        meta: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build one OpenAI chat request, then apply engine policy."""
        chat_req = meta.get("__chat_request__")
        if chat_req and isinstance(chat_req, dict):
            payload = dict(chat_req)
            payload["model"] = _cfg.MODEL_NAME
            payload["stream"] = False
        else:
            max_tokens = meta.get("max_tokens", 128)
            temperature = meta.get("temperature", 0.0)

            payload = {
                "model": _cfg.MODEL_NAME,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": temperature,
            }
            # Preserve the historical vLLM request exactly.  SGLang, however,
            # must use its tokenizer's configured default unless the caller
            # explicitly supplied an override; silently forcing False here can
            # make engine tokenization diverge from the router's KV hash.
            if self._profile.name == "vllm" or "enable_thinking" in meta:
                payload["chat_template_kwargs"] = {
                    "enable_thinking": bool(meta.get("enable_thinking", False)),
                }
            if "min_tokens" in meta:
                payload["min_tokens"] = int(meta["min_tokens"])

        if meta.get("ignore_eos"):
            payload["ignore_eos"] = bool(meta["ignore_eos"])

        if _cfg.FORCE_IGNORE_EOS:
            payload["ignore_eos"] = True

        return self._profile.adapt_payload(payload)

    # ------------------------------------------------------------

    def _loop(self):
        session = requests.Session()
        inference_url = f"{_cfg.INFERENCE_URL}/v1/chat/completions"

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
                    # Build inference request
                    # ----------------------------------------------------
                    payload = self._build_payload(prompt, meta)

                    # ----------------------------------------------------
                    # Legacy trace key retained for compatibility.
                    # ----------------------------------------------------
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        tr = dict(meta.get("__trace__") or {})
                        tr["t_vllm_send"] = time.time()
                        meta["__trace__"] = tr

                    # ----------------------------------------------------
                    # Call inference engine
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
                            f"[sidecar] inference send req_id={req_id} model={_cfg.MODEL_NAME} "
                            f"ignore_eos={_eos} min_tokens={_mt} max_tokens={_mx} "
                            f"stream={use_stream}"
                        )

                    output_text: str
                    finish_reason: Optional[str] = None
                    usage: Optional[Dict[str, Any]] = None
                    raw_vllm: Optional[Dict[str, Any]] = None
                    latency_s: Optional[float] = None
                    ttft_s: Optional[float] = None
                    upstream_error: Optional[Dict[str, Any]] = None

                    if use_stream:
                        # --------------------------------------------------
                        # Streaming path: SSE from engine, accumulate locally
                        # and optionally forward chunks to router
                        # --------------------------------------------------
                        router_chunk_url = f"{_cfg.ROUTER_URL}/result_chunk"
                        forward_stream = bool(meta.get("__chat_request__", {}).get("stream"))

                        t_vllm_send = time.time()
                        resp = session.post(
                            inference_url,
                            json=payload,
                            timeout=_cfg.INFERENCE_TIMEOUT_S,
                            stream=True,
                        )

                        if not resp.ok:
                            print(f"[sidecar] inference stream error: {resp.status_code} {resp.text}")
                            # Passthrough upstream 4xx (client-request errors, e.g.
                            # context-length overflow) verbatim so the gateway sees
                            # the engine's real status + body. 5xx keeps the legacy marker.
                            if 400 <= resp.status_code < 500:
                                upstream_error = {
                                    "status": resp.status_code,
                                    "body": resp.text,
                                    "content_type": resp.headers.get(
                                        "content-type", "application/json"
                                    ),
                                }
                                output_text = ""
                            else:
                                output_text = f"[inference error {resp.status_code}]"
                            resp.close()
                        else:
                            import json as _json
                            parts: list = []
                            tool_call_acc: Dict[int, Dict[str, Any]] = {}
                            stream_response_id: Optional[str] = None
                            stream_created: Optional[int] = None
                            stream_model: Optional[str] = None
                            t_first_token: Optional[float] = None
                            chunk_idx = 0
                            pending_final_payload = None
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

                                    if stream_response_id is None and isinstance(chunk.get("id"), str):
                                        stream_response_id = chunk.get("id")
                                    if stream_created is None and isinstance(chunk.get("created"), int):
                                        stream_created = chunk.get("created")
                                    if stream_model is None and isinstance(chunk.get("model"), str):
                                        stream_model = chunk.get("model")

                                    delta_content = None
                                    delta_tool_calls = None
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
                                            tool_calls = delta.get("tool_calls")
                                            if isinstance(tool_calls, list) and tool_calls:
                                                if t_first_token is None:
                                                    t_first_token = time.time()
                                                delta_tool_calls = tool_calls
                                                _merge_tool_call_delta(tool_call_acc, tool_calls)

                                        fr = c0.get("finish_reason")
                                        if isinstance(fr, str):
                                            finish_reason = fr
                                            chunk_fr = fr

                                    u = chunk.get("usage")
                                    if isinstance(u, dict) and u:
                                        usage = u

                                    # If a buffered is_final is waiting and usage just arrived, flush it
                                    if forward_stream and pending_final_payload is not None and usage:
                                        pending_final_payload["usage"] = usage
                                        try:
                                            session.post(
                                                router_chunk_url,
                                                json=pending_final_payload,
                                                timeout=5.0,
                                            )
                                        except Exception as ce:
                                            if _cfg.LOG_LEVEL == "debug":
                                                print(f"[sidecar] chunk forward failed: {ce}")
                                        chunk_idx += 1
                                        pending_final_payload = None

                                    if forward_stream and (delta_content or delta_tool_calls or chunk_fr):
                                        is_final = chunk_fr is not None
                                        chunk_payload = {
                                            "req_id": req_id,
                                            "chunk_idx": chunk_idx,
                                            "delta": delta_content or "",
                                            "is_final": is_final,
                                            "endpoint_id": _cfg.CONTAINER_NAME,
                                        }
                                        if delta_tool_calls:
                                            chunk_payload["tool_calls"] = delta_tool_calls
                                        if chunk_fr:
                                            chunk_payload["finish_reason"] = chunk_fr
                                        if is_final and usage:
                                            chunk_payload["usage"] = usage
                                        if is_final and not usage:
                                            # Buffer for the trailing usage chunk.
                                            pending_final_payload = chunk_payload
                                        else:
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
                                # Flush any buffered is_final that never got a usage chunk
                                if forward_stream and pending_final_payload is not None:
                                    if usage:
                                        pending_final_payload["usage"] = usage
                                    try:
                                        session.post(
                                            router_chunk_url,
                                            json=pending_final_payload,
                                            timeout=5.0,
                                        )
                                    except Exception as ce:
                                        if _cfg.LOG_LEVEL == "debug":
                                            print(f"[sidecar] chunk forward failed: {ce}")
                                resp.close()

                            t_vllm_recv = time.time()
                            output_text = "".join(parts) if parts else ""
                            latency_s = t_vllm_recv - t_vllm_send
                            completed_tool_calls = _complete_tool_calls(tool_call_acc)
                            raw_vllm = {
                                "id": stream_response_id or f"chatcmpl-{req_id}",
                                "object": "chat.completion",
                                "created": stream_created or int(t_vllm_send),
                                "model": stream_model or _cfg.MODEL_NAME,
                                "choices": [
                                    {
                                        "index": 0,
                                        "message": {
                                            "role": "assistant",
                                            "content": output_text,
                                            "tool_calls": completed_tool_calls,
                                        },
                                        "finish_reason": finish_reason,
                                    }
                                ],
                                "usage": usage or {},
                            }

                            if t_first_token is not None:
                                ttft_s = t_first_token - t_vllm_send

                    else:
                        # --------------------------------------------------
                        # Non-streaming path (original)
                        # --------------------------------------------------
                        resp = session.post(
                            inference_url,
                            json=payload,
                            timeout=_cfg.INFERENCE_TIMEOUT_S,
                        )

                        try:
                            latency_s = float(resp.elapsed.total_seconds())
                        except Exception:
                            latency_s = None

                        if not resp.ok:
                            print(f"[sidecar] inference error: {resp.status_code} {resp.text}")
                            # Passthrough upstream 4xx (client-request errors, e.g.
                            # context-length overflow) verbatim so the gateway sees
                            # the engine's real status + body. 5xx keeps the legacy marker.
                            if 400 <= resp.status_code < 500:
                                upstream_error = {
                                    "status": resp.status_code,
                                    "body": resp.text,
                                    "content_type": resp.headers.get(
                                        "content-type", "application/json"
                                    ),
                                }
                                output_text = ""
                            else:
                                output_text = f"[inference error {resp.status_code}]"
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
                                output_text = "[parse error in inference response]"
                                raw_vllm = None
                                finish_reason = None
                                usage = None

                    # ----------------------------------------------------
                    # Legacy trace key retained for compatibility.
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
                    # Build result object (full engine response preserved)
                    # ----------------------------------------------------
                    result_obj: Dict[str, Any] = {
                        "output": output_text,
                        "endpoint_id": _cfg.CONTAINER_NAME,
                    }

                    if finish_reason is not None:
                        result_obj["finish_reason"] = finish_reason

                    if raw_vllm is not None:
                        try:
                            tc = (
                                raw_vllm.get("choices", [{}])[0]
                                .get("message", {})
                                .get("tool_calls")
                            )
                            if isinstance(tc, list) and tc:
                                result_obj["tool_calls"] = tc
                        except Exception:
                            pass

                    # HTTP-level latency as seen by sidecar -> engine
                    if latency_s is not None:
                        result_obj["latency_s"] = latency_s

                    if ttft_s is not None:
                        result_obj["ttft_sidecar_s"] = ttft_s

                    # Full raw OpenAI-compatible engine JSON
                    if raw_vllm is not None:
                        result_obj["raw"] = raw_vllm

                    # Token usage from engine (prompt/completion/total)
                    if usage is not None:
                        result_obj["usage"] = usage

                    # Non-2xx upstream (4xx) passthrough marker: carries vLLM's
                    # original status + body so the router can relay it verbatim.
                    if upstream_error is not None:
                        result_obj["upstream_error"] = upstream_error

                    # Attach trace dictionary into result, if enabled
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        result_obj["trace"] = dict(meta.get("__trace__") or {})

                    # ----------------------------------------------------
                    # Send result back to router (ASYNC via ResultPoster)
                    #
                    # The top-level "endpoint" field is the request-completion
                    # signal the router uses to decrement its per-endpoint
                    # in-flight counters (PushRouter.notify_result for
                    # push-leastq-local, and RouterState.release_inflight for
                    # pull fairness / central-push capacity). Send it explicitly
                    # instead of relying on result["endpoint_id"] so the contract
                    # is symmetric with the error path below.
                    # ----------------------------------------------------
                    result_payload = {
                        "req_id": req_id,
                        "endpoint": _cfg.CONTAINER_NAME,
                        "result": result_obj,
                    }

                    # Prom: completed (sidecar produced a result payload)
                    inc_completed(_cfg.CONTAINER_NAME)

                    if _cfg.LOG_LEVEL == "debug":
                        _tok = usage.get("completion_tokens", "?") if usage else "?"
                        latency_log = f"{latency_s:.2f}s" if latency_s is not None else "unknown"
                        print(
                            f"[sidecar] inference done req_id={req_id} "
                            f"latency={latency_log} tokens={_tok}"
                        )

                    if self._result_poster is not None:
                        # Non-blocking: avoids tying up workers on router backpressure.
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
                    print(f"[sidecar] inference request failed for req_id={req_id}: {e}")
                    # Carry the top-level "endpoint" on the error path too — an
                    # errored request still occupied a slot, so the router must
                    # decrement its in-flight counter. Without this the counter
                    # leaks on every failure and least-queue selection skews.
                    error_result = {
                        "req_id": req_id,
                        "endpoint": _cfg.CONTAINER_NAME,
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


# Public compatibility alias for existing imports and callers.
VLLMWorker = InferenceWorker
