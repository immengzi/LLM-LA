# http_client.py
# Thin wrapper around:
#   - POST /enqueue (sync router backend)
#   - POST /submit (async_pubsub router backend)
#   - POST /v1/chat/completions (AIBrix backend)
#   - POST /v1/chat/completions (LiteLLM proxy backend)

from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
import time
import uuid

import requests
from requests.exceptions import RequestException

from config import (
    AIBrixConfig,
    GenerationConfig,
    LiteLLMConfig,
    BooMConfig,
    generation_effective_ignore_eos,
)


def _build_aibrix_extra_generation_fields(gen_cfg: GenerationConfig) -> Dict[str, Any]:
    """
    Build the extra generation fields that should be forwarded to AIBrix so the
    AIBrix path receives the same relevant generation controls as the working
    sidecar/vLLM path.

    Important:
    - Thinking control must be nested under chat_template_kwargs, matching the
      payload shape used by sidecar/vllm_client.py.
    - Other non-standard fields are forwarded only when present.
    """
    extra: Dict[str, Any] = {
        "chat_template_kwargs": {
            "enable_thinking": bool(gen_cfg.think),
        }
    }

    if gen_cfg.length_mode is not None:
        extra["length_mode"] = gen_cfg.length_mode

    if gen_cfg.target_output_tokens is not None:
        extra["target_output_tokens"] = int(gen_cfg.target_output_tokens)

    if gen_cfg.target_total_tokens is not None:
        extra["target_total_tokens"] = int(gen_cfg.target_total_tokens)

    if generation_effective_ignore_eos(gen_cfg):
        extra["ignore_eos"] = True

    return extra


def send_one(
    session: requests.Session,
    router_url: str,
    prompt: str,
    meta: Dict[str, Any] | None = None,
    slo_fields: Dict[str, Any] | None = None,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Send one synchronous /enqueue request.

    The router blocks until the sidecar posts /result or timeout.

    Returns:
        (req_id, result_dict_or_None)
    """
    t_enq = time.time()

    payload: Dict[str, Any] = {
        "prompt": prompt,
        "t_enq_client": t_enq,
        "meta": meta or {},
    }
    if slo_fields:
        payload.update(slo_fields)

    url = f"{router_url}/enqueue"

    try:
        resp = session.post(url, json=payload, timeout=1000000.0)
    except RequestException as e:
        print(f"[client] ✗ HTTP error talking to router: {e}")
        raise

    if not resp.ok:
        print(f"[client] ✗ /enqueue failed: {resp.status_code} {resp.text}")
        raise RuntimeError(f"/enqueue failed: {resp.status_code} {resp.text}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"/enqueue returned non-JSON body: {resp.text!r}")

    if "req_id" not in data:
        raise RuntimeError(f"/enqueue response missing 'req_id': {data!r}")

    rid = str(data["req_id"])
    result = data.get("result")

    if result is not None and not isinstance(result, dict):
        print(
            f"[client] WARNING: unexpected 'result' type for req_id={rid}: {type(result)}"
        )
        result = None

    return rid, result


def submit_one(
    session: requests.Session,
    router_url: str,
    submit_path: str,
    prompt: str,
    meta: Dict[str, Any] | None = None,
    run_id: Optional[str] = None,
    slo_fields: Dict[str, Any] | None = None,
) -> str:
    """
    Send one async submit request (submit+ack).

    Expected router behavior:
      - Accept request, allocate req_id, enqueue/dispatch
      - Immediately return 202 Accepted with {"req_id": "<id>"}.

    Returns:
        req_id (string)
    """

    t_enq = time.time()

    m: Dict[str, Any] = dict(meta or {})

    if run_id is not None and str(run_id).strip():
        m.setdefault("__run_id", str(run_id).strip())

    payload: Dict[str, Any] = {
        "prompt": prompt,
        "t_enq_client": t_enq,
        "meta": m,
    }
    if slo_fields:
        payload.update(slo_fields)

    sp = submit_path or "/submit"
    if not sp.startswith("/"):
        sp = "/" + sp

    url = f"{router_url}{sp}"

    try:
        resp = session.post(url, json=payload, timeout=10.0)
    except RequestException as e:
        print(f"[client] ✗ HTTP error talking to router submit endpoint: {e}")
        raise

    if resp.status_code not in (200, 202):
        print(f"[client] ✗ {sp} failed: {resp.status_code} {resp.text}")
        raise RuntimeError(f"{sp} failed: {resp.status_code} {resp.text}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"{sp} returned non-JSON body: {resp.text!r}")

    if "req_id" not in data:
        raise RuntimeError(f"{sp} response missing 'req_id': {data!r}")

    rid = str(data["req_id"]).strip()

    if not rid:
        raise RuntimeError(f"{sp} returned empty 'req_id': {data!r}")

    return rid


def send_one_aibrix(
    session: requests.Session,
    aibrix_cfg: AIBrixConfig,
    prompt: str,
    gen_cfg: GenerationConfig,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Send one request to the AIBrix gateway using the OpenAI-compatible chat API.

    This is a normal HTTP request whose connection remains open until the response
    completes. Open-loop concurrency is handled by load_runner.py via multiple
    in-flight threads/tasks, not via ZMQ.

    NOTE:
    - This helper supports non-streaming JSON responses only.
    - If stream=True is needed later, SSE parsing should be implemented separately.
    """

    if bool(aibrix_cfg.stream):
        raise RuntimeError("AIBrix streaming responses are not supported by send_one_aibrix()")

    t_send = time.time()

    url = f"{str(aibrix_cfg.base_url).rstrip('/')}{str(aibrix_cfg.chat_path)}"

    payload: Dict[str, Any] = {
        "model": str(aibrix_cfg.model),
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "max_tokens": int(gen_cfg.max_tokens),
        "temperature": float(gen_cfg.temperature),
    }

    if gen_cfg.min_tokens is not None:
        payload["min_tokens"] = int(gen_cfg.min_tokens)

    if bool(getattr(aibrix_cfg, "forward_extra_generation_fields", True)):
        payload.update(_build_aibrix_extra_generation_fields(gen_cfg))

    headers = {
        "Content-Type": "application/json",
        "model": str(aibrix_cfg.model),
        "routing-strategy": str(aibrix_cfg.routing_strategy),
    }

    try:
        resp = session.post(
            url,
            json=payload,
            headers=headers,
            timeout=(10, float(aibrix_cfg.timeout_s)),
            stream=False,
        )
    except RequestException as e:
        print(f"[client] ✗ HTTP error talking to AIBrix gateway: {e}")
        raise

    t_recv = time.time()

    # ---------- inspection block ----------
    if not resp.ok:
        print("[client] ✗ AIBrix request failed")
        print(f"[client] status_code = {resp.status_code}")
        print(f"[client] reason      = {resp.reason}")
        print(f"[client] url         = {resp.url}")
        print(f"[client] elapsed_s   = {resp.elapsed.total_seconds():.3f}")

        print("[client] response_headers:")
        for k, v in resp.headers.items():
            print(f"    {k}: {v}")

        body_preview = resp.text
        if len(body_preview) > 2000:
            body_preview = body_preview[:2000] + "...<truncated>"

        print("[client] response_body:")
        print(body_preview)

        req = resp.request

        print("[client] request_headers:")
        for k, v in req.headers.items():
            print(f"    {k}: {v}")

        if req.body:
            print("[client] request_body:")
            print(req.body)

        raise RuntimeError(
            f"AIBrix request failed: {resp.status_code} {resp.reason}"
        )
    # ---------- end inspection ----------

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"AIBrix returned non-JSON body: {resp.text!r}")

    rid_raw = data.get("id")
    if isinstance(rid_raw, str) and rid_raw.strip():
        rid = rid_raw.strip()
    else:
        rid = f"aibrix-{uuid.uuid4().hex}"

    output: Optional[str] = None
    finish_reason: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None

    choices = data.get("choices")

    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        c0 = choices[0]

        msg = c0.get("message")
        if isinstance(msg, dict):
            content = msg.get("content")
            if isinstance(content, str):
                output = content

        fr = c0.get("finish_reason")
        if isinstance(fr, str):
            finish_reason = fr

    if isinstance(data.get("usage"), dict):
        usage = data["usage"]

    # Extract endpoint identity from response headers or body.
    # BooM may set x-litellm-model-id or similar; vLLM sets system_fingerprint.
    endpoint_id: Optional[str] = None
    for hdr in ("x-litellm-model-id", "x-upstream", "x-backend-server", "x-served-by"):
        v = resp.headers.get(hdr)
        if v:
            endpoint_id = v
            break
    if not endpoint_id:
        sf = data.get("system_fingerprint")
        if isinstance(sf, str) and sf.strip():
            endpoint_id = sf.strip()

    result: Dict[str, Any] = {
        "output": output,
        "finish_reason": finish_reason,
        "latency_s": float(t_recv - t_send),
        "trace": {
            "trace_mode": "client_only",
            "t_send_client": t_send,
            "t_recv_client": t_recv,
            "client_roundtrip_s": float(t_recv - t_send),
        },
        "raw": data,
    }

    if endpoint_id:
        result["endpoint_id"] = endpoint_id

    if usage is not None:
        result["usage"] = usage

    return rid, result


# ============================================================
# Streaming variant of send_one_litellm (SSE)
# ============================================================

def _iter_sse_chunks(resp: requests.Response):
    """Yield parsed JSON objects from an OpenAI SSE stream."""
    import json as _json
    for raw_line in resp.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue
        line = raw_line.rstrip("\r\n")
        if line == "":
            continue
        if line.startswith("data: "):
            data_str = line[len("data: "):]
            if data_str.strip() == "[DONE]":
                return
            try:
                yield _json.loads(data_str)
            except Exception:
                pass


def send_one_litellm_stream(
    session: requests.Session,
    litellm_cfg: LiteLLMConfig,
    prompt: str,
    gen_cfg: GenerationConfig,
    label: str = "LiteLLM",
    messages: Optional[List[Dict[str, str]]] = None,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Streaming variant of send_one_litellm.

    Sends stream=true to the proxy, parses SSE chunks, accumulates the full
    response, and returns the same (req_id, result_dict) shape as the
    non-streaming version — plus ttft_s and tpot_avg_s.

    If messages is provided, it is used directly; otherwise a single user
    message is built from prompt.
    """
    label_lower = label.lower()
    t_send = time.time()

    url = f"{str(litellm_cfg.base_url).rstrip('/')}{str(litellm_cfg.chat_path)}"

    payload: Dict[str, Any] = {
        "model": str(litellm_cfg.model),
        "messages": messages if messages is not None else [{"role": "user", "content": prompt}],
        "stream": True,
        "stream_options": {"include_usage": True},
        "max_tokens": int(gen_cfg.max_tokens),
        "temperature": float(gen_cfg.temperature),
    }

    if gen_cfg.min_tokens is not None:
        payload["min_tokens"] = int(gen_cfg.min_tokens)

    payload.update(_build_aibrix_extra_generation_fields(gen_cfg))

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {litellm_cfg.api_key}",
    }

    try:
        resp = session.post(
            url,
            json=payload,
            headers=headers,
            timeout=(10, float(litellm_cfg.timeout_s)),
            stream=True,
            proxies={"http": None, "https": None},
        )
    except RequestException as e:
        print(f"[client] ✗ HTTP error talking to {label} proxy (stream): {e}")
        raise

    if not resp.ok:
        print(f"[client] ✗ {label} streaming request failed: {resp.status_code} {resp.reason}")
        body_preview = resp.text
        if len(body_preview) > 2000:
            body_preview = body_preview[:2000] + "...<truncated>"
        print(f"[client] response_body: {body_preview}")
        raise RuntimeError(
            f"{label} streaming request failed: {resp.status_code} {resp.reason}"
        )

    rid: Optional[str] = None
    output_parts: list = []
    finish_reason: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None
    stream_endpoint_id: Optional[str] = None

    # Check response headers for endpoint identity before consuming the stream
    for hdr in ("x-litellm-model-id", "x-upstream", "x-backend-server", "x-served-by"):
        v = resp.headers.get(hdr)
        if v:
            stream_endpoint_id = v
            break

    t_first_token: Optional[float] = None
    token_timestamps: list = []
    chunk_count = 0

    try:
        for chunk in _iter_sse_chunks(resp):
            now = time.time()
            chunk_count += 1

            if rid is None:
                cid = chunk.get("id")
                if isinstance(cid, str) and cid.strip():
                    rid = cid.strip()

            if stream_endpoint_id is None:
                sf = chunk.get("system_fingerprint")
                if isinstance(sf, str) and sf.strip():
                    stream_endpoint_id = sf.strip()

            choices = chunk.get("choices")
            if isinstance(choices, list) and choices:
                c0 = choices[0]
                delta = c0.get("delta")
                if isinstance(delta, dict):
                    content = delta.get("content")
                    if isinstance(content, str) and content:
                        if t_first_token is None:
                            t_first_token = now
                        output_parts.append(content)
                        token_timestamps.append(now)

                fr = c0.get("finish_reason")
                if isinstance(fr, str):
                    finish_reason = fr

            u = chunk.get("usage")
            if isinstance(u, dict) and u:
                usage = u
    finally:
        resp.close()

    t_done = time.time()

    if rid is None:
        rid = f"{label_lower}-{uuid.uuid4().hex}"

    output = "".join(output_parts) if output_parts else None

    ttft_s: Optional[float] = None
    if t_first_token is not None:
        ttft_s = t_first_token - t_send

    tpot_avg_s: Optional[float] = None
    if len(token_timestamps) >= 2 and t_first_token is not None:
        decode_duration = token_timestamps[-1] - t_first_token
        tpot_avg_s = decode_duration / (len(token_timestamps) - 1)

    result: Dict[str, Any] = {
        "output": output,
        "finish_reason": finish_reason,
        "latency_s": float(t_done - t_send),
        "ttft_s": ttft_s,
        "tpot_avg_s": tpot_avg_s,
        "streaming_chunks": chunk_count,
        "trace": {
            "trace_mode": "client_only_stream",
            "t_send_client": t_send,
            "t_first_token_client": t_first_token,
            "t_recv_client": t_done,
            "client_roundtrip_s": float(t_done - t_send),
        },
    }

    if stream_endpoint_id:
        result["endpoint_id"] = stream_endpoint_id

    if usage is not None:
        result["usage"] = usage

    return rid, result

# ============================================================
# LiteLLM proxy backend
#
# Sends OpenAI-format requests to the LiteLLM proxy pod, which
# enforces virtual key auth + spend tracking before forwarding
# to the router's /v1/chat/completions shim.
#
# Intentionally kept separate from send_one_aibrix() so:
#   - LiteLLM-specific headers (Authorization: Bearer) are clean
#   - AIBrix-specific headers (routing-strategy, model) are not sent
#   - Logging clearly identifies the LiteLLM path
#   - Future LiteLLM-specific features (streaming, tool calls) can
#     be added here without touching the AIBrix path
#
# Not used in benchmarking sweeps — only for production/demo validation.
# ============================================================

def send_one_litellm(
    session: requests.Session,
    litellm_cfg: LiteLLMConfig,
    prompt: str,
    gen_cfg: GenerationConfig,
    label: str = "LiteLLM",
    messages: Optional[List[Dict[str, str]]] = None,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Send one request to an OpenAI-compatible proxy (LiteLLM or BooM Gateway).

    The proxy enforces virtual key auth, spend tracking, and rate limits,
    then forwards to the router's /v1/chat/completions shim.

    Args:
        label: Human-readable name for log messages (e.g. "LiteLLM", "BooM").
        messages: If provided, used directly as the messages array; otherwise
                  a single user message is built from prompt.

    Returns:
        (req_id, result_dict)
    """
    label_lower = label.lower()

    t_send = time.time()

    url = f"{str(litellm_cfg.base_url).rstrip('/')}{str(litellm_cfg.chat_path)}"

    payload: Dict[str, Any] = {
        "model": str(litellm_cfg.model),
        "messages": messages if messages is not None else [{"role": "user", "content": prompt}],
        "stream": False,
        "max_tokens": int(gen_cfg.max_tokens),
        "temperature": float(gen_cfg.temperature),
    }

    if gen_cfg.min_tokens is not None:
        payload["min_tokens"] = int(gen_cfg.min_tokens)

    # Match send_one_aibrix(): length_mode, targets, ignore_eos, thinking kwargs.
    payload.update(_build_aibrix_extra_generation_fields(gen_cfg))

    # Standard OpenAI auth header — LiteLLM validates the virtual key here
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {litellm_cfg.api_key}",
    }

    try:
        resp = session.post(
            url,
            json=payload,
            headers=headers,
            timeout=(10, float(litellm_cfg.timeout_s)),
            stream=False,
            proxies={"http": None, "https": None},
        )
    except RequestException as e:
        print(f"[client] ✗ HTTP error talking to {label} proxy: {e}")
        raise

    t_recv = time.time()

    if not resp.ok:
        print(f"[client] ✗ {label} request failed")
        print(f"[client] status_code = {resp.status_code}")
        print(f"[client] reason      = {resp.reason}")
        print(f"[client] url         = {resp.url}")
        print(f"[client] elapsed_s   = {resp.elapsed.total_seconds():.3f}")

        print("[client] response_headers:")
        for k, v in resp.headers.items():
            print(f"    {k}: {v}")

        body_preview = resp.text
        if len(body_preview) > 2000:
            body_preview = body_preview[:2000] + "...<truncated>"

        print("[client] response_body:")
        print(body_preview)

        raise RuntimeError(
            f"{label} request failed: {resp.status_code} {resp.reason}"
        )

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"{label} returned non-JSON body: {resp.text!r}")

    # Parse OpenAI-format response (identical structure to AIBrix / router shim)
    rid_raw = data.get("id")
    if isinstance(rid_raw, str) and rid_raw.strip():
        rid = rid_raw.strip()
    else:
        rid = f"{label_lower}-{uuid.uuid4().hex}"

    output: Optional[str] = None
    finish_reason: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None

    choices = data.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        c0 = choices[0]
        msg = c0.get("message")
        if isinstance(msg, dict):
            content = msg.get("content")
            if isinstance(content, str):
                output = content
        fr = c0.get("finish_reason")
        if isinstance(fr, str):
            finish_reason = fr

    if isinstance(data.get("usage"), dict):
        usage = data["usage"]

    result: Dict[str, Any] = {
        "output": output,
        "finish_reason": finish_reason,
        "latency_s": float(t_recv - t_send),
        "trace": {
            "trace_mode": "client_only",
            "t_send_client": t_send,
            "t_recv_client": t_recv,
            "client_roundtrip_s": float(t_recv - t_send),
        },
        "raw": data,
    }

    if usage is not None:
        result["usage"] = usage

    return rid, result