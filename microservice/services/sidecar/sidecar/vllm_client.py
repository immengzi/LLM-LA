# sidecar/vllm_client.py
# -*- coding: utf-8 -*-
import time
import threading
from typing import Dict, Any, Optional

import requests

from .config import get_config
from .local_queue import LocalQueue
from .router_client import RouterPullWorker

_cfg = get_config()


class VLLMWorker:
    """
    Worker loop:
      - Pop from local queue
      - POST to vLLM /v1/chat/completions
      - POST result back to router /result

    Trace fields added (if TRACE_ENABLED):
      * t_dequeue_sidecar
      * t_vllm_send
      * t_vllm_recv
      * t_post_result_sidecar
    """

    def __init__(self, local_q: LocalQueue, pull_worker: Optional[RouterPullWorker] = None):
        self.local_q = local_q
        self._pull_worker = pull_worker

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
        router_result_url = f"{_cfg.ROUTER_URL}/result"

        # Idle-poke configuration
        idle_sleep_s = 0.01
        spins_per_second = int(1.0 / idle_sleep_s)
        spins_per_poke_worker = max(
            1,
            int(spins_per_second * _cfg.PULL_INTERVAL_S * _cfg.BATCH_SIZE),
        )
        idle_spins = 0

        try:
            while not self._stop_evt.is_set():

                # --------------------------------------------------------
                # Attempt to dequeue work
                # --------------------------------------------------------
                item = self.local_q.get_nowait()
                if not item:
                    idle_spins += 1

                    # Occasional idle pull
                    if (
                        self._pull_worker is not None
                        and idle_spins >= spins_per_poke_worker
                    ):
                        try:
                            self._pull_worker.pull_if_capacity()
                        except Exception as e:
                            print(f"[sidecar] idle-poke pull_if_capacity error: {e}")
                        idle_spins = 0

                    time.sleep(idle_sleep_s)
                    continue

                # Reset idle counter
                idle_spins = 0

                req_id, prompt, meta = item

                # --------------------------------------------------------
                # Trace: dequeue timestamp
                # --------------------------------------------------------
                if getattr(_cfg, "TRACE_ENABLED", False):
                    tr = dict(meta.get("__trace__") or {})
                    tr["t_dequeue_sidecar"] = time.time()
                    meta["__trace__"] = tr

                try:
                    # ----------------------------------------------------
                    # Build vLLM request
                    # ----------------------------------------------------
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
                    resp = session.post(
                        vllm_url,
                        json=payload,
                        timeout=_cfg.VLLM_TIMEOUT_S,
                    )

                    # ----------------------------------------------------
                    # Trace: vLLM recv timestamp
                    # ----------------------------------------------------
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        tr = dict(meta.get("__trace__") or {})
                        tr["t_vllm_recv"] = time.time()
                        meta["__trace__"] = tr

                    if not resp.ok:
                        print(f"[sidecar] vLLM error: {resp.status_code} {resp.text}")
                        output_text = f"[vLLM error {resp.status_code}]"
                    else:
                        # Parse OpenAI-style output
                        try:
                            data = resp.json()
                            choices = data.get("choices") or []
                            if choices and "message" in choices[0]:
                                output_text = choices[0]["message"]["content"]
                            else:
                                output_text = str(data)
                        except Exception as e:
                            print(f"[sidecar] parse error for req_id={req_id}: {e}")
                            output_text = "[parse error in vLLM response]"

                    # ----------------------------------------------------
                    # Send result back to router
                    # ----------------------------------------------------
                    result_payload = {
                        "req_id": req_id,
                        "output": output_text,
                    }

                    # Add trace fields to router callback payload
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        tr = dict(meta.get("__trace__") or {})
                        tr["t_post_result_sidecar"] = time.time()
                        result_payload["trace"] = tr  # router/api merges this

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

                finally:
                    # Mark job done
                    self.local_q.task_done()

                    # ----------------------------------------------------
                    # Busy-path capacity top-up
                    # ----------------------------------------------------
                    if self._pull_worker is not None:
                        try:
                            self._pull_worker.pull_if_capacity()
                        except Exception as e:
                            print(f"[sidecar] post-completion pull_if_capacity error: {e}")

        finally:
            session.close()
