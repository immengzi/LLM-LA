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
      - Send result back to router /result so the client gets a response.

    Event-biased pull integration:
      - After each completion, call pull_if_capacity() to top up immediately.
      - When idle (no work in local queue), occasionally do an "idle poke"
        via pull_if_capacity(), but throttled to avoid constant polling.
    """

    def __init__(self, local_q: LocalQueue, pull_worker: Optional[RouterPullWorker] = None):
        self.local_q = local_q
        self._pull_worker = pull_worker

        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None

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

    def _loop(self):
        session = requests.Session()
        vllm_url = f"{_cfg.VLLM_URL}/v1/chat/completions"
        router_result_url = f"{_cfg.ROUTER_URL}/result"

        # Idle-poke configuration:
        idle_sleep_s = 0.01
        spins_per_second = int(1.0 / idle_sleep_s)

        # Each worker pokes roughly once every (PULL_INTERVAL_S * BATCH_SIZE)
        spins_per_poke_worker = max(
            1,
            int(spins_per_second * _cfg.PULL_INTERVAL_S * _cfg.BATCH_SIZE),
        )

        idle_spins = 0

        try:
            while not self._stop_evt.is_set():
                item = self.local_q.get_nowait()
                if not item:
                    # No work → idle path
                    idle_spins += 1

                    # Occasional throttle idle poke
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

                # We have work
                idle_spins = 0
                req_id, prompt, meta = item

                try:
                    # 1) Call vLLM
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

                    resp = session.post(vllm_url, json=payload, timeout=30.0)
                    if not resp.ok:
                        print(f"[sidecar] vLLM error: {resp.status_code} {resp.text}")
                        output_text = f"[vLLM error {resp.status_code}]"
                    else:
                        # 2) Extract text from OpenAI-style response
                        try:
                            data = resp.json()
                            choices = data.get("choices") or []
                            if choices and "message" in choices[0]:
                                output_text = choices[0]["message"]["content"]
                            else:
                                output_text = str(data)
                        except Exception as e:
                            print(f"[sidecar] failed to parse vLLM response for req_id={req_id}: {e}")
                            output_text = "[parse error in vLLM response]"

                    # 3) Send result back to router so /enqueue can return it
                    try:
                        r2 = session.post(
                            router_result_url,
                            json={
                                "req_id": req_id,
                                "output": output_text,
                            },
                            timeout=5.0,
                        )
                        if not r2.ok:
                            print(
                                f"[sidecar] router /result error for req_id={req_id}: "
                                f"{r2.status_code} {r2.text}"
                            )
                    except Exception as e:
                        print(f"[sidecar] router /result request failed for req_id={req_id}: {e}")

                except Exception as e:
                    print(f"[sidecar] vLLM request failed for req_id={req_id}: {e}")

                finally:
                    # Mark the job complete in the local queue
                    self.local_q.task_done()

                    # Busy-path top-up pull
                    if self._pull_worker is not None:
                        try:
                            self._pull_worker.pull_if_capacity()
                        except Exception as e:
                            print(f"[sidecar] post-completion pull_if_capacity error: {e}")

        finally:
            session.close()
