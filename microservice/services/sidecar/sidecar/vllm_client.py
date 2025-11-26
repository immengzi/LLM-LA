# -*- coding: utf-8 -*-
import time
import threading
from typing import Dict, Any

import requests

from .config import get_config
from .local_queue import LocalQueue

_cfg = get_config()


class VLLMWorker:
    """
    Simple loop:
      - Pop from local queue
      - POST to vLLM /v1/chat/completions
    """

    def __init__(self, local_q: LocalQueue):
        self.local_q = local_q
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
        url = f"{_cfg.VLLM_URL}/v1/chat/completions"
        while not self._stop_evt.is_set():
            item = self.local_q.get_nowait()
            if not item:
                time.sleep(0.01)
                continue

            req_id, prompt, meta = item
            try:
                payload: Dict[str, Any] = {
                    "model": _cfg.MODEL_NAME,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": meta.get("max_tokens", 128),
                    "temperature": meta.get("temperature", 0.0),
                }
                resp = session.post(url, json=payload, timeout=30.0)
                if not resp.ok:
                    print(f"[sidecar] vLLM error: {resp.status_code} {resp.text}")
                # you can log or forward the completion here

            except Exception as e:
                print(f"[sidecar] vLLM request failed: {e}")
            finally:
                self.local_q.task_done()
        session.close()
