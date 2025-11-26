# -*- coding: utf-8 -*-
import time
import threading
from typing import Dict, Any

import requests

from .config import get_config
from .local_queue import LocalQueue

_cfg = get_config()


class RouterPullWorker:
    """
    Periodically:
      1) compute want from local queue
      2) call router /pull
      3) push jobs into local queue
    """

    def __init__(self, local_q: LocalQueue, endpoint_url: str):
        self.local_q = local_q
        self.endpoint_url = endpoint_url  # this sidecar's vLLM endpoint
        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        if self._thread is not None:
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print("[sidecar] RouterPullWorker started")

    def stop(self):
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        print("[sidecar] RouterPullWorker stopped")

    def _loop(self):
        session = requests.Session()
        while not self._stop_evt.is_set():
            want = self.local_q.compute_want(_cfg.TARGET_LOCAL_QUEUE)
            if want > 0:
                try:
                    resp = session.post(
                        f"{_cfg.ROUTER_URL}/pull",
                        json={"endpoint": self.endpoint_url, "want": want},
                        timeout=1.0,
                    )
                    if resp.ok:
                        data = resp.json()
                        items = data.get("items", [])
                        for item in items:
                            rid = int(item["req_id"])
                            prompt = str(item["prompt"])
                            meta: Dict[str, Any] = item.get("meta") or {}
                            self.local_q.put(rid, prompt, meta)
                    else:
                        print(f"[sidecar] /pull failed: {resp.status_code} {resp.text}")
                except Exception as e:
                    print(f"[sidecar] /pull error: {e}")

            time.sleep(_cfg.PULL_INTERVAL_S)
        session.close()
