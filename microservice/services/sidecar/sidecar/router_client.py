# sidecar/router_client.py
# -*- coding: utf-8 -*-
import threading
import time
from typing import Dict, Any

import requests

from .config import get_config
from .local_queue import LocalQueue

_cfg = get_config()


class RouterPullWorker:
    """
    Event-biased pull helper for a single vLLM pod.

    Responsibilities:
      - Compute capacity from local_q.state() and global BATCH_SIZE.
      - Call router /pull when there is spare capacity.
      - Never exceed BATCH_SIZE = pending + inflight on this pod.
      - No background polling; pull() is triggered by workers
        (busy-path and idle-poke).

    NOTE: endpoint_id must match what the router sees as the endpoint identity.
    """

    def __init__(self, local_q: LocalQueue, endpoint_id: str):
        self.local_q = local_q
        self.endpoint_id = endpoint_id  # sidecar identity used by router
        self._stop_evt = threading.Event()
        self._lock = threading.RLock()
        self._session: requests.Session | None = None

        # Initial “discovery” flags
        self._first_success: bool = False
        self._printed_wait_msg: bool = False

    # ---------------- lifecycle ----------------

    def start(self):
        """
        Initialize HTTP session. No polling thread, all pulls are event-driven.
        """
        if self._session is not None:
            return
        self._stop_evt.clear()
        self._session = requests.Session()
        print(f"[sidecar] RouterPullWorker ready (endpoint_id={self.endpoint_id})")

    def stop(self):
        self._stop_evt.set()
        session, self._session = self._session, None
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
        print("[sidecar] RouterPullWorker stopped")

    # ---------------- core logic ----------------

    def pull_if_capacity(self) -> None:
        """
        Event-biased pull:
          - If pending+inflight < BATCH_SIZE, compute want and /pull.
          - Insert returned jobs into the local queue.
        """
        if self._stop_evt.is_set():
            return

        with self._lock:
            pending, inflight = self.local_q.state()
            batch_size = _cfg.BATCH_SIZE
            total_reserved = pending + inflight

            if total_reserved >= batch_size:
                return

            want = batch_size - total_reserved
            if want <= 0:
                return

            session = self._session
            tmp_session = False
            if session is None:
                session = requests.Session()
                tmp_session = True

            try:
                # ---------------------
                # ROUTER /pull request
                # ---------------------
                resp = session.post(
                    f"{_cfg.ROUTER_URL}/pull",
                    json={"endpoint": self.endpoint_id, "want": want},
                    timeout=_cfg.ROUTER_PULL_TIMEOUT_S,
                )

                if not resp.ok:
                    # Before first success: suppress raw spam
                    if not self._first_success:
                        if not self._printed_wait_msg:
                            print(
                                "[sidecar] waiting for first successful /pull "
                                "from router-service ..."
                            )
                            self._printed_wait_msg = True
                    else:
                        print(f"[sidecar] /pull failed: {resp.status_code} {resp.text}")
                    return

                data = resp.json()
                items = data.get("items", [])

                if not self._first_success:
                    print("[sidecar] first successful /pull; normal logging enabled.")
                    self._first_success = True

                if not items:
                    return

                # ---------------------
                # Process returned jobs
                # ---------------------
                now_pull = time.time()

                for item in items:
                    rid = str(item["req_id"])
                    prompt = str(item["prompt"])
                    meta: Dict[str, Any] = item.get("meta") or {}

                    # Trace injection for pull arrival
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        tr = dict(meta.get("__trace__") or {})
                        tr["t_arrive_sidecar_pull"] = now_pull
                        meta["__trace__"] = tr

                    # Push into local queue
                    self.local_q.put(rid, prompt, meta)

            except Exception as e:
                if not self._first_success:
                    if not self._printed_wait_msg:
                        print(
                            "[sidecar] waiting for first successful /pull "
                            "from router-service ..."
                        )
                        self._printed_wait_msg = True
                else:
                    print(f"[sidecar] /pull error: {e}")

            finally:
                if tmp_session:
                    try:
                        session.close()
                    except Exception:
                        pass
