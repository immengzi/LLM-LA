# sidecar/router_client.py
# -*- coding: utf-8 -*-
import threading
import time
from typing import Dict, Any

import requests
from requests.adapters import HTTPAdapter

from .config import get_config
from .local_queue import LocalQueue
from .metrics import inc_received

_cfg = get_config()


def _make_pooled_session(pool_connections: int, pool_maxsize: int) -> requests.Session:
    """
    Create a requests.Session with a larger urllib3 connection pool.
    This reduces TCP churn and makes connection reuse much more reliable.
    """
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=int(pool_connections),
        pool_maxsize=int(pool_maxsize),
        max_retries=0,
        pool_block=False,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


class RouterPullWorker:
    """
    Pull helper for a single vLLM pod.

    Responsibilities:
      - Compute capacity from local_q.state() and pull_cap (BATCH_SIZE + PREFETCH).
      - Call router /pull when there is spare capacity.
      - Background poller thread continuously tops up the queue so workers
        never starve waiting for a reactive pull after completion.
      - Workers still call pull_if_capacity() on the busy-path for
        immediate top-up after each completion.

    NOTE: endpoint_id must match what the router sees as the endpoint identity.
    """

    def __init__(self, local_q: LocalQueue, endpoint_id: str):
        self.local_q = local_q
        self.endpoint_id = endpoint_id  # sidecar identity used by router
        self._stop_evt = threading.Event()
        self._lock = threading.RLock()
        self._session: requests.Session | None = None
        self._poll_thread: threading.Thread | None = None

        # Initial "discovery" flags
        self._first_success: bool = False
        self._printed_wait_msg: bool = False

    # ---------------- lifecycle ----------------

    def start(self):
        """
        Initialize HTTP session and start background pull poller.
        """
        if self._session is not None:
            return
        self._stop_evt.clear()

        # pooled session (no semantic change)
        self._session = _make_pooled_session(
            pool_connections=_cfg.ROUTER_POOL_CONNECTIONS,
            pool_maxsize=_cfg.ROUTER_POOL_MAXSIZE,
        )

        self._poll_thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="pull-poller",
        )
        self._poll_thread.start()

        pull_cap = _cfg.BATCH_SIZE + _cfg.PREFETCH
        print(
            f"[sidecar] RouterPullWorker ready (endpoint_id={self.endpoint_id}, "
            f"BATCH_SIZE={_cfg.BATCH_SIZE}, PREFETCH={_cfg.PREFETCH}, pull_cap={pull_cap})"
        )

    def stop(self):
        self._stop_evt.set()
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=2.0)
            self._poll_thread = None
        session, self._session = self._session, None
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
        print("[sidecar] RouterPullWorker stopped")

    # ---------------- background poller ----------------

    def _poll_loop(self):
        """
        Continuously check capacity and pull from router.
        Ensures the local queue stays warm even when all workers are
        blocked on long vLLM calls and no busy-path pulls fire.
        """
        interval = max(0.01, float(_cfg.PULL_INTERVAL_S))
        while not self._stop_evt.is_set():
            try:
                self.pull_if_capacity()
            except Exception as e:
                if self._first_success:
                    print(f"[sidecar] poll-loop pull error: {e}")
            self._stop_evt.wait(timeout=interval)

    # ---------------- core logic ----------------

    def pull_if_capacity(self) -> None:
        """
        Event-biased pull:
          - If pending+inflight < pull_cap, compute want and /pull.
          - Insert returned jobs into the local queue.

        When TRACE_ENABLED=true, also stamps:
          - t_arrive_sidecar_pull
          - sidecar_queue_len_before_pull
          - sidecar_inflight_before_pull
          - sidecar_logical_before_pull
          - sidecar_queue_len_after_pull
          - sidecar_logical_after_pull
        """
        if self._stop_evt.is_set():
            return

        with self._lock:
            pending, inflight = self.local_q.state()
            pull_cap = _cfg.BATCH_SIZE + _cfg.PREFETCH
            total_reserved = pending + inflight

            if total_reserved >= pull_cap:
                return

            want = pull_cap - total_reserved
            if want <= 0:
                return

            session = self._session
            tmp_session = False
            if session is None:
                session = _make_pooled_session(
                    pool_connections=_cfg.ROUTER_POOL_CONNECTIONS,
                    pool_maxsize=_cfg.ROUTER_POOL_MAXSIZE,
                )
                tmp_session = True

            try:
                # ---------------------
                # ROUTER /pull request
                # ---------------------
                resp = session.post(
                    f"{_cfg.ROUTER_URL}/pull",
                    json={"endpoint": self.endpoint_id, "want": want, "model": _cfg.MODEL_NAME},
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

                if _cfg.LOG_LEVEL == "debug" and items:
                    print(f"[sidecar] /pull want={want} got={len(items)} pending={pending} inflight={inflight}")

                if not items:
                    return

                # ---------------------
                # Process returned jobs
                # ---------------------
                now_pull = time.time()

                # Snapshot queue state *before* enqueueing pulled items
                pending_before, inflight_before = self.local_q.state()
                logical_before = pending_before + inflight_before
                queue_len_after = pending_before + len(items)
                logical_after = logical_before + len(items)

                for item in items:
                    rid = str(item["req_id"])
                    prompt = str(item["prompt"])
                    meta: Dict[str, Any] = item.get("meta") or {}

                    # Prom: received (router -> sidecar) for each pulled item
                    inc_received(self.endpoint_id)

                    # Trace injection for pull arrival + queue lengths
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        tr = dict(meta.get("__trace__") or {})
                        tr["t_arrive_sidecar_pull"] = now_pull

                        # Sidecar-local queue lengths at pull time
                        tr["sidecar_queue_len_before_pull"] = pending_before
                        tr["sidecar_inflight_before_pull"] = inflight_before
                        tr["sidecar_logical_before_pull"] = logical_before
                        tr["sidecar_queue_len_after_pull"] = queue_len_after
                        tr["sidecar_logical_after_pull"] = logical_after

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
