# sidecar/result_poster.py
# -*- coding: utf-8 -*-
import queue
import threading
import time
import random
from typing import Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter

from .config import get_config

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
        max_retries=0,  # IMPORTANT: no urllib3 retries; we control policy ourselves
        pool_block=False,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


class ResultPoster:
    """
    Async poster for sidecar -> router result ingestion.

    Backward compatible:
      - RESULT_TRANSPORT_MODE="sync"       -> POST {ROUTER_URL}/result      (old behavior)
      - RESULT_TRANSPORT_MODE="submit_ack" -> POST {ROUTER_URL}{RESULT_SUBMIT_PATH}
                                            (new behavior: router ACKs immediately)

    Key property: vLLM workers never block on router backpressure.

    Policy:
      - By default: NO RETRY (drop on failure) because you explicitly asked for that.
      - Optional: bounded retry if RESULT_POST_RETRY=true
    """

    def __init__(self, maxsize: int = 100000):
        self._q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._session: Optional[requests.Session] = None

        # Resolve URL once (config is static per process)
        self._result_url = self._resolve_result_url()

    # ----------------------------
    # URL selection
    # ----------------------------

    def _resolve_result_url(self) -> str:
        mode = str(getattr(_cfg, "RESULT_TRANSPORT_MODE", "sync") or "sync").lower().strip()

        if mode == "submit_ack":
            submit_path = str(getattr(_cfg, "RESULT_SUBMIT_PATH", "/result_submit") or "/result_submit")
            if not submit_path.startswith("/"):
                submit_path = "/" + submit_path
            return f"{_cfg.ROUTER_URL}{submit_path}"

        # Default: old behavior
        return f"{_cfg.ROUTER_URL}/result"

    # ----------------------------
    # Lifecycle
    # ----------------------------

    def start(self):
        if self._thread is not None:
            return
        self._stop.clear()
        self._session = _make_pooled_session(
            pool_connections=_cfg.ROUTER_POOL_CONNECTIONS,
            pool_maxsize=_cfg.ROUTER_POOL_MAXSIZE,
        )
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

        mode = str(getattr(_cfg, "RESULT_TRANSPORT_MODE", "sync") or "sync").lower().strip()
        print(
            "[sidecar] ResultPoster started "
            f"(mode={mode}, url={self._result_url}, retry={_cfg.RESULT_POST_RETRY}, "
            f"pool_maxsize={_cfg.ROUTER_POOL_MAXSIZE})"
        )

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
        print("[sidecar] ResultPoster stopped")

    # ----------------------------
    # Public API
    # ----------------------------

    def submit(self, payload: Dict[str, Any]) -> None:
        # No blocking: never stall vLLM workers.
        try:
            self._q.put_nowait(payload)
        except queue.Full:
            rid = payload.get("req_id", "?")
            print(f"[sidecar] ResultPoster queue FULL; dropping result for req_id={rid}")

    # ----------------------------
    # Internals
    # ----------------------------

    def _try_post_once(self, session: requests.Session, url: str, payload: Dict[str, Any]) -> bool:
        try:
            r = session.post(url, json=payload, timeout=_cfg.ROUTER_RESULT_TIMEOUT_S)
            return bool(r.ok)
        except Exception:
            return False

    def _loop(self):
        session = self._session or _make_pooled_session(
            pool_connections=_cfg.ROUTER_POOL_CONNECTIONS,
            pool_maxsize=_cfg.ROUTER_POOL_MAXSIZE,
        )
        url = self._result_url

        while not self._stop.is_set():
            try:
                payload = self._q.get(timeout=0.5)
            except queue.Empty:
                continue

            rid = payload.get("req_id", "?")

            if not _cfg.RESULT_POST_RETRY:
                ok = self._try_post_once(session, url, payload)
                if not ok:
                    print(f"[sidecar] result post FAILED (no-retry) req_id={rid} url={url}")
                try:
                    self._q.task_done()
                except Exception:
                    pass
                continue

            # Optional bounded retry mode
            attempt = 0
            max_tries = max(1, int(_cfg.RESULT_POST_MAX_RETRIES) + 1)  # include first attempt
            ok = False

            while not self._stop.is_set() and attempt < max_tries:
                attempt += 1
                ok = self._try_post_once(session, url, payload)
                if ok:
                    break

                # backoff with jitter
                base = float(_cfg.RESULT_POST_BACKOFF_BASE_S)
                cap = float(_cfg.RESULT_POST_BACKOFF_CAP_S)
                sleep_s = min(cap, base * (2 ** min(attempt, 6)))
                sleep_s *= (0.8 + 0.4 * random.random())
                time.sleep(sleep_s)

            if attempt >= max_tries and not ok:
                print(
                    f"[sidecar] result post FAILED after retries req_id={rid} tries={attempt} url={url}"
                )

            try:
                self._q.task_done()
            except Exception:
                pass
