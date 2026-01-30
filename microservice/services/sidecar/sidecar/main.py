# sidecar/main.py
# -*- coding: utf-8 -*-
import signal
import threading
import time

import uvicorn

from .config import get_config
from .local_queue import LocalQueue
from .router_client import RouterPullWorker
from .vllm_client import VLLMWorker
from .result_poster import ResultPoster
from .zmq_subscriber import KVSubscriber
from .api import app, bind_local_queue

_cfg = get_config()


def _run_server():
    uvicorn.run(
        "sidecar.api:app",
        host="0.0.0.0",
        port=_cfg.SIDECAR_PORT,
        log_level="info",
    )


def main():
    # For KV-aware routing: endpoint identity = pod name
    endpoint_id = _cfg.CONTAINER_NAME

    local_q = LocalQueue(endpoint_id=endpoint_id)
    bind_local_queue(local_q)

    mode = (_cfg.SIDECAR_MODE or "pull").lower()

    pull_worker = None
    if mode == "pull":
        pull_worker = RouterPullWorker(local_q, endpoint_id)
        pull_worker.start()  # initializes session; this is not a polling thread

    # ------------------------------------------------------------
    # Result poster (async router result delivery)
    #   - sync: POST /result (legacy)
    #   - submit_ack: POST RESULT_SUBMIT_PATH (default /result_submit), router ACKs 202 immediately
    # ------------------------------------------------------------
    result_poster = ResultPoster(maxsize=100000)
    result_poster.start()

    # ------------------------------------------------------------
    # vLLM workers (unchanged concurrency / logic)
    # ------------------------------------------------------------
    vllm_workers = []
    for _ in range(_cfg.BATCH_SIZE):
        w = VLLMWorker(
            local_q,
            pull_worker=pull_worker,
            result_poster=result_poster,
        )
        w.start()
        vllm_workers.append(w)

    kv_sub = KVSubscriber()

    stop_evt = threading.Event()

    def handle_sig(*_args):
        stop_evt.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    # Start KV subscriber
    kv_sub.start()

    # Start HTTP server in a background thread
    server_thread = threading.Thread(target=_run_server, daemon=True)
    server_thread.start()

    # Explicitly print which result transport we're using (helps debug env wiring)
    result_transport = str(getattr(_cfg, "RESULT_TRANSPORT_MODE", "sync")).lower()
    result_path = str(getattr(_cfg, "RESULT_SUBMIT_PATH", "/result_submit"))

    print(
        f"[sidecar] running in {mode.upper()} mode "
        f"(BATCH_SIZE={_cfg.BATCH_SIZE}, port={_cfg.SIDECAR_PORT}, endpoint_id={endpoint_id})"
    )
    print(
        f"[sidecar] result transport={result_transport} "
        f"(sync -> POST /result, submit_ack -> POST {result_path})"
    )

    try:
        while not stop_evt.is_set():
            time.sleep(0.5)
    finally:
        print("[sidecar] shutting down")

        if pull_worker:
            pull_worker.stop()

        for w in vllm_workers:
            w.stop()

        result_poster.stop()

        kv_sub.stop()


if __name__ == "__main__":
    main()
