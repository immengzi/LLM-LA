#!/usr/bin/env python3
"""
main.py
Entry point for the LMCache-Ascend disaggregated prefill/decode orchestrator.

Run with:
    python3 main.py

What happens:
  1. Start the Docker container (Ascend NPU, shared-memory, host networking)
  2. Prepare the Prometheus metrics directory inside the container
  3. Start Prefiller  (NPUs 0,1 → port 7100)
  4. Start Decoder    (NPUs 2,3 → port 7200)
  5. Start Proxy      (port 9101, bridges prefiller ↔ decoder)
  6. Run comprehensive HTTP health checks on all three services
  7. Run an end-to-end inference test through the proxy
  8. Stream & log all service output until Ctrl+C

Modules:
  container.py   — Docker lifecycle (start, exec, cleanup, port probe)
  services.py    — Service launch, wait-for-ready, health checks, e2e test
  log_monitor.py — Real-time log streaming + LMCache CSV statistics
"""

import sys
import signal

import modules.container as container
import modules.services as services
import modules.log_monitor as log_monitor 
from modules.utils import wait_for_service

CONTAINER_NAME  = "lmcache-ascend-haiting"
CONTAINER_IMAGE = "lmcache-ascend:env-v1"
NPU_DEVICES     = [0, 1, 2, 3]   
PREFILLER_NPUS  = [0, 1]
DECODER_NPUS    = [2, 3]
MODEL_PATH      = "/mnt/nvme1/haiting_jd/models/qwen3-8b"  
TOTAL_MEMORY_GB = 32
PROXY_PORT      = 9102
PREFILL_PORT    = 7101
DECODER_PORT    = 7201

# ─────────────────────────────────────────────────────────────────────────────
# Global process list (filled as services come up)
# ─────────────────────────────────────────────────────────────────────────────
processes = []


def handle_exit(signum=None, frame=None):
    container.cleanup(processes, CONTAINER_NAME)
    sys.exit(0)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    log_monitor.init_csv()

    # ── 1. Container ──────────────────────────────────────────────────────────
    container.start_container(
    container_name=CONTAINER_NAME,
    image=CONTAINER_IMAGE,
    npu_devices=NPU_DEVICES,
    total_memory_gb=TOTAL_MEMORY_GB,
    model_path=MODEL_PATH,)
    container.prepare_prometheus_directory(CONTAINER_NAME)

    # ── 2. Prefiller ──────────────────────────────────────────────────────────
    prefiller_proc = services.start_prefiller(CONTAINER_NAME, PREFILL_PORT, PREFILLER_NPUS)
    processes.append(prefiller_proc)
    if not wait_for_service(PREFILL_PORT, "Prefiller", prefiller_proc, CONTAINER_NAME, timeout=180):
        print(" Prefiller failed to start")
        handle_exit()

    # ── 3. Decoder ────────────────────────────────────────────────────────────
    decoder_proc = services.start_decoder(CONTAINER_NAME, DECODER_PORT, DECODER_NPUS)
    processes.append(decoder_proc)
    if not wait_for_service(DECODER_PORT, "Decoder", decoder_proc, CONTAINER_NAME, timeout=180):
        print(" Decoder failed to start")
        handle_exit()

    # ── 4. Proxy ──────────────────────────────────────────────────────────────
    proxy_proc = services.start_proxy(CONTAINER_NAME,PROXY_PORT, PREFILL_PORT, DECODER_PORT)
    processes.append(proxy_proc)
    if not wait_for_service(PROXY_PORT, "Proxy", proxy_proc, CONTAINER_NAME, timeout=60):
        print(" Proxy failed to start")
        handle_exit()

    print("\n All services started!")
    print(f"   Prefiller → http://localhost:{PREFILL_PORT}")
    print(f"   Decoder   → http://localhost:{DECODER_PORT}")
    print(f"   Proxy    → http://localhost:{PROXY_PORT}")

    # ── 5. Health checks ──────────────────────────────────────────────────────
    if not services.comprehensive_health_check(processes, PROXY_PORT, PREFILL_PORT, DECODER_PORT):
        print(" Health checks failed — aborting")
        handle_exit()

    # ── 6. End-to-end test ────────────────────────────────────────────────────
    test_ok = services.run_end_to_end_test(processes, PROXY_PORT)

    if test_ok:
        print(f"\n System ready. Send requests to http://localhost:{PROXY_PORT}/v1/completions")
    else:
        print("\n  End-to-end test failed — check logs below for details")

    # ── 7. Stream logs until Ctrl+C ───────────────────────────────────────────
    log_monitor.stream_logs(processes)

    handle_exit()


if __name__ == "__main__":
    main()