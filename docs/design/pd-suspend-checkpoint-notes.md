# Dynamic P/D suspend/checkpoint: feasibility notes

## Background

warm-standby uses vLLM sleep/wakeup to flip Prefill/Decode roles on a shared
card. The reverse flip (sleep a KV producer that has served requests) hits a
vLLM 500 from `reset_prefix_cache`: finished requests waiting for remote KV
transfer keep their blocks alive, and `finished_sending` is only consumed while
the engine keeps stepping.

## Investigation: Device snapshot availability on Ascend

- vLLM upstream RFC #34303 (`SleepModeBackend`, `vllm/device_allocator/sleep_mode_backend.py`)
  makes sleep/wake a pluggable backend, but only ships the default `cumem`
  backend (offload weights, discard KV). CRIU / CUDA-checkpoint / durable
  snapshot are described as third-party backends and are not implemented.
- Dynamo checkpoint is CUDA-only (`cuCheckpointProcessCheckpoint` /
  `cuCheckpointProcessRestore` via `cuda-checkpoint-helper`), so it does not
  apply to Ascend NPU.
- Motor suspend/resume is implemented by the MindIE native engine. Motor's
  vllm/sglang adapters do not expose `suspend`/`resume`, so it cannot be reused
  by vllm-ascend directly.
- vllm-ascend only exposes CaMem allocator offload (`vllm_ascend/device_allocator/camem.py`).
  Weights are tagged `weights`; KV cache is tagged `kv_cache`
  (`worker.initialize_from_config`). `CaMemAllocator.sleep(offload_tags=...)`
  can offload any tag to CPU, so KV could in principle be frozen too.

## Conclusion

A true device-state snapshot (the CUDA-checkpoint equivalent) is not available
on Ascend through vllm-ascend today. The pragmatic, architecture-correct fix is
to keep sleep's discard-KV semantics (KV is worthless after drain, see the
handoff discussion) and fix the release timing instead:

1. Upstream vLLM #43433 (commit `82536acc54`) keeps the scheduler alive for
   delayed KV connector frees, so an idle engine keeps stepping to consume
   `finished_sending` (present from vLLM v0.22.0; absent in v0.18.x).
2. The rebalancer retries `/sleep` with backoff so a producer drains its
   delayed connector frees between attempts.

## Implemented

- `files/pd_rebalancer.py` `_sleep_engine`: retry `/sleep` on HTTP 500 or
  `is_sleeping` timeout with backoff. Configurable via
  `PD_REBALANCER_SLEEP_RETRIES` (default 5) and
  `PD_REBALANCER_SLEEP_BACKOFF_SECONDS` (default 2).
- Requires a vLLM image that contains #43433 (vLLM >= v0.22.0). The deployed
  `vllm-ascend:v0.18.0` image does not contain it, so the image must be bumped
  for the retry to actually drain between attempts.

## Not pursued now

- CaMem-based KV offload (freeze weights + KV instead of discarding) is a
  possible "suspend" approximation, but it needs upstream vLLM to stop
  discarding KV on level-1 sleep (`clear_prefix_cache = level >= 1` in
  `EngineCore.sleep`), and KV is worthless after drain, so the benefit does not
  justify the upstream change yet.
