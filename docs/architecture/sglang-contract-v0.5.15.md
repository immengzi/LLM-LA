# SGLang v0.5.15 Integration Contract

This adapter supports only the annotated upstream tag `v0.5.15`, which resolves
to commit `f63458b5beaceabbd9d749b9fc956370e1b649e6`. The HTTP API is
OpenAI-compatible, but the hashing, publisher descriptor, msgpack event shapes,
and replay protocol are versioned internal interfaces. A different image tag or
source revision must not be assumed compatible.

The production default is the official
`lmsysorg/sglang:v0.5.15-cu129` image. The unqualified historical image
`lmsysorg/sglang:v0.5.15` now resolves to a CUDA 13 runtime and fails accelerator
discovery on NVIDIA 570-series drivers. The complete CUDA 12.9 image keeps the
same SGLang contract while relying on CUDA minor-version compatibility and
includes the dependencies required by the OpenAI serving stack. Operators may
override the image for their installed driver.

The implemented profile is `engine.type=sglang`, either
`serviceImpl=python` or `serviceImpl=go`, inline hashing, one standard
Kubernetes `Deployment` per model, NVIDIA GPUs, and SGLang's local GPU radix
cache. Both service implementations consume the same environment and wire
contracts. See
[`docs/deployment/sglang.md`](../deployment/sglang.md) for deployment steps.

## Inference and discovery API

The engine container listens on port `8200` and is launched with:

- `python -m sglang.launch_server`, model and tokenizer at `/model`;
- OpenAI-compatible `POST /v1/chat/completions`, including `stream=false` JSON
  responses and `stream=true` SSE responses;
- `GET /health` for lightweight startup, liveness, and default readiness;
- optional generation-backed `GET /health_generate` only when explicitly
  selected for readiness (the chart never uses it for liveness);
- `GET /metrics`, enabled by `--enable-metrics`; and
- `GET /server_info`.

`/server_info.version` must equal `0.5.15`. Its `kv_events` value must be an
object with `publisher="zmq"`, string `endpoint_host` and `topic`, positive
`block_size` and `dp_size`, and `endpoint_port_base` in `1..65535`. The sidecar
also requires `block_size`, topic, base port, and DP size to match its configured
values. Wildcard endpoint hosts (`*`, `0.0.0.0`, or `::`) are bind addresses;
consumers dial the engine pod host. Rank `r` uses base port plus `r`.

The chart currently renders only `dp_size=1` for SGLang because SGLang
data-parallel/LWS is rejected. The discovery code is rank-aware, but that is not
a statement of deployed DP support.

Tool calling is opt-in. Setting a compatible `toolCallParser` adds
`--tool-call-parser`; SGLang v0.5.15 does not accept vLLM's
`--enable-auto-tool-choice` flag. A reasoning parser is also optional.
Parser/model compatibility is not inferred or guaranteed.

## Exact prefix hash contract

The hash implementation is
`router/hash_backends/sglang_v0_5_15.py`, used directly by the Python router and
staged with `prefix_hash.py` into the Go router image's private Python hasher.
It is selected only when all of these agree:
`INFERENCE_ENGINE=sglang`, `KV_HASH_BACKEND=sglang`,
`KV_HASH_SOURCE=inline`, a positive page size, and
`SGLANG_CONTRACT_VERSION=0.5.15`. A mismatch produces no SGLang block hashes, so
the request falls back to non-KV routing.

For ordinary token IDs, pinned upstream converts every ID to `uint32`, on
little-endian Linux, and hashes the native four-byte words. For page `i`:

```text
digest[0] = SHA256(uint32_le(tokens[0:page_size]))
digest[i] = SHA256(digest[i-1] || uint32_le(tokens_for_page_i))
event_hash[i] = signed_int64(big_endian(digest[i][0:8]))
```

The event hash conversion is equivalent to parsing the first 16 hex characters
as unsigned 64-bit and subtracting `2^64` when the value is at least `2^63`.
The router hashes only complete request pages because only complete pages are
eligible for prefix-owner scoring; it intentionally omits a trailing partial
page. Upstream's native helper itself can hash a final partial page when asked,
so this omission is a routing policy, not a different digest algorithm.

Upstream also has tuple-width and speculative bigram inputs. The adapter does
not reproduce those encodings and marks such requests ineligible. Token IDs
outside `uint32` are errors.

## Official KV wire and replay protocol

The Helm profile passes:

```json
{
  "publisher": "zmq",
  "endpoint": "tcp://*:5557",
  "replay_endpoint": "tcp://*:5558",
  "topic": "kv@<pod-name>@<served-model-name>"
}
```

Each publisher starts its own sequence at zero. A live PUB message is exactly
three frames:

1. UTF-8 topic bytes;
2. an unsigned 64-bit, eight-byte big-endian sequence; and
3. msgpack for an array-like `KVEventBatch`.

The batch is `[timestamp, events, optional_attn_dp_rank]`. Events are tagged,
array-like msgspec structures:

- `["BlockStored", block_hashes, parent_block_hash, token_ids, block_size,
  lora_id, optional_medium]`;
- `["BlockRemoved", block_hashes, optional_medium]`;
- `["AllBlocksCleared"]`.

The sidecar accepts omitted optional trailing fields for compatibility. It
mirrors only `medium` absent/`null` or `"GPU"`; `CPU_PINNED`, `DISK`, and
`EXTERNAL` events are ignored. A stored event whose `block_size` differs from
the discovered/configured page size invalidates ownership.

The replay endpoint is a ZMQ `ROUTER`. A DEALER client sends an empty delimiter
and the desired starting sequence as eight-byte unsigned big-endian bytes. The
publisher returns ordered sequence/payload replies (with the ROUTER identity and
REQ delimiter on the ROUTER side), then sequence `0xffffffffffffffff` with an
empty payload as the end marker. The buffer holds only the latest 10,000 batches
by default; replay is gap repair, not a complete cache snapshot.

Duplicates are ignored. A forward gap attempts replay for that publisher only.
A lower sequence is treated as publisher restart: ownership is cleared before
the new batch is accepted. Malformed frames, topics, msgpack, rank values, page
sizes, or unrecoverable gaps also clear ownership before processing the current
valid batch. Losing events beyond the replay window means the ownership index
cannot be reconstructed completely until later remove/clear/store activity;
operators should restart/flush deliberately rather than claim full cache state.

## Redis ownership and fail-closed behavior

For served model `M` and pod `P`, the sidecar writes:

- `M:kvblock:H`, a hash of owner pod to last-store Unix timestamp;
- `M:podblocks:P`, the set of hashes attributed to that pod; and
- `M:kvblocks`, a compatibility hash from event hash to its `kvblock` key.

`BlockStored` adds these mappings. `BlockRemoved` removes `P` from the block hash
and the hash from `P`'s set. `AllBlocksCleared`, subscriber startup, discovery
failure, corruption, restart, or unrecoverable gap removes every ownership entry
listed in `M:podblocks:P` and deletes that set. Empty `kvblock` hashes and the
compatibility `M:kvblocks` entries are not garbage-collected by this path; router
lookups use the owner hash contents, not key existence.

The default router owner source performs targeted Redis `HGETALL` calls for the
request's hashes. Redis errors return no owners, so KV scoring contributes no
placement preference and affinity/load-aware scheduling continues. If the
sidecar cannot perform the conservative Redis invalidation after a stream error,
it stops consuming that stream rather than writing newer ownership on top of
state it could not clear. The legacy `ownerSource=watcher` background scan has
weaker freshness guarantees and is not the recommended SGLang profile.

Redis is coordination state, not an authorization boundary. Keep it
cluster-internal, apply NetworkPolicies in production, and do not expose its
unauthenticated default NodePort to untrusted networks.

## Health, metrics, and Helm profile

The chart uses SGLang `/health` for startup and liveness. Readiness defaults to
the same path or can explicitly use `/health_generate`. The sidecar exposes
`/health` (liveness) and `/ready` (readiness), reporting generic `engine_*`
fields while retaining `vllm_healthy` as a compatibility field.

SGLang pods and services are annotated for `/metrics` on port `8200`. The
integration recognizes both colon and underscore spellings of these required
families:

- `sglang:num_running_reqs`;
- `sglang:num_queue_reqs`;
- `sglang:token_usage`; and
- `sglang:gen_throughput`.

KEDA's engine signal uses running requests, queued requests, and token usage;
metric availability alone does not validate an autoscaling policy.

The chart pins `lmsysorg/sglang:v0.5.15-cu129`, sets
`runtimeClassName` to `nvidia` by default, requests `nvidia.com/gpu` equal to
tensor-parallel size, mounts the selected model subpath read-only at `/model`,
sets offline Hugging Face/Transformers flags, exposes HTTP `8200`, publisher
`5557`, and replay `5558`, and keeps vLLM as the default profile.

## Unsupported boundaries

Two different fail-closed behaviors apply. Do not conflate them:

### Deploy-time rejects (install fails)

SGLang cannot be combined with:

- the external CPU hash service (`router.hashSource=external`)
- data-parallel LeaderWorkerSet
- Mooncake
- LMCache

The chart also rejects `serviceImpl` values other than `python` or `go`, and
rejects `trustRemoteCode` for the entire v0.5.15 profile (router tokenizer
initialization cannot mirror engine-side remote tokenizer code). When router
prefix hashing or measurement is enabled, SGLang is limited to one model, the
global page size, and the router-mounted model tokenizer. Mixed engines and
model/tokenizer overrides are rejected; multi-model SGLang is allowed only while
router hashing and prefix measurement are both disabled.

### Request-time skips (traffic still accepted)

If a request uses features whose engine cache identity the router cannot safely
reproduce, the router **does not apply KV-aware scoring** for that request. The
request is still served; the router simply avoids guessing cache identity.

Skipped features include:

- cache salts or extra cache keys
- LoRA / adapters
- non-text multimodal message parts
- speculative / draft / bigram keys
- request-level chat-template, tokenizer, special-token, alternate prompt /
  token-ID, or custom processor overrides

Tool schemas remain supported because the router includes tools in chat-template
hashing. Non-GPU storage tiers are not mirrored into the affinity map.

The Go parity boundary intentionally does not port Hugging Face tokenization to
Go. Its router image runs the shared Python hash package on loopback and the Go
gateway calls it. The Go sidecar implements SGLang descriptor discovery,
publisher/replay consumption, fail-closed Redis projection, and `/ready`
semantics natively. Liveness `/health` does not require KV readiness.

The supported NVIDIA deployment is Kubernetes plus GPU nodes with model weights
already available through a host path or PVC. Ascend NPUs are supported through
the same `hardware: ascend` switch used by vLLM (set `images.sglang` to an
Ascend-capable build). CPU-only execution, P/D disaggregation, cross-replica KV
transfer, mixed engine versions, direct BooM routing, and arbitrary SGLang extra
arguments are outside the pinned contract. Extra arguments can invalidate this
contract and are the operator's responsibility.

## Security and operations

- `trustRemoteCode` is unsupported and rejected by client and chart validation.
- Pin production images by digest in an overlay; the version tag is the
  compatibility pin but not an immutable supply-chain reference.
- The engine API has no chart-provided authentication. Keep engine, publisher,
  replay, sidecar, and Redis ports private; put authenticated gateways in front.
- `server_info` exposes runtime configuration. Treat it as an internal
  operational endpoint.
- Size accelerator memory conservatively and verify model licensing, device
  plugins, storage throughput, probes, quotas, and log retention.
- Render and unit-test locally with `pytest src/core/tests/helm` (engine ×
  hardware matrix) before deploying.

## Pinned upstream references

- [v0.5.15 release](https://github.com/sgl-project/sglang/releases/tag/v0.5.15)
  and [resolved commit](https://github.com/sgl-project/sglang/tree/f63458b5beaceabbd9d749b9fc956370e1b649e6)
- [HTTP health and `/server_info`](https://github.com/sgl-project/sglang/blob/f63458b5beaceabbd9d749b9fc956370e1b649e6/python/sglang/srt/entrypoints/http_server.py)
- [`kv_events` descriptor](https://github.com/sgl-project/sglang/blob/f63458b5beaceabbd9d749b9fc956370e1b649e6/python/sglang/srt/server_args.py)
- [KV event and replay implementation](https://github.com/sgl-project/sglang/blob/f63458b5beaceabbd9d749b9fc956370e1b649e6/python/sglang/srt/disaggregation/kv_events.py)
- [Hash API and signed event conversion](https://github.com/sgl-project/sglang/blob/f63458b5beaceabbd9d749b9fc956370e1b649e6/python/sglang/srt/mem_cache/utils.py)
- [Native input conversion](https://github.com/sgl-project/sglang/blob/f63458b5beaceabbd9d749b9fc956370e1b649e6/python/sglang/srt/mem_cache/cpp_utils/native_hash.py)
- [Native SHA-256 page implementation](https://github.com/sgl-project/sglang/blob/f63458b5beaceabbd9d749b9fc956370e1b649e6/python/sglang/srt/mem_cache/cpp_utils/hash_binding.cpp)
- [OpenAI-compatible API](https://docs.sglang.ai/basic_usage/openai_api_completions.html)
- [Production metrics](https://docs.sglang.ai/references/production_metrics.html)
