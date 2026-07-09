# Prefix Hashing and Prefix-Aware Routing

> For a high-level overview of the routing strategies, see
> [router-strategies.md](router-strategies.md).

KV-aware routing needs a **stable, vLLM-compatible identifier for every block of
a prompt's prefix**. With those identifiers the router can ask "which pod already
has the KV blocks for this request's prefix cached?" and bias the request toward
that pod, turning a cold prefill into a cache hit.

This document describes how those block identifiers are produced and how they
drive routing.

The identifiers are produced from a single source of truth, `router/prefix_hash.py`,
selected by the `KV_HASH_SOURCE` knob (`inline` by default):

- **`inline` (default)** — both routers hash with `router/prefix_hash.py`:
  - the **Python router** computes hashes in-process (no network hop);
  - the **Go gateway** runs the exact same `prefix_hash.py` as a tiny hasher
    inside its own container (`hasher/hasher_app.py`, listening on
    `127.0.0.1:9095`), so Go and Python produce byte-identical hashes.
- **`external` (legacy)** — both routers instead call the standalone
  `vllm-cpu-hash` pod over HTTP. This is the former path, preserved for
  compatibility; see [Legacy: the external hasher](#legacy-the-external-hasher).

Whichever source is used feeds the identical KV-aware scoring path
(`register_request_blocks` -> `_REQ_BLOCKS`, scored by `prefix_len` against
`_BLOCK_OWNERS`). See [kv-cache-flow.md](kv-cache-flow.md) for how block
*ownership* is learned from vLLM and how scoring/tiering then works.

---

## The block-hash algorithm

The identifiers mirror vLLM's chained block hashing, reproduced without importing
vLLM. For a token sequence split into blocks of size `B`:

```
NONE_HASH      = sha256(cbor2.dumps(PYTHONHASHSEED))
block_hash[i]  = sha256(cbor2.dumps((
    parent_block_hash_or_NONE_HASH,   # digest of block i-1 (NONE_HASH for i=0)
    tuple(token_ids[i*B : (i+1)*B]),
    extra_keys,                        # None here
)))
```

Properties that matter for routing:

- **Chained.** Each block hash folds in the previous block's digest, so a block
  identity depends on the entire prefix before it. This is what makes a *prefix*
  match meaningful: identical block `i` implies identical blocks `0..i`.
- **Full blocks only.** A trailing partial block is not emitted.
- **64-bit wire form.** The external value is the low 64 bits of the 32-byte
  digest, matching the integer block hashes vLLM emits in its KV events.

The hashing is not cryptographic; the goal is **consistency** with vLLM, not
security.

---

## Inline hashing (default, both routers)

`router/prefix_hash.py` is the single hashing implementation. In `inline` mode:

- **Python router**: loaded at startup, the tokenizer is initialized once from
  `KV_TOKENIZER_PATH` (`/model`). For each request, `_maybe_register_kv_blocks`
  in `router/api.py` calls `compute_request_block_hashes_int(...)` in-process.
- **Go gateway**: the same `prefix_hash.py` is shipped inside the gateway image
  and served by `hasher/hasher_app.py` on `127.0.0.1:9095`; the gateway's
  `hash_client.go` posts `messages`+`tools` to it. The image entrypoint starts
  this hasher only in `inline` mode and waits for its `/health`.

Both register the result before the request is scheduled. Input handling matches
what the model path actually serves so the hashes line up:

- A plain `prompt` is tokenized directly.
- OpenAI `messages` are rendered with the model's tokenizer chat template
  (`add_generation_prompt=True`) when one is present; otherwise they fall back to
  a simple deterministic `role: content` text join.
- Tool/assistant message shapes are normalized to what chat templates expect:
  tool message string content is expanded into a text-block list, and assistant
  tool-call `arguments` JSON strings are parsed into objects. Model-specific
  reasoning-field rewrites are intentionally **not** done here.

### Tool canonicalization

Tool schemas are JSON objects whose key order is semantically irrelevant, but key
order becomes tokenization-visible once a chat template renders the tools into
prompt text. Canonicalizing OpenAI `tools` before hashing (gated by
`KV_CANONICALISE_TOOLS`, **default off**) rebuilds them as:

- each outer tool object is rebuilt in a fixed order: `type`, then `function`
- the function object is rebuilt as `name`, optional `description`, then
  `parameters`
- only `function.parameters` is recursively key-sorted

This is router-side only: it does not modify the gateway and does not change the
body sent to vLLM. Its single purpose is to make the router's pre-routing hashes
match the tool serialization the OpenAI-compatible path produces. If that
serialization changes, this hook must be revalidated.

It ships **off by default** because the current BooM gateway serializes request
tools with serde_json `preserve_order` (insertion order); sorting keys here would
drift from what vLLM tokenizes and cause block-level KV hash mismatches. Set
`KV_CANONICALISE_TOOLS=1` only against a legacy BooM build that alphabetically
sorts tools (serde_json `BTreeMap` behaviour).

---

## Legacy: the external hasher

> Opt-in only, via `KV_HASH_SOURCE=external`. The default (`inline`) needs no
> external pod. Use this only if you specifically want to offload hashing.

`src/core/services/prefix_hash/prefix_hash_service.py` packages a CPU-only HTTP hasher
(the `vllm-cpu-hash` image, deployed by `templates/20-cpu-hash.yaml`). When
`KV_HASH_SOURCE=external`, both routers call its `POST /compute_hashes` over HTTP
and treat any error as "no hashes" (best-effort, fail-open).

Auto-deploy: the `vllm-cpu-hash` Deployment+Service is rendered **only** when a
router is present and `router.hashSource=external`; in `inline` mode it is not
deployed at all.

Caveat: this legacy service uses a different (real-vLLM) hashing implementation
than `prefix_hash.py`, and the Go gateway sends it only a flat `prompt`. So
`external` can produce **different** hashes than `inline`; do not mix the two
modes within one cluster if cross-pod KV ownership must agree.

---

## Prefix-aware pull scheduling

Block identifiers only help if the router can surface cache-warm requests to the
pulling pod. In pull mode the router samples a candidate pool from the central
queue and ranks it with the existing KV-aware (and length-aware / SLO-aware)
logic:

- the head scan size is `want * POOL_FACTOR`
- when `POOL_BIDIRECTIONAL` is enabled, the router *also* samples up to `want`
  items from the **tail** of the queue, so agentic follow-up turns stay visible
  even when older cold requests dominate the head
- unselected head items are returned to the front; unselected tail samples are
  returned to the back; hard key-affinity hold-backs are returned to the front

---

## Data-parallel sidecar fan-in

In data-parallel (DP) deployments a single **leader** sidecar aggregates KV state
for all local DP engines so the router sees one ownership view per pod. The
leader subscribes to its local vLLM KV stream, resolves each worker rank's ZMQ
endpoint via the LeaderWorkerSet DNS convention (retrying DNS for ranks that are
not ready at startup), and writes all observed ownership into Redis under the
leader pod identity. The router routes to the leader endpoint, never directly to
an internal DP engine. Configured via `DP_SIZE` / `DP_SIZE_LOCAL`.

---

## Configuration

Router-side (env, set by `templates/31-router.yaml`):

| Setting | Default | Purpose |
| --- | --- | --- |
| `KV_AWARE` | `true` | Master switch for KV-aware scoring (and inline hashing) |
| `KV_HASH_SOURCE` | `inline` | `inline` (in-process/in-container) or `external` (legacy `vllm-cpu-hash` pod) |
| `HASH_SERVICE_URL` | `127.0.0.1:9095` (go inline) / `vllm-cpu-hash:9095` (external) | External hasher endpoint; unused for Python inline |
| `KV_TOKENIZER_PATH` | `/model` | Tokenizer/model directory; must match the vLLM pods |
| `KV_BLOCK_SIZE` | `128` | Block size; must match the vLLM pods' `--block-size` |
| `PYTHONHASHSEED` | `0` | Seed for `NONE_HASH`; must match the vLLM pods |
| `KV_CANONICALISE_TOOLS` | `0` (off) | Canonicalize OpenAI `tools` before hashing (set `1` only vs a legacy sorting BooM) |
| `POOL_FACTOR` | `4` | Head-scan multiple of `want` |
| `POOL_BIDIRECTIONAL` | `false` | Also sample from the queue tail |
| `DP_SIZE`, `DP_SIZE_LOCAL` | `1`, `1` | DP sidecar fan-in topology |

Corresponding Helm values: `router.kvAware`, `router.hashSource`,
`router.kvBlockSize`, `router.kvCanonicaliseTools`, `router.poolBidirectional`
(client config: `router_hash_source`). Both router images bundle `cbor2`,
`transformers`, and `tokenizers` for inline tokenization (the Go gateway image
ships the same `prefix_hash.py`).

---

## Correctness invariant

KV-hit routing is only as good as the agreement between the request hashes
(`_REQ_BLOCKS`) and the ownership hashes vLLM emits (`_BLOCK_OWNERS`). For them to
match, the producer (inline hasher, or the legacy external service) must match
each vLLM pod on **all** of:

- the tokenizer files,
- the block size (`KV_BLOCK_SIZE` vs vLLM `--block-size`),
- `PYTHONHASHSEED`,
- the logical request shape after normalization and tool canonicalization.

If any of these diverge the request still executes correctly, but routing either
sends it to a pod that does not actually have the prefix (false positive ->
recompute) or misses a warm pod (false negative -> cold prefill). There is no
runtime validation of hash agreement, so this is the single most important thing
to keep aligned when changing models, block size, or the gateway serialization.

---

## Boundaries

- No gateway or client code changes; compatibility hooks stay router-side and
  model-neutral.
- No model-specific behavior in the hash implementation.
- No direct scheduling to a specific DP engine.
- No dependency on vLLM Python internals inside the router.
