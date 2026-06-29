# Prefix Hash Service

This component provides a lightweight HTTP endpoint for computing **KV-cache
block identifiers** for model prompts. These identifiers are the same type
of hashes that the vLLM runtime uses internally to decide whether a request's
prefix can reuse an existing KV block on a worker.

The service exists so that other parts of the system (router-service,
sidecars, orchestration tools, etc.) can obtain these identifiers without
running a full model. It produces repeatable, model-consistent block hashes
using the same tokenization and hashing logic that vLLM expects.

---

## Purpose

Many LLM serving systems reuse attention states (KV blocks) when multiple
requests share the same prompt prefix. To enable this reuse, components need
a **stable identifier** that represents the prefix at the block level.

The prefix-hash service provides exactly that:

- Converts user prompts or chat messages into model tokens
- Splits those tokens into blocks of fixed size
- Computes a deterministic hash for each block
- Returns block-level identifiers that match the vLLM server's behavior

Routers and sidecars can then use these identifiers to:

- Detect when a request matches an existing cached prefix
- Pin requests to workers holding compatible KV blocks
- Decide when new blocks should be created
- Coordinate block reuse across distributed components

---

## How Inputs Are Interpreted

The service accepts either:

- `prompt`: a plain text string, or
- `messages`: a list of chat-style messages (OpenAI format)

It follows the same formatting behavior that vLLM uses:

- If the tokenizer includes a chat template, that template is applied.
- Otherwise, messages are combined into a simple deterministic text form.
- The exact same tokenizer files as the model server must be available.

This ensures that the token sequence, and therefore the block hashes, matches
what the model server would generate.

---

## How Block Hashes Are Formed

Once the token IDs are produced, they are processed into fixed-size blocks:

- Block size is supplied at startup (e.g., 128 IDs per block).
- Each block is encoded internally and hashed using a consistent digest method.
- The hash is turned into an integer suitable for routing and indexing.

The hashing approach is stable across processes and machines so long as:

- the tokenizer,
- the block size, and
- the hashing logic

are kept consistent with the model servers.

The intent is not cryptographic security, but **consistency and collision
resistance** across the cluster.

---

## API Overview

Two main endpoints are exposed:

### `/compute_hashes`

Accepts a JSON body containing either `prompt` or `messages`.
Returns:

- `block_hashes`: identifiers for each KV block
- `token_ids`: the tokenized sequence
- metadata (number of tokens, number of blocks, model path, block size)

This endpoint is what routers and sidecars call during KV-aware decision-making.

### `/health`

Simple readiness check.

### `/debug_config`

Reports the model path and block configuration the service is using.

---

## Configuration and Deployment

The service starts with command-line options specifying:

- model path
- block size
- TCP host/port
- EOS token ID
- default max_tokens (for internal Request construction)

It loads the tokenizer once, initializes the block hasher, and stays resident.
No GPU is required.

The Dockerfile builds a small CPU-only service image intended to run alongside
model workers or router components. Typical usage:

- One instance per model version
- Model files mounted at `/model`
- Called by router-service or sidecar before deciding how to route a request

---

## How It Fits Into KV-Aware Routing

Other components use this service to reason about KV reuse:

- The router calls `/compute_hashes` to check whether a request's blocks match
  those cached on a worker, and if so, route the request there. (Only the
  router calls this service; sidecars report KV state over ZMQ -> Redis and do
  not call prefix-hash.)
- Future caching strategies (prefetching, warming, migration) can use these
  identifiers to coordinate behavior.

Consistency between this service and the model servers ensures that all
components agree on which requests share the same prefix, enabling predictable
and efficient KV reuse.

---

# Prefix-Aware Routing Design

This section describes an evolution of the prefix-hash idea: instead of the
router calling an external HTTP hash service at request time, the router
computes vLLM-compatible KV block hashes **inline** before enqueueing a request,
and routes toward pods that already hold matching prefix KV blocks based on
sidecar-reported ownership in Redis.

## Goals

- Compute prefix block identifiers without a network round-trip to an external
  hash service on the request path.
- Keep the identifiers bit-for-bit compatible with the block hashes vLLM emits
  over KV events, so the router and the model servers agree on prefix identity.
- Bias routing toward workers with cache hits while staying model-neutral and
  confined to the router (no gateway or client changes).

## Inline Hash Algorithm

The router mirrors vLLM's chained block-hash semantics without importing vLLM.
For a token sequence split into blocks of size `B`:

```
NONE_HASH      = sha256(cbor2.dumps(PYTHONHASHSEED))
block_hash[i]  = sha256(cbor2.dumps((
    parent_block_hash_or_NONE_HASH,
    tuple(token_ids[i*B : (i+1)*B]),
    extra_keys,
)))
```

Key properties:

- Each block is hashed together with the digest of the previous block, forming a
  chain so that a block hash depends on the entire prefix preceding it.
- Only **full** blocks are emitted; a trailing partial block is ignored.
- The external wire value is the low 64 bits of the 32-byte digest, matching the
  integer block-hash form observed in vLLM KV events.

The router must use the same tokenizer directory and block size as the vLLM
runtime, and `PYTHONHASHSEED` must match so that `NONE_HASH` is identical.

## Input Normalization

The router hashes the same logical chat input it forwards for inference:

- plain prompts are tokenized directly
- OpenAI `messages` are rendered with the model tokenizer chat template when
  available, otherwise combined into a simple deterministic text form
- tool message string content is expanded into a text-block list, and assistant
  tool-call argument JSON strings are parsed into objects, matching how the chat
  template expects these fields; model-specific field rewrites are intentionally
  not applied here

### Tool Canonicalization

Tool schemas are JSON-like objects whose key order is semantically irrelevant,
but it can become tokenization-visible once a tokenizer chat template renders
tools into prompt text. Routing hashes must therefore be computed from the same
logical request shape that the model path actually serializes.

To keep hashes stable, OpenAI `tools` are optionally canonicalized before
hashing:

- each outer tool object is rebuilt in a stable order: `type`, then `function`
- the function object is rebuilt as `name`, optional `description`, then
  `parameters`
- only `function.parameters` is recursively key-sorted, because JSON Schema
  object field ordering should not change semantics but can change the rendered
  token prefix

This is gated by `KV_CANONICALISE_TOOLS` (default on). It is intentionally
router-side: it does not modify the gateway and does not change the request body
sent to vLLM. Its only purpose is to make the router's pre-routing block hashes
line up with the stable tool serialization shape used by the existing
OpenAI-compatible path. If that serialization changes later, this compatibility
hook should be revalidated rather than assumed permanent.

## Router Integration

Inline hashing is part of the normal request-handling path:

- the tokenizer is initialized once at router startup
- request block hashes are computed inline for each request
- those block identifiers are registered through the existing KV-aware router
  state so they participate in routing decisions
- for chat completions, both `messages` and `tools` are passed into the hash
  path so the computed prefix reflects the full rendered prompt

Block ownership is learned independently: sidecars observe vLLM KV events and
publish block ownership to Redis. The router consults that ownership view to
prefer pods with matching blocks, rather than calling `/compute_hashes` at
request time.

## Pull-Mode Scheduling

When the router operates in pull mode, queue selection can sample from both ends
of the central queue:

- the head scan remains controlled by `POOL_FACTOR`
- when `POOL_BIDIRECTIONAL` is enabled, the router also samples up to the desired
  number of requests from the queue tail
- sampled requests are ranked using the existing KV-aware and length-aware logic
- unselected head items are returned to the front, unselected tail samples are
  returned to the back, and hard key-affinity hold-backs are returned to the
  front before other leftovers

Sampling both ends helps agentic follow-up turns become visible to the router
even when older cold requests dominate the head of the queue.

## Data-Parallel Sidecar Fan-In

In data-parallel (DP) deployments, a single leader sidecar aggregates KV state
for all local DP engines so the router sees one unified ownership view. The
leader sidecar:

- subscribes to its local vLLM KV event stream
- resolves worker pod DNS names derived from the LeaderWorkerSet naming pattern
- connects to each worker rank's ZMQ endpoint, retrying DNS resolution for
  workers not yet available at startup
- writes all observed block ownership into Redis under the leader pod identity

The router still routes to the leader endpoint; it does not route directly to an
internal DP engine. This is configured via `DP_SIZE` and `DP_SIZE_LOCAL`.

## Configuration

| Setting | Default | Purpose |
| --- | --- | --- |
| `KV_TOKENIZER_PATH` | `/model` | Tokenizer/model directory; must match the vLLM pods |
| `KV_BLOCK_SIZE` | `128` | Block size; must match the vLLM pods |
| `PYTHONHASHSEED` | `0` | Seed for `NONE_HASH`; must match the vLLM pods |
| `KV_CANONICALISE_TOOLS` | enabled | Canonicalize OpenAI `tools` before hashing |
| `POOL_BIDIRECTIONAL` | disabled | Sample from both ends of the pull queue |
| `DP_SIZE`, `DP_SIZE_LOCAL` | — | DP sidecar fan-in topology |

The router image depends on `cbor2`, `transformers`, and `tokenizers` for inline
tokenization and hashing. No GPU is required.

## Deployment

The Helm chart wires the runtime path:

- mounts the model/tokenizer directory into the router at `/model`
- sets router environment variables for inline hashing, message normalization,
  tool canonicalization, and bidirectional pooling
- sets DP sidecar environment variables (`DP_SIZE`, `DP_SIZE_LOCAL`)
- configures vLLM DP workers to emit KV events via `--kv-events-config`
- exposes chart values such as `router.kvBlockSize`,
  `router.kvCanonicaliseTools`, and `router.poolBidirectional`

## Invariants and Boundaries

For KV-hit routing to be accurate, the router's hash inputs must match the vLLM
pods exactly: the tokenizer files, the block size, the `PYTHONHASHSEED`, and the
logical request shape (after normalization and tool canonicalization). If any of
these diverge, requests still execute correctly, but cache-hit routing becomes
inaccurate.

The design deliberately keeps the following boundaries:

- no gateway code changes; compatibility hooks stay router-side and model-neutral
- no model-specific behavior in the hash implementation
- no direct scheduling to a specific DP engine
- no dependency on vLLM Python internals inside the router
