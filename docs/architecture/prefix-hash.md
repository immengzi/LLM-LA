# Prefix Hash Service

This component provides a lightweight HTTP endpoint for computing **KV-cache
block identifiers** for model prompts. These identifiers are the same type
of hashes that the vLLM runtime uses internally to decide whether a request’s
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
- Returns block-level identifiers that match the vLLM server’s behavior

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

This ensures that the token sequence—and therefore the block hashes—matches what
the model server would generate.

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

- The router calls `/compute_hashes` to check whether a request’s blocks match
  those cached on a worker, and if so, route the request there. (Only the
  router calls this service; sidecars report KV state over ZMQ → Redis and do
  not call prefix-hash.)
- Future caching strategies (prefetching, warming, migration) can use these
  identifiers to coordinate behavior.

Consistency between this service and the model servers ensures that all
components agree on which requests share the same prefix, enabling predictable
and efficient KV reuse.

---
