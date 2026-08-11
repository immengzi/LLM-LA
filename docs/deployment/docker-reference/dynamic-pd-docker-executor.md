# Dynamic P/D rebalancer — Docker executor (verification backend)

`pd_rebalancer_docker.py` is the Docker-only execution backend for the same
fixed-budget P/D transition semantics implemented by the Kubernetes controller
`pd_rebalancer.py`. It was written so the *code* itself, not only a manual shell
script, can be verified on a bare Docker host (e.g. an Ascend A3 node) before any
Kubernetes environment is available.

## Relationship to the Kubernetes controller

Both backends share the same transition core (`transition_plan` in
`pd_rebalancer.py`):

```text
P2,D1 -> P1,D1 -> P1,D2
P1,D2 -> P1,D1 -> P2,D1
```

The Kubernetes controller additionally supports `pdRebalancer.dryRun` (log-only
reconcile) and a two-phase `propose`/`commit` target API; the Docker backend
keeps its direct `apply --prefill N --decode M` flow for the verification lab.

The Docker executor:

- only ever starts/stops containers matching its own `lzm-dynpd-` prefix;
- validates every NPU against the configured allocation (default `0-7`);
- removes the source role from the proxy **before** deleting its container;
- waits for the proxy healthcheck topology after every add, because
  `/instances/add` is asynchronous;
- never touches other users' containers, images, or directories.

## Prerequisites on the host

The verified A3 lab uses:

- image: `quay.nju.edu.cn/ascend/vllm-ascend:v0.22.1rc1-a3`
- model: `/mnt/sdb/models/Qwen3-0.6B`
- served model name: `qwen3-0.6b-pd-lab` (override with `served_model_name` in
  `config.json`)
- NPUs: `0`, `1`, `2` (subset of the allocated `0-7`)
- proxy: `127.0.0.1:19090`

## Engine configuration

The vLLM launch arguments are configurable through `config.json` instead of
being hardcoded:

| field | default | meaning |
|-------|---------|---------|
| `tensor_parallel_size` | `1` | tensor parallel size per engine |
| `max_model_len` | `4096` | maximum model context length |
| `max_num_batched_tokens` | `4096` | scheduler batch token budget |
| `max_num_seqs` | `16` | maximum concurrent sequences per engine |
| `gpu_memory_utilization` | `0.60` | fraction of device memory used for weights + KV |
| `enforce_eager` | `true` | disable graph capture (`--enforce-eager`) |

The defaults match the verified A3 lab (Qwen3-0.6B, TP1). For another model,
set `max_model_len` to its context, raise `tensor_parallel_size` when the model
does not fit one card (a convertible P/D topology needs at least `3 * TP`
NPUs), and tune `gpu_memory_utilization` so KV-cache headroom remains. This
mirrors how Dynamo exposes TP per engine (`prefill_engine_num_gpu` /
`decode_engine_num_gpu`, `--tensor-parallel-size`) and how MindIE Motor
configures engines (`tensor_parallel_size`, `max_model_len`,
`gpu_memory_utilization`).

Adjust the lab values in `docker-pd-lab/config.json` for another host or model.

## Usage

```bash
cd src/core/vllm-kv-stack/files
export LZM_DYNPD_CONFIG=/path/to/docker-pd-lab/config.json

# Show owned containers and current role counts
python3 pd_rebalancer_docker.py status

# Bring up P2,D1 (base P1,D1 + proxy are created automatically)
python3 pd_rebalancer_docker.py apply --prefill 2 --decode 1

# Send one fixed-X-Request-Id request through the proxy
python3 pd_rebalancer_docker.py smoke --label before-p2d1

# Convert to P1,D2 (scale down source, wait topology, scale up target)
python3 pd_rebalancer_docker.py apply --prefill 1 --decode 2
python3 pd_rebalancer_docker.py smoke --label after-p1d2

# Convert back to P2,D1
python3 pd_rebalancer_docker.py apply --prefill 2 --decode 1
python3 pd_rebalancer_docker.py smoke --label after-p2d1-restore

# Remove only owned containers when the lab is no longer needed
python3 pd_rebalancer_docker.py cleanup
```

## Acceptance evidence

For each topology (`P2,D1`, `P1,D2`, restored `P2,D1`) collect:

1. proxy `healthcheck` reporting the expected `prefill_instances` /
   `decode_instances`;
2. a `200` chat completion response carrying the same `X-Request-Id`;
3. Mooncake KV-transfer evidence in the decode/prefill logs, e.g.
   `KV cache transfer for request <id> took ... ms`;
4. `npu-smi` output proving only allocated NPUs (`0-7`) are active.

The bundled `docker-pd-lab/run_verify.sh` runs the full
`P2,D1 -> P1,D2 -> P2,D1` sequence, saves responses, logs, and `npu-smi`
snapshots, then removes owned containers.

## Safety invariants

- container name filter: `lzm-dynpd-(p\d+|d\d+|proxy)` only;
- NPU validation: every slot NPU must be within `allowed_npus`;
- transition order: source role is removed and its port released before the
  target role is started on the freed slot;
- cleanup is scoped to the same owned prefix; it never runs `docker rm` on
  anything else.
