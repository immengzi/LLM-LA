# Fake OpenAI backend

This campaign reuses the e2e mock server at:

[`../../core/tests/e2e/mock_vllm/`](../../core/tests/e2e/mock_vllm/)

Compose builds it directly (see root `docker-compose.yml` → `mock-vllm`).

Useful env vars:

| Env | Default | Meaning |
|-----|---------|---------|
| `MOCK_LATENCY_MS` | `0` | Artificial backend sleep |
| `MOCK_MAX_MODEL_LEN` | `196608` | Context overflow threshold |
| `MOCK_HOST_PORT` | `18000` | Host port for direct mock access |

Responses include `x-mock-backend-duration-ms` so Locust can derive gateway overhead as `e2e − backend`.
