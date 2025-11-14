#!/usr/bin/env bash
set -euo pipefail

MODEL_PATH="/home/saeid/llm-lb/qwen-test"
PORT="${PORT:-8200}"

if [[ ! -f "$MODEL_PATH/config.json" ]]; then
  echo "ERROR: $MODEL_PATH/config.json not found. Point MODEL_PATH to a valid model directory." >&2
  exit 1
fi

export HF_HUB_OFFLINE=1
export HF_HUB_DISABLE_TELEMETRY=1

echo "Starting vLLM:"
echo "  model: $MODEL_PATH"
echo "  port : $PORT"
echo

exec vllm serve "$MODEL_PATH" \
    --served-model-name qwen-local \
    --port "$PORT" \
    --enforce-eager
