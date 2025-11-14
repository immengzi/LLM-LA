#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8200}"
USER_MSG="${USER_MSG:-Hello! How are you today?}"
MAX_TOKENS="${MAX_TOKENS:-50}"
MODEL_NAME="qwen-local"  # must match --served-model-name in server.sh
LOOPS="${LOOPS:-0}"      # number of times to send requests; set to 0 for infinite

count=0
while [[ "$LOOPS" -eq 0 || $count -lt $LOOPS ]]; do
  echo "---- Request #$((count+1)) ----"
  
  curl -s "http://localhost:${PORT}/v1/chat/completions" \
    -H "Content-Type: application/json" \
    -d "{
      \"model\": \"${MODEL_NAME}\",
      \"messages\": [{\"role\": \"user\", \"content\": \"${USER_MSG}\"}],
      \"max_tokens\": ${MAX_TOKENS}
    }" | (command -v jq >/dev/null 2>&1 && jq || cat)

  echo
  echo
  count=$((count+1))
  
  # optional delay between requests
  sleep 1
done
