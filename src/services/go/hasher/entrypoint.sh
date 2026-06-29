#!/usr/bin/env bash
# Router (Go gateway) container entrypoint.
#
# In inline mode (KV_HASH_SOURCE=inline, the default) we launch the in-container
# Python hasher on 127.0.0.1:9095 and wait for it to become healthy before
# starting the gateway. In external mode we skip it entirely (the gateway calls
# the legacy vllm-cpu-hash service instead).
set -euo pipefail

HASH_SOURCE="$(printf '%s' "${KV_HASH_SOURCE:-inline}" | tr '[:upper:]' '[:lower:]')"

if [ "$HASH_SOURCE" = "inline" ]; then
  echo "[router-entrypoint] KV_HASH_SOURCE=inline -> starting in-container hasher on 127.0.0.1:9095"
  cd /app
  uvicorn hasher_app:app --host 127.0.0.1 --port 9095 --log-level warning &
  HASHER_PID=$!

  ready=0
  for _ in $(seq 1 120); do
    if python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9095/health', timeout=1)" >/dev/null 2>&1; then
      echo "[router-entrypoint] hasher ready"
      ready=1
      break
    fi
    if ! kill -0 "$HASHER_PID" 2>/dev/null; then
      echo "[router-entrypoint] FATAL: in-container hasher exited during startup"
      exit 1
    fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    echo "[router-entrypoint] FATAL: in-container hasher did not become healthy in time"
    exit 1
  fi
else
  echo "[router-entrypoint] KV_HASH_SOURCE=$HASH_SOURCE -> using external hasher; not starting in-container hasher"
fi

exec gateway
