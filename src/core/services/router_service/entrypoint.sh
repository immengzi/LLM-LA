#!/usr/bin/env bash
set -e

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8080}"

# ACCESS_LOG defaults to true if not set
ACCESS_LOG="${ACCESS_LOG:-true}"

if [ "$ACCESS_LOG" = "true" ]; then
  echo "[entrypoint] Starting uvicorn WITH access log (ACCESS_LOG=true)"
  exec uvicorn router.api:app --host "$HOST" --port "$PORT"
else
  echo "[entrypoint] Starting uvicorn with NO access log (ACCESS_LOG=false)"
  exec uvicorn router.api:app --host "$HOST" --port "$PORT" --no-access-log
fi
