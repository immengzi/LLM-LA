#!/usr/bin/env bash
# Start one gateway path against the shared fake OpenAI backend, run Locust,
# write results under results/<path>_<N>inst/.
#
# Usage:
#   ./scripts/run_path.sh <path> <instances>
#
# Env:
#   USERS SPAWN_RATE RUN_TIME MOCK_LATENCY_MS SKIP_LOCUST=1 SKIP_DOWN=1
#
# Locust request failures do not abort the cell (LiteLLM-style). Teardown always
# runs unless SKIP_DOWN=1.
#
# Instance scaling meaning:
#   * litellm / boom / *sidecarless → scale gateway replicas to N
#   * *sidecar (pull) → keep 1 router, scale sidecars to N
#     (N routers + pull breaks the queue: sidecars DNS-RR to the wrong router)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PATH_NAME="${1:-}"
INSTANCES="${2:-2}"
COMPOSE=()
SCALE_ARGS=()
RESULT_KEY=""
OUT_DIR=""
_teardown_done=0

teardown() {
  if [[ "${SKIP_DOWN:-0}" == "1" || "${_teardown_done}" == "1" ]]; then
    return 0
  fi
  if [[ ${#COMPOSE[@]} -eq 0 ]]; then
    return 0
  fi
  _teardown_done=1
  echo "==> Tearing down"
  "${COMPOSE[@]}" down --remove-orphans || true
}
trap teardown EXIT

usage() {
  echo "Usage: $0 <litellm|litellm_network_mock|llmla_sidecarless|llmla_sidecar|llmla_go_sidecarless|llmla_go_sidecar|boom_direct> <2|4>" >&2
  exit 1
}

[[ -n "$PATH_NAME" ]] || usage
case "$INSTANCES" in
  2|4) ;;
  *) usage ;;
esac

COMPOSE=(docker compose -f "$ROOT/docker-compose.yml")
RESULT_KEY="${PATH_NAME}_${INSTANCES}inst"
export BENCH_MODEL="${BENCH_MODEL:-}"
export CHAT_PATH="${CHAT_PATH:-/v1/chat/completions}"
export API_KEY="${API_KEY:-sk-1234}"
# Keep prompts heavy but not pathological for pull-mode under 1k users.
export PROMPT_REPEAT="${PROMPT_REPEAT:-40}"

case "$PATH_NAME" in
  litellm)
    COMPOSE+=(-f "$ROOT/configs/litellm/compose.override.yml")
    SCALE_ARGS=(--scale litellm="$INSTANCES")
    RESULT_KEY="litellm_${INSTANCES}inst"
    export BENCH_MODEL="${BENCH_MODEL:-fake-openai-endpoint}"
    export LITELLM_CONFIG=proxy.yaml
    ;;
  litellm_network_mock)
    COMPOSE+=(-f "$ROOT/configs/litellm/compose.override.yml")
    SCALE_ARGS=(--scale litellm="$INSTANCES")
    RESULT_KEY="litellm_network_mock_${INSTANCES}inst"
    export BENCH_MODEL="${BENCH_MODEL:-db-openai-endpoint}"
    export LITELLM_CONFIG=proxy.network_mock.yaml
    ;;
  llmla_sidecarless)
    COMPOSE+=(-f "$ROOT/configs/llmla_sidecarless/compose.override.yml")
    SCALE_ARGS=(--scale router="$INSTANCES")
    export BENCH_MODEL="${BENCH_MODEL:-served-model}"
    ;;
  llmla_sidecar)
    COMPOSE+=(-f "$ROOT/configs/llmla_sidecar/compose.override.yml")
    # Pull mode: one central queue (router=1), N sidecars as capacity.
    SCALE_ARGS=(--scale router=1 --scale sidecar="$INSTANCES")
    export BENCH_MODEL="${BENCH_MODEL:-served-model}"
    ;;
  llmla_go_sidecarless)
    COMPOSE+=(-f "$ROOT/configs/llmla_go_sidecarless/compose.override.yml")
    SCALE_ARGS=(--scale router="$INSTANCES")
    export BENCH_MODEL="${BENCH_MODEL:-served-model}"
    ;;
  llmla_go_sidecar)
    COMPOSE+=(-f "$ROOT/configs/llmla_go_sidecar/compose.override.yml")
    SCALE_ARGS=(--scale router=1 --scale sidecar="$INSTANCES")
    export BENCH_MODEL="${BENCH_MODEL:-served-model}"
    ;;
  boom_direct)
    COMPOSE+=(-f "$ROOT/configs/boom_direct/compose.override.yml")
    SCALE_ARGS=(--scale boom="$INSTANCES")
    export BENCH_MODEL="${BENCH_MODEL:-served-model}"
    ;;
  *)
    usage
    ;;
esac

OUT_DIR="$ROOT/results/$RESULT_KEY"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
export OUT_DIR
export CSV_PREFIX="$OUT_DIR/locust"

echo "==> Bringing up path=$PATH_NAME instances=$INSTANCES (${SCALE_ARGS[*]})"
"${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --build --force-recreate "${SCALE_ARGS[@]}"

echo "==> Waiting for gateway-lb on :${GATEWAY_HOST_PORT:-14000}"
ready=0
for i in $(seq 1 120); do
  code="$(
    curl -sS -o /tmp/bench_mock_ping_body.txt -w "%{http_code}" \
      -H "Authorization: Bearer $API_KEY" \
      -H "Content-Type: application/json" \
      -d "{\"model\":\"${BENCH_MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}],\"max_tokens\":1}" \
      "http://127.0.0.1:${GATEWAY_HOST_PORT:-14000}/v1/chat/completions" \
      2>/dev/null || echo "000"
  )"
  if [[ "$code" == "200" ]]; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" != "1" ]]; then
  echo "Gateway did not return HTTP 200 in time (last=$code)" >&2
  "${COMPOSE[@]}" ps >&2 || true
  "${COMPOSE[@]}" logs --tail=120 >&2 || true
  exit 1
fi

# Reload nginx so scaled upstream DNS is fresh
"${COMPOSE[@]}" exec -T gateway-lb nginx -s reload >/dev/null 2>&1 || true

META="$OUT_DIR/run_meta.json"
python3 "$ROOT/scripts/write_meta.py" "$META" --path "$PATH_NAME" --instances "$INSTANCES"

if [[ "${SKIP_LOCUST:-0}" != "1" ]]; then
  echo "==> Locust → results/$RESULT_KEY"
  set +e
  HOST="http://127.0.0.1:${GATEWAY_HOST_PORT:-14000}" \
    "$ROOT/scripts/run_locust.sh"
  locust_rc=$?
  set -e
  if [[ -f "$OUT_DIR/locust_stats.csv" ]] || compgen -G "$OUT_DIR/*_stats.csv" >/dev/null; then
    python3 "$ROOT/scripts/summarize.py" "$OUT_DIR" | tee "$OUT_DIR/summary.md" || true
  else
    echo "WARNING: no Locust stats CSV in $OUT_DIR" >&2
  fi
  python3 "$ROOT/scripts/update_meta.py" "$META" \
    --out-dir "$OUT_DIR" \
    --locust-exit-code "$locust_rc" || true
fi

teardown
trap - EXIT

echo "Done. Artifacts: $OUT_DIR"
