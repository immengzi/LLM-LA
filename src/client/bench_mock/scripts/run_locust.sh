#!/usr/bin/env bash
# Run Locust against the gateway LB (default http://127.0.0.1:14000).
# LiteLLM defaults: 1000 users, 500 ramp-up.
#
# Matches LiteLLM-style reporting: a few failed requests must NOT abort the
# campaign. Locust --exit-code-on-error 0 keeps process exit 0 when failures
# are recorded in CSV/HTML (same spirit as publishing tables despite errors).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${HOST:-http://127.0.0.1:14000}"
USERS="${USERS:-1000}"
SPAWN_RATE="${SPAWN_RATE:-500}"
RUN_TIME="${RUN_TIME:-5m}"
OUT_DIR="${OUT_DIR:-$ROOT/results/latest}"
CSV_PREFIX="${CSV_PREFIX:-$OUT_DIR/locust}"

mkdir -p "$OUT_DIR"

if [[ ! -d "$ROOT/locust/.venv" ]]; then
  python3 -m venv "$ROOT/locust/.venv"
  "$ROOT/locust/.venv/bin/pip" install -q -r "$ROOT/locust/requirements.txt"
fi

# shellcheck disable=SC2086
set +e
"$ROOT/locust/.venv/bin/locust" \
  -f "$ROOT/locust/locustfile.py" \
  --host "$HOST" \
  --users "$USERS" \
  --spawn-rate "$SPAWN_RATE" \
  --run-time "$RUN_TIME" \
  --headless \
  --csv "$CSV_PREFIX" \
  --html "$OUT_DIR/report.html" \
  --exit-code-on-error 0 \
  ${LOCUST_EXTRA_ARGS:-}
rc=$?
set -e

echo "$rc" >"$OUT_DIR/locust_exit_code.txt"
if [[ "$rc" -ne 0 ]]; then
  echo "WARNING: Locust exited with code $rc (stats still written if present)" >&2
fi
exit 0
