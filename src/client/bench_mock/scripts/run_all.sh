#!/usr/bin/env bash
# Run the full LiteLLM-style matrix: each path × {2,4} instances.
# Continues across cells even if one fails (report + next).
#
# Env:
#   RUN_GO=1|0          include Go LLM-LA cells (default 1)
#   RUN_BOOM=1|0        include BooM (default 0)
#   RUN_LITELLM_NETWORK_MOCK=1|0
#   CLEAR_RESULTS=1     wipe results/ before starting (default 1)
#   USERS SPAWN_RATE RUN_TIME PROMPT_REPEAT GOPROXY ...
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ "${CLEAR_RESULTS:-1}" == "1" ]]; then
  echo "==> Clearing $ROOT/results/*"
  find "$ROOT/results" -mindepth 1 -maxdepth 1 ! -name '.gitkeep' -exec rm -rf {} +
fi

PATHS=(litellm llmla_sidecarless llmla_sidecar)
if [[ "${RUN_GO:-1}" == "1" ]]; then
  PATHS+=(llmla_go_sidecarless llmla_go_sidecar)
fi
if [[ "${RUN_BOOM:-0}" == "1" ]]; then
  PATHS+=(boom_direct)
fi
if [[ "${RUN_LITELLM_NETWORK_MOCK:-0}" == "1" ]]; then
  PATHS+=(litellm_network_mock)
fi

echo "==> Matrix: ${PATHS[*]} × {2,4}"
failed_cells=()
for p in "${PATHS[@]}"; do
  for n in 2 4; do
    echo ""
    echo "======== $p × $n ========"
    set +e
    "$ROOT/scripts/run_path.sh" "$p" "$n"
    rc=$?
    set -e
    if [[ "$rc" -ne 0 ]]; then
      echo "WARNING: cell $p × $n exited $rc — continuing matrix" >&2
      failed_cells+=("${p}_${n}inst(rc=${rc})")
    fi
    # Ensure no leftover compose project between cells
    for f in "$ROOT"/configs/*/compose.override.yml; do
      docker compose -f "$ROOT/docker-compose.yml" -f "$f" down --remove-orphans >/dev/null 2>&1 || true
    done
  done
done

echo ""
echo "All done. See $ROOT/results/"
ls -1 "$ROOT/results"/*/summary.md 2>/dev/null || echo "(no summary.md files yet)"
if [[ ${#failed_cells[@]} -gt 0 ]]; then
  echo "Cells with non-zero script exit: ${failed_cells[*]}" >&2
fi
exit 0
