#!/usr/bin/env bash
set -euo pipefail

# uninstall-llmd-and-ports.sh
# Stops background port-forwards for Prometheus & Grafana
# and uninstalls llm-d from the cluster

LLMD_DIR="$HOME/llm-lb/llm-d-infra"

echo "[cleanup] Stopping port-forwards..."
PIDS=$(ps -ef | grep 'kubectl port-forward -n llm-d-monitoring' | grep -v grep | awk '{print $2}')
if [[ -n "${PIDS}" ]]; then
  kill ${PIDS}
  echo "[cleanup] Port-forward processes killed: ${PIDS}"
else
  echo "[cleanup] No port-forward processes found."
fi

echo "[cleanup] Checking if llm-d repo exists at ${LLMD_DIR}..."
if [[ -d "${LLMD_DIR}" ]]; then
  echo "[cleanup] Running llm-d uninstaller..."
  pushd "${LLMD_DIR}/quickstart" >/dev/null
  chmod +x ./llmd-infra-installer.sh
  ./llmd-infra-installer.sh --uninstall
  popd >/dev/null
else
  echo "[cleanup] llm-d repo not found — skipping uninstall."
fi

echo "[cleanup] Done."
