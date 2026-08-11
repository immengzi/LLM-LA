#!/usr/bin/env bash
# Full Docker-only dynamic P/D verification:
#   P2,D1 -> P1,D2 -> P2,D1  on the allocated NPU range, with evidence capture.
set -euo pipefail

LAB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$LAB_DIR"
mkdir -p results logs

PY=python3
CORE="$LAB_DIR/pd_rebalancer.py"
EXEC="$LAB_DIR/pd_rebalancer_docker.py"

if [[ ! -f "$CORE" || ! -f "$EXEC" ]]; then
  echo "executor files not found under $CORE / $EXEC" >&2
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -q '^lzm-dynpd-'; then
  echo "owned lzm-dynpd-* containers still exist; clean them up first" >&2
  exit 1
fi

dump_logs() {
  local label=$1
  for name in $(docker ps --format '{{.Names}}' | grep '^lzm-dynpd-' || true); do
    docker logs --tail 3000 "$name" > "logs/${label}-${name}.log" 2>&1 || true
  done
  npu-smi info > "logs/${label}-npu-smi.txt" 2>&1 || true
}

export LZM_DYNPD_CONFIG="$LAB_DIR/config.json"

echo "=== P2,D1 ==="
python3 "$EXEC" apply --prefill 2 --decode 1
python3 "$EXEC" smoke --label before-p2d1 | tee "results/before-p2d1.json"
dump_logs before-p2d1

echo "=== P1,D2 ==="
python3 "$EXEC" apply --prefill 1 --decode 2
python3 "$EXEC" smoke --label after-p1d2 | tee "results/after-p1d2.json"
dump_logs after-p1d2

echo "=== P2,D1 restored ==="
python3 "$EXEC" apply --prefill 2 --decode 1
python3 "$EXEC" smoke --label after-p2d1-restore | tee "results/after-p2d1-restore.json"
dump_logs after-p2d1-restore

python3 "$EXEC" status | tee "results/final-status.json"

if [[ "${KEEP_CONTAINERS:-0}" != "1" ]]; then
  echo "=== cleanup owned containers ==="
  python3 "$EXEC" cleanup
fi

echo "verification complete; see $LAB_DIR/results and $LAB_DIR/logs"
