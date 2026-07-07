#!/usr/bin/env bash
#
# Dump vLLM / LMCache source snippets from a running pod so we can verify
# patches before rebuilding the image.
#
# Usage:
#   bash infra/patches/dump-running-vllm-patch-context.sh
#   bash infra/patches/dump-running-vllm-patch-context.sh vllm vllm-minimax-m2-1
#
# Output:
#   ./patch-verify-YYYYMMDD-HHMMSS/
#     README.txt
#     pod-meta.txt
#     vllm-loggers.py
#     vllm-loggers-crash-region.txt   (numbered lines around counter .inc)
#     vllm-distributed-utils.py
#     lmcache-kv_controller.py
#     lmcache-registry-utils-snippet.txt
#     process-cmdline.txt
#     patch-dry-run.txt               (if patch(1) exists in pod)
#
set -euo pipefail

NS="${1:-vllm}"
POD="${2:-}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl not found" >&2
  exit 1
fi

if [[ -z "${POD}" ]]; then
  # Prefer a leader pod name ending in -0 or -1 without "-worker" in the name.
  POD="$(kubectl get pods -n "${NS}" -o name 2>/dev/null \
    | sed 's|pod/||' \
    | grep -E 'vllm.*m2-[01]$' \
    | head -1 || true)"
  if [[ -z "${POD}" ]]; then
    POD="$(kubectl get pods -n "${NS}" -o name 2>/dev/null \
      | sed 's|pod/||' \
      | grep -E 'vllm' \
      | grep -v worker \
      | head -1 || true)"
  fi
fi

if [[ -z "${POD}" ]]; then
  echo "ERROR: no pod found in namespace ${NS}. Pass explicitly:" >&2
  echo "  $0 <namespace> <pod-name>" >&2
  exit 1
fi

if ! kubectl get pod -n "${NS}" "${POD}" >/dev/null 2>&1; then
  echo "ERROR: pod ${NS}/${POD} not found" >&2
  exit 1
fi

OUT_DIR="$(pwd)/patch-verify-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${OUT_DIR}"

exec_in() {
  kubectl exec -n "${NS}" "${POD}" -- "$@"
}

echo "Collecting from ${NS}/${POD} -> ${OUT_DIR}"

{
  echo "namespace=${NS}"
  echo "pod=${POD}"
  echo "collected_at=$(date -Is)"
  kubectl get pod -n "${NS}" "${POD}" -o wide
  echo "---"
  kubectl get pod -n "${NS}" "${POD}" -o jsonpath='{.spec.containers[0].image}{"\n"}'
} > "${OUT_DIR}/pod-meta.txt"

# Main process args (APIServer / vllm serve)
exec_in ps auxww 2>/dev/null | head -30 > "${OUT_DIR}/process-ps.txt" || true
exec_in sh -c 'tr "\0" " " < /proc/1/cmdline; echo' \
  > "${OUT_DIR}/process-cmdline.txt" 2>/dev/null || true

# Paths inside the container
read -r VLLM_ROOT LMCACHE_ROOT VLLM_ASCEND_ROOT <<EOF
$(exec_in sh -c '
  VLLM="/vllm-workspace/vllm"
  ASCEND="/vllm-workspace/vllm-ascend"
  LMC="$(python3 -c "import lmcache, os; print(os.path.dirname(lmcache.__file__))" 2>/dev/null || echo UNKNOWN)"
  echo "$VLLM $LMC $ASCEND"
')
EOF

{
  echo "VLLM_ROOT=${VLLM_ROOT}"
  echo "LMCACHE_ROOT=${LMCACHE_ROOT}"
  echo "VLLM_ASCEND_ROOT=${VLLM_ASCEND_ROOT}"
} > "${OUT_DIR}/paths.txt"

LOGGERS="${VLLM_ROOT}/vllm/v1/metrics/loggers.py"
UTILS="${VLLM_ROOT}/vllm/distributed/utils.py"
KV_CTRL="${LMCACHE_ROOT}/v1/cache_controller/controllers/kv_controller.py"
REG_UTILS="${LMCACHE_ROOT}/v1/cache_controller/utils.py"

dump_file() {
  local remote="$1"
  local local_name="$2"
  if exec_in test -f "${remote}" 2>/dev/null; then
    exec_in cat "${remote}" > "${OUT_DIR}/${local_name}"
    echo "  OK  ${remote}"
  else
    echo "MISSING ${remote}" > "${OUT_DIR}/${local_name}"
    echo "  MISSING ${remote}" >&2
  fi
}

echo "Dumping files..."
dump_file "${LOGGERS}" "vllm-loggers.py"
dump_file "${UTILS}" "vllm-distributed-utils.py"
dump_file "${KV_CTRL}" "lmcache-kv_controller.py"
dump_file "${REG_UTILS}" "lmcache-registry-utils.py"

# Numbered region around the crash (search for counter_prompt_tokens_by_source)
exec_in sh -c "
  if [ -f '${LOGGERS}' ]; then
    nl -ba '${LOGGERS}' | grep -n 'counter_prompt_tokens_by_source' | head -5
    echo '---'
    START=\$(grep -n 'counter_prompt_tokens_by_source' '${LOGGERS}' | head -1 | cut -d: -f1)
    if [ -n \"\$START\" ]; then
      START=\$((START - 15))
      [ \"\$START\" -lt 1 ] && START=1
      END=\$((START + 45))
      sed -n \"\${START},\${END}p\" '${LOGGERS}' | nl -ba -v \"\$START\"
    fi
  fi
" > "${OUT_DIR}/vllm-loggers-crash-region.txt" 2>/dev/null || true

# Verify vllm-utils.diff marker (ARM sched_yield guard)
exec_in sh -c "
  if [ -f '${UTILS}' ]; then
    echo '=== USE_SCHED_YIELD region ==='
    grep -n 'USE_SCHED_YIELD\|CpuArchEnum\|ARM' '${UTILS}' || true
  fi
" > "${OUT_DIR}/vllm-utils-markers.txt" 2>/dev/null || true

# Verify lmcache-controller.diff marker (target_worker_id)
exec_in sh -c "
  if [ -f '${REG_UTILS}' ]; then
    echo '=== find_worker_key / target_worker_id ==='
    grep -n 'find_worker_key\|target_worker_id' '${REG_UTILS}' || true
  fi
  if [ -f '${KV_CTRL}' ]; then
    echo '=== kv_controller worker_id ==='
    grep -n 'worker_id' '${KV_CTRL}' | head -20 || true
  fi
" > "${OUT_DIR}/lmcache-controller-markers.txt" 2>/dev/null || true

# Dry-run patch inside pod (best-effort)
PATCH_LOCAL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/vllm-metrics-loggers.diff"
if [[ -f "${PATCH_LOCAL}" ]]; then
  kubectl cp -n "${NS}" "${PATCH_LOCAL}" "${POD}:/tmp/vllm-metrics-loggers.diff" >/dev/null 2>&1 || true
  exec_in sh -c "
    if command -v patch >/dev/null 2>&1 && [ -f '${LOGGERS}' ] && [ -f /tmp/vllm-metrics-loggers.diff ]; then
      cp '${LOGGERS}' /tmp/loggers.py.bak
      cd '${VLLM_ROOT}' && patch -p1 --dry-run < /tmp/vllm-metrics-loggers.diff
    else
      echo 'patch dry-run skipped (no patch(1) or missing files)'
    fi
  " > "${OUT_DIR}/patch-dry-run.txt" 2>&1 || true
else
  echo "Local patch not found at ${PATCH_LOCAL}" > "${OUT_DIR}/patch-dry-run.txt"
fi

cat > "${OUT_DIR}/README.txt" <<README
Patch verification bundle from ${NS}/${POD}

Share this entire folder (or the .tar.gz) back in chat.

What to check:
1. vllm-loggers-crash-region.txt — lines around counter_prompt_tokens_by_source
2. patch-dry-run.txt — should say "succeeded" if our diff applies cleanly
3. vllm-utils-markers.txt — should mention CpuArchEnum / ARM if vllm-utils.diff applied
4. lmcache-controller-markers.txt — should mention find_worker_key if controller diff applied
5. process-cmdline.txt — confirms --enable-prompt-tokens-details on/off

Create tarball:
  tar czf patch-verify.tgz -C "$(dirname "${OUT_DIR}")" "$(basename "${OUT_DIR}")"
README

TARBALL="${OUT_DIR}.tar.gz"
tar czf "${TARBALL}" -C "$(dirname "${OUT_DIR}")" "$(basename "${OUT_DIR}")"

echo ""
echo "Done."
echo "  Directory: ${OUT_DIR}"
echo "  Tarball:   ${TARBALL}"
echo ""
echo "Share the tarball or paste vllm-loggers-crash-region.txt + patch-dry-run.txt here."
