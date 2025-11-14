#!/usr/bin/env bash
set -euo pipefail

SCRIPT="${1:-matmul_practice.py}"          # script to run
OVERRIDE_LABEL="${2:-${RUN_LABEL:-}}"      # optional label arg or RUN_LABEL env
OUTDIR="${OUTDIR:-./ncu_runs}"
PYTHON_BIN="${PYTHON_BIN:-python}"

# Stop dcgm-exporter (profiling counters conflict with ncu)
if [[ "${STOP_DCGM_FOR_NCU:-1}" == "1" ]]; then
  sudo systemctl stop nvidia-dcgm dcgm-exporter 2>/dev/null || true
fi

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0}"
echo quit | nvidia-cuda-mps-control 2>/dev/null || true

SET_NAME="${NCU_SET:-speedOfLight}"   # good default; try "full" later
CUSTOM_METRICS="${METRICS:-}"
KERNEL_REGEX="${KERNEL_REGEX:-"(gemm|mma|matmul|cublas|attn|attention)"}"
LAUNCH_SKIP="${LAUNCH_SKIP:-50}"
LAUNCH_COUNT="${LAUNCH_COUNT:-100}"

detect_label() {
  local f="$1" s
  s=$(grep -Eo 'mname\s*=\s*"[A-Za-z0-9._/\-]+"' "$f" 2>/dev/null || true)
  [[ -n "$s" ]] && { echo "$s" | head -n1 | sed -E 's/.*"([^"]+)".*/\1/' ; return; }
  s=$(grep -Eo 'from_pretrained\(\s*"[^"]+"\s*\)' "$f" 2>/dev/null || true)
  [[ -n "$s" ]] && { echo "$s" | head -n1 | sed -E 's/.*"([^"]+)".*/\1/' ; return; }
  s=$(grep -Eo 'torchvision(\.models|\.models\.quantization)?\.[a-z0-9_]+' "$f" 2>/dev/null || true)
  [[ -n "$s" ]] && { echo "$s" | head -n1 | sed -E 's/.*models\.([a-z0-9_]+).*/\1/' ; return; }
  s=$(grep -Eo 'tv\.models\.[a-z0-9_]+' "$f" 2>/dev/null || true)
  [[ -n "$s" ]] && { echo "$s" | head -n1 | sed -E 's/.*models\.([a-z0-9_]+).*/\1/' ; return; }
  basename "${f%.*}"
}

mkdir -p "$OUTDIR"
stamp="$(date +%Y%m%d_%H%M%S)"

raw_label="${OVERRIDE_LABEL:-$(detect_label "$SCRIPT")}"
label="$(echo "$raw_label" | sed 's#[/ ]#-#g')"
rep="${OUTDIR}/ncu_${label}_${stamp}.ncu-rep"

echo ">>> Nsight Compute on ${SCRIPT}  (label=${label})"
echo ">>> Report: ${rep}"

# First pass: preset (fast)
ncu --target-processes all \
    --set "${SET_NAME}" \
    --kernel-name regex:"${KERNEL_REGEX}" \
    --launch-skip "${LAUNCH_SKIP}" \
    --launch-count "${LAUNCH_COUNT}" \
    --export "${rep}" \
    "${PYTHON_BIN}" "${SCRIPT}"

echo "→ NCU report: ${rep}"

# Optional custom metrics pass
if [[ -n "${CUSTOM_METRICS}" ]]; then
  rep2="${OUTDIR}/ncu_${label}_${stamp}_custom.ncu-rep"
  echo ">>> Custom metrics → ${rep2}"
  ncu --target-processes all \
      --metrics "${CUSTOM_METRICS}" \
      --kernel-name regex:"${KERNEL_REGEX}" \
      --launch-skip "${LAUNCH_SKIP}" \
      --launch-count "${LAUNCH_COUNT}" \
      --export "${rep2}" \
      "${PYTHON_BIN}" "${SCRIPT}"
  echo "→ NCU custom report: ${rep2}"
fi

echo ">>> CLI peek (sections available):"
ncu --import "${rep}" --list-sections || true
echo ">>> e.g.:"
echo "    ncu --import ${rep} --section \"SpeedOfLight\" --csv | column -t -s,"
echo "    nsight-cu ${rep}    # or: ncu-ui ${rep}"
