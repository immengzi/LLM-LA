#!/usr/bin/env bash
set -euo pipefail

SCRIPT="${1:-matmul_practice.py}"                         # script to run
OVERRIDE_LABEL="${2:-${RUN_LABEL:-}}"                     # optional label arg or RUN_LABEL env
OUTDIR="${OUTDIR:-./nsys_runs}"                           # where to save .nsys-rep
CUDA_PATH="${NSYS_CUDA_INSTALL_PATH:-/usr/local/cuda-12.1}"  # fix GUI decode
TAGS="${NSYS_TAGS:-cuda,nvtx,cublas,cudnn,osrt}"          # trace domains
PYTHON_BIN="${PYTHON_BIN:-python}"                        # interpreter

export NSYS_CUDA_INSTALL_PATH="${CUDA_PATH}"
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0}"

detect_label() {
  local f="$1" s l
  # 1) mname = "..."
  s=$(grep -Eo 'mname\s*=\s*"[A-Za-z0-9._/\-]+"' "$f" 2>/dev/null || true)
  [[ -n "$s" ]] && { echo "$s" | head -n1 | sed -E 's/.*"([^"]+)".*/\1/' ; return; }
  # 2) from_pretrained("...") (Transformers)
  s=$(grep -Eo 'from_pretrained\(\s*"[^"]+"\s*\)' "$f" 2>/dev/null || true)
  [[ -n "$s" ]] && { echo "$s" | head -n1 | sed -E 's/.*"([^"]+)".*/\1/' ; return; }
  # 3) torchvision model symbol
  s=$(grep -Eo 'torchvision(\.models|\.models\.quantization)?\.[a-z0-9_]+' "$f" 2>/dev/null || true)
  [[ -n "$s" ]] && { echo "$s" | head -n1 | sed -E 's/.*models\.([a-z0-9_]+).*/\1/' ; return; }
  s=$(grep -Eo 'tv\.models\.[a-z0-9_]+' "$f" 2>/dev/null || true)
  [[ -n "$s" ]] && { echo "$s" | head -n1 | sed -E 's/.*models\.([a-z0-9_]+).*/\1/' ; return; }
  # 4) fallback: script basename
  basename "${f%.*}"
}

mkdir -p "$OUTDIR"
stamp="$(date +%Y%m%d_%H%M%S)"

raw_label="${OVERRIDE_LABEL:-$(detect_label "$SCRIPT")}"
label="$(echo "$raw_label" | sed 's#[/ ]#-#g')"
base="${OUTDIR}/nsys_${label}_${stamp}"
want="${base}.nsys-rep"

echo ">>> Nsight Systems on ${SCRIPT}  (label=${label})"
echo ">>> Output: ${want}"
echo ">>> Tags: ${TAGS}"

rm -f "${want}" 2>/dev/null || true

nsys profile -t "${TAGS}" \
  -o "${base}" --force-overwrite true \
  "${PYTHON_BIN}" "${SCRIPT}" || true

report=""
if [[ -f "${want}" ]]; then
  report="${want}"
else
  tmp="$(ls -t /tmp/nsys-report-*.nsys-rep 2>/dev/null | head -n1 || true)"
  if [[ -n "${tmp}" ]]; then
    report="${want}"
    cp -v "${tmp}" "${report}"
  fi
fi

if [[ ! -f "${report}" ]]; then
  echo "!!! NSYS report not found"
  exit 1
fi

echo ">>> Report: ${report}"
echo ">>> nsys stats summary:"
nsys stats "${report}" || true

# auto-open GUI if available
if command -v /opt/nvidia/nsight-systems/2024.2.3/host-linux-x64/nsight-sys >/dev/null 2>&1; then
  /opt/nvidia/nsight-systems/2024.2.3/host-linux-x64/nsight-sys "${report}" >/dev/null 2>&1 &
elif command -v nsight-sys >/dev/null 2>&1; then
  NSYS_CUDA_INSTALL_PATH="${CUDA_PATH}" nsight-sys "${report}" >/dev/null 2>&1 &
else
  echo ">>> Open later with:"
  echo "    NSYS_CUDA_INSTALL_PATH=${CUDA_PATH} nsight-sys ${report}"
fi
