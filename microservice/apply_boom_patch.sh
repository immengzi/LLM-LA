#!/usr/bin/env bash
#
# apply_boom_patch.sh — Copy ALL BooM Gateway integration files from one
#                       microservice repo into another.
#
# Usage:
#   ./apply_boom_patch.sh <source-microservice-dir> <target-microservice-dir>
#
# What it does:
#   1. For every file created or modified by the BooM Gateway feature (client
#      code, Helm chart, configs, docs), copies it from source to target.
#   2. Creates directories as needed.
#   3. Backs up any overwritten files into a timestamped directory inside target.
#   4. Prints a summary.
#
# Example:
#   ./apply_boom_patch.sh ~/microservice ~/external-microservice
#
# To undo: restore from the backup directory printed at the end.

set -euo pipefail

# -------------------------------------------------------
# File lists (relative to the microservice root)
# -------------------------------------------------------

# New files created by the BooM Gateway feature
NEW_FILES=(
    # Helm chart
    "vllm-kv-stack/templates/75-boom.yaml"

    # Configs
    "configs/boom.yaml"
    "configs/boom_master.yaml"

    # Dockerfile for building the BooM Gateway image
    "BooMGateway-main/Dockerfile"

    # Docs
    "docs/boom_gateway.md"

    # Patch script itself
    "apply_boom_patch.sh"
)

# Existing files modified by the BooM Gateway feature
MODIFIED_FILES=(
    # Client-side (repo root)
    "config.py"
    "http_client.py"
    "load_runner.py"
    "main.py"
    "sweep_methods.py"

    # Helm chart
    "vllm-kv-stack/values.yaml"

    # Helm templates — backend guard updated to include "boom"
    "vllm-kv-stack/templates/10-redis.yaml"
    "vllm-kv-stack/templates/20-cpu-hash.yaml"
    "vllm-kv-stack/templates/30-router-rbac.yaml"
    "vllm-kv-stack/templates/31-router.yaml"
    "vllm-kv-stack/templates/40-vllm.yaml"
    "vllm-kv-stack/templates/50-podmonitors.yaml"
    "vllm-kv-stack/templates/60-keda-scaledobject.yaml"

    # Master sweep config (commented-out entry added)
    "configs/1-master_config.yaml"

    # Docs
    "docs/quickstart.md"
)

# -------------------------------------------------------
# Argument parsing
# -------------------------------------------------------

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <source-microservice-dir> <target-microservice-dir>"
    echo ""
    echo "  <source-microservice-dir>  Root of the microservice repo containing"
    echo "                             the BooM Gateway changes (has config.py, etc.)"
    echo ""
    echo "  <target-microservice-dir>  Root of the external microservice repo to patch"
    echo ""
    echo "Example:"
    echo "  $0 ~/microservice ~/external-microservice"
    exit 1
fi

SRC_ROOT="$(cd "$1" 2>/dev/null && pwd)" || {
    echo "ERROR: Source directory does not exist: $1"
    exit 1
}

TARGET="$(cd "$2" 2>/dev/null && pwd)" || {
    echo "ERROR: Target directory does not exist: $2"
    exit 1
}

# Sanity: source should look like a microservice repo
if [[ ! -f "${SRC_ROOT}/config.py" ]]; then
    echo "ERROR: ${SRC_ROOT}/config.py not found."
    echo "       The source must be the microservice repo root."
    exit 1
fi
if [[ ! -d "${SRC_ROOT}/vllm-kv-stack/templates" ]]; then
    echo "ERROR: ${SRC_ROOT}/vllm-kv-stack/templates/ not found."
    echo "       The source must be the microservice repo root."
    exit 1
fi

# Sanity: target should look like a microservice repo (warn, don't block)
if [[ ! -f "${TARGET}/config.py" ]] && [[ ! -d "${TARGET}/vllm-kv-stack" ]]; then
    echo "WARNING: ${TARGET} does not look like a microservice repo"
    echo "         (no config.py and no vllm-kv-stack/ directory)."
    read -rp "         Continue anyway? [y/N] " confirm
    if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
        echo "Aborted."
        exit 1
    fi
fi

# Prevent source == target
if [[ "${SRC_ROOT}" == "${TARGET}" ]]; then
    echo "ERROR: Source and target directories are the same: ${SRC_ROOT}"
    exit 1
fi

echo "Source:  ${SRC_ROOT}"
echo "Target:  ${TARGET}"
echo ""

# -------------------------------------------------------
# Backup
# -------------------------------------------------------

BACKUP_DIR="${TARGET}/.boom_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${BACKUP_DIR}"
echo "Backup directory: ${BACKUP_DIR}"
echo ""

# -------------------------------------------------------
# Copy helper
# -------------------------------------------------------

COPIED=0
OVERWRITTEN=0
CREATED=0
ERRORS=0

copy_file() {
    local rel_path="$1"
    local src="${SRC_ROOT}/${rel_path}"
    local dst="${TARGET}/${rel_path}"

    if [[ ! -f "${src}" ]]; then
        echo "  SKIP (source missing): ${rel_path}"
        ((ERRORS++)) || true
        return
    fi

    local dst_dir
    dst_dir="$(dirname "${dst}")"
    mkdir -p "${dst_dir}"

    if [[ -f "${dst}" ]]; then
        local backup_path="${BACKUP_DIR}/${rel_path}"
        mkdir -p "$(dirname "${backup_path}")"
        cp "${dst}" "${backup_path}"

        cp "${src}" "${dst}"
        echo "  OVERWRITE: ${rel_path}"
        ((OVERWRITTEN++)) || true
    else
        cp "${src}" "${dst}"
        echo "  CREATE:    ${rel_path}"
        ((CREATED++)) || true
    fi
    ((COPIED++)) || true
}

# -------------------------------------------------------
# Apply
# -------------------------------------------------------

echo "=== New files ==="
for f in "${NEW_FILES[@]}"; do
    copy_file "${f}"
done

echo ""
echo "=== Modified files ==="
for f in "${MODIFIED_FILES[@]}"; do
    copy_file "${f}"
done

# -------------------------------------------------------
# Summary
# -------------------------------------------------------

echo ""
echo "========================================"
echo " Done."
echo "   Total copied:    ${COPIED}"
echo "   New (created):   ${CREATED}"
echo "   Overwritten:     ${OVERWRITTEN}"
echo "   Errors/skipped:  ${ERRORS}"
echo ""
echo " Backups of overwritten files saved to:"
echo "   ${BACKUP_DIR}"
echo ""
echo " To undo, restore from the backup directory:"
echo "   cp -r ${BACKUP_DIR}/* ${TARGET}/"
echo "========================================"
