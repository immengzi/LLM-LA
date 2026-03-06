#!/bin/bash
set -euo pipefail

REGISTRY_DIR="/mnt/nvme1/registry-data"
EXPORT_SUBNET="10.175.112.0/22"

echo "=== Creating registry storage directory ==="

if [ ! -d "${REGISTRY_DIR}" ]; then
    sudo mkdir -p "${REGISTRY_DIR}"
    echo "Created ${REGISTRY_DIR}"
else
    echo "Directory already exists: ${REGISTRY_DIR}"
fi

echo "=== Setting permissions ==="
sudo chmod 777 "${REGISTRY_DIR}"
sudo chown -R nobody:nobody "${REGISTRY_DIR}" 2>/dev/null || true

echo "=== Verifying directory ==="
sudo ls -ld "${REGISTRY_DIR}"

echo
echo "=== Checking /etc/exports ==="

EXPORT_LINE="${REGISTRY_DIR} ${EXPORT_SUBNET}(rw,sync,no_subtree_check)"

if ! grep -q "${REGISTRY_DIR}" /etc/exports 2>/dev/null; then
    echo "Adding export entry to /etc/exports"
    echo "${EXPORT_LINE}" | sudo tee -a /etc/exports
else
    echo "Export entry already exists in /etc/exports"
fi

echo
echo "=== Reloading NFS exports ==="
sudo exportfs -rav

echo
echo "=== Current exports ==="
sudo exportfs -v | grep registry-data || true

echo
echo "Registry storage setup completed."