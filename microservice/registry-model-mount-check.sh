#!/bin/bash
set -e

SERVER="nfs.local"
MNT="/tmp/nfstest"

echo "=== DNS resolution ==="
getent hosts ${SERVER}

echo
echo "=== Route used ==="
ip route get $(getent hosts ${SERVER} | awk '{print $1}')

echo
echo "=== Mounting NFS ==="
sudo mkdir -p ${MNT}
sudo mount -v -t nfs -o nfsvers=4.1 ${SERVER}:/ ${MNT}

echo
echo "=== Listing model files ==="
ls -lh ${MNT} | head

echo
echo "=== Checking safetensors shards ==="
ls ${MNT}/*.safetensors 2>/dev/null | head || echo "No shards found"

echo
echo "=== Unmounting ==="
sudo umount ${MNT}

echo
echo "NFS test completed successfully ✅"
