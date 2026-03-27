#!/bin/bash
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Building kv-router-go ==="
bash "$DIR/build-router.sh"

echo "=== Building kv-sidecar-go ==="
bash "$DIR/build-sidecar.sh"

echo "=== Building kv-prefixhash-go ==="
bash "$DIR/build-prefixhash.sh"

echo "=== All Go images built and pushed ==="
