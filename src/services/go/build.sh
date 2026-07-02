#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Shared, cluster-agnostic registry + proxy resolution (bz/yz/...).
# Override with REGISTRY=... / PROXY_URL=... / TAG=... if needed. The resolved
# proxy is reused for the router image's Python (pip) layer.
source "$SCRIPT_DIR/../build-common.sh"

# -------------------------------------------------------
# Go module settings (bypass checksum/TLS issues behind proxy)
# -------------------------------------------------------
export GONOSUMCHECK=*
export GONOSUMDB=*
export GOFLAGS=-insecure
export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"

echo "=== Building Go services (host compile + minimal Docker image) ==="
echo "  registry = $REGISTRY"
echo "  tag      = $TAG"
echo "  GOPROXY  = $GOPROXY"
echo

# -------------------------------------------------------
# Step 1: Compile on the host
# -------------------------------------------------------

echo "[1/4] go mod tidy..."
rm -f go.sum
go mod tidy

echo "[2/4] Building gateway binary..."
CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -o gateway ./cmd/gateway

echo "[3/4] Building sidecar binary..."
CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -o sidecar ./cmd/sidecar

# -------------------------------------------------------
# Step 2: Docker images (just copy the binary)
# -------------------------------------------------------

echo "[4/4] Building Docker images..."
# Stage the router's prefix_hash.py into the build context so the in-container
# hasher runs the exact same code (single source of truth; not committed).
mkdir -p hasher
cp ../router_service/router/prefix_hash.py hasher/prefix_hash.py
docker build -f Dockerfile.router \
  "${PROXY_BUILD_ARGS[@]}" \
  -t "$PUSH_REGISTRY/kv-router-go:$TAG" .
docker build -f Dockerfile.sidecar -t "$PUSH_REGISTRY/kv-sidecar-go:$TAG" .

# Cleanup binaries + staged source
rm -f gateway sidecar hasher/prefix_hash.py

# -------------------------------------------------------
# Step 3: Push to registry (PUSH_REGISTRY=localhost:32000 is insecure-allowed)
# -------------------------------------------------------

echo "[5/5] Pushing to $PUSH_REGISTRY..."
docker push "$PUSH_REGISTRY/kv-router-go:$TAG"
docker push "$PUSH_REGISTRY/kv-sidecar-go:$TAG"

echo
echo "=== Done (cluster pulls these as $REGISTRY/...) ==="
echo "  $PUSH_REGISTRY/kv-router-go:$TAG  ->  $REGISTRY/kv-router-go:$TAG"
echo "  $PUSH_REGISTRY/kv-sidecar-go:$TAG  ->  $REGISTRY/kv-sidecar-go:$TAG"
