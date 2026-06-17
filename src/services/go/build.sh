#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

REGISTRY="${REGISTRY:-reg.local:32000}"
TAG="${TAG:-latest}"

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
docker build -f Dockerfile.router -t "$REGISTRY/kv-router-go:$TAG" .
docker build -f Dockerfile.sidecar -t "$REGISTRY/kv-sidecar-go:$TAG" .

# Cleanup binaries
rm -f gateway sidecar

# -------------------------------------------------------
# Step 3: Push to registry
# -------------------------------------------------------

echo "[5/5] Pushing to $REGISTRY..."
docker push "$REGISTRY/kv-router-go:$TAG"
docker push "$REGISTRY/kv-sidecar-go:$TAG"

echo
echo "=== Done ==="
echo "  $REGISTRY/kv-router-go:$TAG"
echo "  $REGISTRY/kv-sidecar-go:$TAG"
