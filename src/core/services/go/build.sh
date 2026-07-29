#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

cleanup() {
  rm -f gateway sidecar hasher/wheels/*.whl
}
trap cleanup EXIT

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

# -------------------------------------------------------
# Resolve a Go new enough for go.mod. The system `go` may be too old (e.g. 1.17)
# to parse a newer go.mod or auto-switch toolchains (GOTOOLCHAIN needs Go >=
# 1.21). Prefer $GO, then a new-enough `go` on PATH, then known local SDKs.
# GOTOOLCHAIN=auto lets the chosen launcher fetch the exact toolchain pinned in
# go.mod via GOPROXY if it is not already installed.
# -------------------------------------------------------
export GOTOOLCHAIN="${GOTOOLCHAIN:-auto}"

_go_ok() {  # 0 if "$1" is a usable go >= 1.21
  local v major minor
  v=$("$1" version 2>/dev/null | sed -n 's/.*go\([0-9]\+\)\.\([0-9]\+\).*/\1 \2/p') || return 1
  [ -n "$v" ] || return 1
  # shellcheck disable=SC2086
  set -- $v; major=$1; minor=$2
  [ "$major" -gt 1 ] || { [ "$major" -eq 1 ] && [ "$minor" -ge 21 ]; }
}

GO="${GO:-go}"
if ! _go_ok "$GO"; then
  for _cand in "$HOME/go-sdk/go/bin/go" /usr/local/go/bin/go "$HOME"/sdk/go*/bin/go; do
    if [ -x "$_cand" ] && _go_ok "$_cand"; then GO="$_cand"; break; fi
  done
fi
if ! _go_ok "$GO"; then
  echo "ERROR: need Go >= 1.21 to build (go.mod pins 'go $(sed -n 's/^go //p' go.mod)')." >&2
  echo "       Found: $("$GO" version 2>&1)." >&2
  echo "       Install a newer Go or set GO=/path/to/go before running." >&2
  exit 1
fi

echo "=== Building Go services (host compile + minimal Docker image) ==="
echo "  registry = $REGISTRY"
echo "  tag      = $TAG"
echo "  GOPROXY  = $GOPROXY"
echo "  go       = $("$GO" version) [$GO]"
echo

HASHER_BUILD_ARGS=()
if [ "${HASHER_OFFLINE_WHEELS:-0}" = "1" ]; then
  echo "Preparing Python 3.11 manylinux wheelhouse for offline Docker build..."
  python3 -m pip download \
    --dest hasher/wheels \
    --only-binary=:all: \
    --platform manylinux2014_x86_64 \
    --python-version 311 \
    --implementation cp \
    --abi cp311 \
    -r hasher/requirements.txt
  HASHER_BUILD_ARGS=(--build-arg HASHER_OFFLINE_WHEELS=1)
fi

# -------------------------------------------------------
# Step 1: Compile on the host
# -------------------------------------------------------

echo "[1/4] go mod tidy..."
rm -f go.sum
"$GO" mod tidy

echo "[2/4] Building gateway binary..."
CGO_ENABLED=0 GOOS=linux "$GO" build -buildvcs=false -o gateway ./cmd/gateway

echo "[3/4] Building sidecar binary..."
CGO_ENABLED=0 GOOS=linux "$GO" build -buildvcs=false -o sidecar ./cmd/sidecar

# -------------------------------------------------------
# Step 2: Docker images (just copy the binary)
# -------------------------------------------------------

echo "[4/4] Building Docker images..."
docker build -f Dockerfile.router \
  "${PROXY_BUILD_ARGS[@]}" \
  "${HASHER_BUILD_ARGS[@]}" \
  -t "$PUSH_REGISTRY/kv-router-go:$TAG" ..
docker build -f Dockerfile.sidecar -t "$PUSH_REGISTRY/kv-sidecar-go:$TAG" .

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
