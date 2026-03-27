#!/bin/bash
set -e

# Deploy CRDs and RBAC for the Go-based vLLM services.
# Usage:
#   ./scripts/deploy.sh                  # apply CRDs + RBAC
#   ./scripts/deploy.sh --build          # build images first, then apply
#   ./scripts/deploy.sh --samples        # also apply sample CRs
#   ./scripts/deploy.sh --delete         # tear down CRDs + RBAC + samples

DIR="$(cd "$(dirname "$0")/.." && pwd)"

BUILD=false
SAMPLES=false
DELETE=false

for arg in "$@"; do
  case "$arg" in
    --build)   BUILD=true ;;
    --samples) SAMPLES=true ;;
    --delete)  DELETE=true ;;
  esac
done

if $DELETE; then
  echo "--- Deleting samples ---"
  kubectl delete -f "$DIR/config/samples/" --ignore-not-found
  echo "--- Deleting RBAC ---"
  kubectl delete -f "$DIR/config/rbac/" --ignore-not-found
  echo "--- Deleting CRDs ---"
  kubectl delete -f "$DIR/config/crd/" --ignore-not-found
  exit 0
fi

if $BUILD; then
  echo "--- Building all images ---"
  bash "$DIR/scripts/build-all.sh"
fi

echo "--- Applying CRDs ---"
kubectl apply -f "$DIR/config/crd/"

echo "--- Applying RBAC ---"
kubectl apply -f "$DIR/config/rbac/"

if $SAMPLES; then
  echo "--- Applying sample CRs ---"
  kubectl apply -f "$DIR/config/samples/"
fi

echo "--- Done ---"
kubectl get crd | grep kvstack.llm.io || true
