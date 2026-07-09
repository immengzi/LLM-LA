#!/usr/bin/env bash
# Shared build configuration for service images. Sourced by the per-service
# build.sh scripts so the SAME files work across clusters (bz, yz, ...) without
# per-cluster edits.
#
# Resolves two environment-specific things automatically:
#
#   REGISTRY  - defaults to the per-cluster alias `reg.local:32000` (each cluster
#               resolves reg.local to its own registry). Override by exporting
#               REGISTRY=... before running.
#
#   PROXY     - some clusters have direct internet (e.g. bz), others require an
#               HTTP proxy for pip/apt during `docker build` (e.g. yz). We probe
#               and pick automatically:
#                 1. If PROXY_URL is set in the env (even to ""), honor it verbatim.
#                 2. Else if PyPI is reachable directly, use NO proxy.
#                 3. Else if the known fallback proxy reaches PyPI, use it.
#                 4. Else use no proxy (let the build try direct and fail loudly).
#               Force a choice with `PROXY_URL=...` or `PROXY_URL=""`.
#
# Outputs (for the sourcing script):
#   $REGISTRY, $TAG            - registry host and image tag
#   $PROXY_URL_RESOLVED        - the resolved proxy URL (may be empty)
#   ${PROXY_BUILD_ARGS[@]}     - docker --build-arg flags (empty array if no proxy)

REGISTRY="${REGISTRY:-reg.local:32000}"
TAG="${TAG:-latest}"

# Push target. The build daemon only trusts insecure (HTTP) registries under
# 127.0.0.0/8, and the cluster registry is exposed cluster-wide on NodePort
# 32000 — so localhost:32000 is the reliable push endpoint on any k8s node and
# writes the SAME repos that reg.local:32000 serves (one registry, two names).
# Override with PUSH_REGISTRY=... (e.g. to push from a non-node host).
PUSH_REGISTRY="${PUSH_REGISTRY:-localhost:32000}"

# Known fallback proxy for offline/proxied clusters (e.g. yz). Override anytime
# with the PROXY_URL env var.
_DEFAULT_PROXY="${DEFAULT_PROXY:-http://peulerosweb:EulerOS_123@172.18.100.92:8080}"
_PROXY_PROBE_URL="${PROXY_PROBE_URL:-https://pypi.org/simple/}"
_PROXY_PROBE_TIMEOUT="${PROXY_PROBE_TIMEOUT:-6}"

_resolve_proxy() {
  # 1. Explicit override (set, even if empty) wins.
  if [ "${PROXY_URL+set}" = "set" ]; then
    printf '%s' "$PROXY_URL"
    return
  fi
  # 2. Direct connection works -> no proxy.
  if curl -fsS -m "$_PROXY_PROBE_TIMEOUT" -o /dev/null "$_PROXY_PROBE_URL" 2>/dev/null; then
    printf ''
    return
  fi
  # 3. Fallback proxy reaches the probe URL -> use it.
  if [ -n "$_DEFAULT_PROXY" ] && \
     curl -fsS -m "$_PROXY_PROBE_TIMEOUT" -o /dev/null -x "$_DEFAULT_PROXY" "$_PROXY_PROBE_URL" 2>/dev/null; then
    printf '%s' "$_DEFAULT_PROXY"
    return
  fi
  # 4. Neither works -> no proxy (build will surface the real error).
  printf ''
}

PROXY_URL_RESOLVED="$(_resolve_proxy)"

PROXY_BUILD_ARGS=()
if [ -n "$PROXY_URL_RESOLVED" ]; then
  PROXY_BUILD_ARGS=(--build-arg "HTTP_PROXY=$PROXY_URL_RESOLVED" --build-arg "HTTPS_PROXY=$PROXY_URL_RESOLVED")
fi

echo "[build-common] registry=$REGISTRY push=$PUSH_REGISTRY tag=$TAG proxy=${PROXY_URL_RESOLVED:-<direct>}"
