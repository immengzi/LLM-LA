#!/usr/bin/env bash
# Shared build configuration for service images. Sourced by the per-service
# build.sh scripts so the same files work across clusters without per-cluster
# edits.
#
# Usage (from a service dir, or via that service's build.sh):
#   ./build.sh                              # auto-detect direct vs proxy
#   PROXY_URL='http://user:pass@proxy:8080' ./build.sh
#                                           # force HTTP(S) proxy for the build
#   PROXY_URL='' ./build.sh                 # force no proxy (direct)
#   REGISTRY=reg.local:32000 TAG=dev ./build.sh
#   PUSH_REGISTRY=localhost:32000 ./build.sh
#
# Resolves two environment-specific things automatically:
#
#   REGISTRY  - defaults to the per-cluster alias `reg.local:32000` (each cluster
#               resolves reg.local to its own registry). Override by exporting
#               REGISTRY=... before running.
#
#   PROXY     - some clusters have direct internet; others require an HTTP proxy
#               for pip/apt during `docker build`. We probe and pick automatically:
#                 1. If PROXY_URL is set in the env (even to ""), honor it verbatim.
#                 2. Else if PyPI is reachable with --noproxy '*' (true direct),
#                    use NO proxy.
#                 3. Else if the known fallback proxy reaches PyPI, use it.
#                 4. Else if the shell already has HTTP_PROXY/http_proxy, use that.
#                 5. Else use no proxy (let the build try direct and fail loudly).
#               Force a choice with `PROXY_URL=...` or `PROXY_URL=""`.
#
# Outputs (for the sourcing script):
#   $REGISTRY, $TAG            - registry host and image tag
#   $PROXY_URL_RESOLVED        - the resolved proxy URL (may be empty)
#   ${PROXY_BUILD_ARGS[@]}     - docker --build-arg / --network flags
#   docker_build ...           - run docker build with proxy wired correctly
#   docker_push_noproxy ...    - push with client proxy env cleared

REGISTRY="${REGISTRY:-reg.local:32000}"
TAG="${TAG:-latest}"

# Push target. The build daemon only trusts insecure (HTTP) registries under
# 127.0.0.0/8, and the cluster registry is exposed cluster-wide on NodePort
# 32000 — so localhost:32000 is the reliable push endpoint on any k8s node and
# writes the SAME repos that reg.local:32000 serves (one registry, two names).
# Override with PUSH_REGISTRY=... (e.g. to push from a non-node host).
PUSH_REGISTRY="${PUSH_REGISTRY:-localhost:32000}"

# Known fallback proxy for offline/proxied clusters. Override anytime with the
# PROXY_URL env var.
_DEFAULT_PROXY="${DEFAULT_PROXY:-http://peulerosweb:EulerOS_123@172.18.100.92:8080}"
_PROXY_PROBE_URL="${PROXY_PROBE_URL:-https://pypi.org/simple/}"
_PROXY_PROBE_TIMEOUT="${PROXY_PROBE_TIMEOUT:-15}"

# Keep local registry / cluster traffic off the proxy during build + push.
_DEFAULT_NO_PROXY="localhost,127.0.0.1,reg.local,${REGISTRY%%:*},${PUSH_REGISTRY%%:*},10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
NO_PROXY_RESOLVED="${NO_PROXY:-${no_proxy:-$_DEFAULT_NO_PROXY}}"

_resolve_proxy() {
  # 1. Explicit override (set, even if empty) wins.
  if [ "${PROXY_URL+set}" = "set" ]; then
    printf '%s' "$PROXY_URL"
    return
  fi
  # 2. True direct connection (ignore ambient shell proxy) -> no proxy.
  if curl -fsS -m "$_PROXY_PROBE_TIMEOUT" --noproxy '*' -o /dev/null \
       "$_PROXY_PROBE_URL" 2>/dev/null; then
    printf ''
    return
  fi
  # 3. Fallback proxy reaches the probe URL -> use it.
  if [ -n "$_DEFAULT_PROXY" ] && \
     curl -fsS -m "$_PROXY_PROBE_TIMEOUT" -o /dev/null -x "$_DEFAULT_PROXY" \
       "$_PROXY_PROBE_URL" 2>/dev/null; then
    printf '%s' "$_DEFAULT_PROXY"
    return
  fi
  # 4. Ambient shell proxy -> reuse it.
  if [ -n "${HTTP_PROXY:-${http_proxy:-}}" ]; then
    printf '%s' "${HTTP_PROXY:-$http_proxy}"
    return
  fi
  # 5. Neither works -> no proxy (build will surface the real error).
  printf ''
}

PROXY_URL_RESOLVED="$(_resolve_proxy)"

# --network=host so the build container can reach a host/corp proxy (bridge NAT
# often cannot). Dockerfiles consume the uppercase ARGs and mirror them into
# lowercase ENV for pip/urllib.
PROXY_BUILD_ARGS=(--network=host)
if [ -n "$PROXY_URL_RESOLVED" ]; then
  PROXY_BUILD_ARGS+=(
    --build-arg "HTTP_PROXY=$PROXY_URL_RESOLVED"
    --build-arg "HTTPS_PROXY=$PROXY_URL_RESOLVED"
    --build-arg "NO_PROXY=$NO_PROXY_RESOLVED"
  )
fi

# docker build with proxy applied to BOTH the client (base-image pulls) and the
# build container (pip/apt via build-args). Without client env, FROM pulls can
# still fail even when --build-arg is set.
docker_build() {
  if [ -n "$PROXY_URL_RESOLVED" ]; then
    env \
      HTTP_PROXY="$PROXY_URL_RESOLVED" \
      HTTPS_PROXY="$PROXY_URL_RESOLVED" \
      http_proxy="$PROXY_URL_RESOLVED" \
      https_proxy="$PROXY_URL_RESOLVED" \
      NO_PROXY="$NO_PROXY_RESOLVED" \
      no_proxy="$NO_PROXY_RESOLVED" \
      docker build "${PROXY_BUILD_ARGS[@]}" "$@"
  else
    # Strip any ambient proxy so a "direct" decision is honored.
    env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY \
      docker build "${PROXY_BUILD_ARGS[@]}" "$@"
  fi
}

# Local registry must not go through the corp proxy (hangs / 502 via Cntlm).
docker_push_noproxy() {
  env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
    docker push "$@"
}

echo "[build-common] registry=$REGISTRY push=$PUSH_REGISTRY tag=$TAG proxy=${PROXY_URL_RESOLVED:-<direct>}"
