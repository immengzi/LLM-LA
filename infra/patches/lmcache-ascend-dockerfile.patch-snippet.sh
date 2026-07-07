#!/bin/bash
# Append these RUN lines to LMCache-Ascend/docker/Dockerfile.a2.openEuler
# AFTER the existing "pip install lmcache && pip install LMCache-Ascend" step.
#
# Assumes docker/*.diff files are copied with the repo and
# infra/patches/vllm-metrics-loggers.diff is copied into docker/ as well.

set -euo pipefail

cat <<'DOCKERFILE_SNIPPET'
# Apply lmcache-controller.diff to the pip-installed lmcache package
RUN LMCACHE_ROOT="$(python -c 'import lmcache, os; print(os.path.dirname(lmcache.__file__))')" && \
    cd "$(dirname "${LMCACHE_ROOT}")" && \
    patch -p1 < /workspace/LMCache-Ascend/docker/lmcache-controller.diff

# Apply vLLM patches (utils + negative-counter guard for LMCache rollbacks)
RUN cd /vllm-workspace/vllm && \
    patch -p1 < /workspace/LMCache-Ascend/docker/vllm-utils.diff && \
    patch -p1 < /workspace/LMCache-Ascend/docker/vllm-metrics-loggers.diff
DOCKERFILE_SNIPPET
