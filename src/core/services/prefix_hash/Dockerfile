# CPU-only vLLM hash service: exposes /compute_hashes HTTP endpoint

# IMPORTANT: pick a tag that matches your Ascend vLLM version
FROM vllm-arm64:vllm

# ---------- Proxy only for build (cluster-agnostic) ----------
# Passed by build.sh via --build-arg; empty on clusters with direct internet
# (e.g. bz), set to the cluster proxy where required (e.g. yz).
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ENV http_proxy=${HTTP_PROXY} \
    https_proxy=${HTTPS_PROXY} \
    HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=${NO_PROXY} \
    no_proxy=${NO_PROXY}
# -------------------------------------------------------------

# Workdir where we'll place the service code
WORKDIR /workspace

# Copy service code into the image
# (build context will be services/prefix_hash, see build command below)
COPY prefix_hash_estimation.py prefix_hash_service.py ./kv_cache_aware/

# Install minimal runtime dependencies for the HTTP service
# - fastapi + uvicorn for API
# - cbor2 for sha256_cbor encoding (if not already present)
RUN pip install --no-cache-dir \
    fastapi \
    "uvicorn[standard]" \
    cbor2

# ---------- Clear proxy for runtime ----------
ENV http_proxy="" \
    https_proxy="" \
    HTTP_PROXY="" \
    HTTPS_PROXY="" \
    NO_PROXY="" \
    no_proxy=""
# ---------------------------------------------

# Environment defaults (override via k8s if needed)
ENV PYTHONHASHSEED=0 \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1

# Expose the service port
EXPOSE 9095

# Default command:
#   - model is mounted at /model via PVC
#   - host / port can be overridden by k8s args if needed
CMD ["python", "kv_cache_aware/prefix_hash_service.py", \
     "--model-path", "/model", \
     "--host", "0.0.0.0", \
     "--port", "9095", \
     "--block-size", "128"]
