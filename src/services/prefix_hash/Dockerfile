# Dockerfile.hash-service
# CPU-only vLLM hash service: exposes /compute_hashes HTTP endpoint

# IMPORTANT: pick a tag that matches your Ascend vLLM version
FROM vllm-arm64:vllm

# ENV http_proxy=http://peulerosweb:EulerOS_123@172.18.100.92:8080
# ENV https_proxy=http://peulerosweb:EulerOS_123@172.18.100.92:8080

# Workdir where we'll place the service code
WORKDIR /workspace

# Copy service code into the image
# If your repo has these in kv_cache_aware/, adjust paths accordingly.
COPY prefix_hash_estimation.py prefix_hash_service.py ./kv_cache_aware/

# Install minimal runtime dependencies for the HTTP service
# - fastapi + uvicorn for API
# - cbor2 for sha256_cbor encoding (if not already present)
RUN pip install --no-cache-dir \
    fastapi \
    "uvicorn[standard]" \
    cbor2

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
