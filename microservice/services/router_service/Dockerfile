FROM python:3.11-slim

WORKDIR /app

# ---------- Proxy only for build ----------
# Build args (provided at `docker build` time)
ARG HTTP_PROXY
ARG HTTPS_PROXY

# Make them visible to pip/urllib3 during build
ENV http_proxy=${HTTP_PROXY}
ENV https_proxy=${HTTPS_PROXY}
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
# ------------------------------------------

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Clear proxy for runtime ----------
ENV http_proxy=""
ENV https_proxy=""
ENV HTTP_PROXY=""
ENV HTTPS_PROXY=""
# ---------------------------------------------

# Copy app code
COPY router ./router

# Uvicorn config (can still be overridden in K8s)
ENV HOST=0.0.0.0
ENV PORT=8080

CMD ["uvicorn", "router.api:app", "--host", "0.0.0.0", "--port", "8080"]
