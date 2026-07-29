FROM python:3.11-slim

WORKDIR /app

# ---------- Proxy only for build ----------
# Build args (provided at `docker build` time)
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

# Make them visible to pip/urllib3 during build
ENV http_proxy=${HTTP_PROXY}
ENV https_proxy=${HTTPS_PROXY}
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV NO_PROXY=${NO_PROXY}
ENV no_proxy=${NO_PROXY}
# ------------------------------------------

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Clear proxy for runtime ----------
ENV http_proxy=""
ENV https_proxy=""
ENV HTTP_PROXY=""
ENV HTTPS_PROXY=""
ENV NO_PROXY=""
ENV no_proxy=""
# ---------------------------------------------

# Copy app code
COPY router ./router

# Uvicorn config (can still be overridden in K8s)
ENV HOST=0.0.0.0
ENV PORT=8080

# Add entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]
