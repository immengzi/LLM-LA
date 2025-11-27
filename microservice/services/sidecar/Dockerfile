FROM python:3.11-slim

WORKDIR /app

# ---------- Proxy only for build ----------
ARG HTTP_PROXY
ARG HTTPS_PROXY

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
COPY sidecar ./sidecar

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "sidecar.main"]
