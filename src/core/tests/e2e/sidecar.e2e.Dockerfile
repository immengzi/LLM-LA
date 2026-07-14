# E2E sidecar image. Build context is the sidecar dir; uses requirements-dev.txt
# (identical runtime deps plus test tooling — all lightweight).
FROM python:3.11-slim

WORKDIR /app

COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY sidecar ./sidecar

ENV PYTHONUNBUFFERED=1
EXPOSE 9000

CMD ["python", "-m", "sidecar.main"]
