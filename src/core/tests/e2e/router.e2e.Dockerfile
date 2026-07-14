# E2E router image. Build context is the router_service dir; this uses the
# lightweight requirements-dev.txt (no transformers/tokenizers) because the e2e
# stack runs with ROUTER_STRATEGY=none / KV_AWARE=false and never tokenizes.
FROM python:3.11-slim

WORKDIR /app

COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY router ./router

ENV HOST=0.0.0.0
ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "router.api:app", "--host", "0.0.0.0", "--port", "8080"]
