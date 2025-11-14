# Dockerfile.kv-listener
# Lightweight image for KV event listener sidecar

FROM python:3.10-slim

ENV http_proxy=http://peulerosweb:EulerOS_123@172.18.100.92:8080
ENV https_proxy=http://peulerosweb:EulerOS_123@172.18.100.92:8080

WORKDIR /app

# Install system deps if needed (usually not many for pyzmq)
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the subscriber script
COPY zmq_subscriber.py .

ENV PYTHONUNBUFFERED=1

# Default command: run the subscriber
CMD ["python", "-u", "zmq_subscriber.py"]
