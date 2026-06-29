# Stage 1: Base image
FROM python:3.11-slim AS base
WORKDIR /app
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Stage 2: Builder
FROM base AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt
COPY . .
RUN pip install --no-cache-dir --user .

# Stage 3: Runtime base
FROM base AS runtime
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH
COPY agyqueue/ agyqueue/

# Stage 4: Server build target
FROM runtime AS server
EXPOSE 8000
ENV AGYQUEUE_TRANSPORT=sse \
    AGYQUEUE_HOST=0.0.0.0 \
    AGYQUEUE_PORT=8000
CMD ["python", "-m", "agyqueue.mcp_server"]

# Stage 5: Worker build target
FROM runtime AS worker
CMD ["python", "agyqueue/worker.py"]
