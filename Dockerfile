# Multi-stage production Dockerfile for Real-Time Pricing Engine
FROM python:3.13-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Final runtime image
FROM python:3.13-slim

WORKDIR /app

# Create unprivileged system user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy installed python dependencies from builder
COPY --from=builder /root/.local /home/appuser/.local

# Copy application source code
COPY app/ ./app/
COPY scripts/ ./scripts/
COPY .env.example .env

ENV PATH=/home/appuser/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OUTBOUND_WS_HOST=0.0.0.0 \
    OUTBOUND_WS_PORT=8765

# Ensure correct ownership
RUN chown -R appuser:appuser /app /home/appuser

USER appuser

EXPOSE 8765

# Container Healthcheck verifying TCP listener readiness
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import socket; s = socket.create_connection(('127.0.0.1', 8765), timeout=2); s.close()" || exit 1

CMD ["python", "-m", "app.main"]
