# Use Python 3.12 slim image
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    python3-dev \
    libkrb5-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Create non-root user first
RUN useradd --create-home --shell /bin/bash app

# 소유권을 먼저 변경하여 app 사용자가 /app에 쓸 수 있게 함
RUN chown -R app:app /app

# 이후 모든 작업은 app 사용자로 수행
USER app

# Env vars for uv (user-level)
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

# Copy dependency files
COPY --chown=app:app pyproject.toml uv.lock ./

# Install dependencies as 'app' user
RUN uv sync --frozen --no-dev

# Copy application code as 'app' user
COPY --chown=app:app src/ ./src/
COPY --chown=app:app config/ ./config/
COPY --chown=app:app main.py ./

# Run the application
CMD ["uv", "run", "python", "main.py"]
