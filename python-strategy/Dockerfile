FROM python:3.12-slim-bookworm

# Install system dependencies (for hiredis/psycopg2 compilation if needed)
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Force uv to use the system Python 3.12
ENV UV_PYTHON=/usr/local/bin/python3.12
ENV UV_SYSTEM_PYTHON=1

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen

# Copy source code
COPY src/ ./src/

# Create non-root user and fix ownership
RUN useradd -m -u 1000 -s /bin/bash trading && chown -R trading:trading /app
USER trading

# Set Python path and add venv to PATH
ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "src/main.py"]
