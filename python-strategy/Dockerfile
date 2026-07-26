FROM rust:1.82.0-bookworm AS extension-builder

WORKDIR /build
COPY rust-data-service/Cargo.toml rust-data-service/Cargo.lock rust-data-service/build.rs ./
COPY rust-data-service/src/ ./src/
RUN cargo build --lib --release

FROM python:3.12-slim-bookworm

# Install system dependencies (for hiredis/psycopg2 compilation if needed)
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /bin/uv

WORKDIR /app

# Force uv to use the system Python 3.12
ENV UV_PYTHON=/usr/local/bin/python3.12
ENV UV_SYSTEM_PYTHON=1

# Copy dependency files
COPY python-strategy/pyproject.toml python-strategy/uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy source code and the clean-checkout PyO3 build.
COPY python-strategy/src/ ./src/
COPY database/alembic.ini ./database/alembic.ini
COPY database/alembic/env.py ./database/alembic/env.py
COPY database/alembic/versions/ ./database/alembic/versions/
COPY --from=extension-builder /build/target/release/libfluxtrade_core.so ./fluxtrade_core.so

# Create non-root user and writable application data directory
RUN useradd -m -u 1000 -s /bin/bash trading \
    && mkdir -p /app/data \
    && chown -R trading:trading /app
USER trading

# Set Python path and add venv to PATH
ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "src/main.py"]
