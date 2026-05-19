# Installation

This guide covers installing FluxTrade and all its dependencies from source.

## Prerequisites

| Dependency   | Version   | Notes                                      |
|--------------|-----------|--------------------------------------------|
| Python       | 3.12+     | Required by `pyproject.toml`               |
| Rust         | 1.82.0    | Pinned in `rust-toolchain.toml`            |
| PostgreSQL   | 15        | Used for trade logs and backtest results   |
| Redis        | Latest    | Pub/Sub message bus between services       |
| uv           | Latest    | Python package manager                     |

## Clone the Repository

```bash
git clone https://github.com/your-org/FluxTrade.git
cd FluxTrade
```

## Environment Configuration

Copy the example environment file and edit it with your credentials:

```bash
cp .env.example .env
```

The `.env` file contains:

```ini
POSTGRES_USER=fluxtrade
POSTGRES_PASSWORD=fluxtrade_password
POSTGRES_DB=fluxtrade
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

EXCHANGE_ID=binance
EXCHANGE_API_KEY=
EXCHANGE_SECRET=
EXCHANGE_TESTNET=true

DASHBOARD_PASSWORD=
```

For local development and backtesting, the exchange API keys are optional.

## Python Strategy Service

```bash
cd python-strategy
uv sync
```

This installs all runtime and development dependencies defined in `pyproject.toml`, including:

- **Runtime**: `ccxt`, `redis`, `sqlalchemy`, `pydantic`, `pandas`, `pandas-ta`, `structlog`, `prometheus-client`
- **Dev**: `pytest`, `ruff`, `pyright`, `maturin`, `pytest-cov`

## Rust Data Service

Ensure Rust 1.82.0 is installed. The `rust-toolchain.toml` in the project root pins the version automatically:

```bash
cd rust-data-service
cargo build
```

This compiles the data service binary and the `fluxtrade_core` PyO3 library.

## PyO3 Extension (Rust Matching Engine for Python)

The Python backtest engine relies on `fluxtrade_core.so`, a Rust-compiled extension that provides the matching engine (`PyMatchingEngine`). You must compile it manually:

```bash
cd rust-data-service

RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup" \
  cargo build --lib --release
```

Then copy the compiled library into the Python source tree:

```bash
# macOS
cp target/release/libfluxtrade_core.dylib ../python-strategy/src/fluxtrade_core.so

# Linux
cp target/release/libfluxtrade_core.so ../python-strategy/src/fluxtrade_core.so
```

!!! warning "Do NOT use `maturin develop`"
    The command `uv run maturin develop` will fail due to an `edition2024` transitive dependency issue. Always use the manual `cargo build --lib --release` workflow above.

!!! note "The `.so` file is not committed to git"
    You must rebuild `fluxtrade_core.so` after every Rust code change. The file is ignored by `.gitignore`.

Verify the extension loads correctly:

```bash
cd ../python-strategy
python -c "from fluxtrade_core import PyMatchingEngine; print('PyO3 extension loaded successfully')"
```

## Database Setup

Start PostgreSQL and Redis (if not using Docker):

```bash
# macOS (Homebrew)
brew services start postgresql@15
brew services start redis

# Linux (systemd)
sudo systemctl start postgresql redis
```

With PostgreSQL running, apply migrations:

```bash
cd database
alembic upgrade head
```

## Docker Setup (Full System)

To run all services (Redis, PostgreSQL, Rust data service, Python strategy, Dashboard, Prometheus, Grafana) via Docker:

```bash
docker-compose -f docker-compose.prod.yml up -d
```

Stop all services:

```bash
docker-compose -f docker-compose.prod.yml down
```

The Docker Compose file requires `POSTGRES_PASSWORD` and `GRAFANA_PASSWORD` to be set in your `.env` file.

## Verification

### Run Rust Tests

```bash
cd rust-data-service
cargo test --no-default-features
```

The `--no-default-features` flag disables the `extension-module` feature, which is only needed when building the `.so` for Python.

### Run Python Tests

```bash
cd python-strategy
uv run pytest
```

To run only unit tests (excluding integration tests that need Docker services):

```bash
uv run pytest -m "not integration"
```

### Lint Checks

```bash
# Python
cd python-strategy
uv run ruff check .

# Rust
cd rust-data-service
cargo fmt --check
cargo clippy -- -D warnings
```

If all tests pass and linters are clean, the installation is complete. Proceed to the [Quick Start](quickstart.md) guide.
