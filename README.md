![CI](https://github.com/AustinChen123/FluxTrade/actions/workflows/ci.yml/badge.svg?branch=develop)

# FluxTrade

A microservices-based cryptocurrency trading system built with Rust and Python. Rust handles real-time market data ingestion; Python handles strategy logic, risk management, and backtesting. Services communicate via Redis Streams.

**Documentation**: [繁體中文](docs/README.zh-TW.md) | [Developer Guide (EN)](docs/en/developer_guide.md) | [開發者指南 (中文)](docs/zh-TW/developer_guide.md) | [User Guide](docs/user_guide.md)

## Architecture

```
Exchange WebSocket
        │
        ▼
┌──────────────────────┐
│  Rust Data Service   │
│  WebSocket → Candle  │
│  Aggregator → Redis  │
└──────────────────────┘
        │ Redis Streams (per product × timeframe)
        ▼
┌──────────────────────────────────────┐
│  Python Strategy Service             │
│                                      │
│  Consumer → StrategyEngine           │
│    ├─ Strategy.on_candle() → Signal  │
│    ├─ RiskManager → validation       │
│    ├─ ExecutionEngine → Adapter      │
│    └─ OrderManager → persistence     │
│                                      │
│  Adapters:                           │
│    ├─ LiveBinanceAdapter (CCXT)      │
│    └─ SimulatedAdapter (backtest)    │
└──────────────────────────────────────┘
        │
        ▼
┌──────────────────────┐
│  Streamlit Dashboard │
│  PnL / Positions /   │
│  Strategy Status      │
└──────────────────────┘
```

### Components

**Rust Data Service** (`rust-data-service/`)
- WebSocket connections to Binance, Bybit, Backpack
- Multi-timeframe candlestick aggregation (1m → 5m/15m via bucketing)
- Publishes to per-timeframe Redis Streams
- PyO3 bindings expose `fluxtrade_core` matching engine for backtesting

**Python Strategy Service** (`python-strategy/`)
- Event-driven `StrategyEngine` orchestrating all components
- Adapter pattern: `IExchangeAdapter` interface with live and simulated implementations
- `IDataSource` interface for pluggable data backends (PostgreSQL, CSV, in-memory)
- Hot-pluggable strategies from `strategies_hot/` directory (no restart required)
- Risk manager with balance checks and position limits

**Dashboard** (`dashboard/`)
- Streamlit-based real-time monitoring
- PnL tracking, position visualization, strategy status

### Backtesting

The backtest pipeline reuses the same `StrategyEngine` and strategy code as live trading. The only difference is the adapter and data source:

| | Live | Backtest |
|---|---|---|
| Data | Redis Streams (real-time) | `IDataSource` (DB / CSV / memory) |
| Execution | `LiveBinanceAdapter` (CCXT → exchange API) | `SimulatedAdapter` (Rust matching engine) |
| Clock | Wall clock | `BacktestClock` (simulated time) |

The `SimulatedAdapter` uses a Rust matching engine compiled as a Python extension via PyO3. This processes candle-by-candle order matching (market fills at open, limit fills on price touch within high/low range).

## Benchmark: Matching Engine Performance

All engines run the same SMA(10/30) crossover strategy on identical synthetic data (Ornstein-Uhlenbeck price model, seed=42, no fees).

| Framework | Type | 10K candles | 100K candles | 500K candles |
|---|---|---|---|---|
| **fluxtrade_core (Rust/PyO3)** | Event-driven, bar-by-bar | ~0.003s | ~0.025s | ~0.13s |
| **backtesting.py** | Event-driven, bar-by-bar | ~0.02s | ~0.18s | ~0.9s |
| **vectorbt** | Vectorized (NumPy/pandas) | ~0.04s | ~0.06s | ~0.15s |
| **Pure Python** | Event-driven, bar-by-bar | ~0.01s | ~0.10s | ~0.50s |

*Times are approximate and vary by hardware. Run `tools/benchmark_matching_engine.py` to reproduce.*

**What the benchmark measures**: Order matching throughput only — feeding candles into the matching engine and processing fills. This is the innermost loop of a backtest.

**What it does not measure**: Full backtest pipeline overhead (data loading, strategy logic, persistence, analytics).

### Comparison Notes

**vs. vectorbt**: vectorbt uses NumPy vectorization to process the entire price series at once, which is efficient for simple signal-based strategies. FluxTrade's Rust engine is event-driven (bar-by-bar), which supports stateful logic like trailing stops, partial fills, and position netting — operations that are difficult to express in a purely vectorized form. At small scale vectorbt's constant overhead dominates; at large scale the two converge.

**vs. backtesting.py**: Both are event-driven bar-by-bar engines. backtesting.py is a pure-Python framework with built-in plotting and optimization. FluxTrade's Rust engine handles the same matching logic but runs in compiled code via PyO3, resulting in lower per-bar overhead.

**vs. Freqtrade / Jesse / Hummingbot**: These are complete trading platforms with their own strategy DSLs, exchange integrations, and CLI/UI. FluxTrade differs architecturally:
- **Language split**: Rust for data ingestion and matching; Python for strategy logic. Other platforms are Python-only (Freqtrade, Jesse) or mixed C++/Python (Hummingbot).
- **Microservice deployment**: Each service runs independently and communicates via Redis. Other platforms typically run as a single process.
- **Strategy interface**: Strategies implement `BaseStrategy.on_candle()` and receive pre-aggregated candles. There is no custom DSL or decorator-based signal system.
- **Backtest/live parity**: The same `StrategyEngine` code path runs in both modes, switching only the adapter and data source. Some platforms have separate backtest and live engines.

FluxTrade does not include features that mature platforms provide: web UI for strategy management, hyperparameter optimization, marketplace, built-in indicator libraries, or multi-exchange arbitrage. It focuses on the data pipeline and execution path.

## Quick Start

### Docker (recommended)

```bash
cp .env.example .env
# Set FLUXTRADE_ENVIRONMENT=live and configure the required passwords,
# API key, adapter, market-data venue, symbols, and risk settings.
mkdir -p ./secrets
chmod 700 ./secrets
# Set FLUXTRADE_SECRETS_DIR to this directory's absolute path.
# Non-Rithmic deployments may leave the directory empty.

docker compose -f docker-compose.prod.yml up -d

# Localhost-only endpoints:
# dashboard http://127.0.0.1:8501
# control plane http://127.0.0.1:8080
# Grafana http://127.0.0.1:3000
```

The Compose migration job upgrades PostgreSQL before database-backed services
start. The default local images do not contain the licensed Rithmic protocol
inputs, and setting Rithmic environment variables does not enable that feature.
For Rithmic deployment, build approved private Rust and Python images with
`RITHMIC_PROTO_DIR` pointing to the licensed proto directory outside the
repository, then set `RUST_DATA_IMAGE` and `PYTHON_STRATEGY_IMAGE`.

Browser sessions are optional and must only be enabled when the control plane
is reached through a trusted Tailscale Serve proxy on the localhost-published
port. Configure the browser origin and operator/step-up app capability names,
which must be distinct, then set `CONTROL_PLANE_TRUSTED_PROXY_AUTH=true`.
Trusted login identities are limited to 64 characters to match the audit
schema. Browser kill-switch requests require a unique `Idempotency-Key` header
in addition to confirmation. API-key authentication remains available for CLI
and service clients; never place that key in browser code.

### Manual Setup

Requires: Python 3.12+, Rust stable, PostgreSQL 15, Redis

```bash
# Rust Data Service
cd rust-data-service
cargo build --release

# Python Strategy Service
cd python-strategy
uv sync
uv run maturin develop  # Build PyO3 extension

# Database
cd database
alembic upgrade head
```

## Configuration

Copy `.env.example` to `.env` and set:

| Variable | Description |
|---|---|
| `POSTGRES_USER` / `PASSWORD` / `DB` / `HOST` | PostgreSQL connection |
| `REDIS_HOST` | Redis connection |
| `EXCHANGE_ID` | Target exchange (e.g., `binance`) |
| `EXCHANGE_API_KEY` / `SECRET` | API credentials |
| `EXCHANGE_TESTNET` | Use testnet (`true` / `false`) |

## Development

```bash
# Python — lint and test
cd python-strategy
uv run ruff check .
uv run pytest

# Rust — format, lint, test
cd rust-data-service
cargo fmt
cargo clippy -- -D warnings
cargo test

# Benchmark (from repo root)
cd python-strategy
uv run python ../tools/benchmark_matching_engine.py
```

## Project Structure

```
FluxTrade/
├── rust-data-service/       # Rust: WebSocket, aggregation, PyO3 bindings
│   └── src/
│       ├── connector/       # Exchange WebSocket clients
│       ├── aggregator/      # Multi-timeframe candle aggregation
│       ├── publisher/       # Redis stream publisher
│       ├── binding/         # PyO3 matching engine for Python
│       └── model/           # Shared data models
├── python-strategy/         # Python: strategy engine, backtesting
│   └── src/
│       ├── core/
│       │   ├── engine.py            # StrategyEngine orchestrator
│       │   ├── risk_manager.py      # Risk checks
│       │   ├── order_manager.py     # Order lifecycle
│       │   ├── execution_engine.py  # Signal → order execution
│       │   ├── consumer.py          # Redis stream consumer
│       │   ├── backtest_runner.py   # Backtest orchestrator
│       │   ├── adapters/            # IExchangeAdapter implementations
│       │   ├── interfaces/          # Abstract interfaces
│       │   └── data_sources/        # IDataSource implementations
│       └── strategies/              # Strategy implementations
├── dashboard/               # Streamlit monitoring UI
├── database/                # Alembic migrations
└── tools/                   # Benchmarks, data generation, utilities
```
