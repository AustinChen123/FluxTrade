# Configuration

This guide covers all configuration options for FluxTrade, including environment variables, adapter selection, backtest settings, and Docker deployment.

---

## Environment Variables

FluxTrade uses a `.env` file at the project root for all service configuration. Copy the example file to get started:

```bash
cp .env.example .env
```

### Database (PostgreSQL)

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `fluxtrade` | PostgreSQL username |
| `POSTGRES_PASSWORD` | *(required)* | PostgreSQL password |
| `POSTGRES_DB` | `fluxtrade` | Database name |
| `POSTGRES_HOST` | `localhost` | Database host (`db` in Docker) |
| `POSTGRES_PORT` | `5432` | Database port |

The Python strategy service builds the connection URL automatically:

```
postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}
```

The database engine is created lazily on first use (thread-safe with double-checked locking).

### Redis

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis host (`redis` in Docker) |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | *(empty)* | Redis password (leave empty for local dev) |

When `REDIS_PASSWORD` is empty or unset, the client connects without authentication.

### Exchange Credentials

| Variable | Default | Description |
|----------|---------|-------------|
| `EXCHANGE_ID` | `binance` | CCXT exchange identifier |
| `EXCHANGE_API_KEY` | *(empty)* | Exchange API key |
| `EXCHANGE_SECRET` | *(empty)* | Exchange API secret |
| `EXCHANGE_TESTNET` | `true` | Use exchange testnet (sandbox) mode |

For Binance-specific live trading with WebSocket fast path, use:

| Variable | Description |
|----------|-------------|
| `BINANCE_API_KEY` | Binance-specific API key |
| `BINANCE_SECRET` | Binance-specific API secret |

### Strategy Service

| Variable | Default | Description |
|----------|---------|-------------|
| `HOT_STRATEGIES_PATH` | `/app/strategies_hot` | Directory watched for `.py` strategy files; drop a file here to load it without restarting the service |
| `METRICS_ENABLED` | `false` | Enable Prometheus metrics HTTP server |
| `METRICS_PORT` | `9090` | Port for Prometheus metrics endpoint |
| `LOG_FORMAT` | *(text)* | Set to `json` for structured JSON logging |

### Dashboard

| Variable | Default | Description |
|----------|---------|-------------|
| `DASHBOARD_PASSWORD` | *(empty)* | Dashboard login password (empty disables auth) |

### Monitoring

| Variable | Default | Description |
|----------|---------|-------------|
| `GRAFANA_PASSWORD` | *(required)* | Grafana admin password |

### Full `.env.example`

```bash
POSTGRES_USER=fluxtrade
POSTGRES_PASSWORD=fluxtrade_password
POSTGRES_DB=fluxtrade
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

REDIS_HOST=localhost
REDIS_PORT=6379
# Redis auth (leave empty for no password in local dev)
REDIS_PASSWORD=

HOT_STRATEGIES_PATH=/app/strategies_hot

# Exchange Config (Optional for Mock Mode)
EXCHANGE_ID=binance
EXCHANGE_API_KEY=
EXCHANGE_SECRET=
EXCHANGE_TESTNET=true

# Dashboard auth (leave empty to disable login)
DASHBOARD_PASSWORD=
```

---

## Adapter Configuration

FluxTrade uses the Adapter Pattern to isolate exchange interaction behind `IExchangeAdapter`. The `create_adapter(config)` factory function in `src.core.adapters` selects the correct implementation based on a configuration dictionary.

### Configuration Dictionary

```python
from src.core.adapters import create_adapter

adapter = create_adapter({
    "mode": "simulated",       # "simulated" | "live"
    "exchange": "binance",     # CCXT exchange id (live only)
    "api_key": "...",          # API key (falls back to EXCHANGE_API_KEY env var)
    "secret": "...",           # API secret (falls back to EXCHANGE_SECRET env var)
    "testnet": True,           # Use sandbox mode (default: True)
    "balance": 100000,         # Initial simulated balance (simulated only)
    "maker_fee": 0.0002,       # Maker fee rate (simulated only)
    "taker_fee": 0.0006,       # Taker fee rate (simulated only)
    "enable_ws": False,        # Enable WebSocket fast path (live Binance only)
    "extra_config": {},        # Extra CCXT configuration dict
})
```

### Adapter Selection Logic

```
mode == "simulated"?
  └── Yes → SimulatedAdapter (Rust matching engine, no network)
  └── No (mode == "live")
        └── exchange == "binance" AND enable_ws == True?
              └── Yes → LiveBinanceAdapter (CCXT + WebSocket fast path)
              └── No  → CcxtExchangeAdapter (universal CCXT REST)
```

| `mode` | `exchange` | `enable_ws` | Adapter Created |
|--------|-----------|-------------|-----------------|
| `simulated` | *(ignored)* | *(ignored)* | `SimulatedAdapter` (Rust matching engine) |
| `live` | `binance` | `True` | `LiveBinanceAdapter` (CCXT + WebSocket) |
| `live` | `binance` | `False` | `CcxtExchangeAdapter` (REST only) |
| `live` | any other | *(ignored)* | `CcxtExchangeAdapter` (universal CCXT) |

### Simulated Mode (Backtesting)

The `SimulatedAdapter` delegates all order matching to the Rust `PyMatchingEngine` via PyO3. It supports Market, Limit, Stop Loss, Take Profit, Trailing Stop, and OCO orders with full fee accounting:

```python
adapter = create_adapter({
    "mode": "simulated",
    "balance": 10000,
    "maker_fee": 0.0002,
    "taker_fee": 0.0006,
})
```

### Live Mode

Live adapters connect to real exchanges via CCXT. API credentials fall back to environment variables if not provided in the config:

```python
adapter = create_adapter({
    "mode": "live",
    "exchange": "binance",
    "testnet": True,        # always start with testnet
    "enable_ws": True,      # optional WebSocket for market orders
})
```

The `LiveBinanceAdapter` extends `CcxtExchangeAdapter` with an optional WebSocket fast path for market orders. If WebSocket initialization fails, it falls back to REST silently.

---

## Backtest Configuration

### BacktestRunner Parameters

```python
from src.core.backtest_runner import BacktestRunner

runner = BacktestRunner(
    start_time=1700000000000,            # Unix ms (required)
    end_time=1700500000000,              # Unix ms (required)
    product_id="BINANCE:BTCUSDT-PERP",   # required
    timeframe="15m",                      # required
    initial_balance=10000.0,              # starting balance in USD
    max_drawdown_limit=0.20,              # circuit breaker: stop at 20% drawdown
    data_source=ds,                       # IDataSource (None = use PostgreSQL)
    fee_config={                          # maker/taker fee rates
        "maker": 0.0002,
        "taker": 0.0006,
    },
    report_config={                       # output file toggles
        "csv_trades": True,
        "equity_curve": True,
        "markdown_report": True,
        "journal_export": True,
        "output_dir": "backtest_output/",
    },
)
```

### Fee Configuration

Fees are passed to the Rust matching engine as `Decimal` values. They are applied on every order fill:

| Key | Description | Typical Value |
|-----|-------------|---------------|
| `maker` | Fee rate for limit orders | `0.0002` (0.02%) |
| `taker` | Fee rate for market orders, SL/TP triggers | `0.0006` (0.06%) |

Common exchange fee rates:

| Exchange | Maker | Taker |
|----------|-------|-------|
| Binance Futures | 0.0002 | 0.0005 |
| Bybit | 0.0001 | 0.0006 |
| Backpack | 0.0002 | 0.0006 |

### Circuit Breaker

The `max_drawdown_limit` stops the backtest if the account balance drops below:

```
stop_threshold = initial_balance * (1 - max_drawdown_limit)
```

For example, with `initial_balance=10000.0` and `max_drawdown_limit=0.20`, the backtest halts if balance falls below 8000.

### Data Source Selection

| Data Source | Import | Use Case |
|-------------|--------|----------|
| `CsvDataSource` | `from src.core.data_sources.csv_source import CsvDataSource` | CSV files (TradingView, Yahoo Finance, etc.) |
| `MemoryDataSource` | `from src.core.data_sources.memory import MemoryDataSource` | Unit tests, synthetic data |
| `DatabaseDataSource` | `from src.core.data_sources.database import DatabaseDataSource` | Production (ingested via Rust data service) |
| `YahooFinanceDataSource` | `from src.core.data_sources.yahoo import YahooFinanceDataSource` | Quick prototyping with traditional assets |

When `data_source=None`, `BacktestRunner` falls back to the PostgreSQL database using `get_candles_generator()`.

### Report Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `csv_trades` | `bool` | `True` | Write `trades.csv` with all closed trades |
| `equity_curve` | `bool` | `True` | Write `equity_curve.csv` with cumulative PnL |
| `markdown_report` | `bool` | `True` | Write `report.md` performance summary |
| `journal_export` | `bool` | `True` | Write `journal.jsonl` structured event log |
| `output_dir` | `str` | `"backtest_output/"` | Output directory path |

---

## Docker Deployment Configuration

FluxTrade runs as a multi-container application via `docker-compose.prod.yml`. All services read from the same `.env` file.

### Service Overview

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| `redis` | `fluxtrade-redis` | 6379 | Message broker (Redis Streams) |
| `db` | `fluxtrade-db` | 5432 | PostgreSQL database |
| `rust-data` | `fluxtrade-rust` | -- | Market data ingestion (WebSocket + aggregation) |
| `python-strategy` | `fluxtrade-python` | 9090 | Strategy engine + Prometheus metrics |
| `dashboard` | `fluxtrade-dashboard` | 8501 | Streamlit monitoring dashboard |
| `prometheus` | `fluxtrade-prometheus` | 9091 | Metrics collection |
| `grafana` | `fluxtrade-grafana` | 3000 | Metrics visualization |

### Resource Limits

| Service | Memory | CPU |
|---------|--------|-----|
| Redis | 256M | 0.5 |
| PostgreSQL | 512M | 1.0 |
| Rust Data Service | 512M | 1.0 |
| Python Strategy | 1G | 1.5 |
| Dashboard | 512M | 0.5 |
| Prometheus | 512M | 0.5 |
| Grafana | 256M | 0.5 |

### Startup Order

Services start with health check dependencies:

1. **Redis** and **PostgreSQL** start first with health checks
2. **Rust Data Service** waits for both Redis and DB to be healthy
3. **Python Strategy Service** waits for Redis, DB (healthy) and Rust Data (started)
4. **Dashboard** waits for Redis and DB to be healthy
5. **Prometheus** and **Grafana** start independently

### Volume Mounts

| Volume | Purpose |
|--------|---------|
| `postgres_data` | Persistent database storage |
| `prometheus_data` | Prometheus metrics retention (30 days) |
| `grafana_data` | Grafana dashboards and configuration |
| `./python-strategy/strategies_hot` | Hot-pluggable strategy files (bind mount) |

### Starting and Stopping

```bash
# Start all services
docker-compose -f docker-compose.prod.yml up -d

# View logs
docker-compose -f docker-compose.prod.yml logs -f

# View logs for a specific service
docker-compose -f docker-compose.prod.yml logs -f python-strategy

# Stop all services
docker-compose -f docker-compose.prod.yml down

# Stop and remove volumes (destructive)
docker-compose -f docker-compose.prod.yml down -v
```

---

## Next Steps

- [Writing Strategies](writing-strategies.md) -- Create custom trading strategies
- [Backtesting](backtesting.md) -- Run backtests with full metrics and reporting
- [Live Trading](live-trading.md) -- Deploy strategies to live exchanges
