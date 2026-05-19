# Live Trading

This guide covers deploying FluxTrade strategies to live exchanges. The same strategy code that runs in backtesting runs in live mode without modification -- the Adapter Pattern ensures your `on_candle()` logic never knows which mode it is in.

---

## Prerequisites

Before going live, ensure you have:

1. **Exchange API keys** with trading permissions (start with testnet/sandbox keys)
2. **Infrastructure**: PostgreSQL 15, Redis, and the Rust data service running
3. **A backtested strategy** with acceptable performance metrics
4. **The `.env` file** populated with all required variables (see [Configuration](configuration.md))

---

## Architecture: The Live Pipeline

In live trading, data flows through the full microservices pipeline:

```
Exchange WebSocket
    |
    v
[Rust Data Service]         -- Connects to exchange WS, aggregates raw trades into candles
    |
    v (Redis Streams)
    |                       -- stream:market:{exchange}:{symbol}:{timeframe}
    v
[DataConsumer]              -- XREADGROUP with consumer groups, conflation logic
    |
    v
[StrategyEngine]            -- Routes candles to registered strategies
    |
    v
[Strategy.on_candle()]      -- Your strategy code, returns a Signal
    |
    v
[ExecutionEngine]           -- Converts Signal to Order, applies risk checks
    |
    v
[IExchangeAdapter]          -- CcxtExchangeAdapter or LiveBinanceAdapter
    |
    v
Exchange API                -- REST (or WebSocket for Binance market orders)
```

### Key Components

**Rust Data Service** (`rust-data-service/`): Connects to exchange WebSocket feeds (Binance, Bybit, Backpack), aggregates raw trades into OHLCV candles at multiple timeframes (1m, 5m, 15m, 1h, etc.), and publishes them to Redis Streams. Stream keys include the timeframe: `stream:market:{exchange}:{symbol}:{tf}`.

**DataConsumer** (`src/core/consumer.py`): A Python Redis Streams consumer that reads candles via `XREADGROUP` with consumer groups. It implements:

- **Reconnection** with exponential backoff (up to 10 retries, max 300s backoff)
- **Conflation**: If consumer lag exceeds 100ms, it synthesizes a batch of messages into a single candle to catch up, preserving OHLC invariants
- **Consumer groups**: Multiple Python instances can share the workload

**StrategyEngine** (`src/core/engine.py`): The event-driven core that manages strategy lifecycle, signal processing, risk checks, and audit trails. It provides:

- Hot-pluggable strategy discovery via filesystem scanning
- Strategy lifecycle management (DISCOVERED, READY, ACTIVE, STOPPED, ERROR)
- Concurrency-safe strategy registration with thread locks
- Redis heartbeat (1s interval) for health monitoring
- Command listener for remote control via Redis Pub/Sub

---

## Setting Up Live Adapter Config

### Generic CCXT Adapter (any exchange)

```python
adapter_config = {
    "mode": "live",
    "exchange": "binance",     # any CCXT-supported exchange
    "api_key": "your_key",     # or set EXCHANGE_API_KEY env var
    "secret": "your_secret",   # or set EXCHANGE_SECRET env var
    "testnet": True,           # always start with testnet
}
```

The `CcxtExchangeAdapter` supports any exchange in the CCXT library. It:

- Enables rate limiting by default
- Sets `defaultType: "swap"` for perpetual futures
- Falls back to `EXCHANGE_API_KEY` and `EXCHANGE_SECRET` environment variables when credentials are not provided in the config

### Binance with WebSocket Fast Path

For Binance, you can enable a WebSocket fast path for market order execution:

```python
adapter_config = {
    "mode": "live",
    "exchange": "binance",
    "testnet": True,
    "enable_ws": True,         # enables WS market order fast path
}
```

The `LiveBinanceAdapter` extends `CcxtExchangeAdapter` and attempts to send market orders via WebSocket for lower latency. If WebSocket initialization fails or a WS order fails, it falls back to REST transparently.

---

## Strategy Registration and Deployment

### Method 1: Hot-Pluggable Strategies (Recommended)

Place strategy files in the `strategies_hot/` directory. The engine scans this directory on startup and on `SCAN` commands:

```
python-strategy/strategies_hot/
    my_strategy.py
    another_strategy.py
```

Each file must contain a class that extends `BaseStrategy`. The engine discovers, instantiates, and manages the lifecycle automatically.

**Strategy Lifecycle States:**

| State | Meaning |
|-------|---------|
| `DISCOVERED` | File found, class loaded successfully |
| `READY` | Data availability check passed |
| `WARNING` | Insufficient historical data (can still be manually started) |
| `ACTIVE` | Running and processing market data |
| `STOPPED` | Manually stopped |
| `ERROR` | Load failure or runtime error |

**Remote Control via Redis Pub/Sub:**

The engine listens on the `cmd:strategy:control` channel for commands:

```python
import redis, json

r = redis.Redis()

# Scan for new strategy files
r.publish("cmd:strategy:control", json.dumps({"command": "SCAN"}))

# Test-run a strategy (check data availability)
r.publish("cmd:strategy:control", json.dumps({
    "command": "TEST_RUN",
    "params": {"id": "my_strategy", "days": 1},
}))

# Start a strategy
r.publish("cmd:strategy:control", json.dumps({
    "command": "START",
    "params": {"id": "my_strategy"},
}))

# Stop a strategy
r.publish("cmd:strategy:control", json.dumps({
    "command": "STOP",
    "params": {"id": "my_strategy"},
}))
```

### Method 2: Static Registration (Legacy)

For simpler setups, you can register strategies directly via `add_strategy()`:

```python
from src.core.engine import StrategyEngine
from src.core.clock import Clock
from src.core.db import SessionLocal

db_session = SessionLocal()
clock = Clock()

engine = StrategyEngine(
    db_session,
    clock,
    adapter_config={
        "mode": "live",
        "exchange": "binance",
        "testnet": True,
    },
)

# Register strategy instances
from src.strategies.golden_cross import GoldenCrossStrategy

strategy = GoldenCrossStrategy(
    strategy_id="golden_cross_btc",
    product_id="BINANCE:BTCUSDT-PERP",
)
engine.add_strategy(strategy)

# Start engine (heartbeat, command listener, strategy scanner)
engine.startup()
```

### Connecting the Consumer

After the engine is started, connect it to the Redis stream via `DataConsumer`:

```python
from src.core.consumer import DataConsumer

# Build stream keys from registered strategy requirements
channels = engine.build_stream_channels()
# e.g., ["stream:market:binance:btcusdt:1h"]

consumer = DataConsumer(
    channels=channels,
    on_message_callback=engine.on_market_data,
)

# This blocks and processes messages until stopped
consumer.start()
```

The consumer automatically creates consumer groups, handles reconnection, and applies conflation when lagging.

---

## Monitoring and Health Checks

### Redis Heartbeat

The `StrategyEngine` sends a heartbeat to Redis every second:

```
Key: heartbeat:python
Value: "1"
TTL: 3 seconds
```

If this key expires, the Rust data service watchdog can trigger alerts or reconnection.

### System State Lock

The engine checks `system:state` on startup. If set to `LOCKDOWN`, the engine enters a paused loop until the state is cleared:

```bash
# Emergency stop (via Redis CLI)
redis-cli SET system:state LOCKDOWN

# Resume operations
redis-cli DEL system:state
```

### Prometheus Metrics

When `METRICS_ENABLED=true`, the Python strategy service exposes metrics on the configured port (default 9090):

| Metric | Type | Description |
|--------|------|-------------|
| `fluxtrade_signals_total` | Counter | Signals emitted, labeled by strategy, type, risk status |
| `fluxtrade_orders_total` | Counter | Orders submitted, labeled by type and status |
| `fluxtrade_execution_latency_seconds` | Histogram | Adapter `place_order()` latency |
| `fluxtrade_balance_usdt` | Gauge | Current USDT account balance |
| `fluxtrade_consumer_lag_ms` | Gauge | Redis stream consumer lag per stream key |
| `fluxtrade_active_strategies` | Gauge | Number of currently active strategies |

### Grafana Dashboard

The monitoring stack (Prometheus + Grafana) is included in `docker-compose.prod.yml`:

- **Prometheus**: `http://localhost:9091` -- scrapes metrics from the Python strategy service
- **Grafana**: `http://localhost:3000` -- dashboards for balance, signals, latency, consumer lag

---

## Safety Considerations

### Start with Testnet

Always set `testnet: True` in your adapter config when developing. This connects to the exchange sandbox where trades use simulated funds. Only switch to `testnet: False` after thorough backtesting and testnet validation.

### Risk Management

The `RiskManager` checks every signal before execution:

- **Balance check**: Ensures sufficient funds for the order
- **Position limits**: Enforces maximum position size
- **Exposure calculation**: Uses `current_price` (candle close) for accurate risk assessment

Signals that fail risk checks are logged with `risk_status="REJECT"` in the audit trail but are not executed.

### Circuit Breaker (Backtest Only)

The `max_drawdown_limit` circuit breaker is a `BacktestRunner` feature and does not apply in live trading. For live risk management, rely on:

- Exchange-level stop-loss orders (managed by the matching engine in backtests, by the exchange in live)
- The `RiskManager` signal validation
- The `system:state LOCKDOWN` emergency stop

### Signal Audit Trail

Every signal processed by the engine is recorded in the `signal_audits` database table with:

- Timestamp, strategy ID, product ID, signal type
- Risk check result (PASS/REJECT) with reason
- Associated order ID (if executed)
- Full candle and signal metadata as JSON

### Graceful Shutdown

Call `engine.shutdown()` to cleanly stop the engine:

```python
engine.shutdown(timeout=30.0)
```

This stops the heartbeat and command listener threads, drains the thread pool executor, and closes the Redis connection.

---

## Docker Deployment

For production deployment, use the provided Docker Compose configuration:

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 2. Start all services
docker-compose -f docker-compose.prod.yml up -d

# 3. Verify health
docker-compose -f docker-compose.prod.yml ps

# 4. Check logs
docker-compose -f docker-compose.prod.yml logs -f python-strategy
docker-compose -f docker-compose.prod.yml logs -f rust-data
```

The Python strategy service mounts `./python-strategy/strategies_hot` as a volume, so you can add or update strategy files without rebuilding the container. Use the `SCAN` Redis command to reload after changes.

See [Configuration](configuration.md) for the full Docker service reference and resource limits.

---

## Next Steps

- [Writing Strategies](writing-strategies.md) -- Build custom strategies with `BaseStrategy`
- [Backtesting](backtesting.md) -- Validate strategies before going live
- [Configuration](configuration.md) -- Full environment and adapter configuration reference
