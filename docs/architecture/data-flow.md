# Data Flow

This document traces the complete data flow through FluxTrade for each operational mode: live trading, backtesting, and monitoring.

## Live Trading Path

```
Exchange WebSocket
       |
       v
connector/*.rs          Parse exchange-specific JSON into unified candle format
       |
       v
aggregator/mod.rs       Bucket 1m candles into 5m/15m/1h
       |
       v
publisher/mod.rs        Publish completed candles via bounded mpsc channel
       |
       v
Redis Stream            stream:market:{exchange}:{symbol}:{tf}
       |
       v
consumer.py             XREADGROUP with consumer group, parse candle from stream
       |
       v
engine.py               Dispatch candle to registered strategies (thread-safe copy)
       |
       v
strategy.on_candle()    Strategy evaluates indicators, may emit Signal
       |
       v
execution.py            Signal -> Order creation (with SL/TP/Trailing conditionals)
       |
       v
risk_manager.py         Pre-trade validation: balance, position limits, exposure
       |
       v
ccxt_adapter.py         Convert LONG/SHORT -> buy/sell, call exchange REST API
       |
       v
Exchange API            Order placed on exchange
```

### Step-by-Step Detail

**1. Exchange WebSocket -> Connector**

Each connector (`binance.rs`, `bybit.rs`, `backpack.rs`) maintains a persistent WebSocket connection. Incoming trade/kline messages are parsed from exchange-specific JSON into a unified internal candle representation. Connectors handle:

- Reconnection with exponential backoff
- Ping/pong keepalive via the write half of the split WebSocket
- Deduplication of trade data (Binance-specific)

**2. Connector -> Aggregator**

The aggregator receives 1-minute candles and maintains rolling OHLCV buckets for each configured higher timeframe. When a timeframe boundary is crossed (e.g., minute 5 completes a 5m bar), the aggregator emits a completed candle. All arithmetic uses `Decimal` to prevent floating-point drift. An OHLC invariant check (`low <= open,close <= high`) validates every emitted candle.

**3. Aggregator -> Publisher -> Redis Stream**

Completed candles are sent to the publisher via a bounded `mpsc` channel (capacity: 10,000 messages). The publisher writes each candle to a Redis Stream with a key that encodes the full routing context:

```
stream:market:binance:BTCUSDT:5m
stream:market:bybit:ETHUSDT:1h
```

This lock-free channel architecture replaced an earlier `Arc<Mutex<RedisPublisher>>` design.

**4. Redis Stream -> Consumer**

The Python `consumer.py` uses `XREADGROUP` to consume from Redis Streams as part of a consumer group. Each consumer instance tracks its own offset, enabling:

- Resumption after disconnects (no message loss)
- Multiple independent consumers on the same stream
- Backpressure via blocking reads with configurable timeout

The consumer parses the Redis hash into a `Candlestick` Pydantic model (all Decimal fields).

**5. Consumer -> Engine -> Strategy**

The `engine.py` event loop receives parsed candles and dispatches them to all registered strategies. Before dispatching:

- A thread-safe copy of the strategy list is made (prevents mutation during iteration)
- A timeframe safety guard filters candles that don't match the strategy's declared timeframe (defense-in-depth; the stream key already provides isolation)

**6. Strategy -> Execution -> Exchange**

When a strategy returns a `Signal` from `on_candle()`, the execution pipeline:

1. Creates a primary `Order` from the signal's entry parameters
2. Creates conditional orders for SL, TP, and Trailing Stop if specified
3. Passes each order through `risk_manager.py` for pre-trade validation
4. Calls `adapter.place_order()` which routes to `CcxtExchangeAdapter` (or `LiveBinanceAdapter`)
5. The adapter converts `LONG/SHORT` to `buy/sell` and calls the exchange API

Execution latency is measured via `time.monotonic()` and recorded to a Prometheus histogram.

## Backtest Path

```
IDataSource                 CSV, Database, Yahoo Finance, or Memory
       |
       v
backtest_runner.py          Iterate candles with circuit breaker
       |
       v
engine.py                  Same event loop as live mode
       |
       v
strategy.on_candle()       Same strategy code as live mode
       |
       v
execution.py               Same Signal -> Order pipeline
       |
       v
simulated.py               SimulatedAdapter (Python)
       |
       v
PyMatchingEngine           Rust matching engine via PyO3
  (matcher.rs)             Market/Limit/SL/TP/Trailing/OCO + fees
       |
       v
analytics.py               Sharpe, Sortino, Calmar, monthly returns
                           FIFO trade pairing, all Decimal
```

### Key Differences from Live Path

| Aspect | Live | Backtest |
|--------|------|----------|
| Data source | Redis Stream (real-time) | IDataSource (historical) |
| Adapter | CcxtExchangeAdapter | SimulatedAdapter |
| Matching | Exchange matching engine | Rust PyMatchingEngine |
| Order routing | Exchange REST/WS API | In-process Rust call |
| Latency | Network-bound (50-500ms) | CPU-bound (~11us/candle) |
| Post-analysis | Real-time dashboard | analytics.py report |

### IDataSource Implementations

The `IDataSource` interface (`src/core/interfaces/data_source.py`) abstracts historical data retrieval:

```python
class IDataSource(ABC):
    @abstractmethod
    def get_candles(self, symbol, timeframe, start, end) -> list[Candlestick]: ...

    @abstractmethod
    def get_candles_df(self, symbol, timeframe, start, end) -> pd.DataFrame: ...

    @abstractmethod
    def get_available_range(self, symbol, timeframe) -> tuple[datetime, datetime]: ...
```

Available implementations:

| Implementation | Source | Use Case |
|---------------|--------|----------|
| `DatabaseDataSource` | PostgreSQL | Production backtests with stored data |
| `CsvDataSource` | CSV files | Local development, reproducible tests |
| `YahooFinanceDataSource` | Yahoo Finance API | Quick prototyping with free data |
| `MemoryDataSource` | In-memory list | Unit testing, synthetic data |

### BacktestRunner Execution Loop

```python
# Simplified backtest loop
for candle in data_source.get_candles(symbol, tf, start, end):
    # Feed candle to simulated adapter (ticks Rust matching engine)
    await adapter.on_market_data(candle)

    # Engine dispatches to strategies (identical to live path)
    await engine.on_market_data(candle)

    # Circuit breaker: halt if drawdown exceeds threshold
    if circuit_breaker.should_halt(current_equity):
        break
```

The `BacktestRunner` wraps this loop with progress tracking, performance measurement, and report generation. After the loop completes, `analytics.py` computes trade-level and portfolio-level metrics.

### Rust Matching Engine (matcher.rs)

The `PyMatchingEngine` processes each candle against all open orders:

```
process_candle(open, high, low, close, volume, timestamp)
    |
    for each open order:
    |   -> Market order?  Fill at open price
    |   -> Limit order?   Check if price touched limit
    |   -> Stop-Loss?     Check if low breached SL level
    |   -> Take-Profit?   Check if high reached TP level
    |   -> Trailing Stop?  Adjust stop level, check trigger
    |   -> OCO?           Cancel paired order on fill
    |
    -> Apply maker/taker fees to each fill
    -> Return list of Fill results (as dicts with String values)
```

All internal arithmetic uses `rust_decimal::Decimal`. The PyO3 boundary serializes values as `String` to preserve precision when crossing into Python.

!!! note "Performance"
    The Rust matching engine processes ~89,000 candles/second with full order matching and fee calculation. A 100K-candle backtest completes in approximately 1.12 seconds.

## Monitoring Path

```
engine.py                  Heartbeat loop (periodic)
       |
       v
Redis                      Publish heartbeat + status metrics
       |
       v
dashboard/data_provider.py Read heartbeat, positions, orders from Redis
       |
       v
app.py (Streamlit)         Render real-time dashboard
```

### Heartbeat Data

The engine publishes periodic heartbeats containing:

- Active strategy count and status
- Current USDT balance (via `BALANCE_USDT` Prometheus gauge)
- Consumer lag per stream (via `CONSUMER_LAG_MS` gauge)
- Signal and order counters

### Prometheus Metrics

Six metrics are exposed on port 9090 for Prometheus scraping:

| Metric | Type | Description |
|--------|------|-------------|
| `SIGNALS_TOTAL` | Counter | Total signals emitted by strategies |
| `ORDERS_TOTAL` | Counter | Total orders placed (success/failure) |
| `EXECUTION_LATENCY` | Histogram | Time from signal to order placement |
| `BALANCE_USDT` | Gauge | Current account balance |
| `CONSUMER_LAG_MS` | Gauge | Redis consumer lag per stream |
| `ACTIVE_STRATEGIES` | Gauge | Number of running strategies |

## Redis Stream Key Format

All inter-service communication uses Redis Streams with a structured key format:

```
stream:market:{exchange}:{symbol}:{timeframe}
```

Examples:

```
stream:market:binance:BTCUSDT:1m
stream:market:binance:BTCUSDT:5m
stream:market:binance:ETHUSDT:15m
stream:market:bybit:BTCUSDT:1h
```

### Timeframe Channel Isolation

Each strategy declares the timeframe it operates on. The system enforces isolation at two levels:

**Level 1 — Stream Subscription (Primary)**

The consumer subscribes only to streams matching the strategy's declared timeframe. A strategy configured for `5m` candles subscribes to `stream:market:*:*:5m` and never sees 1m or 1h data.

**Level 2 — Engine Guard (Defense-in-Depth)**

Even if a candle somehow reaches the engine with a mismatched timeframe, the engine checks the candle's timeframe against the strategy's declaration before dispatching. This is a safety guard, not the primary filtering mechanism.

```
Strategy declares: timeframe = "5m"

Stream subscription:  stream:market:binance:BTCUSDT:5m  -- only 5m data
Engine guard:         candle.timeframe == "5m"?          -- defense-in-depth
Strategy receives:    only 5m candles, guaranteed
```

!!! tip "Why Two Levels?"
    The stream-level isolation is efficient (no unnecessary network traffic or parsing). The engine guard protects against configuration errors or future refactoring that might accidentally mix timeframes. Defense-in-depth is a core reliability principle in FluxTrade.

## Data Integrity Across the Pipeline

Every stage of the pipeline enforces `Decimal` arithmetic for financial values:

| Stage | Type | Boundary Handling |
|-------|------|-------------------|
| Rust connector | `rust_decimal::Decimal` | Parsed from exchange JSON strings |
| Rust aggregator | `rust_decimal::Decimal` | Native Decimal arithmetic |
| Redis Stream | `String` | Serialized as string, no precision loss |
| Python consumer | `decimal.Decimal` | Parsed from Redis string values |
| Python models | `decimal.Decimal` | Pydantic models with Decimal fields |
| Rust matching engine | `rust_decimal::Decimal` | String at PyO3 boundary |
| Python analytics | `decimal.Decimal` | All metric calculations in Decimal |

`float` is **never** used for any monetary value at any stage of the pipeline.
