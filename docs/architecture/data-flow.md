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
engine.py               Dispatch candle to active strategies (state-guarded)
       |
       v
strategy.on_candle()    Strategy evaluates indicators, may emit Signal
       |
       v
signal_processor.py     Block stopped/error strategies before execution
       |
       v
execution.py            Signal -> Order creation (with SL/TP/Trailing conditionals)
       |
       v
risk_manager.py         Rule-based validation: balance, notional, price, rate, daily loss
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
- `StrategyStateManager` state is consulted so stopped/error strategies do not emit executable signals

**6. Strategy -> Execution -> Exchange**

When a strategy returns a `Signal` from `on_candle()`, the execution pipeline:

1. Creates a primary `Order` from the signal's entry parameters
2. Creates conditional orders for SL, TP, and Trailing Stop if specified
3. Passes each order through `risk_manager.py` for pre-trade validation
4. Calls `adapter.place_order()` which routes to `CcxtExchangeAdapter` (or `LiveBinanceAdapter`)
5. The adapter converts `LONG/SHORT` to `buy/sell` and calls the exchange API

Execution latency is measured via `time.monotonic()` and recorded to a Prometheus histogram.

Risk validation is rule based. The current checks include balance, single-order notional, daily-loss circuit breaker, optional price sanity, max-position notional, and optional order-rate limiting. Violations return `(False, reason)` and block execution; they do not resize orders silently.

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
BacktestAccountService     Reads balance/position from SimulatedAdapter
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
                           average-entry trade pairing, all Decimal
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
    def get_candles(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> Generator[Candlestick, None, None]: ...

    @abstractmethod
    def get_candles_df(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> pd.DataFrame: ...

    @abstractmethod
    def get_available_range(
        self, product_id: str, timeframe: str
    ) -> Optional[tuple[int, int]]: ...
```

Note: `get_candles()` returns a **Generator** (not a list), yielding `Candlestick` objects ordered by timestamp ascending. The `start` and `end` parameters are millisecond timestamps (int), and `get_available_range()` returns `Optional[tuple[int, int]]` (min/max ms timestamps) or `None` if no data exists.

Available implementations:

| Implementation | Source | Use Case |
|---------------|--------|----------|
| `DatabaseDataSource` | PostgreSQL | Production backtests with stored data |
| `CsvDataSource` | CSV files | Local development, reproducible tests |
| `YahooFinanceDataSource` | Yahoo Finance API | Quick prototyping with free data |
| `MemoryDataSource` | In-memory list | Unit testing, synthetic data |

### BacktestRunner Execution Loop

```python
# Simplified backtest loop (synchronous, not async)
stop_threshold = initial_balance * (1 - max_drawdown_limit)

for candle in data_source.get_candles(product_id, tf, start, end):
    # Update simulation clock
    self.clock.set_time(candle.timestamp / 1000)

    # Engine dispatches to strategies + adapter processes fills
    self.engine.on_market_data(candle)

    # Circuit breaker: halt if balance drops below threshold
    current_balance = mock_account.get_balance()
    if current_balance < stop_threshold:
        break
```

The loop is **synchronous** (no `await`). It calls only `engine.on_market_data(candle)`, which internally dispatches to strategies and the adapter. The circuit breaker compares the current balance against a pre-computed `stop_threshold` (initial balance times one minus max drawdown limit) -- there is no `.should_halt()` method.

The `BacktestRunner` wraps this loop with progress tracking, performance measurement, and report generation. After the loop completes, `analytics.py` computes trade-level and portfolio-level metrics.

In backtests, the Rust matcher is the single source of truth for balances and positions. `BacktestAccountService` reads through `SimulatedAdapter`, and RiskManager checks in backtest mode therefore see the matcher-backed account state. The invariant suite verifies this after fills and during position-limit checks.

### Rust Matching Engine

The `SimulatedAdapter` delegates all matching to `PyMatchingEngine` in Rust. For each candle, the engine processes open orders by priority (Market > SL/TP/Trailing > Limit), applies fees, manages positions, and returns fill events.

!!! note "Performance"
    ~89,000 candles/second with full order matching and fee calculation. A 100K-candle backtest completes in approximately 1.12 seconds.

For detailed matching logic, order types, and position management, see [Rust Matching Engine](rust-engine.md).

Backtest PnL currently uses average-entry netting. The matcher balance delta should equal recomputed closed-trade PnL minus fill fees within one satoshi for deterministic fill sequences.

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

The engine's `_start_heartbeat()` runs a background thread that performs three operations every second:

1. **Redis key**: Sets `heartbeat:python` with a 3-second TTL (`self.redis_client.setex("heartbeat:python", 3, "1")`)
2. **Prometheus gauge**: Updates `BALANCE_USDT` with the current account balance
3. **DB last_heartbeat**: Updates `StrategyState.last_heartbeat` for all active strategies with the current timestamp

The heartbeat does **not** publish strategy count, consumer lag, or signal counters.

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
