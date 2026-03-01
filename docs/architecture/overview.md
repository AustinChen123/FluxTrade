# FluxTrade Architecture Overview

## System Diagram

```
                         +---------------------+
                         |   Exchange APIs      |
                         | Binance/Bybit/Backpack|
                         +----------+----------+
                                    |
                          WebSocket | REST (backfill)
                                    |
                         +----------v----------+
                         |  Rust Data Service   |
                         |                      |
                         |  connector/*.rs      |  WebSocket handlers
                         |  aggregator/mod.rs   |  1m -> 5m/15m/1h bucketing
                         |  publisher/mod.rs    |  Redis Stream publish
                         |  historical/mod.rs   |  REST candle backfill
                         |  watchdog.rs         |  Heartbeat monitoring
                         +----------+-----------+
                                    |
                           Redis Streams (ordered, persistent)
                           stream:market:{exchange}:{symbol}:{tf}
                                    |
                         +----------v-----------+
                         | Python Strategy Svc   |
                         |                       |
                         |  consumer.py          |  XREADGROUP consumer
                         |  engine.py            |  Event-driven core
                         |  execution.py         |  Signal -> Order -> Fill
                         |  risk_manager.py      |  Balance/position checks
                         |  backtest_runner.py   |  Backtesting framework
                         |  analytics.py         |  Sharpe/Sortino/Calmar
                         +---+-------------+-----+
                             |             |
                    +--------v---+   +-----v--------+
                    | PostgreSQL |   | Exchange API  |
                    | (persist)  |   | (live orders) |
                    +--------+---+   +--------------+
                             |
                    +--------v-----------+
                    | Streamlit Dashboard |
                    | (monitoring & viz)  |
                    +--------------------+
```

## Rust Data Service (`rust-data-service/`)

The Rust service handles all real-time market data ingestion and serves as the high-performance matching engine for backtesting via PyO3.

### Module Breakdown

| Module | File | Responsibility |
|--------|------|----------------|
| **Entry Point** | `src/main.rs` | Tokio runtime, signal handler, graceful shutdown |
| **PyO3 Bridge** | `src/lib.rs` | Exports `fluxtrade_core` Python module |
| **Matching Engine** | `src/binding/matcher.rs` | Market/Limit/SL/TP/Trailing/OCO matching, all Decimal arithmetic |
| **Data Models** | `src/binding/models.rs` | PyO3 models with String boundary, Decimal internals |
| **Connectors** | `src/connector/*.rs` | Exchange WebSocket handlers (Binance, Bybit, Backpack) |
| **Aggregator** | `src/aggregator/mod.rs` | K-line bucketing from 1-minute bars to 5m/15m/1h |
| **Publisher** | `src/publisher/mod.rs` | Redis Stream publishing via bounded mpsc channel |
| **Historical** | `src/historical/mod.rs` | REST-based historical candle backfill with configurable concurrency |
| **Watchdog** | `src/watchdog.rs` | Python heartbeat monitoring, exchange reconnect triggers |

### Connector Architecture

Each exchange connector implements a common pattern:

1. Establish WebSocket connection to the exchange
2. Subscribe to trade/kline streams for configured symbols
3. Parse exchange-specific JSON into unified internal candle format
4. Forward parsed data to the aggregator

The connectors handle reconnection, ping/pong keepalive (via the write half of the split WebSocket), and deduplication of trade data.

### Aggregator (K-Line Bucketing)

The aggregator receives 1-minute candles from connectors and maintains rolling buckets for higher timeframes:

```
1m candle in -> update 5m bucket
                update 15m bucket
                update 1h bucket

when bucket boundary crossed -> emit completed higher-TF candle
```

Each bucket tracks OHLCV data using Decimal arithmetic. An OHLC invariant check ensures `low <= open,close <= high` before any candle is emitted.

### Publisher (Redis Stream)

The publisher receives completed candles via a bounded `mpsc` channel (`PublishMessage` enum, 10K capacity) and writes them to Redis Streams. Stream keys encode the full context:

```
stream:market:{exchange}:{symbol}:{timeframe}
```

This design replaced an earlier `Arc<Mutex<RedisPublisher>>` with a lock-free channel architecture.

### Task Supervision

All async tasks (connectors, aggregator, publisher, watchdog) run under a `JoinSet` supervisor with:

- `TaskId` enum for identification
- `TaskFailureTracker` with exponential backoff
- 3 consecutive failures trigger graceful shutdown
- Panic in any task triggers immediate shutdown

## Python Strategy Service (`python-strategy/`)

The Python service contains the trading logic, execution pipeline, risk management, and backtesting infrastructure.

### Core Engine (`src/core/`)

| Module | Responsibility |
|--------|----------------|
| `engine.py` | Event-driven core: receives market data, dispatches to strategies, manages lifecycle |
| `execution.py` | Signal-to-Order-to-Fill pipeline: creates SL/TP/Trailing conditional orders from signals |
| `risk_manager.py` | Pre-trade validation: balance checks, position limits, exposure calculation |
| `order_manager.py` | Order lifecycle management with Redis Lua atomic operations |
| `consumer.py` | Redis Stream XREADGROUP consumer, candle parsing and timeframe synthesis |
| `backtest_runner.py` | Backtesting framework: IDataSource -> candle loop -> circuit breaker -> report |
| `analytics.py` | Post-trade analytics: Sharpe, Sortino, Calmar, monthly returns, FIFO trade pairing |
| `models.py` | Pydantic data models (Candlestick, Order, Trade, Signal, Position), all Decimal |
| `journal.py` | Structured trade event logging to JSONL |

### Interfaces (`src/core/interfaces/`)

Three core abstractions decouple the system:

**IExchangeAdapter** (`interfaces/exchange.py`):

```python
class IExchangeAdapter(ABC):
    @abstractmethod
    def place_order(self, order: Order) -> str: ...
    @abstractmethod
    def cancel_order(self, order_id: str, product_id: str) -> bool: ...
    @abstractmethod
    def get_balance(self, asset: str) -> Decimal: ...
    @abstractmethod
    def get_position(self, product_id: str) -> Optional[Position]: ...
    def on_market_data(self, candle: Candlestick) -> List[Dict]: ...
```

**IDataSource** (`interfaces/data_source.py`):

```python
class IDataSource(ABC):
    @abstractmethod
    def get_candles(self, product_id: str, timeframe: str, start: int, end: int) -> Generator[Candlestick, None, None]: ...
    @abstractmethod
    def get_candles_df(self, product_id: str, timeframe: str, start: int, end: int) -> pd.DataFrame: ...
    @abstractmethod
    def get_available_range(self, product_id: str, timeframe: str) -> Optional[tuple[int, int]]: ...
```

**IOrderRepository** (`interfaces/repository.py`):

```python
class IOrderRepository(ABC):
    @abstractmethod
    def add_order(self, order: Order) -> None: ...
    @abstractmethod
    def update_order(self, order: Order) -> None: ...
    @abstractmethod
    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None: ...
    @abstractmethod
    def add_trade(self, trade: Trade) -> None: ...
    @abstractmethod
    def update_position(self, strategy_id: str, product_id: str, side: str, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None: ...
    @abstractmethod
    def get_position(self, strategy_id: str, product_id: str, side: str) -> Optional[Position]: ...
```

### Strategies (`src/strategies/`)

All strategies extend `BaseStrategy` and implement `on_candle()`. They emit `Signal` objects with entry parameters, stop-loss, take-profit, and trailing stop configuration. Strategies never manage order lifecycle directly.

Available strategies: `golden_cross`, `rsi_scalper`, `bb_reversion`, `macd_momentum`, `market_structure_strategy`, `smc_strategy`, `callable_strategy`, `csv_signal_strategy`.

Hot-pluggable strategies can be loaded at runtime from the `strategies_hot/` directory without system restart.

## Key Design Decisions

### Why Rust for the Matching Engine

The matching engine processes every candle in a backtest against all open orders. At 100K candles, this is the tightest loop in the system. The Rust implementation achieves **~89K candles/second** with full order matching (Market, Limit, SL, TP, Trailing Stop, OCO) and fee calculation — all using Decimal arithmetic.

The PyO3 bridge exposes `PyMatchingEngine` to Python, keeping the hot path in Rust while the strategy logic remains in Python for rapid iteration.

!!! note "Compilation"
    The Rust library is compiled as a shared object (`fluxtrade_core.so`) and loaded directly by Python. Do **not** use `maturin develop` due to edition2024 transitive dependency issues. Instead, compile with `cargo build --lib --release` and copy the `.dylib` to the Python source directory.

### Why the Adapter Pattern

The core promise of FluxTrade is **live trading = backtesting**. The same strategy code runs in both modes without modification. This is achieved through `IExchangeAdapter`:

- **Live**: `CcxtExchangeAdapter` calls real exchange APIs
- **Backtest**: `SimulatedAdapter` delegates to the Rust `PyMatchingEngine`

Strategies call `adapter.place_order()` and never know which mode they are in. See [Adapter Pattern](adapter-pattern.md) for details.

### Why Redis Streams

Redis Streams were chosen over Pub/Sub or message queues for three reasons:

1. **Ordered**: Messages are strictly ordered by stream ID, critical for candle sequencing
2. **Persistent**: Messages survive consumer disconnects; consumers can resume from their last-read position via consumer groups (`XREADGROUP`)
3. **Consumer Groups**: Multiple strategy instances can independently consume the same stream, each tracking their own offset

Stream keys encode exchange, symbol, and timeframe, enabling **timeframe channel isolation**: each strategy only receives candles matching its declared timeframe, with the engine providing a safety guard as defense-in-depth.

### Data Integrity: Decimal Everywhere

All financial calculations use `Decimal` (Python) or `rust_decimal::Decimal` (Rust). Float is **forbidden** for monetary values. The PyO3 boundary uses `String` serialization to preserve precision across the language boundary.

## Test Coverage

- **750 total tests**: 616 Python unit + 69 integration + 65 Rust
- **Python coverage**: 78.26% (CI gate: 77%)
- **Key test patterns**: Factory-based fixtures via `conftest.py` (700+ lines), `spec`-based mocks, selective failure injection via `MockExchangeAdapter`
