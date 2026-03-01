# Interfaces API Reference

FluxTrade defines three core interfaces (Abstract Base Classes) that decouple the strategy engine from specific implementations. This enables the same strategy code to run against live exchanges, simulated backtests, or any custom backend.

---

## IExchangeAdapter

**Module:** `src.core.interfaces.exchange`

Interface for exchange adapters (real and simulated). Decouples order execution from specific exchange implementations.

### Exceptions

| Exception | Parent | Description |
| :--- | :--- | :--- |
| `ExchangeError` | `Exception` | Base exception for all exchange-related errors |
| `InsufficientFundsError` | `ExchangeError` | Raised when the account has insufficient funds for the order |
| `NetworkError` | `ExchangeError` | Raised when there is a network connectivity issue with the exchange |

### Abstract Methods

#### `place_order(order: Order) -> str`

Place an order on the exchange.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `order` | `orm_models.Order` | The internal Order object (ORM model) containing all details |

**Returns:** `str` -- The exchange's order ID.

**Raises:** `ExchangeError` if the order fails.

---

#### `cancel_order(order_id: str, product_id: str) -> bool`

Cancel an existing order.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `order_id` | `str` | The exchange's order ID (not internal DB ID) |
| `product_id` | `str` | The product/symbol identifier (e.g., `BINANCE:BTCUSDT-PERP`) |

**Returns:** `bool` -- `True` if cancellation was successful, `False` otherwise.

---

#### `get_balance(asset: str) -> Decimal`

Retrieve the available balance for a specific asset.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `asset` | `str` | The asset symbol (e.g., `USDT`, `BTC`) |

**Returns:** `Decimal` -- The available balance.

---

#### `get_position(product_id: str) -> Optional[Position]`

Retrieve the current open position for a product.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `product_id` | `str` | The product/symbol identifier |

**Returns:** `Optional[Position]` -- The position details (Pydantic `models.Position`) or `None` if no position.

### Concrete Methods

#### `on_market_data(candle: Candlestick) -> List[Dict]`

Process market data to check for simulated order fills. Override in simulated adapters to implement matching logic. Live adapters return an empty list (the exchange manages SL/TP natively).

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `candle` | `models.Candlestick` | The latest candlestick data |

**Returns:** `List[Dict]` -- List of fill dicts with keys: `order`, `price`, `quantity`, `fee`, `fill_type`. Returns `[]` by default.

### Implementations

| Class | Module | Description |
| :--- | :--- | :--- |
| `SimulatedAdapter` | `src.core.adapters.simulated` | Delegates to Rust `PyMatchingEngine` for backtest matching |
| `CcxtExchangeAdapter` | `src.core.adapters.ccxt_adapter` | Generic CCXT-based live exchange adapter |
| `LiveBinanceAdapter` | `src.core.adapters.live_binance` | Extends `CcxtExchangeAdapter` with WebSocket fast path |

---

## IDataSource

**Module:** `src.core.interfaces.data_source`

Abstract data source for candlestick data. Implementations provide candle data from various backends through a unified interface.

### Abstract Methods

#### `get_candles(product_id: str, timeframe: str, start: int, end: int) -> Generator[Candlestick, None, None]`

Yield `Candlestick` objects ordered by timestamp ascending.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `product_id` | `str` | Product identifier (e.g., `BINANCE:BTCUSDT-PERP`) |
| `timeframe` | `str` | Candle timeframe (e.g., `1m`, `5m`, `15m`) |
| `start` | `int` | Start timestamp in milliseconds (inclusive) |
| `end` | `int` | End timestamp in milliseconds (inclusive) |

**Returns:** `Generator[Candlestick, None, None]`

---

#### `get_candles_df(product_id: str, timeframe: str, start: int, end: int) -> pd.DataFrame`

Return a DataFrame with OHLCV columns indexed by timestamp.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `product_id` | `str` | Product identifier |
| `timeframe` | `str` | Candle timeframe |
| `start` | `int` | Start timestamp in milliseconds (inclusive) |
| `end` | `int` | End timestamp in milliseconds (inclusive) |

**Returns:** `pd.DataFrame` -- Columns: `open`, `high`, `low`, `close`, `volume` (float). Index: `timestamp` (int, milliseconds).

---

#### `get_available_range(product_id: str, timeframe: str) -> Optional[tuple[int, int]]`

Return the available data range.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `product_id` | `str` | Product identifier |
| `timeframe` | `str` | Candle timeframe |

**Returns:** `Optional[tuple[int, int]]` -- `(min_timestamp, max_timestamp)` or `None` if no data exists.

### Concrete Methods

#### `validate() -> bool`

Check if data source is accessible and contains valid data. Returns `True` by default.

### Implementations

| Class | Module | Description |
| :--- | :--- | :--- |
| `DatabaseDataSource` | `src.core.data_sources` | Reads from PostgreSQL via ORM |
| `CsvDataSource` | `src.core.data_sources` | Reads from CSV files |
| `YahooFinanceDataSource` | `src.core.data_sources` | Fetches from Yahoo Finance API |
| `MemoryDataSource` | `src.core.data_sources` | In-memory data for testing |

---

## IOrderRepository

**Module:** `src.core.interfaces.repository`

Persistence interface for orders, trades, and positions. All types referenced here are ORM models from `src.core.orm_models`, not the Pydantic models in `src.core.models`.

### Abstract Methods

#### `add_order(order: Order) -> None`

Persist a new order.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `order` | `orm_models.Order` | The order to persist |

---

#### `update_order(order: Order) -> None`

Update an existing order's fields (e.g., status, filled quantity).

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `order` | `orm_models.Order` | The order with updated fields |

---

#### `update_order_exchange_id(order: Order, exchange_order_id: str) -> None`

Set the exchange-assigned order ID after placement.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `order` | `orm_models.Order` | The order to update |
| `exchange_order_id` | `str` | The exchange-assigned order ID |

---

#### `add_trade(trade: Trade) -> None`

Persist a trade fill.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `trade` | `orm_models.Trade` | The trade to persist |

---

#### `update_position(strategy_id: str, product_id: str, side: str, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None`

Update or create a position record after a fill.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `strategy_id` | `str` | The strategy that owns this position |
| `product_id` | `str` | The product/symbol identifier |
| `side` | `str` | The order side (`"buy"` or `"sell"`) |
| `fill_quantity` | `Decimal` | The filled quantity |
| `fill_price` | `Decimal` | The fill price |
| `position_side` | `str` | The position side (`"LONG"` or `"SHORT"`) |

---

#### `get_position(strategy_id: str, product_id: str, side: str) -> Optional[Position]`

Retrieve a position by strategy, product, and side.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `strategy_id` | `str` | The strategy identifier |
| `product_id` | `str` | The product/symbol identifier |
| `side` | `str` | The position side (`"LONG"` or `"SHORT"`) |

**Returns:** `Optional[orm_models.Position]` -- The position or `None` if not found.

### Implementations

| Class | Module | Description |
| :--- | :--- | :--- |
| `LiveOrderRepository` | `src.core.repositories` | PostgreSQL-backed persistence via SQLAlchemy |
| `BacktestOrderRepository` | `src.core.repositories` | In-memory storage for backtests |
