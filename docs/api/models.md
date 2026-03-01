# Models API Reference

**Module:** `src.core.models`

All financial values use `decimal.Decimal` -- `float` is forbidden for monetary calculations. All models extend `BaseFluxModel`, which provides automatic `Decimal -> str` serialization when converting to JSON.

The `product_id` field on all models is validated against the regex pattern `^[A-Z0-9]+:[A-Z0-9_]+-PERP$` (e.g., `BINANCE:BTCUSDT-PERP`).

---

## Enums

### `OrderSide(str, Enum)`

Direction of an order. Inherits from `str` for backward compatibility with string comparisons.

| Member | Value | Description |
| :--- | :--- | :--- |
| `BUY` | `"buy"` | Buy order (opens LONG or closes SHORT) |
| `SELL` | `"sell"` | Sell order (opens SHORT or closes LONG) |

**Static Methods:**

| Method | Signature | Description |
| :--- | :--- | :--- |
| `from_position_side` | `(ps: PositionSide) -> OrderSide` | LONG -> BUY, SHORT -> SELL |
| `closing_side` | `(ps: PositionSide) -> OrderSide` | LONG -> SELL, SHORT -> BUY |

### `PositionSide(str, Enum)`

Direction of a position. Inherits from `str` for backward compatibility.

| Member | Value | Description |
| :--- | :--- | :--- |
| `LONG` | `"LONG"` | Long position |
| `SHORT` | `"SHORT"` | Short position |

**Static Methods:**

| Method | Signature | Description |
| :--- | :--- | :--- |
| `from_order_side` | `(os: OrderSide) -> PositionSide` | BUY -> LONG, SELL -> SHORT |

### `SignalType(str, Enum)`

The intent of a trading signal.

| Member | Value | Description |
| :--- | :--- | :--- |
| `LONG` | `"LONG"` | Open a long position |
| `SHORT` | `"SHORT"` | Open a short position |
| `EXIT_LONG` | `"EXIT_LONG"` | Close a long position |
| `EXIT_SHORT` | `"EXIT_SHORT"` | Close a short position |
| `NO_SIGNAL` | `"NO_SIGNAL"` | No action required |

### `StrategyStatus(str, Enum)`

Lifecycle states for a hot-pluggable strategy.

| Member | Value | Description |
| :--- | :--- | :--- |
| `DISCOVERED` | `"DISCOVERED"` | Strategy file detected but not yet loaded |
| `READY` | `"READY"` | Strategy loaded and validated |
| `WARNING` | `"WARNING"` | Strategy running but with warnings |
| `ACTIVE` | `"ACTIVE"` | Strategy actively processing candles |
| `STOPPED` | `"STOPPED"` | Strategy explicitly stopped |
| `ERROR` | `"ERROR"` | Strategy encountered a fatal error |

---

## Base Model

### `BaseFluxModel(BaseModel)`

Base model with common configuration for all FluxTrade Pydantic models.

- `populate_by_name = True` -- Allows population by alias or field name.
- Automatic `Decimal -> str` serialization in JSON mode via `serialize_decimal` class method.

---

## Pydantic Models

### `Trade`

Represents a single trade execution.

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `id` | `str` | *required* | Unique trade identifier |
| `product_id` | `str` | *required* | Product identifier (validated) |
| `price` | `Decimal` | *required* | Execution price |
| `quantity` | `Decimal` | *required* | Trade quantity |
| `side` | `OrderSide` | *required* | `BUY` or `SELL` |
| `timestamp` | `int` | *required* | Unix timestamp in milliseconds |

### `Candlestick`

Represents a single OHLCV candle.

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `product_id` | `str` | *required* | Product identifier (validated) |
| `timeframe` | `str` | *required* | e.g., `1m`, `5m`, `1h` |
| `timestamp` | `int` | *required* | Unix timestamp in milliseconds (open time) |
| `open` | `Decimal` | *required* | Open price |
| `high` | `Decimal` | *required* | High price |
| `low` | `Decimal` | *required* | Low price |
| `close` | `Decimal` | *required* | Close price |
| `volume` | `Decimal` | *required* | Volume in base asset |

### `Signal`

The output of a strategy's decision logic. Contains all parameters needed for order creation including risk management (SL/TP/Trailing).

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `strategy_id` | `str` | *required* | Strategy that generated this signal |
| `product_id` | `str` | *required* | Target product (validated) |
| `timeframe` | `str` | *required* | Candle timeframe that triggered this signal |
| `timestamp` | `int` | *required* | Creation timestamp in milliseconds |
| `type` | `SignalType` | *required* | Action to take (LONG, SHORT, EXIT_LONG, EXIT_SHORT, NO_SIGNAL) |
| `value` | `Optional[Decimal]` | `None` | Indicator value for logging. Also used as limit price fallback when `price` is not set. |
| `quantity` | `Optional[Decimal]` | `None` | Position size. If `None`, determined by execution/risk layer. |
| `price` | `Optional[Decimal]` | `None` | Explicit entry price (limit order). Takes priority over `value`. |
| `stop_loss` | `Optional[Decimal]` | `None` | Stop-loss price level |
| `take_profit` | `Optional[Decimal]` | `None` | Take-profit price level |
| `trailing_distance` | `Optional[Decimal]` | `None` | Trailing stop distance from price |
| `metadata` | `Optional[dict]` | `None` | Key-value pairs for debugging or logging |

### `Position`

Represents the current holding state for a strategy on a product.

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `strategy_id` | `str` | *required* | Owner strategy |
| `product_id` | `str` | *required* | Product identifier (validated) |
| `side` | `PositionSide` | *required* | `LONG` or `SHORT` (enum type) |
| `quantity` | `Decimal` | *required* | Absolute position size (positive) |
| `entry_price` | `Decimal` | *required* | Average entry price |
| `unrealized_pnl` | `Decimal` | *required* | Estimated unrealized PnL (snapshot) |

---

## Analytics Models

**Module:** `src.core.analytics`

### `ClosedTrade` (dataclass)

A completed round-trip trade with entry/exit details, built from FIFO netting of raw trades. Uses `@dataclass(slots=True)` for memory efficiency.

| Field | Type | Description |
| :--- | :--- | :--- |
| `entry_time` | `int` | Entry timestamp in milliseconds |
| `exit_time` | `int` | Exit timestamp in milliseconds |
| `entry_price` | `Decimal` | Average entry price |
| `exit_price` | `Decimal` | Average exit price |
| `side` | `PositionSide` | `LONG` or `SHORT` |
| `quantity` | `Decimal` | Trade quantity |
| `pnl` | `Decimal` | Realized profit/loss |
