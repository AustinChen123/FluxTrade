# Rust Bindings API Reference

**Module:** `fluxtrade_core` (compiled from `rust-data-service/src/binding/`)

The Rust matching engine is compiled as a Python extension via PyO3. It handles all order matching in backtests with bar-by-bar replay. The Python `SimulatedAdapter` delegates entirely to this engine.

**Key design principle:** All financial values use `String` at the PyO3 boundary and `rust_decimal::Decimal` internally. Python strategies pass string representations (e.g., `"50000.00"`) which are parsed into lossless `Decimal` values in Rust.

---

## PyMatchingEngine

The core matching engine. Processes candlesticks against pending orders and manages positions with full support for Market, Limit, Stop-Loss, Take-Profit, Trailing Stop, and OCO order types.

### Constructor

```python
from fluxtrade_core import PyMatchingEngine

engine = PyMatchingEngine(
    initial_balance="10000",    # Required: starting account balance
    maker_fee="0.001",          # Optional: maker fee rate (default: "0")
    taker_fee="0.001",          # Optional: taker fee rate (default: "0")
)
```

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `initial_balance` | `str` | *required* | Starting account balance (parsed to Decimal) |
| `maker_fee` | `str` | `"0"` | Maker fee rate applied to Limit order fills |
| `taker_fee` | `str` | `"0"` | Taker fee rate applied to Market/SL/TP/Trailing fills |

### Properties

| Property | Type | Description |
| :--- | :--- | :--- |
| `balance` | `str` | Current account balance (Decimal as string) |
| `positions` | `Dict[str, Position]` | Open positions keyed by `"{strategy_id}:{product_id}"` |
| `open_orders` | `List[Order]` | Pending orders awaiting matching |

### Methods

#### `submit_order(order: Order) -> str`

Add an order to the pending order book. It will be evaluated on the next `on_candle()` call.

**Parameters:**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `order` | `Order` | The order to submit |

**Returns:** `str` -- The order ID.

**Example:**

```python
from fluxtrade_core import Order

order = Order(
    id="entry-001",
    product_id="BINANCE:BTCUSDT-PERP",
    side="LONG",
    order_type="MARKET",
    price="0",
    quantity="0.1",
    timestamp=1700000000000,
    strategy_id="my_strategy",
)
engine.submit_order(order)
```

---

#### `on_candle(candle: Candlestick) -> List[FillEvent]`

Process a candlestick against all pending orders. This is the primary method called in the backtest loop.

**Processing order:**

1. Update trailing stop trigger prices based on candle high/low
2. Match **Market** orders at candle open (taker fee)
3. Match **Conditional** orders (SL/TP/Trailing) at trigger price if triggered (taker fee)
4. Match **Limit** orders at limit price if price touches (maker fee)

**Side effects per fill:**

- Position is updated (open/increase/reduce/flip/close)
- Realized PnL is added to balance on position close
- Fee is deducted from balance (capped at available balance)
- OCO linked orders are cancelled

**Parameters:**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `candle` | `Candlestick` | The candlestick to process |

**Returns:** `List[FillEvent]` -- All fills that occurred during this candle.

**Example:**

```python
from fluxtrade_core import Candlestick

candle = Candlestick(
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1m",
    timestamp=1700000060000,
    open="50000", high="50500", low="49800", close="50200",
    volume="100",
)

fills = engine.on_candle(candle)
for fill in fills:
    print(f"Order {fill.order_id}: {fill.fill_type} at {fill.price}, fee={fill.fee}")
```

---

#### `on_matching_tick(candle: Candlestick) -> List[FillEvent]`

Alias for `on_candle()`. Identical behavior and signature.

---

#### `cancel_order(order_id: str) -> bool`

Remove a pending order by ID.

**Returns:** `True` if the order was found and removed, `False` if not found.

---

#### `get_positions() -> Dict[str, Position]`

Return all open positions. Keys are `"{strategy_id}:{product_id}"`.

---

#### `get_position(strategy_id: str, product_id: str) -> Optional[Position]`

Get a specific position. Returns `None` if no position exists for the given strategy and product.

---

## Boundary Types

### Candlestick

```python
from fluxtrade_core import Candlestick

candle = Candlestick(
    product_id: str,     # "EXCHANGE:SYMBOL-PERP"
    timeframe: str,      # "1m", "5m", etc.
    timestamp: int,      # Unix ms (i64)
    open: str,           # Decimal as string
    high: str,           # Decimal as string
    low: str,            # Decimal as string
    close: str,          # Decimal as string
    volume: str,         # Decimal as string
)
```

All OHLCV fields accept `str` on construction and return `str` on access. Internally stored as `rust_decimal::Decimal`.

### Order

```python
from fluxtrade_core import Order

order = Order(
    id: str,                          # Unique order ID
    product_id: str,                  # "EXCHANGE:SYMBOL-PERP"
    side: str,                        # "LONG" or "SHORT"
    order_type: str,                  # "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
    price: str,                       # Limit/reference price (Decimal as string)
    quantity: str,                    # Order quantity (Decimal as string)
    timestamp: int,                   # Unix ms (i64)
    trigger_price: Optional[str],     # SL/TP trigger price (default: None)
    trailing_distance: Optional[str], # Trailing stop distance (default: None)
    linked_order_id: Optional[str],   # OCO linked order ID (default: None)
    strategy_id: str,                 # Strategy owner (default: "")
)
```

### FillEvent

Returned by `on_candle()` when an order is matched. All fields are read/write.

```python
fill.order_id: str          # ID of the filled order
fill.product_id: str        # Product identifier
fill.strategy_id: str       # Strategy that owns the order
fill.price: str             # Fill price (Decimal as string)
fill.quantity: str           # Fill quantity (Decimal as string)
fill.fee: str               # Fee charged (Decimal as string)
fill.timestamp: int         # Candle timestamp (i64)
fill.fill_type: str         # "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
```

### Position

Tracked internally by `PyMatchingEngine`. Keyed by `"{strategy_id}:{product_id}"`.

```python
pos.product_id: str         # Product identifier
pos.strategy_id: str        # Strategy owner
pos.side: str               # "LONG", "SHORT", or "FLAT"
pos.quantity: str            # Position size (Decimal as string)
pos.entry_price: str        # Average entry price (Decimal as string)
pos.unrealized_pnl: str     # Unrealized PnL (Decimal as string)
```

---

## Matching Logic Reference

### Fill Prices by Order Type

| Order Type | Fill Price | Fee Type | Trigger Condition (LONG) | Trigger Condition (SHORT) |
| :--- | :--- | :--- | :--- | :--- |
| `MARKET` | candle.open | taker | Always (next candle) | Always (next candle) |
| `LIMIT` | order.price | maker | candle.low <= order.price | candle.high >= order.price |
| `STOP_LOSS` | trigger_price | taker | candle.low <= trigger_price | candle.high >= trigger_price |
| `TAKE_PROFIT` | trigger_price | taker | candle.high >= trigger_price | candle.low <= trigger_price |
| `TRAILING_STOP` | trigger_price | taker | candle.low <= trigger_price | candle.high >= trigger_price |

### Trailing Stop Ratchet

Before matching, trailing stops are updated:

- **LONG:** `new_trigger = candle.high - trailing_distance`. Only applied if `new_trigger > current_trigger` (ratchets up).
- **SHORT:** `new_trigger = candle.low + trailing_distance`. Only applied if `new_trigger < current_trigger` (ratchets down).

### Position Updates

| Scenario | Behavior |
| :--- | :--- |
| No existing position | Open new position at fill price |
| Same side | Increase position with weighted average entry price |
| Opposite side, partial | Reduce position, realize PnL on closed portion |
| Opposite side, full | Close position, realize PnL |
| Opposite side, excess | Flip position: close old, open new with excess quantity |
| SL/TP/Trailing fill | Close (partial or full), realize PnL |

### Fee Formula

```
fee = price * quantity * rate
```

Where `rate` is `taker_fee` for Market/SL/TP/Trailing, `maker_fee` for Limit. Fee is capped at available balance (`min(fee, balance)`).
