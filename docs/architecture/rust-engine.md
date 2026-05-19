# Rust Matching Engine

The Rust matching engine (`PyMatchingEngine`) is the core of FluxTrade's backtesting system. It handles all order matching, position management, fee calculation, and balance tracking using `rust_decimal::Decimal` arithmetic. It is exposed to Python via PyO3 as the `fluxtrade_core` module.

## PyMatchingEngine

Defined in `src/binding/matcher.rs`:

```rust
#[pyclass]
pub struct PyMatchingEngine {
    pub balance: Decimal,
    pub positions: HashMap<String, Position>,
    pub open_orders: Vec<Order>,
    maker_fee: Decimal,
    taker_fee: Decimal,
}
```

### Constructor

```python
PyMatchingEngine(
    initial_balance: str,          # e.g., "100000"
    maker_fee: str = "0",          # e.g., "0.0002"
    taker_fee: str = "0",          # e.g., "0.0004"
)
```

All parameters are strings. They are parsed to `rust_decimal::Decimal` internally. Invalid decimal strings raise `PyValueError`.

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `submit_order` | `(order: Order) -> str` | Add an order to the open orders list. Returns the order ID. |
| `on_candle` | `(candle: Candlestick) -> list[FillEvent]` | Process a candle against all open orders. Returns fills. |
| `cancel_order` | `(order_id: str) -> bool` | Remove an order by ID. Returns True if found and removed. |
| `get_positions` | `() -> dict[str, Position]` | Return all positions as a dict. |
| `get_position` | `(strategy_id: str, product_id: str) -> Optional[Position]` | Return position for a specific strategy/product pair. |

### Properties

| Property | Type (Python) | Description |
|----------|---------------|-------------|
| `balance` | `str` | Current account balance as a decimal string. |
| `positions` | `dict[str, Position]` | All positions keyed by `strategy_id:product_id`. |
| `open_orders` | `list[Order]` | All pending orders. |

## Order Types

The engine supports five order types, processed in strict priority order within each candle:

### 1. Market Orders (highest priority)

- Fill at the candle's **open** price
- Charged **taker fee**
- Processed first to simulate immediate execution

### 2. Conditional Orders (Stop-Loss, Take-Profit, Trailing Stop)

- Processed after market orders, before limit orders
- Charged **taker fee** (market-like execution on trigger)

**Stop-Loss**:

- For LONG positions: triggers when `candle.low <= trigger_price`
- For SHORT positions: triggers when `candle.high >= trigger_price`
- Fills at the `trigger_price`

**Take-Profit**:

- For LONG positions: triggers when `candle.high >= trigger_price`
- For SHORT positions: triggers when `candle.low <= trigger_price`
- Fills at the `trigger_price`

**Trailing Stop**:

- Before matching, `update_trailing_stops()` adjusts trigger prices:
    - For LONG: `new_trigger = candle.high - trailing_distance` (only moves up)
    - For SHORT: `new_trigger = candle.low + trailing_distance` (only moves down)
- Trigger logic is identical to Stop-Loss after adjustment

### 3. Limit Orders (lowest priority)

- For LONG: fills when `candle.low <= order.price`
- For SHORT: fills when `candle.high >= order.price`
- Fills at the **order price** (not the candle price)
- Charged **maker fee**

### 4. OCO (One-Cancels-Other)

OCO is not a separate order type but a linking mechanism. Orders have an optional `linked_order_id` field. When an order fills, its linked counterpart is marked for cancellation via a `HashSet<String>`. After all matching is complete, cancelled IDs are filtered from remaining orders.

Typical usage: SL and TP orders are linked as an OCO pair. When SL fills, TP is cancelled (and vice versa).

## Position Management

Positions are stored in a `HashMap<String, Position>` keyed by composite keys:

```
{strategy_id}:{product_id}
```

This enables **multi-strategy isolation**: two strategies trading the same product maintain separate positions.

### Position State Machine

```
FLAT -> LONG  (Market/Limit buy)
FLAT -> SHORT (Market/Limit sell)
LONG -> FLAT  (SL/TP/Trailing close, or opposing Market/Limit that fully closes)
SHORT -> FLAT (SL/TP/Trailing close, or opposing Market/Limit that fully closes)
LONG -> SHORT (opposing order exceeds position size -- flip)
SHORT -> LONG (opposing order exceeds position size -- flip)
```

### Position Update Logic

**Opening / Increasing** (same side as existing position):

- Weighted average entry price: `(old_qty * old_entry + new_qty * fill_price) / total_qty`

**Closing** (conditional orders: SL/TP/Trailing):

- PnL realized: `(fill_price - entry_price) * close_qty` for LONG, inverted for SHORT
- Realized PnL added to balance
- Position quantity reduced; set to FLAT if fully closed

**Reducing / Flipping** (opposing Market/Limit order):

- Partial close: realize PnL on the closed portion
- If order quantity exceeds position: close existing, open new position in opposite direction with the excess

### Position Struct

```rust
#[pyclass]
pub struct Position {
    pub product_id: String,
    pub strategy_id: String,
    pub side: String,             // "LONG", "SHORT", or "FLAT"
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub unrealized_pnl: Decimal,
}
```

## FillEvent Structure

Every fill produces a `FillEvent`:

```rust
#[pyclass]
pub struct FillEvent {
    pub order_id: String,
    pub product_id: String,
    pub strategy_id: String,
    pub price: Decimal,           // Fill price
    pub quantity: Decimal,        // Filled quantity
    pub fee: Decimal,             // Calculated fee
    pub timestamp: i64,           // Candle timestamp
    pub fill_type: String,        // "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
}
```

All `Decimal` fields are exposed to Python as `String` via PyO3 getters.

## Fee Calculation

Fees are computed as:

```
fee = price * quantity * rate
```

Where `rate` is:

- `taker_fee` for Market, Stop-Loss, Take-Profit, and Trailing Stop orders
- `maker_fee` for Limit orders

Fees are capped at the current balance (`min(fee, balance)`) to prevent negative balances, then deducted from the engine's balance after each fill.

**Example**: Market buy 0.5 BTC at $50,000, taker fee 0.04%:

```
fee = 50000 * 0.5 * 0.0004 = 10.0 USDT
```

The fill event reports `fee = "10.0"` and the engine deducts 10 USDT from balance.

## Order Struct

```rust
#[pyclass]
pub struct Order {
    pub id: String,
    pub product_id: String,
    pub strategy_id: String,       // Used for per-strategy position tracking
    pub side: String,              // "LONG" or "SHORT"
    pub order_type: String,        // "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
    pub price: Decimal,            // Limit price (for LIMIT orders)
    pub quantity: Decimal,
    pub timestamp: i64,
    pub trigger_price: Option<Decimal>,      // For SL/TP conditional orders
    pub trailing_distance: Option<Decimal>,  // For trailing stop
    pub linked_order_id: Option<String>,     // For OCO pairing
}
```

### PyO3 Constructor Signature

```python
Order(
    id: str,
    product_id: str,
    side: str,
    order_type: str,
    price: str,
    quantity: str,
    timestamp: int,
    trigger_price: Optional[str] = None,
    trailing_distance: Optional[str] = None,
    linked_order_id: Optional[str] = None,
    strategy_id: str = "",
)
```

## Candlestick Struct

```rust
#[pyclass]
pub struct Candlestick {
    pub product_id: String,
    pub timeframe: String,
    pub timestamp: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}
```

All OHLCV fields are `String` at the PyO3 boundary and `Decimal` internally.

## PyO3 Boundary Contract

The Rust engine uses **String at the boundary, Decimal internally**:

| Direction | Conversion |
|-----------|------------|
| Python -> Rust | Python passes `str` values; Rust parses to `rust_decimal::Decimal` via `Decimal::from_str()` |
| Rust -> Python | Rust exposes `Decimal` fields as `String` via `#[getter]` methods that call `.to_string()` |
| Error handling | Invalid decimal strings raise `PyValueError` with a descriptive message |

This design preserves full decimal precision across the language boundary. Float is never used.

## Multi-Strategy Isolation

Multiple strategies can trade the same product simultaneously without interference:

- Each order carries a `strategy_id` field
- Positions are keyed by `{strategy_id}:{product_id}` in the positions HashMap
- A Market LONG order from strategy A will not affect strategy B's SHORT position on the same product
- The `get_position(strategy_id, product_id)` method retrieves a specific strategy's position

## Processing Pipeline (per candle)

1. **Update trailing stops**: Adjust trigger prices for all TRAILING_STOP orders based on the new candle's high/low
2. **Partition orders**: Split into Market, Conditional (SL/TP/Trailing), and Limit buckets
3. **Process Market orders**: Fill at open price, update positions, deduct fees, cancel linked OCO orders
4. **Process Conditional orders**: Check triggers, fill at trigger price, update positions, deduct fees, cancel linked orders
5. **Process Limit orders**: Check price match, fill at limit price, update positions, deduct fees, cancel linked orders
6. **Filter cancelled orders**: Remove OCO-cancelled orders from remaining open orders
7. **Return fills**: Return `Vec<FillEvent>` to Python

Orders for a different `product_id` than the candle are skipped (left in the open orders list).

## Performance

The Rust matching engine processes approximately **89,000 candles/second** with full order matching and fee calculation. A 100K-candle backtest completes in approximately 1.12 seconds.
