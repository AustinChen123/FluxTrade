# Python Strategy API Reference

This document details the core classes and models available to strategy developers in the `python-strategy` service.

## Core Models (`src.core.models`)

### `SignalType` (Enum)
Defines the intent of a trading signal.
*   `LONG`: Open a long position.
*   `SHORT`: Open a short position.
*   `EXIT_LONG`: Close a long position.
*   `EXIT_SHORT`: Close a short position.
*   `NO_SIGNAL`: No action required.

### `Candlestick` (Pydantic Model)
Represents a single OHLCV candle.

| Field | Type | Description |
| :--- | :--- | :--- |
| `product_id` | `str` | Format: `EXCHANGE:SYMBOL-PERP` (e.g., `BINANCE:BTCUSDT-PERP`) |
| `timeframe` | `str` | e.g., `1m`, `5m`, `1h` |
| `timestamp` | `int` | Unix timestamp in milliseconds (Open time) |
| `open` | `Decimal` | Open price |
| `high` | `Decimal` | High price |
| `low` | `Decimal` | Low price |
| `close` | `Decimal` | Close price |
| `volume` | `Decimal` | Volume in base asset |

### `Signal` (Pydantic Model)
The output of a strategy's decision logic.

| Field | Type | Description |
| :--- | :--- | :--- |
| `strategy_id` | `str` | Unique identifier of the strategy generating the signal. |
| `type` | `SignalType` | The action to take (LONG/SHORT/etc). |
| `product_id` | `str` | The target product. |
| `timestamp` | `int` | Creation timestamp (ms). |
| `value` | `Optional[Decimal]` | **Crucial**: If set, treated as a **Limit Price**. If None, treated as **Market Order**. |
| `metadata` | `dict` | Optional key-value pairs for debugging or logging. |

### `Position` (Pydantic Model)
Represents the current holding state.

| Field | Type | Description |
| :--- | :--- | :--- |
| `strategy_id` | `str` | Owner strategy. |
| `product_id` | `str` | Exchange symbol. |
| `side` | `str` | `LONG` or `SHORT`. |
| `quantity` | `Decimal` | Absolute position size (must be positive). |
| `entry_price` | `Decimal` | Average entry price. |
| `unrealized_pnl` | `Decimal` | Estimated PnL (Snapshot). |

---

## Strategy Base (`src.strategies.base`)

### `StrategyRequirements` (Data Class)
Configuration object returned by `BaseStrategy.requirements`.

| Field | Type | Description |
| :--- | :--- | :--- |
| `product_id` | `str` | Target symbol (e.g. `BINANCE:BTCUSDT-PERP`) |
| `timeframe` | `str` | Candle timeframe (e.g. `1m`) |
| `lookback_window` | `int` | Number of historical candles required before starting. |

### `BaseStrategy` (Abstract Base Class)
The parent class for all strategies.

#### Properties
*   `id` (str): Unique strategy ID (filename::classname).
*   `candles` (deque): A history of recent candles (length = `lookback_window`).

#### Methods to Implement

```python
@property
@abstractmethod
def requirements(self) -> StrategyRequirements:
    """
    Define the data subscription needs.
    """
    pass

@abstractmethod
def on_candle(self, candle: Candlestick) -> Signal:
    """
    Triggered on every NEW closed candle.
    Returns a Signal object.
    """
    pass
```
