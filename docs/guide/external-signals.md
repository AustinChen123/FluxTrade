# External Signals

FluxTrade supports two built-in adapters for feeding signals from external sources (ML models, third-party alerts, pre-computed CSVs) into the strategy engine. Both adapters implement `BaseStrategy`, so they work seamlessly with `BacktestRunner` and `StrategyEngine`.

---

## CallableStrategy

`CallableStrategy` wraps any Python callable as a fully backtestable strategy. This is the primary integration point for **ML models**, **external APIs**, and **custom signal generators**.

### Constructor

```python
from src.strategies.callable_strategy import CallableStrategy

CallableStrategy(
    strategy_id: str,                                  # unique identifier
    predict_fn: Callable[[Candlestick], Signal | None], # your signal function
    product_id: str,                                   # e.g. "BINANCE:BTCUSDT-PERP"
    timeframe: str = "1h",                             # candle timeframe
    lookback_window: int = 1,                          # bars before first signal
)
```

### How It Works

1. On every candle, the engine calls `on_candle(candle)`.
2. `CallableStrategy` delegates to your `predict_fn(candle)`.
3. If `predict_fn` returns a `Signal`, its `strategy_id` is overwritten to match this strategy instance.
4. If `predict_fn` returns `None`, a `NO_SIGNAL` is emitted automatically.

!!! tip "Return None for No Action"
    Your predict function should return `None` when there is no trade setup. Do not construct a `NO_SIGNAL` manually -- the wrapper handles this for you.

### Example: PyTorch Model Integration

```python
import torch
from decimal import Decimal
from src.core.models import Candlestick, Signal, SignalType
from src.strategies.callable_strategy import CallableStrategy

# Load your trained model
model = torch.load("models/btc_classifier_v3.pt")
model.eval()

THRESHOLD_LONG = 0.7
THRESHOLD_SHORT = 0.3


def extract_features(candle: Candlestick) -> list[float]:
    """Convert a candlestick into model input features."""
    return [
        float(candle.open),
        float(candle.high),
        float(candle.low),
        float(candle.close),
        float(candle.volume),
        float(candle.high - candle.low),          # range
        float(candle.close - candle.open),         # body
    ]


def predict(candle: Candlestick) -> Signal | None:
    features = extract_features(candle)
    with torch.no_grad():
        output = model(torch.tensor([features])).item()

    if output > THRESHOLD_LONG:
        return Signal(
            strategy_id="ml",  # will be overwritten by CallableStrategy
            product_id=candle.product_id,
            timeframe="15m",
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            quantity=Decimal("0.01"),
            stop_loss=candle.close * Decimal("0.98"),
            take_profit=candle.close * Decimal("1.04"),
        )
    elif output < THRESHOLD_SHORT:
        return Signal(
            strategy_id="ml",
            product_id=candle.product_id,
            timeframe="15m",
            timestamp=candle.timestamp,
            type=SignalType.SHORT,
            quantity=Decimal("0.01"),
            stop_loss=candle.close * Decimal("1.02"),
            take_profit=candle.close * Decimal("0.96"),
        )

    return None  # no signal


# Wrap as a FluxTrade strategy
strategy = CallableStrategy(
    "ml_btc_v3",
    predict,
    "BINANCE:BTCUSDT-PERP",
    "15m",
    lookback_window=1,
)
```

You can now pass this `strategy` to `BacktestRunner.add_strategy()` or `StrategyEngine.add_strategy()` -- it behaves identically to any hand-coded strategy.

### Example: Webhook/External Alert Adapter

```python
from collections import deque
from decimal import Decimal
from src.core.models import Candlestick, Signal, SignalType
from src.strategies.callable_strategy import CallableStrategy

# External alert queue (populated by a webhook handler elsewhere)
alert_queue: deque[dict] = deque()


def webhook_predict(candle: Candlestick) -> Signal | None:
    """Check if an external alert matches this candle's timestamp."""
    while alert_queue and alert_queue[0]["timestamp"] <= candle.timestamp:
        alert = alert_queue.popleft()
        return Signal(
            strategy_id="webhook",
            product_id=candle.product_id,
            timeframe="15m",
            timestamp=candle.timestamp,
            type=SignalType(alert["direction"]),  # "LONG" or "SHORT"
            quantity=Decimal(str(alert["size"])),
        )
    return None


strategy = CallableStrategy(
    "webhook_alerts",
    webhook_predict,
    "BINANCE:BTCUSDT-PERP",
    "15m",
)
```

---

## CsvSignalStrategy

`CsvSignalStrategy` replays pre-computed signals from a CSV file by matching on candle timestamps. This is useful for:

- Replaying signals generated offline (e.g., from a Jupyter notebook)
- Testing signal sets exported from another system
- Deterministic regression testing

### CSV Format

The CSV file must have a header row. Required columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | `int` | Unix timestamp in milliseconds (must match candle timestamps) |
| `type` | `str` | One of: `LONG`, `SHORT`, `EXIT_LONG`, `EXIT_SHORT` |

Optional columns:

| Column | Type | Description |
|--------|------|-------------|
| `quantity` | `Decimal` | Position size |
| `price` | `Decimal` | Limit price (omit for market orders) |
| `stop_loss` | `Decimal` | Stop loss price |
| `take_profit` | `Decimal` | Take profit price |
| `trailing_distance` | `Decimal` | Trailing stop distance |

**Example CSV** (`signals/btc_replay.csv`):

```csv
timestamp,type,quantity,stop_loss,take_profit
1700000000000,LONG,0.01,29500.00,31000.00
1700003600000,EXIT_LONG,,
1700010800000,SHORT,0.01,31200.00,29800.00
1700018000000,EXIT_SHORT,,
```

!!! note "Empty Optional Fields"
    Leave optional columns blank (not `0` or `null`) when they do not apply. The parser treats empty strings as `None`.

### Constructor

```python
from src.strategies.csv_signal_strategy import CsvSignalStrategy

CsvSignalStrategy(
    strategy_id: str,         # unique identifier
    csv_path: str,            # path to the CSV file
    product_id: str,          # e.g. "BINANCE:BTCUSDT-PERP"
    timeframe: str = "1h",    # candle timeframe
    lookback_window: int = 1, # bars before first signal
)
```

### How It Works

1. On construction, the entire CSV is loaded into a `Dict[int, Signal]` keyed by timestamp.
2. On each `on_candle()` call, the strategy checks if a signal exists for that candle's timestamp.
3. If a match is found, the pre-built Signal is returned.
4. If no match, `NO_SIGNAL` is returned.

!!! warning "Timestamp Matching Must Be Exact"
    The CSV timestamps must exactly match the candle timestamps your data source produces. If your data source uses second-precision timestamps and the CSV uses milliseconds (or vice versa), signals will never match.

### Usage

```python
from src.strategies.csv_signal_strategy import CsvSignalStrategy
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.csv_source import CsvDataSource

# Data source for candles
ds = CsvDataSource(
    "data/btcusdt_15m.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
)

# Signal replay strategy
strategy = CsvSignalStrategy(
    "replay_v1",
    "signals/btc_signals.csv",
    "BINANCE:BTCUSDT-PERP",
    "15m",
)

runner = BacktestRunner(
    start_time=1700000000000,
    end_time=1700100000000,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
    initial_balance=10000.0,
    data_source=ds,
)
runner.add_strategy(strategy)
result = runner.run()
```

---

## Deterministic Testing: Callable vs CSV

A powerful testing pattern is to verify that a `CallableStrategy` and a `CsvSignalStrategy` produce identical results. This ensures your signal generation logic is deterministic and reproducible.

### Step 1: Generate Signals and Export to CSV

```python
import csv
from decimal import Decimal
from src.core.models import Candlestick, SignalType
from src.core.data_sources.csv_source import CsvDataSource

# Your callable predict function
def my_predict(candle: Candlestick):
    # ... your logic ...
    pass

# Run through candles and collect signals
ds = CsvDataSource("data/btcusdt_15m.csv", "BINANCE:BTCUSDT-PERP", "15m")
available = ds.get_available_range("BINANCE:BTCUSDT-PERP", "15m")

signals = []
for candle in ds.get_candles("BINANCE:BTCUSDT-PERP", "15m", available[0], available[1]):
    result = my_predict(candle)
    if result is not None and result.type != SignalType.NO_SIGNAL:
        signals.append(result)

# Export to CSV
with open("signals/exported.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "type", "quantity", "stop_loss", "take_profit", "trailing_distance"])
    for s in signals:
        writer.writerow([
            s.timestamp,
            s.type.value,
            str(s.quantity) if s.quantity else "",
            str(s.stop_loss) if s.stop_loss else "",
            str(s.take_profit) if s.take_profit else "",
            str(s.trailing_distance) if s.trailing_distance else "",
        ])
```

### Step 2: Backtest Both and Compare

```python
from src.strategies.callable_strategy import CallableStrategy
from src.strategies.csv_signal_strategy import CsvSignalStrategy
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.csv_source import CsvDataSource

start, end = 1700000000000, 1700100000000
product = "BINANCE:BTCUSDT-PERP"
tf = "15m"

# Backtest with CallableStrategy
ds1 = CsvDataSource("data/btcusdt_15m.csv", product, tf)
callable_strat = CallableStrategy("callable_v1", my_predict, product, tf)
runner1 = BacktestRunner(
    start_time=start, end_time=end,
    product_id=product, timeframe=tf,
    initial_balance=10000.0, data_source=ds1,
    fee_config={"maker": 0.0002, "taker": 0.0006},
)
runner1.add_strategy(callable_strat)
result1 = runner1.run()

# Backtest with CsvSignalStrategy
ds2 = CsvDataSource("data/btcusdt_15m.csv", product, tf)
csv_strat = CsvSignalStrategy("csv_v1", "signals/exported.csv", product, tf)
runner2 = BacktestRunner(
    start_time=start, end_time=end,
    product_id=product, timeframe=tf,
    initial_balance=10000.0, data_source=ds2,
    fee_config={"maker": 0.0002, "taker": 0.0006},
)
runner2.add_strategy(csv_strat)
result2 = runner2.run()

# They should match exactly
assert result1["total_pnl"] == result2["total_pnl"], (
    f"PnL mismatch: callable={result1['total_pnl']} vs csv={result2['total_pnl']}"
)
assert result1["total_trades"] == result2["total_trades"]
print("Deterministic verification passed.")
```

!!! tip "Why This Matters"
    If your callable and CSV backtests diverge, it means either (a) the signal export missed some signals, (b) timestamp alignment is off, or (c) the predict function has non-deterministic behavior (e.g., random sampling). This test catches all three issues.

---

## Next Steps

- [Writing Strategies](writing-strategies.md) -- Build strategies from scratch with `BaseStrategy`
- [Backtesting](backtesting.md) -- Full backtest configuration, metrics, and reporting
