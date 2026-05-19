# Writing Strategies

This guide covers how to create custom trading strategies for FluxTrade. Every strategy you write runs identically in both **live trading** and **backtesting** -- no code changes required.

---

## Core Concepts

A FluxTrade strategy is a Python class that:

1. Extends `BaseStrategy` (an ABC)
2. Declares its data requirements via the `requirements` property
3. Implements `on_candle()` to process each candlestick and return a `Signal`

The system handles everything else: order placement, SL/TP/Trailing Stop management, position tracking, and fee accounting. **Strategies only emit Signals** -- they never interact with the exchange directly.

---

## The BaseStrategy ABC

```python
# python-strategy/src/strategies/base.py

class BaseStrategy(ABC):
    def __init__(self, strategy_id: str, product_id: str):
        self.strategy_id = strategy_id
        self.product_id = product_id
        self.journal = StrategyJournal(strategy_id)

    @property
    @abstractmethod
    def requirements(self) -> StrategyRequirements:
        """Define data requirements for the strategy."""
        pass

    @abstractmethod
    def on_candle(self, candle: Candlestick) -> Signal:
        """Process a new candlestick and return a trading signal."""
        pass

    def on_trade(self, trade: Trade) -> Optional[Signal]:
        """Optional: react to individual trades (tick-level)."""
        return None

    def run_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run strategy in vectorized mode using Pandas.
        Expected to return DataFrame with 'signal' column."""
        raise NotImplementedError("Vectorized execution not implemented")
```

Every strategy must implement two things:

| Member | Purpose |
|--------|---------|
| `requirements` (property) | Tells the engine what data to feed the strategy |
| `on_candle(candle)` | Called once per bar; returns a `Signal` |
| `on_trade(trade)` | Optional: called on tick-level trade data; returns a `Signal` or `None` |
| `run_vectorized(df)` | Optional: vectorized execution using a Pandas DataFrame (see below) |

---

## StrategyRequirements

```python
from dataclasses import dataclass

@dataclass
class StrategyRequirements:
    product_id: str          # e.g. "BINANCE:BTCUSDT-PERP"
    timeframe: str           # e.g. "15m", "1h", "4h"
    lookback_window: int     # number of historical bars needed before first signal
```

The `lookback_window` tells the engine how many bars the strategy needs to accumulate before it can produce meaningful signals. During those initial bars, your strategy should return `SignalType.NO_SIGNAL`.

!!! tip "Timeframe Channel Isolation"
    The engine only delivers candles matching the strategy's declared `timeframe`. If your strategy declares `"15m"`, it will never see 1h or 4h candles. This isolation is enforced at the Redis stream level.

---

## Signal Model

```python
class Signal(BaseFluxModel):
    strategy_id: str                         # auto-set by engine
    product_id: str                          # e.g. "BINANCE:BTCUSDT-PERP"
    timeframe: str                           # e.g. "15m"
    timestamp: int                           # Unix ms from the candle
    type: SignalType                         # LONG, SHORT, EXIT_LONG, EXIT_SHORT, NO_SIGNAL
    value: Optional[Decimal] = None          # indicator value for logging
    quantity: Optional[Decimal] = None       # position size
    price: Optional[Decimal] = None          # limit price (None = market order)
    stop_loss: Optional[Decimal] = None      # absolute SL price
    take_profit: Optional[Decimal] = None    # absolute TP price
    trailing_distance: Optional[Decimal] = None  # trailing stop distance
    metadata: Optional[dict] = None          # arbitrary extra data
```

### SignalType Enum

| Value | Meaning |
|-------|---------|
| `LONG` | Open a long position (or add to existing) |
| `SHORT` | Open a short position (or add to existing) |
| `EXIT_LONG` | Close an existing long position |
| `EXIT_SHORT` | Close an existing short position |
| `NO_SIGNAL` | No action this bar |

!!! warning "All Prices Must Be Decimal"
    FluxTrade enforces `Decimal` for all financial values. Never use `float` for prices, quantities, or PnL. Import from `decimal` and construct via string: `Decimal("0.01")`.

---

## Complete Example: SMA Crossover Strategy

This strategy goes long when a fast SMA crosses above a slow SMA, and exits when it crosses below.

```python
from collections import deque
from decimal import Decimal
from typing import Deque

from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType


class SmaCrossStrategy(BaseStrategy):
    """Simple Moving Average crossover strategy.

    Goes LONG when fast SMA crosses above slow SMA.
    Exits LONG when fast SMA crosses below slow SMA.
    """

    def __init__(
        self,
        product_id: str,
        timeframe: str = "15m",
        fast_period: int = 10,
        slow_period: int = 30,
        quantity: Decimal = Decimal("0.01"),
        stop_loss_pct: Decimal = Decimal("0.02"),
        take_profit_pct: Decimal = Decimal("0.04"),
    ):
        super().__init__("sma_cross", product_id)
        self._timeframe = timeframe
        self._fast_period = fast_period
        self._slow_period = slow_period
        self._quantity = quantity
        self._sl_pct = stop_loss_pct
        self._tp_pct = take_profit_pct

        # Rolling close price buffer
        self._closes: Deque[Decimal] = deque(maxlen=slow_period + 1)

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=self._timeframe,
            lookback_window=self._slow_period,
        )

    def _sma(self, data: list[Decimal], period: int) -> Decimal:
        """Compute simple moving average of the last `period` values."""
        window = data[-period:]
        return sum(window) / Decimal(str(period))

    def on_candle(self, candle: Candlestick) -> Signal:
        self._closes.append(candle.close)

        # Not enough data yet
        if len(self._closes) <= self._slow_period:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=self.product_id,
                timeframe=self._timeframe,
                timestamp=candle.timestamp,
                type=SignalType.NO_SIGNAL,
            )

        closes = list(self._closes)

        # Current bar SMAs
        fast_now = self._sma(closes, self._fast_period)
        slow_now = self._sma(closes, self._slow_period)

        # Previous bar SMAs (exclude last element)
        prev = closes[:-1]
        fast_prev = self._sma(prev, self._fast_period)
        slow_prev = self._sma(prev, self._slow_period)

        signal_type = SignalType.NO_SIGNAL

        # Golden cross: fast crosses above slow
        if fast_now > slow_now and fast_prev <= slow_prev:
            signal_type = SignalType.LONG

        # Death cross: fast crosses below slow
        elif fast_now < slow_now and fast_prev >= slow_prev:
            signal_type = SignalType.EXIT_LONG

        # Build signal with optional SL/TP
        kwargs = dict(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=self._timeframe,
            timestamp=candle.timestamp,
            type=signal_type,
            value=fast_now,  # log the fast SMA value
        )

        if signal_type == SignalType.LONG:
            kwargs["quantity"] = self._quantity
            kwargs["stop_loss"] = candle.close * (Decimal("1") - self._sl_pct)
            kwargs["take_profit"] = candle.close * (Decimal("1") + self._tp_pct)

        return Signal(**kwargs)
```

!!! note "SL/TP Management"
    You only need to set `stop_loss` and `take_profit` on entry signals. The Rust matching engine (`PyMatchingEngine`) handles monitoring and triggering these orders automatically on every subsequent bar. Never implement SL/TP checking logic inside `on_candle()`.

---

## Running Your Strategy

### Backtesting (recommended starting point)

The most common way to run a strategy is through `BacktestRunner`. This feeds historical candle data through the engine, processes signals via the Rust matching engine, and produces performance metrics:

```python
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.csv_source import CsvDataSource

# 1. Prepare a data source
ds = CsvDataSource(
    "data/btcusdt_15m.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
)

# 2. Create your strategy
strategy = SmaCrossStrategy(
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
    fast_period=10,
    slow_period=30,
)

# 3. Configure and run the backtest
runner = BacktestRunner(
    start_time=1700000000000,
    end_time=1700500000000,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
    initial_balance=10000.0,
    data_source=ds,
    fee_config={"maker": 0.0002, "taker": 0.0006},
)
runner.add_strategy(strategy)
result = runner.run()

print(f"Total PnL: {result['total_pnl']}")
print(f"Win Rate:  {result['win_rate']}")
print(f"Sharpe:    {result['trade_sharpe']}")
```

See the [Backtesting Guide](backtesting.md) for full details on data sources, fee configuration, report output, and result interpretation.

### Live Trading (advanced)

In production, strategies are registered with the `StrategyEngine`, which connects to the Redis-based market data pipeline and routes signals to a live exchange adapter:

```python
from src.core.engine import StrategyEngine

engine = StrategyEngine(
    db_session,
    clock,
    adapter_config={"mode": "live", "exchange": "binance", "testnet": True},
)

strategy = SmaCrossStrategy(
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
)

engine.add_strategy(strategy)
engine.startup()
```

`StrategyEngine.startup()` launches background services (heartbeat, command listener, strategy scanner) and begins processing market data from the Rust data service via Redis streams. See the [Live Trading Guide](live-trading.md) for the full deployment workflow.

---

## Testing Strategies with MemoryDataSource

You can unit-test your strategy without a database or CSV file by using `MemoryDataSource`:

```python
from decimal import Decimal
from src.core.models import Candlestick, SignalType
from src.core.data_sources.memory import MemoryDataSource

# Build synthetic candle data
candles = []
base_ts = 1700000000000  # start timestamp in ms

prices = [
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109,  # rising
    108, 107, 106, 105, 104, 103, 102, 101, 100, 99,    # falling
]

for i, price in enumerate(prices):
    p = Decimal(str(price))
    candles.append(Candlestick(
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="15m",
        timestamp=base_ts + i * 900_000,  # 15 min intervals
        open=p,
        high=p + Decimal("1"),
        low=p - Decimal("1"),
        close=p,
        volume=Decimal("100"),
    ))

# Feed candles through strategy directly
strategy = SmaCrossStrategy(
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
    fast_period=5,
    slow_period=10,
)

signals = []
for candle in candles:
    signal = strategy.on_candle(candle)
    if signal.type != SignalType.NO_SIGNAL:
        signals.append(signal)

# Assert expected behavior
assert any(s.type == SignalType.LONG for s in signals)
```

For a full end-to-end backtest with order fills and PnL, use `BacktestRunner` with `MemoryDataSource`:

```python
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.memory import MemoryDataSource

ds = MemoryDataSource(candles)
runner = BacktestRunner(
    start_time=candles[0].timestamp,
    end_time=candles[-1].timestamp,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
    initial_balance=10000.0,
    data_source=ds,
    fee_config={"maker": 0.0002, "taker": 0.0006},
)
runner.add_strategy(strategy)
result = runner.run()

print(f"Total PnL: {result['total_pnl']}")
print(f"Win Rate: {result['win_rate']}")
```

---

## Vectorized Execution (Optional)

For strategies that benefit from batch computation (e.g., indicator-heavy strategies), you can implement `run_vectorized()`. This method receives a Pandas DataFrame with OHLCV columns and should return a DataFrame with a `signal` column.

```python
import pandas as pd
from src.strategies.base import BaseStrategy, StrategyRequirements

class MyVectorizedStrategy(BaseStrategy):
    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe="1h",
            lookback_window=200,
        )

    def on_candle(self, candle):
        # Event-driven path (used by BacktestRunner and live engine)
        ...

    def run_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        # Vectorized path for fast batch analysis
        df = df.copy()
        df["sma_50"] = df["close"].rolling(50).mean()
        df["sma_200"] = df["close"].rolling(200).mean()
        df["signal"] = 0
        df.loc[df["sma_50"] > df["sma_200"], "signal"] = 1   # Long
        df.loc[df["sma_50"] < df["sma_200"], "signal"] = -1  # Exit
        return df
```

The base class raises `NotImplementedError` by default, so this method is entirely optional. The `BacktestRunner` uses the event-driven `on_candle()` path. `run_vectorized()` is available for custom analysis workflows where you want to compute signals over an entire DataFrame at once.

---

## Strategy Design Guidelines

### Do

- Return `SignalType.NO_SIGNAL` when there is no clear setup -- the engine expects a Signal on every bar.
- Use `Decimal` for all price/quantity calculations.
- Keep a rolling buffer (e.g., `deque(maxlen=...)`) rather than growing a list indefinitely.
- Set `lookback_window` honestly -- the engine skips signal processing during warmup.
- Use `self.journal.log()` for structured event recording during backtests.

### Do Not

- Never call exchange APIs from inside `on_candle()` -- the adapter pattern handles this.
- Never implement SL/TP/Trailing Stop monitoring in your strategy -- the Rust matching engine does this.
- Never use `float` for prices or quantities.
- Never assume whether you are in live or backtest mode -- the same code must work in both.

---

## Next Steps

- [External Signals](external-signals.md) -- Integrate ML models or replay pre-computed signals
- [Backtesting](backtesting.md) -- Run full backtests with metrics and reporting
