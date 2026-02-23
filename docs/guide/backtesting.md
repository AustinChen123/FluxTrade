# Backtesting

FluxTrade provides a full-featured backtesting framework powered by the Rust matching engine. The same strategy code that runs in live trading runs in backtests -- order matching, SL/TP/Trailing Stops, and fee deduction all happen identically via `PyMatchingEngine`.

---

## Quick Start

```python
from decimal import Decimal
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.csv_source import CsvDataSource

# 1. Choose a data source
ds = CsvDataSource(
    "data/btcusdt_15m.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
)

# 2. Configure the runner
runner = BacktestRunner(
    start_time=1700000000000,        # Unix ms
    end_time=1700500000000,          # Unix ms
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
    initial_balance=10000.0,
    data_source=ds,
    fee_config={"maker": 0.0002, "taker": 0.0006},
)

# 3. Add strategies
runner.add_strategy(my_strategy)

# 4. Run
result = runner.run()

print(f"Total PnL:    {result['total_pnl']}")
print(f"Win Rate:     {result['win_rate']}")
print(f"Sharpe Ratio: {result['trade_sharpe']}")
print(f"Max Drawdown: {result['max_drawdown']}")
```

---

## BacktestRunner Constructor

```python
BacktestRunner(
    start_time: int,                           # start timestamp (Unix ms)
    end_time: int,                             # end timestamp (Unix ms)
    product_id: str,                           # e.g. "BINANCE:BTCUSDT-PERP"
    timeframe: str,                            # e.g. "15m", "1h"
    initial_balance: float = 10000.0,          # starting account balance (USD)
    max_drawdown_limit: float = 0.20,          # circuit breaker threshold (0.20 = 20%)
    data_source: Optional[IDataSource] = None, # candle data provider
    fee_config: Optional[Dict] = None,         # maker/taker fees
    report_config: Optional[Dict] = None,      # output file toggles
)
```

### Parameters in Detail

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_time` | `int` | required | Backtest start, Unix milliseconds |
| `end_time` | `int` | required | Backtest end, Unix milliseconds |
| `product_id` | `str` | required | Trading pair identifier |
| `timeframe` | `str` | required | Candle interval (`1m`, `5m`, `15m`, `1h`, `4h`, `1d`) |
| `initial_balance` | `float` | `10000.0` | Starting account balance in USD |
| `max_drawdown_limit` | `float` | `0.20` | Stop backtest if drawdown exceeds this fraction |
| `data_source` | `IDataSource` | `None` | Candle data provider (falls back to database if `None`) |
| `fee_config` | `dict` | `{}` | Maker/taker fee rates |
| `report_config` | `dict` | see below | Controls which output files are generated |

---

## Data Sources

FluxTrade provides four `IDataSource` implementations. All share the same interface:

```python
class IDataSource(ABC):
    def get_candles(self, product_id, timeframe, start, end) -> Generator[Candlestick, ...]
    def get_candles_df(self, product_id, timeframe, start, end) -> pd.DataFrame
    def get_available_range(self, product_id, timeframe) -> Optional[tuple[int, int]]
```

### CsvDataSource

Reads OHLCV data from a CSV file. Auto-detects column naming conventions from TradingView, Yahoo Finance, and standard formats.

```python
from src.core.data_sources.csv_source import CsvDataSource

ds = CsvDataSource(
    file_path="data/btcusdt_15m.csv",
    product_id="BINANCE:BTCUSDT-PERP",  # assigned to all emitted candles
    timeframe="15m",                      # assigned to all emitted candles
)
```

Supported column aliases:

| Standard | Also Recognized |
|----------|-----------------|
| `timestamp` | `time`, `ts`, `date`, `datetime` |
| `open` | `Open`, `o` |
| `high` | `High`, `h` |
| `low` | `Low`, `l` |
| `close` | `Close`, `c`, `adj close`, `Adj Close` |
| `volume` | `Volume`, `vol`, `Vol`, `v` |

!!! tip "Timestamp Formats"
    `CsvDataSource` handles multiple timestamp formats automatically: Unix seconds, Unix milliseconds, and date strings (e.g., `2024-01-15 08:00:00`). Values below `1e12` are treated as seconds and converted to milliseconds.

### MemoryDataSource

In-memory data source for testing and synthetic data. Accepts a list of `Candlestick` objects.

```python
from src.core.data_sources.memory import MemoryDataSource
from src.core.models import Candlestick
from decimal import Decimal

candles = [
    Candlestick(
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="15m",
        timestamp=1700000000000 + i * 900_000,
        open=Decimal(str(30000 + i * 10)),
        high=Decimal(str(30010 + i * 10)),
        low=Decimal(str(29990 + i * 10)),
        close=Decimal(str(30005 + i * 10)),
        volume=Decimal("100"),
    )
    for i in range(1000)
]

ds = MemoryDataSource(candles)
```

You can also add candles incrementally:

```python
ds = MemoryDataSource()
ds.add_candles(first_batch)
ds.add_candles(second_batch)  # auto-sorted by timestamp
```

### DatabaseDataSource

Reads candles from the PostgreSQL database. Used in production when historical data has been ingested by the Rust data service.

```python
from src.core.data_sources.database import DatabaseDataSource

ds = DatabaseDataSource()  # uses project DB connection
```

### YahooFinanceDataSource

Downloads historical data from Yahoo Finance. Useful for quick prototyping with traditional assets.

```python
from src.core.data_sources.yahoo import YahooFinanceDataSource

ds = YahooFinanceDataSource(ticker="BTC-USD")
```

---

## Fee Configuration

Fees are applied by the Rust matching engine on every fill. Configure them as `Decimal`-compatible values:

```python
fee_config = {
    "maker": 0.0002,   # 0.02% -- limit orders
    "taker": 0.0006,   # 0.06% -- market orders, SL/TP triggers
}
```

!!! warning "Fees Are Not Optional"
    Backtest results without fees are misleading. Always configure realistic fee rates. Common exchange fees:

    | Exchange | Maker | Taker |
    |----------|-------|-------|
    | Binance Futures | 0.0002 | 0.0005 |
    | Bybit | 0.0001 | 0.0006 |
    | Backpack | 0.0002 | 0.0006 |

The `BacktestRunner` converts these to `Decimal` internally and passes them to the Rust `SimulatedAdapter`.

---

## Report Configuration

Control which output files are generated after the backtest:

```python
report_config = {
    "csv_trades": True,       # trades.csv -- all closed trades
    "equity_curve": True,     # equity_curve.csv -- cumulative PnL per trade
    "markdown_report": True,  # report.md -- full performance summary
    "journal_export": True,   # journal.jsonl -- structured event log
    "output_dir": "backtest_output/",  # output directory
}
```

Default values (all enabled):

```python
DEFAULT_REPORT_CONFIG = {
    "csv_trades": True,
    "markdown_report": True,
    "equity_curve": True,
    "journal_export": True,
    "output_dir": "backtest_output/",
}
```

### Output Files

| File | Contents |
|------|----------|
| `trades.csv` | `entry_time, exit_time, side, entry_price, exit_price, quantity, pnl` |
| `equity_curve.csv` | `bar, equity` -- cumulative PnL after each closed trade |
| `report.md` | Markdown table with all metrics, monthly returns, and configuration |
| `journal.jsonl` | Structured event log (signal emissions, fills, errors) in JSON Lines format |

---

## Circuit Breaker

The `max_drawdown_limit` parameter acts as a circuit breaker. If the account balance drops below the threshold, the backtest stops immediately.

```python
runner = BacktestRunner(
    ...,
    initial_balance=10000.0,
    max_drawdown_limit=0.20,  # stop if balance < 8000 (20% drawdown)
)
```

The threshold is computed as:

```
stop_threshold = initial_balance * (1 - max_drawdown_limit)
```

!!! note "Circuit Breaker vs Strategy Logic"
    The circuit breaker is a safety mechanism at the runner level. It does not replace risk management inside your strategy (e.g., position sizing, per-trade risk limits). Both should be used together.

---

## Multi-Strategy Backtesting

Run multiple strategies in the same backtest to compare performance or test portfolio behavior:

```python
from src.strategies.golden_cross import GoldenCrossStrategy

strategy_a = SmaCrossStrategy("sma_fast", "BINANCE:BTCUSDT-PERP", "15m")
strategy_b = GoldenCrossStrategy("golden", "BINANCE:BTCUSDT-PERP")

runner = BacktestRunner(
    start_time=1700000000000,
    end_time=1700500000000,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
    initial_balance=10000.0,
    data_source=ds,
    fee_config={"maker": 0.0002, "taker": 0.0006},
)

runner.add_strategy(strategy_a)
runner.add_strategy(strategy_b)

result = runner.run()
```

When multiple strategies are registered, the result includes per-strategy metrics:

```python
# Aggregate metrics
print(result["total_pnl"])
print(result["total_trades"])

# Per-strategy breakdown (only when > 1 strategy)
for strategy_id, metrics in result["per_strategy"].items():
    print(f"\n{strategy_id}:")
    print(f"  PnL:      {metrics['total_pnl']}")
    print(f"  Win Rate: {metrics['win_rate']}")
    print(f"  Sharpe:   {metrics['trade_sharpe']}")
```

### Capital Allocation with CapitalAllocator

When running multiple strategies, use `CapitalAllocator` to partition the account balance and prevent strategies from over-committing shared capital:

```python
from decimal import Decimal
from src.core.capital_allocator import CapitalAllocator

allocator = CapitalAllocator(total_balance=Decimal("10000"))

# Allocate capital to each strategy
allocator.allocate("sma_fast", Decimal("5000"))
allocator.allocate("golden", Decimal("5000"))

# Query available capital
print(allocator.get_available("sma_fast"))   # Decimal('5000')
print(allocator.get_unallocated())            # Decimal('0')

# Track usage when positions open/close
allocator.record_usage("sma_fast", Decimal("1000"))
print(allocator.get_available("sma_fast"))   # Decimal('4000')

allocator.release_usage("sma_fast", Decimal("1000"))
print(allocator.get_available("sma_fast"))   # Decimal('5000')
```

Key `CapitalAllocator` methods:

| Method | Description |
|--------|-------------|
| `allocate(strategy_id, amount)` | Reserve capital for a strategy |
| `deallocate(strategy_id)` | Return capital to the pool (fails if capital still in use) |
| `get_available(strategy_id)` | Allocated minus used |
| `get_allocation(strategy_id)` | Total allocated |
| `get_unallocated()` | Remaining unallocated balance |
| `record_usage(strategy_id, amount)` | Mark capital as in use (position opened) |
| `release_usage(strategy_id, amount)` | Mark capital as free (position closed) |
| `update_total_balance(new_balance)` | Adjust total after PnL changes |

!!! warning "Thread Safety"
    `CapitalAllocator` is thread-safe -- all public methods acquire a lock. All monetary values must be `Decimal`; passing `float` raises `TypeError`.

---

## Interpreting Results

The `runner.run()` return value is a dictionary with these keys:

### Core Metrics

| Key | Type | Description |
|-----|------|-------------|
| `total_pnl` | `Decimal` | Net profit/loss after fees |
| `total_trades` | `int` | Number of completed round-trip trades |
| `win_rate` | `Decimal` | Fraction of profitable trades (0.0 -- 1.0) |
| `profit_factor` | `Decimal` | Gross profit / gross loss (>1.0 is profitable) |
| `max_drawdown` | `Decimal` | Largest peak-to-trough decline |
| `trade_sharpe` | `Decimal` | Sharpe ratio computed from per-trade returns |
| `sortino_ratio` | `Decimal` | Like Sharpe but only penalizes downside volatility |
| `calmar_ratio` | `Decimal` | Annualized return / max drawdown |

### Detailed Metrics

| Key | Type | Description |
|-----|------|-------------|
| `avg_hold_time_hours` | `Decimal` | Average trade duration in hours |
| `max_consecutive_wins` | `int` | Longest winning streak |
| `max_consecutive_losses` | `int` | Longest losing streak |
| `journal_count` | `int` | Number of structured journal events |
| `report_dir` | `str` | Path to the output directory (or `None`) |
| `per_strategy` | `dict` | Per-strategy metrics (only for multi-strategy runs) |
| `journal` | `list[dict]` | Raw journal entries as dictionaries |

### Understanding Key Ratios

**Sharpe Ratio** measures risk-adjusted return. Values above 1.0 indicate good risk-adjusted performance; above 2.0 is excellent.

**Sortino Ratio** is similar to Sharpe but only considers downside deviation. It does not penalize upside volatility, making it more relevant for strategies with asymmetric returns.

**Calmar Ratio** relates annualized return to maximum drawdown. A Calmar above 1.0 means the annualized return exceeds the worst drawdown.

**Profit Factor** is gross profit divided by gross loss. A value of 1.5 means the strategy earns $1.50 for every $1.00 lost.

**Max Drawdown** is the largest peak-to-trough decline in account equity. Combined with `max_drawdown_limit`, this helps assess whether the strategy stays within acceptable risk bounds.

---

## Full Example: End-to-End Backtest

```python
from decimal import Decimal
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.csv_source import CsvDataSource
from src.strategies.callable_strategy import CallableStrategy
from src.core.models import Candlestick, Signal, SignalType


# Define a simple momentum strategy via callable
def momentum_predict(candle: Candlestick) -> Signal | None:
    """Go long when close > open (bullish bar), exit on bearish bar."""
    if candle.close > candle.open:
        return Signal(
            strategy_id="momentum",
            product_id=candle.product_id,
            timeframe="15m",
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            quantity=Decimal("0.01"),
            stop_loss=candle.low,
            take_profit=candle.close + (candle.close - candle.low) * Decimal("2"),
        )
    elif candle.close < candle.open:
        return Signal(
            strategy_id="momentum",
            product_id=candle.product_id,
            timeframe="15m",
            timestamp=candle.timestamp,
            type=SignalType.EXIT_LONG,
        )
    return None


# Setup
ds = CsvDataSource(
    "data/btcusdt_15m.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
)

strategy = CallableStrategy(
    "momentum_v1",
    momentum_predict,
    "BINANCE:BTCUSDT-PERP",
    "15m",
)

runner = BacktestRunner(
    start_time=1700000000000,
    end_time=1700500000000,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="15m",
    initial_balance=10000.0,
    max_drawdown_limit=0.15,
    data_source=ds,
    fee_config={"maker": 0.0002, "taker": 0.0006},
    report_config={
        "csv_trades": True,
        "equity_curve": True,
        "markdown_report": True,
        "journal_export": True,
        "output_dir": "backtest_output/momentum_v1/",
    },
)
runner.add_strategy(strategy)
result = runner.run()

# Print summary
print(f"Total PnL:         {result['total_pnl']}")
print(f"Total Trades:      {result['total_trades']}")
print(f"Win Rate:          {result['win_rate']}")
print(f"Profit Factor:     {result['profit_factor']}")
print(f"Sharpe Ratio:      {result['trade_sharpe']}")
print(f"Sortino Ratio:     {result['sortino_ratio']}")
print(f"Calmar Ratio:      {result['calmar_ratio']}")
print(f"Max Drawdown:      {result['max_drawdown']}")
print(f"Avg Hold Time (h): {result['avg_hold_time_hours']}")
print(f"Journal Events:    {result['journal_count']}")
print(f"Reports:           {result['report_dir']}")
```

---

## Next Steps

- [Writing Strategies](writing-strategies.md) -- Build custom strategies with `BaseStrategy`
- [External Signals](external-signals.md) -- Integrate ML models and CSV signal replay
