# Quick Start

Get a backtest running in under 5 minutes using the built-in Golden Cross strategy and a CSV data file.

## 1. Prepare Sample Data

!!! warning "Data requirement"
    `GoldenCrossStrategy` uses a 200-period SMA by default, so you need **at least 201 candles** of 1h data. A 3-row CSV will run without error but produce zero trades.

Create a CSV file with OHLCV data. The `CsvDataSource` auto-detects common column naming conventions (TradingView, Yahoo Finance, standard).

Place your data at `python-strategy/data/sample.csv`:

```csv
timestamp,open,high,low,close,volume
1704067200000,42000.0,42500.0,41800.0,42300.0,150.5
1704070800000,42300.0,42800.0,42100.0,42700.0,180.2
...
```

Each row represents one candlestick. Timestamps are Unix milliseconds. You can also use date strings (`2024-01-01 00:00:00`) or Unix seconds — the data source handles conversion automatically.

You can download historical data from exchanges or use `YahooFinanceDataSource` (see [First Backtest](first-backtest.md)) to get started without a CSV file.

## 2. Write a Backtest Script

Create `python-strategy/run_quick_backtest.py`:

```python
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources import CsvDataSource
from src.strategies.golden_cross import GoldenCrossStrategy

# 1. Configure data source
data_source = CsvDataSource(
    file_path="data/sample.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
)

# 2. Get the time range from the CSV
time_range = data_source.get_available_range(
    "BINANCE:BTCUSDT-PERP", "1h"
)
if time_range is None:
    raise RuntimeError("No data found — check your CSV path and column names")
start_time, end_time = time_range

# 3. Create the backtest runner
runner = BacktestRunner(
    start_time=start_time,
    end_time=end_time,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
    initial_balance=10000.0,
    data_source=data_source,
    fee_config={"maker": 0.0002, "taker": 0.0004},
)

# 4. Add a strategy
strategy = GoldenCrossStrategy(
    strategy_id="golden-cross-btc",
    product_id="BINANCE:BTCUSDT-PERP",
    short_window=50,
    long_window=200,
)
runner.add_strategy(strategy)

# 5. Run
result = runner.run()

# 6. Print results
print(f"Total PnL:      {result['total_pnl']}")
print(f"Total Trades:   {result['total_trades']}")
print(f"Win Rate:       {result['win_rate']}")
print(f"Profit Factor:  {result['profit_factor']}")
print(f"Max Drawdown:   {result['max_drawdown']}")
print(f"Sharpe Ratio:   {result['trade_sharpe']}")
```

## 3. Run the Backtest

```bash
cd python-strategy
uv run python run_quick_backtest.py
```

## 4. Understanding the Output

The `run()` method returns a dictionary with these keys:

| Key                    | Type      | Description                              |
|------------------------|-----------|------------------------------------------|
| `total_pnl`           | `Decimal` | Net profit/loss after fees               |
| `total_trades`        | `int`     | Number of completed round-trip trades    |
| `win_rate`            | `Decimal` | Fraction of profitable trades            |
| `profit_factor`       | `Decimal` | Gross profit / gross loss                |
| `max_drawdown`        | `Decimal` | Largest peak-to-trough equity decline    |
| `trade_sharpe`        | `Decimal` | Trade-based Sharpe ratio                 |
| `sortino_ratio`       | `Decimal` | Sortino ratio (downside deviation only)  |
| `calmar_ratio`        | `Decimal` | Annualized return / max drawdown         |
| `avg_hold_time_hours` | `Decimal` | Average trade duration in hours          |
| `per_strategy`        | `dict`    | Per-strategy metrics (multi-strategy)    |
| `report_dir`          | `str`     | Path to generated report files           |

By default, `BacktestRunner` also writes report files to `backtest_output/`:

- `report.md` -- Markdown summary with configuration and metrics tables
- `trades.csv` -- All closed trades with entry/exit prices and PnL
- `equity_curve.csv` -- Bar-by-bar equity progression
- `journal.jsonl` -- Structured event log of all strategy actions

## 5. Writing Your Own Strategy

Every strategy extends `BaseStrategy` and implements two methods:

```python
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class MyStrategy(BaseStrategy):
    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe="1h",
            lookback_window=20,
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        # Your logic here
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )
```

A `Signal` can carry entry parameters that the execution engine uses to manage the full order lifecycle:

| Signal Field         | Type              | Description                               |
|----------------------|-------------------|-------------------------------------------|
| `type`               | `SignalType`      | `LONG`, `SHORT`, `EXIT_LONG`, `EXIT_SHORT`, `NO_SIGNAL` |
| `quantity`           | `Decimal` or None | Position size (optional, system can default) |
| `price`              | `Decimal` or None | Limit price (optional, market order if None) |
| `stop_loss`          | `Decimal` or None | Stop-loss price                           |
| `take_profit`        | `Decimal` or None | Take-profit price                         |
| `trailing_distance`  | `Decimal` or None | Trailing stop distance                    |
| `metadata`           | `dict` or None    | Arbitrary strategy metadata               |

Strategies only emit signals. All SL/TP/trailing stop management is handled by the Rust matching engine -- never implement order management logic inside `on_candle()`.

## Next Steps

- [First Backtest](first-backtest.md) -- detailed walkthrough of all data sources and advanced configuration
- [Writing Strategies](../guide/writing-strategies.md) -- complete strategy authoring guide
- [Live Trading](../guide/live-trading.md) -- deploy your strategy to a live exchange
