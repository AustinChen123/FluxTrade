# First Backtest

This guide covers the full `BacktestRunner` API, all available data sources, metrics interpretation, and advanced features like multi-strategy backtesting and the circuit breaker.

## Data Sources

FluxTrade provides four `IDataSource` implementations. All share the same interface:

```python
class IDataSource(ABC):
    def get_candles(self, product_id, timeframe, start, end) -> Generator[Candlestick, None, None]: ...
    def get_candles_df(self, product_id, timeframe, start, end) -> pd.DataFrame: ...
    def get_available_range(self, product_id, timeframe) -> Optional[tuple[int, int]]: ...
    def validate(self) -> bool: ...
```

All timestamps are Unix milliseconds.

### CsvDataSource

Reads OHLCV data from a CSV file. Auto-detects column names from common formats (TradingView, Yahoo Finance, standard OHLCV).

```python
from src.core.data_sources import CsvDataSource

data_source = CsvDataSource(
    file_path="data/btcusdt_1h.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
)
```

**Constructor parameters:**

| Parameter    | Type  | Default            | Description                        |
|-------------|-------|--------------------|------------------------------------|
| `file_path`  | `str` | (required)         | Path to the CSV file               |
| `product_id` | `str` | `"CSV:DATA-PERP"`  | Product ID assigned to candles     |
| `timeframe`  | `str` | `"1m"`             | Timeframe label assigned to candles|

**Supported column names** (case-insensitive):

- Timestamp: `timestamp`, `time`, `ts`, `date`, `datetime`
- OHLCV: `open/Open/o`, `high/High/h`, `low/Low/l`, `close/Close/c/Adj Close`, `volume/Volume/vol/v`

Timestamps can be Unix milliseconds, Unix seconds (auto-detected if value < 1e12), or date strings (parsed by pandas).

### DatabaseDataSource

Reads candles from PostgreSQL via SQLAlchemy. Requires a running database with the FluxTrade schema.

```python
from src.core.data_sources import DatabaseDataSource

data_source = DatabaseDataSource()
# Uses the default SessionLocal from src.core.db
```

You can also pass a custom `session_factory`:

```python
data_source = DatabaseDataSource(session_factory=my_session_factory)
```

### YahooFinanceDataSource

Downloads OHLCV data from Yahoo Finance. Requires the optional `yfinance` package:

```bash
pip install yfinance
```

```python
from src.core.data_sources import YahooFinanceDataSource

data_source = YahooFinanceDataSource(
    ticker="BTC-USD",
    product_id="YAHOO:BTCUSD-PERP",
    timeframe="1d",
)
```

**Constructor parameters:**

| Parameter    | Type  | Default              | Description                     |
|-------------|-------|----------------------|---------------------------------|
| `ticker`     | `str` | `"BTC-USD"`          | Yahoo Finance ticker symbol     |
| `product_id` | `str` | `"YAHOO:BTCUSD-PERP"`| Product ID assigned to candles |
| `timeframe`  | `str` | `"1d"`               | Timeframe (must be in supported set) |

**Supported timeframes**: `1m`, `2m`, `5m`, `15m`, `30m`, `1h`, `1d`, `1w`, `1M`

!!! warning "Yahoo Finance limitations"
    Intraday data (1m-1h) is limited to the last 7-60 days. Daily data has full history.

### MemoryDataSource

In-memory data source for testing and synthetic data generation.

```python
from src.core.data_sources import MemoryDataSource
from src.core.models import Candlestick
from decimal import Decimal

candles = [
    Candlestick(
        product_id="TEST:BTCUSDT-PERP",
        timeframe="1h",
        timestamp=1704067200000,
        open=Decimal("42000"),
        high=Decimal("42500"),
        low=Decimal("41800"),
        close=Decimal("42300"),
        volume=Decimal("150.5"),
    ),
    # ... more candles
]

data_source = MemoryDataSource(candles=candles)
# You can also append later:
data_source.add_candles(more_candles)
```

## Configuring BacktestRunner

```python
from src.core.backtest_runner import BacktestRunner

runner = BacktestRunner(
    start_time=1704067200000,      # Unix ms, inclusive
    end_time=1706745600000,        # Unix ms, inclusive
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
    initial_balance=10000.0,       # Starting balance in quote currency
    max_drawdown_limit=0.20,       # Circuit breaker: stop at 20% drawdown
    data_source=data_source,       # Any IDataSource implementation
    fee_config={                   # Trading fees (applied by Rust engine)
        "maker": 0.0002,           # 0.02%
        "taker": 0.0004,           # 0.04%
    },
    report_config={                # Output file configuration
        "csv_trades": True,
        "markdown_report": True,
        "equity_curve": True,
        "journal_export": True,
        "output_dir": "backtest_output/",
    },
)
```

**Constructor parameters:**

| Parameter            | Type                  | Default          | Description                                    |
|---------------------|-----------------------|------------------|------------------------------------------------|
| `start_time`         | `int`                 | (required)       | Backtest start, Unix milliseconds              |
| `end_time`           | `int`                 | (required)       | Backtest end, Unix milliseconds                |
| `product_id`         | `str`                 | (required)       | Product to trade (e.g. `BINANCE:BTCUSDT-PERP`) |
| `timeframe`          | `str`                 | (required)       | Candle timeframe (e.g. `1h`, `15m`)            |
| `initial_balance`    | `float`               | `10000.0`        | Starting balance in quote currency             |
| `max_drawdown_limit` | `float`               | `0.20`           | Circuit breaker threshold (fraction)           |
| `data_source`        | `IDataSource` or None | `None`           | Data source; falls back to database if None    |
| `fee_config`         | `dict` or None        | `{}`             | `{"maker": float, "taker": float}`             |
| `report_config`      | `dict` or None        | See defaults     | Controls which reports are generated           |

## Adding Strategies

```python
from src.strategies.golden_cross import GoldenCrossStrategy

strategy = GoldenCrossStrategy(
    strategy_id="gc-btc-1h",
    product_id="BINANCE:BTCUSDT-PERP",
    short_window=50,
    long_window=200,
)
runner.add_strategy(strategy)
```

You can add multiple strategies to the same runner. Each strategy receives every candle and can independently emit signals. The runner tracks per-strategy metrics when multiple strategies are present.

## Running the Backtest

```python
result = runner.run()
```

The `run()` method:

1. Registers strategies in the database (creates missing records to satisfy FK constraints)
2. Creates a `BacktestResultSummary` record in PostgreSQL
3. Instantiates a `SimulatedAdapter` backed by the Rust `PyMatchingEngine`
4. Iterates through candles from the data source, feeding each to the `StrategyEngine`
5. Checks the circuit breaker after every candle
6. Computes metrics via `calculate_metrics()` using FIFO trade pairing
7. Writes report files to the output directory

## Interpreting Results

The returned dictionary contains:

### Basic Metrics

| Key              | Type      | Description                                |
|------------------|-----------|--------------------------------------------|
| `total_pnl`     | `Decimal` | Net profit/loss after all fees             |
| `total_trades`  | `int`     | Number of completed round-trip trades      |
| `win_rate`      | `Decimal` | Fraction of trades with positive PnL       |
| `profit_factor` | `Decimal` | Gross profit / gross loss                  |
| `max_drawdown`  | `Decimal` | Largest peak-to-trough equity decline      |
| `trade_sharpe`  | `Decimal` | Trade-based Sharpe ratio (mean/std of PnLs)|

### Advanced Metrics

| Key                           | Type      | Description                                |
|-------------------------------|-----------|--------------------------------------------|
| `sortino_ratio`               | `Decimal` | Return / downside deviation                |
| `calmar_ratio`                | `Decimal` | Annualized return / max drawdown           |
| `avg_trade`                   | `Decimal` | Average PnL per trade                     |
| `avg_hold_time_hours`         | `Decimal` | Average trade duration in hours            |
| `max_drawdown_days`           | `Decimal` | Longest drawdown period in days            |
| `trade_frequency_per_day`     | `Decimal` | Average trades per day                     |
| `max_consecutive_wins`        | `int`     | Longest winning streak                     |
| `max_consecutive_losses`      | `int`     | Longest losing streak                      |
| `max_consecutive_win_amount`  | `Decimal` | Total PnL of longest winning streak        |
| `max_consecutive_loss_amount` | `Decimal` | Total loss of longest losing streak        |
| `gross_profit`                | `Decimal` | Sum of all winning trade PnLs              |
| `gross_loss`                  | `Decimal` | Sum of all losing trade PnLs (absolute)    |

### Additional Output

| Key             | Type   | Description                                      |
|-----------------|--------|--------------------------------------------------|
| `journal`       | `list` | List of structured event dicts from the journal  |
| `journal_count` | `int`  | Number of journal entries                        |
| `report_dir`    | `str`  | Path to the output directory with report files   |
| `per_strategy`  | `dict` | Strategy-keyed metrics dict (multi-strategy only)|

### Monthly Returns

The metrics also include `monthly_returns` (accessible from the full metrics stored in the database), a dictionary keyed by `"YYYY-MM"` strings with `Decimal` PnL values for each month.

## Report Files

By default, `BacktestRunner` writes four files to `backtest_output/`:

| File                | Content                                                |
|---------------------|--------------------------------------------------------|
| `report.md`         | Markdown summary with config table and all metrics     |
| `trades.csv`        | Closed trades: entry/exit time, price, side, PnL       |
| `equity_curve.csv`  | Bar-by-bar cumulative PnL                              |
| `journal.jsonl`     | Structured event log (signals, fills, errors)          |

Disable specific outputs via `report_config`:

```python
runner = BacktestRunner(
    ...,
    report_config={
        "csv_trades": True,
        "markdown_report": True,
        "equity_curve": False,
        "journal_export": False,
        "output_dir": "my_results/",
    },
)
```

## Circuit Breaker

The `max_drawdown_limit` parameter (default `0.20` = 20%) triggers an automatic stop when the account balance drops below `initial_balance * (1 - max_drawdown_limit)`.

For example, with `initial_balance=10000.0` and `max_drawdown_limit=0.20`, the backtest halts if the balance falls below 8000.

```python
runner = BacktestRunner(
    ...,
    max_drawdown_limit=0.30,  # Allow up to 30% drawdown before stopping
)
```

Set `max_drawdown_limit=1.0` to effectively disable the circuit breaker.

## Multi-Strategy Backtesting

Add multiple strategies to test portfolio-level behavior:

```python
from src.strategies.golden_cross import GoldenCrossStrategy
from src.strategies.rsi_scalper import RsiScalperStrategy

runner = BacktestRunner(
    start_time=start_time,
    end_time=end_time,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
    initial_balance=10000.0,
    data_source=data_source,
)

runner.add_strategy(GoldenCrossStrategy(
    strategy_id="gc-btc",
    product_id="BINANCE:BTCUSDT-PERP",
))

runner.add_strategy(RsiScalperStrategy(
    strategy_id="rsi-btc",
    product_id="BINANCE:BTCUSDT-PERP",
))

result = runner.run()

# Per-strategy breakdown (only present when >1 strategy)
for strategy_id, metrics in result["per_strategy"].items():
    print(f"{strategy_id}: PnL={metrics['total_pnl']}, Trades={metrics['total_trades']}")
```

When multiple strategies are present, the `per_strategy` key in the result contains independent metrics for each strategy, computed by filtering trades by `strategy_id`.

## Fees

Fees are applied by the Rust matching engine at fill time. Configure them via `fee_config`:

```python
fee_config = {
    "maker": 0.0002,  # 0.02% for limit orders
    "taker": 0.0004,  # 0.04% for market orders
}
```

All fee deductions are reflected in the `total_pnl` and per-trade PnL values. The matching engine handles Market, Limit, Stop-Loss, Take-Profit, Trailing Stop, and OCO order types.

## Complete Example

```python
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources import CsvDataSource
from src.strategies.golden_cross import GoldenCrossStrategy

# Data
data_source = CsvDataSource(
    file_path="data/btcusdt_2024_1h.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
)

time_range = data_source.get_available_range("BINANCE:BTCUSDT-PERP", "1h")
if time_range is None:
    raise RuntimeError("No data found in CSV")
start_time, end_time = time_range

# Runner
runner = BacktestRunner(
    start_time=start_time,
    end_time=end_time,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
    initial_balance=10000.0,
    max_drawdown_limit=0.25,
    data_source=data_source,
    fee_config={"maker": 0.0002, "taker": 0.0004},
)

# Strategy
runner.add_strategy(GoldenCrossStrategy(
    strategy_id="gc-btc-1h",
    product_id="BINANCE:BTCUSDT-PERP",
    short_window=50,
    long_window=200,
))

# Execute
result = runner.run()

# Summary
print(f"PnL:            {result['total_pnl']}")
print(f"Trades:         {result['total_trades']}")
print(f"Win Rate:       {result['win_rate']}")
print(f"Profit Factor:  {result['profit_factor']}")
print(f"Max Drawdown:   {result['max_drawdown']}")
print(f"Sharpe:         {result['trade_sharpe']}")
print(f"Sortino:        {result['sortino_ratio']}")
print(f"Calmar:         {result['calmar_ratio']}")
print(f"Avg Hold (h):   {result['avg_hold_time_hours']}")
print(f"Reports:        {result['report_dir']}")
```

## Next Steps

- [Writing Strategies](../guide/writing-strategies.md) -- learn how to build custom strategies with SL/TP/trailing stops
- [Backtesting Guide](../guide/backtesting.md) -- advanced backtesting patterns and optimization
- [Architecture Overview](../architecture/overview.md) -- understand how the engine, adapter, and matching engine fit together
