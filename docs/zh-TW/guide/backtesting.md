# 回測 (Backtesting)

FluxTrade 提供由 Rust 撮合引擎驅動的完整回測框架。在實盤交易中執行的相同策略程式碼也可以在回測中執行——訂單撮合、SL/TP/移動停損 (Trailing Stop)，以及手續費扣除全部透過 `PyMatchingEngine` 以相同方式運作。

---

## 快速開始

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

## BacktestRunner 建構子

```python
BacktestRunner(
    start_time: int,                           # 起始時間戳 (Unix ms)
    end_time: int,                             # 結束時間戳 (Unix ms)
    product_id: str,                           # 例如 "BINANCE:BTCUSDT-PERP"
    timeframe: str,                            # 例如 "15m", "1h"
    initial_balance: float = 10000.0,          # 起始帳戶餘額 (USD)
    max_drawdown_limit: float = 0.20,          # 熔斷閾值 (0.20 = 20%)
    data_source: Optional[IDataSource] = None, # K 線資料提供者
    fee_config: Optional[Dict] = None,         # maker/taker 手續費
    report_config: Optional[Dict] = None,      # 輸出檔案開關
)
```

### 參數詳細說明

| 參數 | 型別 | 預設值 | 說明 |
|------|------|--------|------|
| `start_time` | `int` | 必填 | 回測起始時間，Unix 毫秒 |
| `end_time` | `int` | 必填 | 回測結束時間，Unix 毫秒 |
| `product_id` | `str` | 必填 | 交易對識別碼 |
| `timeframe` | `str` | 必填 | K 線間隔（`1m`, `5m`, `15m`, `1h`, `4h`, `1d`） |
| `initial_balance` | `float` | `10000.0` | 起始帳戶餘額（USD） |
| `max_drawdown_limit` | `float` | `0.20` | 若回撤超過此比例則停止回測 |
| `data_source` | `IDataSource` | `None` | K 線資料提供者（若為 `None` 則回退至資料庫） |
| `fee_config` | `dict` | `{}` | Maker/taker 手續費率 |
| `report_config` | `dict` | 見下方 | 控制產生哪些輸出檔案 |

---

## 資料來源 (Data Sources)

FluxTrade 提供四種 `IDataSource` 實作。它們共享相同的介面：

```python
class IDataSource(ABC):
    def get_candles(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> Generator[Candlestick, None, None]: ...

    def get_candles_df(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> pd.DataFrame: ...

    def get_available_range(
        self, product_id: str, timeframe: str
    ) -> Optional[tuple[int, int]]: ...

    def validate(self) -> bool: ...
```

!!! warning "`get_candles()` 回傳的是 Generator，不是 list"
    `get_candles()` 透過 Python 產生器逐一產出 `Candlestick` 物件。這對大型資料集的記憶體效率至關重要。若你需要所有 K 線都在記憶體中，請使用 `list(ds.get_candles(...))` 或使用 `get_candles_df()` 取得 DataFrame。

### CsvDataSource

從 CSV 檔案讀取 OHLCV 資料。自動偵測 TradingView、Yahoo Finance 以及標準格式的欄位命名慣例。

```python
from src.core.data_sources.csv_source import CsvDataSource

ds = CsvDataSource(
    file_path="data/btcusdt_15m.csv",
    product_id="BINANCE:BTCUSDT-PERP",  # assigned to all emitted candles
    timeframe="15m",                      # assigned to all emitted candles
)
```

支援的欄位別名：

| 標準名稱 | 同時識別 |
|----------|----------|
| `timestamp` | `time`, `ts`, `date`, `datetime` |
| `open` | `Open`, `o` |
| `high` | `High`, `h` |
| `low` | `Low`, `l` |
| `close` | `Close`, `c`, `adj close`, `Adj Close` |
| `volume` | `Volume`, `vol`, `Vol`, `v` |

!!! tip "時間戳格式"
    `CsvDataSource` 自動處理多種時間戳格式：Unix 秒、Unix 毫秒，以及日期字串（例如 `2024-01-15 08:00:00`）。低於 `1e12` 的值會被視為秒並轉換為毫秒。

### MemoryDataSource

用於測試和合成資料的記憶體內資料來源。接受 `Candlestick` 物件列表。

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

你也可以逐步新增 K 線：

```python
ds = MemoryDataSource()
ds.add_candles(first_batch)
ds.add_candles(second_batch)  # auto-sorted by timestamp
```

### DatabaseDataSource

從 PostgreSQL 資料庫讀取 K 線。用於正式環境中歷史資料已由 Rust 資料服務匯入的情況。

```python
from src.core.data_sources.database import DatabaseDataSource

ds = DatabaseDataSource()  # uses project DB connection
```

### YahooFinanceDataSource

從 Yahoo Finance 下載歷史資料。適用於傳統資產的快速原型開發。

```python
from src.core.data_sources.yahoo import YahooFinanceDataSource

ds = YahooFinanceDataSource(ticker="BTC-USD")
```

---

## 手續費配置 (Fee Configuration)

手續費由 Rust 撮合引擎在每次成交時套用。以相容 `Decimal` 的值進行配置：

```python
fee_config = {
    "maker": 0.0002,   # 0.02% -- limit orders
    "taker": 0.0006,   # 0.06% -- market orders, SL/TP triggers
}
```

!!! warning "手續費不是選用的"
    不含手續費的回測結果具有誤導性。務必配置真實的手續費率。常見交易所手續費：

    | 交易所 | Maker | Taker |
    |--------|-------|-------|
    | Binance Futures | 0.0002 | 0.0005 |
    | Bybit | 0.0001 | 0.0006 |
    | Backpack | 0.0002 | 0.0006 |

`BacktestRunner` 會在內部將這些值轉換為 `Decimal` 並傳遞給 Rust `SimulatedAdapter`。

---

## 報告配置 (Report Configuration)

控制回測完成後產生哪些輸出檔案：

```python
report_config = {
    "csv_trades": True,       # trades.csv -- all closed trades
    "equity_curve": True,     # equity_curve.csv -- cumulative PnL per trade
    "markdown_report": True,  # report.md -- full performance summary
    "journal_export": True,   # journal.jsonl -- structured event log
    "output_dir": "backtest_output/",  # output directory
}
```

預設值（全部啟用）：

```python
DEFAULT_REPORT_CONFIG = {
    "csv_trades": True,
    "markdown_report": True,
    "equity_curve": True,
    "journal_export": True,
    "output_dir": "backtest_output/",
}
```

### 輸出檔案

| 檔案 | 內容 |
|------|------|
| `trades.csv` | `entry_time, exit_time, side, entry_price, exit_price, quantity, pnl` |
| `equity_curve.csv` | `bar, equity` -- 每筆平倉交易後的累計損益 |
| `report.md` | 包含所有指標、月報酬率和配置的 Markdown 表格 |
| `journal.jsonl` | 結構化事件記錄（訊號發送、成交、錯誤），JSON Lines 格式 |

---

## 熔斷機制 (Circuit Breaker)

`max_drawdown_limit` 參數作為熔斷機制。如果帳戶餘額跌破閾值，回測會立即停止。

```python
runner = BacktestRunner(
    ...,
    initial_balance=10000.0,
    max_drawdown_limit=0.20,  # stop if balance < 8000 (20% drawdown)
)
```

閾值的計算方式為：

```
stop_threshold = initial_balance * (1 - max_drawdown_limit)
```

!!! note "熔斷機制 vs 策略邏輯"
    熔斷機制是 Runner 層級的安全機制。它不能取代策略內部的風險管理（例如部位大小、單筆交易風險限制）。兩者應一起使用。

---

## 多策略回測 (Multi-Strategy Backtesting)

在同一次回測中執行多個策略，以比較績效或測試投資組合行為：

```python
from src.strategies.golden_cross import GoldenCrossStrategy
from src.strategies.rsi_scalper import RSIScalperStrategy

strategy_a = GoldenCrossStrategy(
    strategy_id="golden_50_200",
    product_id="BINANCE:BTCUSDT-PERP",
    short_window=50,
    long_window=200,
)
strategy_b = RSIScalperStrategy(
    strategy_id="rsi_scalper",
    product_id="BINANCE:BTCUSDT-PERP",
)

runner = BacktestRunner(
    start_time=1700000000000,
    end_time=1700500000000,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
    initial_balance=10000.0,
    data_source=ds,
    fee_config={"maker": 0.0002, "taker": 0.0006},
)

runner.add_strategy(strategy_a)
runner.add_strategy(strategy_b)

result = runner.run()
```

當註冊了多個策略時，結果包含各策略的個別指標：

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

### 使用 CapitalAllocator 進行資金分配

執行多個策略時，使用 `CapitalAllocator` 來劃分帳戶餘額，防止策略過度佔用共享資金：

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

`CapitalAllocator` 主要方法：

| 方法 | 說明 |
|------|------|
| `allocate(strategy_id, amount)` | 為策略預留資金 |
| `deallocate(strategy_id)` | 將資金歸還資金池（若資金仍在使用中則失敗） |
| `get_available(strategy_id)` | 已分配減去已使用 |
| `get_allocation(strategy_id)` | 總分配金額 |
| `get_unallocated()` | 剩餘未分配餘額 |
| `record_usage(strategy_id, amount)` | 標記資金為使用中（開倉時） |
| `release_usage(strategy_id, amount)` | 標記資金為可用（平倉時） |
| `update_total_balance(new_balance)` | 損益變動後調整總額 |

!!! warning "執行緒安全 (Thread Safety)"
    `CapitalAllocator` 是執行緒安全的——所有公開方法都會取得鎖。所有金額值必須是 `Decimal`；傳入 `float` 會拋出 `TypeError`。

---

## 解讀結果

`runner.run()` 的回傳值是一個字典，包含以下鍵值：

### 核心指標

| 鍵 | 型別 | 說明 |
|----|------|------|
| `total_pnl` | `Decimal` | 扣除手續費後的淨損益 |
| `total_trades` | `int` | 已完成的往返交易數 |
| `win_rate` | `Decimal` | 獲利交易的比例（0.0 -- 1.0） |
| `profit_factor` | `Decimal` | 總獲利 / 總虧損（>1.0 表示獲利） |
| `max_drawdown` | `Decimal` | 最大峰谷回撤 |
| `trade_sharpe` | `Decimal` | 根據每筆交易報酬計算的夏普比率 (Sharpe Ratio) |
| `sortino_ratio` | `Decimal` | 類似夏普比率但僅懲罰下行波動 |
| `calmar_ratio` | `Decimal` | 年化報酬 / 最大回撤 |

### 詳細指標

| 鍵 | 型別 | 說明 |
|----|------|------|
| `avg_hold_time_hours` | `Decimal` | 平均交易持有時間（小時） |
| `max_consecutive_wins` | `int` | 最長連續獲利次數 |
| `max_consecutive_losses` | `int` | 最長連續虧損次數 |
| `journal_count` | `int` | 結構化日誌事件數量 |
| `report_dir` | `str` | 輸出目錄路徑（或 `None`） |
| `per_strategy` | `dict` | 各策略指標（僅多策略執行時） |
| `journal` | `list[dict]` | 原始日誌條目（字典格式） |

### 理解關鍵比率

**夏普比率 (Sharpe Ratio)** 衡量風險調整後報酬。高於 1.0 的值表示良好的風險調整績效；高於 2.0 為優秀。

**索提諾比率 (Sortino Ratio)** 類似於夏普比率，但僅考慮下行偏差。它不懲罰上行波動，因此對於具有不對稱報酬的策略更為相關。

**卡爾瑪比率 (Calmar Ratio)** 將年化報酬與最大回撤聯繫起來。卡爾瑪比率高於 1.0 意味著年化報酬超過最大回撤。

**獲利因子 (Profit Factor)** 是總獲利除以總虧損。1.5 的值意味著策略每虧損 $1.00 就賺取 $1.50。

**最大回撤 (Max Drawdown)** 是帳戶權益中最大的峰谷跌幅。搭配 `max_drawdown_limit`，這有助於評估策略是否保持在可接受的風險範圍內。

---

## 完整範例：端到端回測

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

## 下一步

- [撰寫策略](writing-strategies.md) -- 使用 `BaseStrategy` 建立自訂策略
- [外部訊號](external-signals.md) -- 整合 ML 模型和 CSV 訊號重播
