# 第一次回測

本指南涵蓋完整的 `BacktestRunner` API、所有可用的資料來源、指標解讀，以及多策略回測與熔斷機制 (Circuit Breaker) 等進階功能。

## 資料來源 (Data Sources)

FluxTrade 提供四種 `IDataSource` 實作，它們共享相同的介面：

```python
class IDataSource(ABC):
    def get_candles(self, product_id, timeframe, start, end) -> Generator[Candlestick, None, None]: ...
    def get_candles_df(self, product_id, timeframe, start, end) -> pd.DataFrame: ...
    def get_available_range(self, product_id, timeframe) -> Optional[tuple[int, int]]: ...
    def validate(self) -> bool: ...
```

所有時間戳均為 Unix 毫秒格式。

### CsvDataSource

從 CSV 檔案讀取 OHLCV 資料。自動偵測常見格式的欄位名稱（TradingView、Yahoo Finance、標準 OHLCV）。

```python
from src.core.data_sources import CsvDataSource

data_source = CsvDataSource(
    file_path="data/btcusdt_1h.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
)
```

**建構子參數：**

| 參數         | 型別  | 預設值             | 說明                               |
|-------------|-------|--------------------|------------------------------------|
| `file_path`  | `str` | （必填）           | CSV 檔案路徑                       |
| `product_id` | `str` | `"CSV:DATA-PERP"`  | 指派給 K 線的產品 ID               |
| `timeframe`  | `str` | `"1m"`             | 指派給 K 線的時間週期標籤          |

**支援的欄位名稱**（不區分大小寫）：

- 時間戳：`timestamp`、`time`、`ts`、`date`、`datetime`
- OHLCV：`open/Open/o`、`high/High/h`、`low/Low/l`、`close/Close/c/Adj Close`、`volume/Volume/vol/v`

時間戳可以是 Unix 毫秒、Unix 秒（數值 < 1e12 時自動偵測）或日期字串（由 pandas 解析）。

### DatabaseDataSource

透過 SQLAlchemy 從 PostgreSQL 讀取 K 線資料。需要已運行的資料庫並具備 FluxTrade 的 schema。

```python
from src.core.data_sources import DatabaseDataSource

data_source = DatabaseDataSource()
# 使用 src.core.db 的預設 SessionLocal
```

你也可以傳入自訂的 `session_factory`：

```python
data_source = DatabaseDataSource(session_factory=my_session_factory)
```

### YahooFinanceDataSource

從 Yahoo Finance 下載 OHLCV 資料。需要安裝選用套件 `yfinance`：

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

**建構子參數：**

| 參數         | 型別  | 預設值               | 說明                            |
|-------------|-------|----------------------|---------------------------------|
| `ticker`     | `str` | `"BTC-USD"`          | Yahoo Finance 股票代碼          |
| `product_id` | `str` | `"YAHOO:BTCUSD-PERP"`| 指派給 K 線的產品 ID            |
| `timeframe`  | `str` | `"1d"`               | 時間週期（需在支援的集合中）    |

**支援的時間週期**：`1m`、`2m`、`5m`、`15m`、`30m`、`1h`、`1d`、`1w`、`1M`

!!! warning "Yahoo Finance 限制"
    日內資料（1m-1h）僅限最近 7-60 天。日線資料則有完整歷史。

### MemoryDataSource

記憶體內的資料來源，用於測試與合成資料產生。

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
    # ... 更多 K 線
]

data_source = MemoryDataSource(candles=candles)
# 你也可以之後追加：
data_source.add_candles(more_candles)
```

## 設定 BacktestRunner

```python
from src.core.backtest_runner import BacktestRunner

runner = BacktestRunner(
    start_time=1704067200000,      # Unix 毫秒，含首
    end_time=1706745600000,        # Unix 毫秒，含尾
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
    initial_balance=10000.0,       # 以報價貨幣計的初始餘額
    max_drawdown_limit=0.20,       # 熔斷機制：回撤達 20% 時停止
    data_source=data_source,       # 任何 IDataSource 實作
    fee_config={                   # 交易手續費（由 Rust 引擎套用）
        "maker": 0.0002,           # 0.02%
        "taker": 0.0004,           # 0.04%
    },
    report_config={                # 輸出檔案設定
        "csv_trades": True,
        "markdown_report": True,
        "equity_curve": True,
        "journal_export": True,
        "output_dir": "backtest_output/",
    },
)
```

**建構子參數：**

| 參數                 | 型別                  | 預設值           | 說明                                           |
|---------------------|-----------------------|------------------|------------------------------------------------|
| `start_time`         | `int`                 | （必填）         | 回測開始時間，Unix 毫秒                        |
| `end_time`           | `int`                 | （必填）         | 回測結束時間，Unix 毫秒                        |
| `product_id`         | `str`                 | （必填）         | 交易產品（例如 `BINANCE:BTCUSDT-PERP`）        |
| `timeframe`          | `str`                 | （必填）         | K 線時間週期（例如 `1h`、`15m`）               |
| `initial_balance`    | `float`               | `10000.0`        | 以報價貨幣計的初始餘額                         |
| `max_drawdown_limit` | `float`               | `0.20`           | 熔斷機制閾值（比例）                           |
| `data_source`        | `IDataSource` or None | `None`           | 資料來源；若為 None 則回退至資料庫             |
| `fee_config`         | `dict` or None        | `{}`             | `{"maker": float, "taker": float}`             |
| `report_config`      | `dict` or None        | 參見預設值       | 控制產生哪些報告                               |

## 加入策略

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

你可以在同一個 runner 中加入多個策略。每個策略都會接收每一根 K 線，並可獨立發出訊號。當存在多個策略時，runner 會追蹤各策略的獨立指標。

## 執行回測

```python
result = runner.run()
```

`run()` 方法的執行流程：

1. 在資料庫中註冊策略（建立缺失的記錄以滿足外鍵約束）
2. 在 PostgreSQL 中建立 `BacktestResultSummary` 記錄
3. 實例化由 Rust `PyMatchingEngine` 支撐的 `SimulatedAdapter`
4. 遍歷資料來源的 K 線，將每一根傳入 `StrategyEngine`
5. 每根 K 線後檢查熔斷機制
6. 透過 `calculate_metrics()` 使用 FIFO 交易配對計算指標
7. 將報告檔案寫入輸出目錄

## 解讀結果

回傳的字典包含以下內容：

### 基本指標

| 鍵名             | 型別      | 說明                                       |
|------------------|-----------|--------------------------------------------|
| `total_pnl`     | `Decimal` | 扣除所有手續費後的淨利潤/虧損              |
| `total_trades`  | `int`     | 已完成的來回交易次數                       |
| `win_rate`      | `Decimal` | PnL 為正的交易比例                         |
| `profit_factor` | `Decimal` | 總利潤 / 總虧損                            |
| `max_drawdown`  | `Decimal` | 最大峰谷權益回撤                           |
| `trade_sharpe`  | `Decimal` | 基於交易的夏普比率（PnL 的均值/標準差）    |

### 進階指標

| 鍵名                          | 型別      | 說明                                       |
|-------------------------------|-----------|--------------------------------------------|
| `sortino_ratio`               | `Decimal` | 報酬 / 下行偏差                            |
| `calmar_ratio`                | `Decimal` | 年化報酬 / 最大回撤                        |
| `avg_trade`                   | `Decimal` | 每筆交易的平均 PnL                         |
| `avg_hold_time_hours`         | `Decimal` | 平均持倉時間（小時）                       |
| `max_drawdown_days`           | `Decimal` | 最長回撤持續天數                           |
| `trade_frequency_per_day`     | `Decimal` | 每日平均交易次數                           |
| `max_consecutive_wins`        | `int`     | 最長連勝次數                               |
| `max_consecutive_losses`      | `int`     | 最長連虧次數                               |
| `max_consecutive_win_amount`  | `Decimal` | 最長連勝期間的總 PnL                       |
| `max_consecutive_loss_amount` | `Decimal` | 最長連虧期間的總虧損                       |
| `gross_profit`                | `Decimal` | 所有獲利交易的 PnL 總和                    |
| `gross_loss`                  | `Decimal` | 所有虧損交易的 PnL 總和（絕對值）          |

### 其他輸出

| 鍵名            | 型別   | 說明                                             |
|-----------------|--------|--------------------------------------------------|
| `journal`       | `list` | 來自日誌的結構化事件字典列表                     |
| `journal_count` | `int`  | 日誌條目數量                                     |
| `report_dir`    | `str`  | 包含報告檔案的輸出目錄路徑                       |
| `per_strategy`  | `dict` | 以策略 ID 為鍵的指標字典（僅多策略模式）         |

### 月報酬

指標中還包含 `monthly_returns`（可從儲存在資料庫中的完整指標取得），這是一個以 `"YYYY-MM"` 字串為鍵、`Decimal` PnL 值為值的字典，代表每月的報酬。

## 報告檔案

預設情況下，`BacktestRunner` 會將四個檔案寫入 `backtest_output/`：

| 檔案                | 內容                                                   |
|---------------------|--------------------------------------------------------|
| `report.md`         | Markdown 摘要，含設定表格與所有指標                    |
| `trades.csv`        | 已平倉交易：進出場時間、價格、方向、PnL               |
| `equity_curve.csv`  | 逐根的累計 PnL                                        |
| `journal.jsonl`     | 結構化事件日誌（訊號、成交、錯誤）                    |

透過 `report_config` 停用特定輸出：

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

## 熔斷機制 (Circuit Breaker)

`max_drawdown_limit` 參數（預設 `0.20` = 20%）會在帳戶餘額低於 `initial_balance * (1 - max_drawdown_limit)` 時自動觸發停止。

例如，`initial_balance=10000.0` 且 `max_drawdown_limit=0.20` 時，若餘額跌至 8000 以下，回測將自動停止。

```python
runner = BacktestRunner(
    ...,
    max_drawdown_limit=0.30,  # 允許最多 30% 回撤後才停止
)
```

設定 `max_drawdown_limit=1.0` 可有效停用熔斷機制。

## 多策略回測

加入多個策略以測試投資組合層級的行為：

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

# 各策略的分項指標（僅在 >1 個策略時出現）
for strategy_id, metrics in result["per_strategy"].items():
    print(f"{strategy_id}: PnL={metrics['total_pnl']}, Trades={metrics['total_trades']}")
```

當存在多個策略時，結果中的 `per_strategy` 鍵包含各策略的獨立指標，透過 `strategy_id` 篩選交易來計算。

## 手續費

手續費由 Rust 搓合引擎在成交時套用。透過 `fee_config` 進行設定：

```python
fee_config = {
    "maker": 0.0002,  # 限價單 0.02%
    "taker": 0.0004,  # 市價單 0.04%
}
```

所有手續費扣除都反映在 `total_pnl` 與每筆交易的 PnL 值中。搓合引擎處理 Market、Limit、Stop-Loss、Take-Profit、Trailing Stop 與 OCO 訂單類型。

## 完整範例

```python
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources import CsvDataSource
from src.strategies.golden_cross import GoldenCrossStrategy

# 資料
data_source = CsvDataSource(
    file_path="data/btcusdt_2024_1h.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
)

time_range = data_source.get_available_range("BINANCE:BTCUSDT-PERP", "1h")
if time_range is None:
    raise RuntimeError("No data found in CSV")
start_time, end_time = time_range

# 執行器
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

# 策略
runner.add_strategy(GoldenCrossStrategy(
    strategy_id="gc-btc-1h",
    product_id="BINANCE:BTCUSDT-PERP",
    short_window=50,
    long_window=200,
))

# 執行
result = runner.run()

# 摘要
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

## 後續步驟

- [撰寫策略](../guide/writing-strategies.md) -- 學習如何建立包含 SL/TP/追蹤停損的自訂策略
- [回測指南](../guide/backtesting.md) -- 進階回測模式與優化技巧
- [架構總覽](../architecture/overview.md) -- 了解引擎、適配器與搓合引擎之間的協作方式
