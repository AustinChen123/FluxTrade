# 快速開始

使用內建的 Golden Cross 策略與 CSV 資料檔，在 5 分鐘內完成一次回測。

## 1. 準備範例資料

!!! warning "資料需求"
    `GoldenCrossStrategy` 預設使用 200 週期的 SMA，因此你需要**至少 201 根** 1h K 線資料。僅 3 行的 CSV 可以正常執行但不會產生任何交易。

建立一個包含 OHLCV 資料的 CSV 檔案。`CsvDataSource` 會自動偵測常見的欄位命名慣例（TradingView、Yahoo Finance、標準格式）。

將資料放置於 `python-strategy/data/sample.csv`：

```csv
timestamp,open,high,low,close,volume
1704067200000,42000.0,42500.0,41800.0,42300.0,150.5
1704070800000,42300.0,42800.0,42100.0,42700.0,180.2
...
```

每一行代表一根 K 線 (Candlestick)。時間戳為 Unix 毫秒格式。你也可以使用日期字串（`2024-01-01 00:00:00`）或 Unix 秒——資料來源會自動處理轉換。

你可以從交易所下載歷史資料，或使用 `YahooFinanceDataSource`（參見[第一次回測](first-backtest.md)）來免除 CSV 檔案的準備工作。

## 2. 撰寫回測腳本

建立 `python-strategy/run_quick_backtest.py`：

```python
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources import CsvDataSource
from src.strategies.golden_cross import GoldenCrossStrategy

# 1. 設定資料來源
data_source = CsvDataSource(
    file_path="data/sample.csv",
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
)

# 2. 從 CSV 取得時間範圍
time_range = data_source.get_available_range(
    "BINANCE:BTCUSDT-PERP", "1h"
)
if time_range is None:
    raise RuntimeError("No data found — check your CSV path and column names")
start_time, end_time = time_range

# 3. 建立回測執行器
runner = BacktestRunner(
    start_time=start_time,
    end_time=end_time,
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1h",
    initial_balance=10000.0,
    data_source=data_source,
    fee_config={"maker": 0.0002, "taker": 0.0004},
)

# 4. 加入策略
strategy = GoldenCrossStrategy(
    strategy_id="golden-cross-btc",
    product_id="BINANCE:BTCUSDT-PERP",
    short_window=50,
    long_window=200,
)
runner.add_strategy(strategy)

# 5. 執行
result = runner.run()

# 6. 輸出結果
print(f"Total PnL:      {result['total_pnl']}")
print(f"Total Trades:   {result['total_trades']}")
print(f"Win Rate:       {result['win_rate']}")
print(f"Profit Factor:  {result['profit_factor']}")
print(f"Max Drawdown:   {result['max_drawdown']}")
print(f"Sharpe Ratio:   {result['trade_sharpe']}")
```

## 3. 執行回測

```bash
cd python-strategy
uv run python run_quick_backtest.py
```

## 4. 了解輸出結果

`run()` 方法回傳一個包含以下鍵值的字典：

| 鍵名                   | 型別      | 說明                                     |
|------------------------|-----------|------------------------------------------|
| `total_pnl`           | `Decimal` | 扣除手續費後的淨利潤/虧損               |
| `total_trades`        | `int`     | 已完成的來回交易次數                     |
| `win_rate`            | `Decimal` | 獲利交易的比例                           |
| `profit_factor`       | `Decimal` | 總利潤 / 總虧損                          |
| `max_drawdown`        | `Decimal` | 最大峰谷回撤                             |
| `trade_sharpe`        | `Decimal` | 基於交易的夏普比率 (Sharpe Ratio)        |
| `sortino_ratio`       | `Decimal` | 索提諾比率（僅計算下行偏差）             |
| `calmar_ratio`        | `Decimal` | 年化報酬 / 最大回撤                      |
| `avg_hold_time_hours` | `Decimal` | 平均持倉時間（小時）                     |
| `per_strategy`        | `dict`    | 各策略的獨立指標（多策略模式）           |
| `report_dir`          | `str`     | 產生的報告檔案路徑                       |

預設情況下，`BacktestRunner` 也會將報告檔案寫入 `backtest_output/`：

- `report.md` -- Markdown 摘要，包含設定與指標表格
- `trades.csv` -- 所有已平倉交易，含進出場價格與 PnL
- `equity_curve.csv` -- 逐根的權益曲線
- `journal.jsonl` -- 所有策略動作的結構化事件日誌

## 5. 撰寫你自己的策略

每個策略都繼承 `BaseStrategy` 並實作兩個方法：

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
        # 你的邏輯寫在這裡
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )
```

`Signal` 可以攜帶進場參數，執行引擎會據此管理完整的訂單生命週期：

| Signal 欄位           | 型別              | 說明                                      |
|----------------------|-------------------|-------------------------------------------|
| `type`               | `SignalType`      | `LONG`、`SHORT`、`EXIT_LONG`、`EXIT_SHORT`、`NO_SIGNAL` |
| `quantity`           | `Decimal` or None | 倉位大小（選填，系統可使用預設值）        |
| `price`              | `Decimal` or None | 限價（選填，None 則為市價單）             |
| `stop_loss`          | `Decimal` or None | 停損價格                                  |
| `take_profit`        | `Decimal` or None | 止盈價格                                  |
| `trailing_distance`  | `Decimal` or None | 追蹤停損距離                              |
| `metadata`           | `dict` or None    | 任意策略元資料                            |

策略只負責發出訊號。所有 SL/TP/追蹤停損的管理都由 Rust 搓合引擎處理——切勿在 `on_candle()` 中實作訂單管理邏輯。

## 後續步驟

- [第一次回測](first-backtest.md) -- 所有資料來源與進階設定的詳細說明
- [撰寫策略](../guide/writing-strategies.md) -- 完整的策略撰寫指南
- [實盤交易](../guide/live-trading.md) -- 將你的策略部署到實盤交易所
