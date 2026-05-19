# 外部訊號 (External Signals)

FluxTrade 支援兩種內建適配器，用於將來自外部來源（ML 模型、第三方警報、預先計算的 CSV）的訊號 (Signal) 餵入策略引擎。兩種適配器都實作 `BaseStrategy`，因此可以無縫地與 `BacktestRunner` 和 `StrategyEngine` 搭配使用。

---

## CallableStrategy

`CallableStrategy` 將任何 Python 可呼叫物件 (Callable) 包裝為完全可回測的策略。這是整合 **ML 模型**、**外部 API** 和**自訂訊號產生器**的主要接入點。

### 建構子

```python
from src.strategies.callable_strategy import CallableStrategy

CallableStrategy(
    strategy_id: str,                                  # 唯一識別碼
    predict_fn: Callable[[Candlestick], Signal | None], # 你的訊號函式
    product_id: str,                                   # 例如 "BINANCE:BTCUSDT-PERP"
    timeframe: str = "1h",                             # K 線時間框架
    lookback_window: int = 1,                          # 第一個訊號前需要的 K 線數
)
```

### 運作方式

1. 每根 K 線上，引擎呼叫 `on_candle(candle)`。
2. `CallableStrategy` 委派給你的 `predict_fn(candle)`。
3. 如果 `predict_fn` 回傳一個 `Signal`，其 `strategy_id` 會被覆寫以匹配此策略實例。
4. 如果 `predict_fn` 回傳 `None`，會自動發出 `NO_SIGNAL`。

!!! tip "無動作時回傳 None"
    當沒有交易設置時，你的預測函式應回傳 `None`。不需要手動建構 `NO_SIGNAL`——包裝器會為你處理。

### 範例：PyTorch 模型整合

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

你現在可以將此 `strategy` 傳遞給 `BacktestRunner.add_strategy()` 或 `StrategyEngine.add_strategy()`——它的行為與任何手寫策略完全相同。

### 範例：Webhook/外部警報適配器

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

`CsvSignalStrategy` 透過比對 K 線時間戳來重播 CSV 檔案中預先計算的訊號。適用於：

- 重播離線產生的訊號（例如來自 Jupyter Notebook）
- 測試從其他系統匯出的訊號集
- 確定性回歸測試 (Deterministic Regression Testing)

### CSV 格式

CSV 檔案必須包含標題列。必要欄位：

| 欄位 | 型別 | 說明 |
|------|------|------|
| `timestamp` | `int` | Unix 毫秒時間戳（必須與 K 線時間戳完全匹配） |
| `type` | `str` | 以下之一：`LONG`, `SHORT`, `EXIT_LONG`, `EXIT_SHORT` |

選用欄位：

| 欄位 | 型別 | 說明 |
|------|------|------|
| `quantity` | `Decimal` | 部位大小 |
| `price` | `Decimal` | 限價（省略表示市價單） |
| `stop_loss` | `Decimal` | 停損價格 |
| `take_profit` | `Decimal` | 停利價格 |
| `trailing_distance` | `Decimal` | 移動停損距離 |

**CSV 範例** (`signals/btc_replay.csv`)：

```csv
timestamp,type,quantity,stop_loss,take_profit
1700000000000,LONG,0.01,29500.00,31000.00
1700003600000,EXIT_LONG,,
1700010800000,SHORT,0.01,31200.00,29800.00
1700018000000,EXIT_SHORT,,
```

!!! note "空白的選用欄位"
    當選用欄位不適用時，請留空（而不是 `0` 或 `null`）。解析器會將空字串視為 `None`。

### 建構子

```python
from src.strategies.csv_signal_strategy import CsvSignalStrategy

CsvSignalStrategy(
    strategy_id: str,         # 唯一識別碼
    csv_path: str,            # CSV 檔案路徑
    product_id: str,          # 例如 "BINANCE:BTCUSDT-PERP"
    timeframe: str = "1h",    # K 線時間框架
    lookback_window: int = 1, # 第一個訊號前需要的 K 線數
)
```

### 運作方式

1. 建構時，整個 CSV 會被載入到一個以時間戳為鍵的 `Dict[int, Signal]` 中。
2. 每次 `on_candle()` 呼叫時，策略會檢查該 K 線時間戳是否存在對應訊號。
3. 若找到匹配，回傳預先建構的 Signal。
4. 若無匹配，回傳 `NO_SIGNAL`。

!!! warning "時間戳匹配必須完全精確"
    CSV 中的時間戳必須與你的資料來源產生的 K 線時間戳完全一致。如果你的資料來源使用秒精度的時間戳而 CSV 使用毫秒（或反過來），訊號將永遠不會匹配。

### 使用方式

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

## 確定性測試：Callable vs CSV

一個強大的測試模式是驗證 `CallableStrategy` 和 `CsvSignalStrategy` 產生完全相同的結果。這確保你的訊號產生邏輯是確定性且可重現的。

### 步驟 1：產生訊號並匯出為 CSV

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

### 步驟 2：分別回測並比較

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

!!! tip "為什麼這很重要"
    如果你的 Callable 和 CSV 回測結果產生分歧，代表 (a) 訊號匯出遺漏了某些訊號、(b) 時間戳對齊有誤，或 (c) 預測函式包含非確定性行為（例如隨機抽樣）。此測試能捕捉這三種問題。

---

## 下一步

- [撰寫策略](writing-strategies.md) -- 使用 `BaseStrategy` 從零開始建立策略
- [回測](backtesting.md) -- 完整的回測配置、指標和報告
