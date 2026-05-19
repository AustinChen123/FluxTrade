# 撰寫策略 (Writing Strategies)

本指南涵蓋如何為 FluxTrade 建立自訂交易策略 (Strategy)。你所撰寫的每個策略在**實盤交易 (Live Trading)** 與**回測 (Backtesting)** 中都以完全相同的方式執行——不需要修改任何程式碼。

---

## 核心概念

一個 FluxTrade 策略是一個 Python 類別，它：

1. 繼承 `BaseStrategy`（一個 ABC 抽象基底類別）
2. 透過 `requirements` 屬性宣告其資料需求
3. 實作 `on_candle()` 來處理每根 K 線 (Candlestick) 並回傳一個 `Signal`

系統會處理其他所有事情：下單、SL/TP/移動停損 (Trailing Stop) 管理、持倉追蹤，以及手續費計算。**策略只發出 Signal**——它們從不直接與交易所互動。

---

## BaseStrategy ABC

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

每個策略必須實作兩個項目：

| 成員 | 用途 |
|------|------|
| `requirements`（屬性） | 告知引擎需要餵給策略哪些資料 |
| `on_candle(candle)` | 每根 K 線呼叫一次；回傳一個 `Signal` |
| `on_trade(trade)` | 選用：在逐筆成交資料 (Tick-level) 上呼叫；回傳 `Signal` 或 `None` |
| `run_vectorized(df)` | 選用：使用 Pandas DataFrame 的向量化執行（參見下方） |

---

## StrategyRequirements

```python
from dataclasses import dataclass

@dataclass
class StrategyRequirements:
    product_id: str          # 例如 "BINANCE:BTCUSDT-PERP"
    timeframe: str           # 例如 "15m", "1h", "4h"
    lookback_window: int     # 在第一個訊號前需要多少根歷史 K 線
```

`lookback_window` 告知引擎在策略能產生有意義的訊號之前，需要累積多少根 K 線。在這些初始 K 線期間，你的策略應回傳 `SignalType.NO_SIGNAL`。

!!! tip "時間框架頻道隔離 (Timeframe Channel Isolation)"
    引擎僅投遞與策略宣告的 `timeframe` 相符的 K 線。如果你的策略宣告 `"15m"`，它永遠不會收到 1h 或 4h 的 K 線。此隔離在 Redis Stream 層級強制執行。

---

## Signal 模型

```python
class Signal(BaseFluxModel):
    strategy_id: str                         # 由引擎自動設定
    product_id: str                          # 例如 "BINANCE:BTCUSDT-PERP"
    timeframe: str                           # 例如 "15m"
    timestamp: int                           # 來自 K 線的 Unix 毫秒時間戳
    type: SignalType                         # LONG, SHORT, EXIT_LONG, EXIT_SHORT, NO_SIGNAL
    value: Optional[Decimal] = None          # 用於記錄的指標數值
    quantity: Optional[Decimal] = None       # 部位大小
    price: Optional[Decimal] = None          # 限價 (None = 市價單)
    stop_loss: Optional[Decimal] = None      # 絕對停損價格
    take_profit: Optional[Decimal] = None    # 絕對停利價格
    trailing_distance: Optional[Decimal] = None  # 移動停損距離
    metadata: Optional[dict] = None          # 任意額外資料
```

### SignalType 列舉

| 值 | 意義 |
|----|------|
| `LONG` | 開多倉（或加倉） |
| `SHORT` | 開空倉（或加倉） |
| `EXIT_LONG` | 平掉現有多倉 |
| `EXIT_SHORT` | 平掉現有空倉 |
| `NO_SIGNAL` | 本根 K 線不動作 |

!!! warning "所有價格必須使用 Decimal"
    FluxTrade 對所有財務數值強制使用 `Decimal`。切勿對價格、數量或損益使用 `float`。從 `decimal` 匯入並透過字串建構：`Decimal("0.01")`。

---

## 完整範例：SMA 交叉策略

此策略在快速 SMA 上穿慢速 SMA 時做多，在下穿時平倉。

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

!!! note "SL/TP 管理"
    你只需要在進場訊號上設定 `stop_loss` 和 `take_profit`。Rust 撮合引擎 (`PyMatchingEngine`) 會在後續每根 K 線上自動監控並觸發這些委託單。永遠不要在 `on_candle()` 內部實作 SL/TP 檢查邏輯。

---

## 執行你的策略

### 回測（建議起點）

最常見的策略執行方式是透過 `BacktestRunner`。它將歷史 K 線資料送入引擎，透過 Rust 撮合引擎處理訊號，並產出績效指標：

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

完整詳情請參閱[回測指南](backtesting.md)，包含資料來源、手續費配置、報告輸出和結果解讀。

### 實盤交易（進階）

在正式環境中，策略透過 `StrategyEngine` 註冊，引擎會連接到基於 Redis 的行情資料管線，並將訊號路由到實盤交易所適配器 (Adapter)：

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

`StrategyEngine.startup()` 會啟動背景服務（心跳、指令監聽器、策略掃描器）並開始透過 Redis Streams 從 Rust 資料服務處理行情資料。完整部署流程請參閱[實盤交易指南](live-trading.md)。

---

## 使用 MemoryDataSource 測試策略

你可以不需要資料庫或 CSV 檔案，使用 `MemoryDataSource` 對策略進行單元測試：

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

若要進行包含訂單成交和損益的完整端到端回測，請搭配 `MemoryDataSource` 使用 `BacktestRunner`：

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

## 向量化執行 (Vectorized Execution)（選用）

對於受益於批次計算的策略（例如指標密集型策略），你可以實作 `run_vectorized()`。此方法接收包含 OHLCV 欄位的 Pandas DataFrame，並應回傳帶有 `signal` 欄位的 DataFrame。

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

基底類別預設會拋出 `NotImplementedError`，因此此方法完全是選用的。`BacktestRunner` 使用事件驅動的 `on_candle()` 路徑。`run_vectorized()` 可用於你想一次對整個 DataFrame 計算訊號的自訂分析工作流程。

---

## 策略設計指南

### 應該做的

- 當沒有明確的交易設置時回傳 `SignalType.NO_SIGNAL`——引擎預期每根 K 線都有一個 Signal。
- 對所有價格/數量計算使用 `Decimal`。
- 使用滾動緩衝區（例如 `deque(maxlen=...)`）而非無限增長的列表。
- 誠實設定 `lookback_window`——引擎在暖機期間會跳過訊號處理。
- 使用 `self.journal.log()` 在回測期間進行結構化事件記錄。

### 不應該做的

- 永遠不要在 `on_candle()` 內部呼叫交易所 API——適配器模式 (Adapter Pattern) 會處理這件事。
- 永遠不要在策略中實作 SL/TP/移動停損監控——Rust 撮合引擎會負責。
- 永遠不要對價格或數量使用 `float`。
- 永遠不要假設你處於實盤或回測模式——相同的程式碼必須在兩者中都能運作。

---

## 下一步

- [外部訊號](external-signals.md) -- 整合 ML 模型或重播預先計算的訊號
- [回測](backtesting.md) -- 執行包含指標和報告的完整回測
