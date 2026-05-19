# 適配器模式 (Adapter Pattern) — 實盤/回測一致性

## 核心原則

FluxTrade 的根本承諾是**相同的 Python 策略程式碼在實盤交易和回測中完全一致地運行**。適配器模式 (Adapter Pattern) 是實現這一目標的機制。

策略完全透過 `IExchangeAdapter` 介面與交易所互動。它們永遠不會檢查自己處於哪種模式、永遠不會匯入模式特定的程式碼，也永遠不會根據設定旗標進行分支。適配器在引擎啟動時注入，策略不知道自己接收到的是哪個實作。

## IExchangeAdapter 介面

定義於 `src/core/interfaces/exchange.py`。此介面是**同步的**（非 async）：

```python
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, List, Optional

from src.core.orm_models import Order
from src.core.models import Candlestick, Position


class IExchangeAdapter(ABC):
    """Unified interface for all exchange interactions."""

    @abstractmethod
    def place_order(self, order: Order) -> str:
        """Place an order. Takes an ORM Order object, returns exchange order ID string."""
        ...

    @abstractmethod
    def cancel_order(self, order_id: str, product_id: str) -> bool:
        """Cancel an open order. Returns True if successful."""
        ...

    @abstractmethod
    def get_balance(self, asset: str) -> Decimal:
        """Return available balance for a specific asset as Decimal."""
        ...

    @abstractmethod
    def get_position(self, product_id: str) -> Optional[Position]:
        """Return current open position for a product, or None."""
        ...

    def on_market_data(self, candle: Candlestick) -> List[Dict]:
        """Process market data to check for simulated order fills.

        Override in simulated adapters. Live adapters return empty list.
        Returns list of fill dicts: {order, price, quantity, fee, fill_type}.
        """
        return []
```

系統中的每一次交易所互動都透過此介面進行。`execution.py` 模組呼叫 `adapter.place_order()` 將訊號轉換為訂單；引擎呼叫 `adapter.on_market_data()` 餵入 K 線；風險檢查呼叫 `adapter.get_balance()` 驗證可用資金。

### 例外階層 (Exception Hierarchy)

介面在同一模組中定義三種例外類型：

- `ExchangeError` — 所有交易所相關錯誤的基礎例外
- `InsufficientFundsError(ExchangeError)` — 資金不足無法下單
- `NetworkError(ExchangeError)` — 網路連線問題

## 訊號 (Signal) 與策略類型

策略繼承 `BaseStrategy` 並發出 `Signal` 物件：

```python
class BaseStrategy(ABC):
    def __init__(self, strategy_id: str, product_id: str):
        self.strategy_id = strategy_id
        self.product_id = product_id
        self.journal = StrategyJournal(strategy_id)

    @property
    @abstractmethod
    def requirements(self) -> StrategyRequirements: ...

    @abstractmethod
    def on_candle(self, candle: Candlestick) -> Signal: ...
```

訊號使用 `SignalType` 列舉：

```python
class SignalType(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"
    NO_SIGNAL = "NO_SIGNAL"
```

方向列舉 (Side Enum) 定義如下：

- `OrderSide(str, Enum)`，值為 `BUY = "buy"` 和 `SELL = "sell"`
- `PositionSide(str, Enum)`，值為 `LONG = "LONG"` 和 `SHORT = "SHORT"`

`str, Enum` 基底確保與字串比較的向後相容性。

## 適配器實作

### SimulatedAdapter（回測用）

**檔案**：`src/core/adapters/simulated.py`

`SimulatedAdapter` 將所有訂單搓合委託給透過 PyO3 的 Rust `PyMatchingEngine`。它本身不包含任何搓合邏輯 — 每一筆 Market、Limit、Stop-Loss、Take-Profit、Trailing Stop 和 OCO 訂單都由 Rust 處理。

```
Strategy Signal
    -> execution.py creates Order
    -> SimulatedAdapter.place_order()
    -> PyMatchingEngine.submit_order()  [Rust]
    -> on_market_data() ticks the engine each candle
    -> PyMatchingEngine.on_candle()  [Rust]
    -> fills returned to Python as FillEvent objects
    -> SimulatedAdapter converts to fill dicts
```

主要職責：

- **String/Decimal 邊界**：跨入 Rust 時將 Python `Decimal` 轉換為 `str`，返回時將 `str` 解析回 `Decimal`
- **方向轉換 (Side Conversion)**：透過 `_side_to_rust()` 將 `buy/sell`（ORM OrderSide）轉譯為 `LONG/SHORT`（Rust PositionSide）。對於條件單（SL/TP/Trailing），方向會被反轉，因為 Rust 預期的是被保護的持倉方向
- **餘額追蹤**：餘額完全由 Rust 引擎管理；`get_balance()` 讀取 `self._engine.balance`
- **持倉查詢**：使用 Rust 引擎持倉 HashMap 中的複合鍵 `strategy_id:product_id`，並對純 product_id 鍵提供向後相容的回退機制
- **成交傳播 (Fill Propagation)**：返回與 `ExecutionEngine` 相容的成交結果字典：`{"order": ORM Order, "price": Decimal, "quantity": Decimal, "fee": Decimal, "fill_type": str}`
- **OCO 清理**：成交後，透過移除 Rust 已取消的訂單（例如 OCO 對應方）來同步 `_order_map`

!!! warning "Python 中不得包含搓合邏輯"
    `SimulatedAdapter` 必須**絕對不能**包含訂單搓合邏輯。所有搓合 — 包括 SL/TP 觸發、追蹤停損調整和 OCO 取消 — 完全存在於 Rust `PyMatchingEngine` 中。這防止了模擬與實盤行為之間的分歧。

### CcxtExchangeAdapter（實盤 — 通用）

**檔案**：`src/core/adapters/ccxt_adapter.py`

封裝 [CCXT](https://github.com/ccxt/ccxt) 函式庫，為 100+ 個加密貨幣交易所提供統一介面。處理內容包括：

- 透過 `self.client.create_order()` 下單，搭配交易所特定的參數映射
- 透過 `self.client.fetch_balance()` 查詢餘額，返回 `Decimal(str(free.get(asset, 0)))`
- 透過 `self.client.fetch_positions()` 查詢持倉，搭配 CCXT 交易對轉換
- 訂單取消並處理 `OrderNotFound`
- 透過 CCXT 內建節流器 (Throttler) 進行速率限制

```python
def place_order(self, order: Order) -> str:
    ccxt_symbol = to_ccxt_symbol(order.product_id)
    response = self.client.create_order(
        symbol=ccxt_symbol,
        type=order.type,
        side=order.side,          # 'buy' or 'sell' at CCXT boundary
        amount=str(order.quantity),
        price=str(order.price) if order.price else None,
        params=params,
    )
    return str(response["id"])
```

建構函式參數：

```python
CcxtExchangeAdapter(
    exchange_id: str,          # CCXT exchange name (e.g., "binance", "bybit")
    api_key: str | None,       # Falls back to EXCHANGE_API_KEY env var
    secret: str | None,        # Falls back to EXCHANGE_SECRET env var
    testnet: bool = False,
    extra_config: dict | None = None,
)
```

!!! note "Decimal 紀律"
    適配器將 `str(order.quantity)` 和 `str(order.price)` 傳遞給 CCXT，絕不使用 `float()`。這在整個管線中保持精度。

### LiveBinanceAdapter（實盤 — Binance 最佳化）

**檔案**：`src/core/adapters/live_binance.py`

擴展 `CcxtExchangeAdapter`，為市價單提供 WebSocket 快速通道：

- 嘗試透過 `WebSocketOrderConnector` 以 WebSocket 方式送出市價單
- 若 WebSocket 不可用或下單失敗，回退至 REST（父類別）
- 僅在 `enable_ws=True` 且 WebSocket 連線成功時啟用

```python
class LiveBinanceAdapter(CcxtExchangeAdapter):
    def __init__(
        self,
        api_key: str | None = None,
        secret: str | None = None,
        testnet: bool = True,
        enable_ws: bool = True,
    ): ...

    def place_order(self, order: Order) -> str:
        # Try WS fast path for market orders
        if self.ws_connector and order.type.lower() == "market":
            # ... attempt WebSocket order
        # REST fallback (parent class)
        return super().place_order(order)
```

所有其他方法（`cancel_order`、`get_balance`、`get_position`）繼承自 `CcxtExchangeAdapter`。

## 工廠函式 (Factory Function)

**檔案**：`src/core/adapters/__init__.py`

```python
def create_adapter(config: dict) -> IExchangeAdapter:
    """
    Factory that selects the appropriate adapter based on configuration.

    Config keys:
        mode: "simulated" | "live"  (default: "simulated")
        exchange: CCXT exchange id  (required for live, default: "binance")
        api_key / secret: optional, falls back to env vars
        testnet: bool (default: True)
        balance: initial balance (simulated only, default: 100000)
        maker_fee / taker_fee: fee rates (simulated only, default: 0)
        enable_ws: bool (live only, default: False)
        extra_config: dict (extra CCXT config, optional)

    Selection logic:
    - mode == "simulated" -> SimulatedAdapter(balance, maker_fee, taker_fee)
    - mode == "live" and exchange == "binance" and enable_ws == True -> LiveBinanceAdapter
    - mode == "live" -> CcxtExchangeAdapter (generic CCXT)
    """
```

工廠在引擎啟動時被呼叫一次。返回的適配器被注入引擎並在整個會話中使用。策略永遠不會呼叫此工廠 — 它們透過依賴注入 (Dependency Injection) 接收適配器。

## 方向命名慣例 (Side Naming Convention)

FluxTrade 對訂單/持倉方向使用雙重命名慣例：

| 上下文 | 做多 | 做空 |
|--------|------|------|
| 內部（Python 模型、Rust 引擎） | `LONG` | `SHORT` |
| 交易所邊界（CCXT API 呼叫） | `buy` | `sell` |

方向列舉：

- `OrderSide(str, Enum)`：`BUY = "buy"`、`SELL = "sell"`
- `PositionSide(str, Enum)`：`LONG = "LONG"`、`SHORT = "SHORT"`

轉換**僅**在適配器邊界發生：

- `SimulatedAdapter._side_to_rust()`：在呼叫 Rust 之前將 `buy` 轉換為 `LONG`、`sell` 轉換為 `SHORT`
- 條件單（`STOP_LOSS`、`TAKE_PROFIT`、`TRAILING_STOP`）在 Rust 邊界使用被保護的持倉方向：Python close-long 的 `sell` 會成為 Rust side `LONG`，close-short 的 `buy` 會成為 Rust side `SHORT`
- `CcxtExchangeAdapter`：ORM `Order` 的 `side` 已經是 `buy`/`sell`（符合 CCXT 預期），因此不需要轉換

!!! warning "策略中絕不轉換方向"
    策略發出的 `Signal` 物件使用 `SignalType.LONG`/`SignalType.SHORT`。執行管線和適配器負責所有方向轉換。如果策略直接引用 `buy` 或 `sell`，則違反了抽象邊界。

### 持倉表示

回測持倉使用明確方向加絕對數量：

| 持倉 | 表示方式 |
|------|----------|
| Long 0.2 | `side == LONG`, `quantity == Decimal("0.2")` |
| Short 0.2 | `side == SHORT`, `quantity == Decimal("0.2")` |

不要在回測 adapter 或 Python `Position` 模型中用負 quantity 推斷空單曝險。Signed exposure 由風控規則與 analytics 結合 `side` 和 `quantity` 推導。

Invariant test suite 會鎖住這個邊界：

- `tests/test_invariant_position_sign.py` 檢查 entry-side conversion、conditional close-side conversion，以及 long/short reduce/reverse 行為。
- `tests/test_invariant_position_consistency.py` 檢查 `BacktestAccountService` 和 `RiskManager` 是否從同一個 matcher-backed source of truth 讀取持倉。
- `tests/test_invariant_pnl_consistency.py` 檢查 matcher balance delta 是否等於重算 realized PnL 扣除 fees。

## 同一策略，兩種模式

以下範例展示策略如何在實盤和回測模式中完全一致地運行。唯一的差異是注入了哪個適配器：

```python
# --- Strategy code (unchanged between modes) ---
class GoldenCrossStrategy(BaseStrategy):
    def __init__(self):
        super().__init__(strategy_id="golden_cross", product_id="BINANCE:BTCUSDT-PERP")

    def on_candle(self, candle: Candlestick) -> Signal:
        self.update_indicators(candle)
        if self.fast_ma > self.slow_ma and not self.in_position:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=candle.product_id,
                timeframe=candle.timeframe,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                stop_loss=candle.close - Decimal("50"),
                take_profit=candle.close + Decimal("100"),
            )
        return Signal(
            strategy_id=self.strategy_id,
            product_id=candle.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )

# --- Backtest mode (most common) ---
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources import CsvDataSource

runner = BacktestRunner(
    start_time=1704067200000,   # 2024-01-01 UTC ms
    end_time=1706745600000,     # 2024-02-01 UTC ms
    product_id="BINANCE:BTC-PERP",
    timeframe="1h",
    initial_balance=100000.0,
    data_source=CsvDataSource("btc_1h.csv", product_id="BINANCE:BTC-PERP", timeframe="1h"),
    fee_config={"maker": 0.0002, "taker": 0.0004},
)
strategy = GoldenCrossStrategy("golden_cross_1", "BINANCE:BTC-PERP")
runner.add_strategy(strategy)
result = runner.run()  # Internally creates SimulatedAdapter -> Rust PyMatchingEngine

# --- Live mode (advanced) ---
# StrategyEngine requires db_session, clock, adapter, order_repository, etc.
# See the Live Trading Guide for full setup.
```

策略類別完全相同。它發出 `Signal` 物件；執行管線和適配器處理其餘一切。策略永遠不會直接呼叫交易所 API、永遠不會管理 SL/TP 生命週期，也永遠不會檢查自己是在實盤還是回測中運行。

## 設計規則

1. **策略中不得包含交易所邏輯**：SL/TP/Trailing 管理屬於搓合引擎（Rust）或適配器，絕不在 `on_candle()` 中
2. **實盤/回測一致性不可妥協**：任何新增至回測的功能必須產生與真實交易所執行完全一致的行為
3. **手續費必須反映**：`SimulatedAdapter`（透過 Rust）和 `CcxtExchangeAdapter`（透過交易所回應）都在成交結果中包含 maker/taker 手續費
4. **訊號是策略的唯一輸出**：策略發出包含進場、SL、TP 和 Trailing 參數的 `Signal` 物件。系統從那裡開始處理完整的訂單生命週期
5. **回測帳戶狀態由 matcher 支撐**：回測 balance 與 position 透過 `SimulatedAdapter` / `BacktestAccountService` 讀取，不維護另一份 Python position ledger
