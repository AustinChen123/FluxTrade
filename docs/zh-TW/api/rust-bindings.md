# Rust 綁定 (Rust Bindings) API 參考

**模組：** `fluxtrade_core`（從 `rust-data-service/src/binding/` 編譯）

Rust 搓合引擎透過 PyO3 編譯為 Python 擴展模組。它在回測中以逐根 K 線回放的方式處理所有訂單搓合。Python 的 `SimulatedAdapter` 完全委託此引擎執行。

**核心設計原則：** 所有金融數值在 PyO3 邊界使用 `String`，內部使用 `rust_decimal::Decimal`。Python 策略傳入字串表示（例如 `"50000.00"`），在 Rust 中被解析為無損的 `Decimal` 值。

---

## PyMatchingEngine

核心搓合引擎。根據待處理訂單處理 K 線資料，並管理部位，完整支援 Market、Limit、Stop-Loss、Take-Profit、Trailing Stop 和 OCO 訂單類型。

### 建構子 (Constructor)

```python
from fluxtrade_core import PyMatchingEngine

engine = PyMatchingEngine(
    initial_balance="10000",    # 必填：起始帳戶餘額
    maker_fee="0.001",          # 選填：Maker 手續費率（預設："0"）
    taker_fee="0.001",          # 選填：Taker 手續費率（預設："0"）
)
```

| 參數 | 型別 | 預設值 | 說明 |
| :--- | :--- | :--- | :--- |
| `initial_balance` | `str` | *必填* | 起始帳戶餘額（解析為 Decimal） |
| `maker_fee` | `str` | `"0"` | Maker 手續費率，套用於 Limit 訂單成交 |
| `taker_fee` | `str` | `"0"` | Taker 手續費率，套用於 Market/SL/TP/Trailing 成交 |

### 屬性 (Properties)

| 屬性 | 型別 | 說明 |
| :--- | :--- | :--- |
| `balance` | `str` | 當前帳戶餘額（Decimal 以字串表示） |
| `positions` | `Dict[str, Position]` | 未平倉部位，以 `"{strategy_id}:{product_id}"` 為鍵 |
| `open_orders` | `List[Order]` | 等待搓合的待處理訂單 |

### 方法 (Methods)

#### `submit_order(order: Order) -> str`

將訂單加入待處理訂單簿。該訂單將在下次呼叫 `on_candle()` 時進行評估。

**參數：**

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `order` | `Order` | 要提交的訂單 |

**回傳值：** `str` -- 訂單 ID。

**範例：**

```python
from fluxtrade_core import Order

order = Order(
    id="entry-001",
    product_id="BINANCE:BTCUSDT-PERP",
    side="LONG",
    order_type="MARKET",
    price="0",
    quantity="0.1",
    timestamp=1700000000000,
    strategy_id="my_strategy",
)
engine.submit_order(order)
```

---

#### `on_candle(candle: Candlestick) -> List[FillEvent]`

根據所有待處理訂單處理一根 K 線。這是回測迴圈中呼叫的主要方法。

**處理順序：**

1. 根據 K 線最高/最低價更新追蹤停損觸發價格
2. 以 K 線開盤價搓合 **Market** 訂單（Taker 手續費）
3. 若觸發條件成立，以觸發價格搓合**條件訂單**（SL/TP/Trailing）（Taker 手續費）
4. 若價格觸及，以限價搓合 **Limit** 訂單（Maker 手續費）

**每筆成交的副作用：**

- 更新部位（開倉/加倉/減倉/翻轉/平倉）
- 平倉時將已實現損益加入餘額
- 從餘額中扣除手續費（上限為可用餘額）
- 取消 OCO 關聯訂單

**參數：**

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `candle` | `Candlestick` | 要處理的 K 線 |

**回傳值：** `List[FillEvent]` -- 此根 K 線期間發生的所有成交。

**範例：**

```python
from fluxtrade_core import Candlestick

candle = Candlestick(
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1m",
    timestamp=1700000060000,
    open="50000", high="50500", low="49800", close="50200",
    volume="100",
)

fills = engine.on_candle(candle)
for fill in fills:
    print(f"Order {fill.order_id}: {fill.fill_type} at {fill.price}, fee={fill.fee}")
```

---

#### `on_matching_tick(candle: Candlestick) -> List[FillEvent]`

`on_candle()` 的別名。行為與簽名完全相同。

---

#### `cancel_order(order_id: str) -> bool`

根據 ID 移除待處理訂單。

**回傳值：** 若訂單被找到並移除回傳 `True`，若未找到回傳 `False`。

---

#### `get_positions() -> Dict[str, Position]`

回傳所有未平倉部位。鍵為 `"{strategy_id}:{product_id}"`。

---

#### `get_position(strategy_id: str, product_id: str) -> Optional[Position]`

取得特定部位。若給定策略和產品無部位存在，回傳 `None`。

---

## 邊界型別 (Boundary Types)

### Candlestick

```python
from fluxtrade_core import Candlestick

candle = Candlestick(
    product_id: str,     # "EXCHANGE:SYMBOL-PERP"
    timeframe: str,      # "1m", "5m", etc.
    timestamp: int,      # Unix ms (i64)
    open: str,           # Decimal 以字串表示
    high: str,           # Decimal 以字串表示
    low: str,            # Decimal 以字串表示
    close: str,          # Decimal 以字串表示
    volume: str,         # Decimal 以字串表示
)
```

所有 OHLCV 欄位在建構時接受 `str`，存取時回傳 `str`。內部以 `rust_decimal::Decimal` 儲存。

### Order

```python
from fluxtrade_core import Order

order = Order(
    id: str,                          # 唯一訂單 ID
    product_id: str,                  # "EXCHANGE:SYMBOL-PERP"
    side: str,                        # "LONG" 或 "SHORT"
    order_type: str,                  # "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
    price: str,                       # 限價/參考價格（Decimal 以字串表示）
    quantity: str,                    # 訂單數量（Decimal 以字串表示）
    timestamp: int,                   # Unix ms (i64)
    trigger_price: Optional[str],     # SL/TP 觸發價格（預設：None）
    trailing_distance: Optional[str], # 追蹤停損距離（預設：None）
    linked_order_id: Optional[str],   # OCO 關聯訂單 ID（預設：None）
    strategy_id: str,                 # 策略擁有者（預設：""）
)
```

### FillEvent

當訂單被搓合時由 `on_candle()` 回傳。所有欄位均可讀寫。

```python
fill.order_id: str          # 成交訂單的 ID
fill.product_id: str        # 產品標識符
fill.strategy_id: str       # 擁有該訂單的策略
fill.price: str             # 成交價格（Decimal 以字串表示）
fill.quantity: str           # 成交數量（Decimal 以字串表示）
fill.fee: str               # 收取的手續費（Decimal 以字串表示）
fill.timestamp: int         # K 線時間戳 (i64)
fill.fill_type: str         # "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
```

### Position

由 `PyMatchingEngine` 內部追蹤。以 `"{strategy_id}:{product_id}"` 為鍵。

```python
pos.product_id: str         # 產品標識符
pos.strategy_id: str        # 策略擁有者
pos.side: str               # "LONG"、"SHORT" 或 "FLAT"
pos.quantity: str            # 部位大小（Decimal 以字串表示）
pos.entry_price: str        # 平均進場價格（Decimal 以字串表示）
pos.unrealized_pnl: str     # 未實現損益（Decimal 以字串表示）
```

---

## 搓合邏輯參考 (Matching Logic Reference)

### 各訂單類型的成交價格

| 訂單類型 | 成交價格 | 手續費類型 | 觸發條件（LONG） | 觸發條件（SHORT） |
| :--- | :--- | :--- | :--- | :--- |
| `MARKET` | candle.open | taker | 始終觸發（下一根 K 線） | 始終觸發（下一根 K 線） |
| `LIMIT` | order.price | maker | candle.low <= order.price | candle.high >= order.price |
| `STOP_LOSS` | trigger_price | taker | candle.low <= trigger_price | candle.high >= trigger_price |
| `TAKE_PROFIT` | trigger_price | taker | candle.high >= trigger_price | candle.low <= trigger_price |
| `TRAILING_STOP` | trigger_price | taker | candle.low <= trigger_price | candle.high >= trigger_price |

### 追蹤停損棘輪機制 (Trailing Stop Ratchet)

在搓合之前，追蹤停損會先更新：

- **LONG：** `new_trigger = candle.high - trailing_distance`。僅當 `new_trigger > current_trigger` 時套用（向上棘輪）。
- **SHORT：** `new_trigger = candle.low + trailing_distance`。僅當 `new_trigger < current_trigger` 時套用（向下棘輪）。

### 部位更新

| 情境 | 行為 |
| :--- | :--- |
| 無現有部位 | 以成交價格開立新部位 |
| 同方向 | 以加權平均進場價格增加部位 |
| 反方向，部分 | 減少部位，對已平倉部分實現損益 |
| 反方向，全部 | 平倉，實現損益 |
| 反方向，超額 | 翻轉部位：平倉舊部位，以超額數量開立新部位 |
| SL/TP/Trailing 成交 | 平倉（部分或全部），實現損益 |

### 手續費公式

```
fee = price * quantity * rate
```

其中 `rate` 為 Market/SL/TP/Trailing 的 `taker_fee`，或 Limit 的 `maker_fee`。手續費上限為可用餘額（`min(fee, balance)`）。
