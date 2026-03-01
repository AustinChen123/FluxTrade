# Rust 搓合引擎 (Matching Engine)

Rust 搓合引擎（`PyMatchingEngine`）是 FluxTrade 回測系統的核心。它使用 `rust_decimal::Decimal` 算術處理所有訂單搓合、持倉管理、手續費計算和餘額追蹤。透過 PyO3 以 `fluxtrade_core` 模組暴露給 Python。

## PyMatchingEngine

定義於 `src/binding/matcher.rs`：

```rust
#[pyclass]
pub struct PyMatchingEngine {
    pub balance: Decimal,
    pub positions: HashMap<String, Position>,
    pub open_orders: Vec<Order>,
    maker_fee: Decimal,
    taker_fee: Decimal,
}
```

### 建構函式

```python
PyMatchingEngine(
    initial_balance: str,          # e.g., "100000"
    maker_fee: str = "0",          # e.g., "0.0002"
    taker_fee: str = "0",          # e.g., "0.0004"
)
```

所有參數皆為字串，在內部解析為 `rust_decimal::Decimal`。無效的 Decimal 字串會觸發 `PyValueError`。

### 方法

| 方法 | 簽名 | 說明 |
|------|------|------|
| `submit_order` | `(order: Order) -> str` | 將訂單加入未結訂單列表。返回訂單 ID。 |
| `on_candle` | `(candle: Candlestick) -> list[FillEvent]` | 以一根 K 線處理所有未結訂單。返回成交事件。 |
| `cancel_order` | `(order_id: str) -> bool` | 按 ID 移除訂單。找到並移除則返回 True。 |
| `get_positions` | `() -> dict[str, Position]` | 以字典形式返回所有持倉。 |
| `get_position` | `(strategy_id: str, product_id: str) -> Optional[Position]` | 返回特定策略/交易對的持倉。 |

### 屬性

| 屬性 | 類型（Python） | 說明 |
|------|----------------|------|
| `balance` | `str` | 當前帳戶餘額，以 Decimal 字串表示。 |
| `positions` | `dict[str, Position]` | 所有持倉，以 `strategy_id:product_id` 為鍵。 |
| `open_orders` | `list[Order]` | 所有待處理訂單。 |

## 訂單類型

引擎支援五種訂單類型，在每根 K 線中按嚴格的優先順序處理：

### 1. 市價單 (Market Order)（最高優先順序）

- 以 K 線的**開盤價**成交
- 收取 **taker 手續費**
- 最先處理以模擬即時執行

### 2. 條件單 (Conditional Order)（停損、停利、追蹤停損）

- 在市價單之後、限價單之前處理
- 收取 **taker 手續費**（觸發時類似市價執行）

**停損 (Stop-Loss)**：

- 做多持倉：當 `candle.low <= trigger_price` 時觸發
- 做空持倉：當 `candle.high >= trigger_price` 時觸發
- 以 `trigger_price` 成交

**停利 (Take-Profit)**：

- 做多持倉：當 `candle.high >= trigger_price` 時觸發
- 做空持倉：當 `candle.low <= trigger_price` 時觸發
- 以 `trigger_price` 成交

**追蹤停損 (Trailing Stop)**：

- 在搓合之前，`update_trailing_stops()` 調整觸發價格：
    - 做多：`new_trigger = candle.high - trailing_distance`（僅向上移動）
    - 做空：`new_trigger = candle.low + trailing_distance`（僅向下移動）
- 調整後的觸發邏輯與停損相同

### 3. 限價單 (Limit Order)（最低優先順序）

- 做多：當 `candle.low <= order.price` 時成交
- 做空：當 `candle.high >= order.price` 時成交
- 以**訂單價格**成交（非 K 線價格）
- 收取 **maker 手續費**

### 4. OCO（二擇一委託）

OCO 不是獨立的訂單類型，而是一種連結機制。訂單有一個可選的 `linked_order_id` 欄位。當一個訂單成交時，其連結的對應訂單透過 `HashSet<String>` 被標記為取消。所有搓合完成後，已取消的 ID 從剩餘訂單中過濾掉。

典型用法：SL 和 TP 訂單作為 OCO 對連結。當 SL 成交時，TP 被取消（反之亦然）。

## 持倉管理

持倉存儲在 `HashMap<String, Position>` 中，以複合鍵為索引：

```
{strategy_id}:{product_id}
```

這實現了**多策略隔離 (Multi-Strategy Isolation)**：兩個交易同一產品的策略維護各自獨立的持倉。

### 持倉狀態機 (Position State Machine)

```
FLAT -> LONG  (Market/Limit buy)
FLAT -> SHORT (Market/Limit sell)
LONG -> FLAT  (SL/TP/Trailing close, or opposing Market/Limit that fully closes)
SHORT -> FLAT (SL/TP/Trailing close, or opposing Market/Limit that fully closes)
LONG -> SHORT (opposing order exceeds position size -- flip)
SHORT -> LONG (opposing order exceeds position size -- flip)
```

### 持倉更新邏輯

**開倉 / 加倉**（與現有持倉同方向）：

- 加權平均入場價：`(old_qty * old_entry + new_qty * fill_price) / total_qty`

**平倉**（條件單：SL/TP/Trailing）：

- 實現損益 (Realized PnL)：做多為 `(fill_price - entry_price) * close_qty`，做空則反轉
- 實現損益加入餘額
- 持倉數量減少；完全平倉時設為 FLAT

**減倉 / 反轉**（反向 Market/Limit 單）：

- 部分平倉：在已平倉部分實現損益
- 若訂單數量超過持倉：平掉現有持倉，以超出部分在反向開立新持倉

### Position 結構

```rust
#[pyclass]
pub struct Position {
    pub product_id: String,
    pub strategy_id: String,
    pub side: String,             // "LONG", "SHORT", or "FLAT"
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub unrealized_pnl: Decimal,
}
```

## FillEvent 結構

每次成交產生一個 `FillEvent`：

```rust
#[pyclass]
pub struct FillEvent {
    pub order_id: String,
    pub product_id: String,
    pub strategy_id: String,
    pub price: Decimal,           // Fill price
    pub quantity: Decimal,        // Filled quantity
    pub fee: Decimal,             // Calculated fee
    pub timestamp: i64,           // Candle timestamp
    pub fill_type: String,        // "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
}
```

所有 `Decimal` 欄位透過 PyO3 getter 以 `String` 形式暴露給 Python。

## 手續費計算

手續費計算公式：

```
fee = price * quantity * rate
```

其中 `rate` 為：

- Market、Stop-Loss、Take-Profit 和 Trailing Stop 訂單使用 `taker_fee`
- Limit 訂單使用 `maker_fee`

手續費上限為當前餘額（`min(fee, balance)`）以防止負餘額，每次成交後從引擎餘額中扣除。

**範例**：以 $50,000 市價買入 0.5 BTC，taker 手續費 0.04%：

```
fee = 50000 * 0.5 * 0.0004 = 10.0 USDT
```

成交事件報告 `fee = "10.0"`，引擎從餘額中扣除 10 USDT。

## Order 結構

```rust
#[pyclass]
pub struct Order {
    pub id: String,
    pub product_id: String,
    pub strategy_id: String,       // Used for per-strategy position tracking
    pub side: String,              // "LONG" or "SHORT"
    pub order_type: String,        // "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
    pub price: Decimal,            // Limit price (for LIMIT orders)
    pub quantity: Decimal,
    pub timestamp: i64,
    pub trigger_price: Option<Decimal>,      // For SL/TP conditional orders
    pub trailing_distance: Option<Decimal>,  // For trailing stop
    pub linked_order_id: Option<String>,     // For OCO pairing
}
```

### PyO3 建構函式簽名

```python
Order(
    id: str,
    product_id: str,
    side: str,
    order_type: str,
    price: str,
    quantity: str,
    timestamp: int,
    trigger_price: Optional[str] = None,
    trailing_distance: Optional[str] = None,
    linked_order_id: Optional[str] = None,
    strategy_id: str = "",
)
```

## Candlestick 結構

```rust
#[pyclass]
pub struct Candlestick {
    pub product_id: String,
    pub timeframe: String,
    pub timestamp: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}
```

所有 OHLCV 欄位在 PyO3 邊界為 `String`，內部為 `Decimal`。

## PyO3 邊界契約 (Boundary Contract)

Rust 引擎採用**邊界為 String、內部為 Decimal** 的設計：

| 方向 | 轉換 |
|------|------|
| Python -> Rust | Python 傳遞 `str` 值；Rust 透過 `Decimal::from_str()` 解析為 `rust_decimal::Decimal` |
| Rust -> Python | Rust 透過呼叫 `.to_string()` 的 `#[getter]` 方法將 `Decimal` 欄位以 `String` 暴露 |
| 錯誤處理 | 無效的 Decimal 字串觸發帶有描述訊息的 `PyValueError` |

此設計在語言邊界間保持完整的 Decimal 精度。永遠不使用 Float。

## 多策略隔離 (Multi-Strategy Isolation)

多個策略可同時交易同一產品而互不干擾：

- 每筆訂單攜帶 `strategy_id` 欄位
- 持倉在 positions HashMap 中以 `{strategy_id}:{product_id}` 為鍵
- 策略 A 的 Market LONG 訂單不會影響策略 B 在同一產品上的 SHORT 持倉
- `get_position(strategy_id, product_id)` 方法檢索特定策略的持倉

## 處理管線（每根 K 線）

1. **更新追蹤停損**：根據新 K 線的最高/最低價調整所有 TRAILING_STOP 訂單的觸發價格
2. **訂單分區**：分為 Market、Conditional（SL/TP/Trailing）和 Limit 三個桶
3. **處理 Market 訂單**：以開盤價成交、更新持倉、扣除手續費、取消關聯的 OCO 訂單
4. **處理 Conditional 訂單**：檢查觸發條件、以觸發價成交、更新持倉、扣除手續費、取消關聯訂單
5. **處理 Limit 訂單**：檢查價格匹配、以限價成交、更新持倉、扣除手續費、取消關聯訂單
6. **過濾已取消訂單**：從剩餘未結訂單中移除 OCO 已取消的訂單
7. **返回成交**：將 `Vec<FillEvent>` 返回給 Python

與 K 線不同 `product_id` 的訂單會被跳過（保留在未結訂單列表中）。

## 效能

Rust 搓合引擎在包含完整訂單搓合和手續費計算的情況下，處理速度約為 **89,000 根 K 線/秒**。100K 根 K 線的回測大約在 1.12 秒內完成。
