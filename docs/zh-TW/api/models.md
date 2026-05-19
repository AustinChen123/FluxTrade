# 模型 (Models) API 參考

**模組：** `src.core.models`

所有金融數值使用 `decimal.Decimal` -- 貨幣計算禁止使用 `float`。所有模型繼承自 `BaseFluxModel`，該基底類別在轉換為 JSON 時提供自動的 `Decimal -> str` 序列化。

`product_id` 欄位在所有模型上均透過正規表達式 `^[A-Z0-9]+:[A-Z0-9_]+-PERP$` 進行驗證（例如 `BINANCE:BTCUSDT-PERP`）。

---

## 列舉 (Enums)

### `OrderSide(str, Enum)`

訂單方向。繼承自 `str` 以確保與字串比較的向後相容性。

| 成員 | 值 | 說明 |
| :--- | :--- | :--- |
| `BUY` | `"buy"` | 買入訂單（開多倉或平空倉） |
| `SELL` | `"sell"` | 賣出訂單（開空倉或平多倉） |

**靜態方法：**

| 方法 | 簽名 | 說明 |
| :--- | :--- | :--- |
| `from_position_side` | `(ps: PositionSide) -> OrderSide` | LONG -> BUY, SHORT -> SELL |
| `closing_side` | `(ps: PositionSide) -> OrderSide` | LONG -> SELL, SHORT -> BUY |

### `PositionSide(str, Enum)`

部位方向。繼承自 `str` 以確保向後相容性。

| 成員 | 值 | 說明 |
| :--- | :--- | :--- |
| `LONG` | `"LONG"` | 多頭部位 |
| `SHORT` | `"SHORT"` | 空頭部位 |

**靜態方法：**

| 方法 | 簽名 | 說明 |
| :--- | :--- | :--- |
| `from_order_side` | `(os: OrderSide) -> PositionSide` | BUY -> LONG, SELL -> SHORT |

### `SignalType(str, Enum)`

交易訊號的意圖。

| 成員 | 值 | 說明 |
| :--- | :--- | :--- |
| `LONG` | `"LONG"` | 開多倉 |
| `SHORT` | `"SHORT"` | 開空倉 |
| `EXIT_LONG` | `"EXIT_LONG"` | 平多倉 |
| `EXIT_SHORT` | `"EXIT_SHORT"` | 平空倉 |
| `NO_SIGNAL` | `"NO_SIGNAL"` | 無需動作 |

### `StrategyStatus(str, Enum)`

熱插拔策略的生命週期狀態。

| 成員 | 值 | 說明 |
| :--- | :--- | :--- |
| `DISCOVERED` | `"DISCOVERED"` | 偵測到策略檔案但尚未載入 |
| `READY` | `"READY"` | 策略已載入並驗證通過 |
| `WARNING` | `"WARNING"` | 策略執行中但有警告 |
| `ACTIVE` | `"ACTIVE"` | 策略正在積極處理 K 線 |
| `STOPPED` | `"STOPPED"` | 策略已明確停止 |
| `ERROR` | `"ERROR"` | 策略遇到致命錯誤 |

---

## 基底模型 (Base Model)

### `BaseFluxModel(BaseModel)`

所有 FluxTrade Pydantic 模型的基底模型，提供共用設定。

- `populate_by_name = True` -- 允許透過別名或欄位名稱填充。
- 透過 `serialize_decimal` 類別方法在 JSON 模式下自動執行 `Decimal -> str` 序列化。

---

## Pydantic 模型

### `Trade`

表示單筆成交執行。

| 欄位 | 型別 | 預設值 | 說明 |
| :--- | :--- | :--- | :--- |
| `id` | `str` | *必填* | 唯一成交標識符 |
| `product_id` | `str` | *必填* | 產品標識符（已驗證） |
| `price` | `Decimal` | *必填* | 成交價格 |
| `quantity` | `Decimal` | *必填* | 成交數量 |
| `side` | `OrderSide` | *必填* | `BUY` 或 `SELL` |
| `timestamp` | `int` | *必填* | Unix 時間戳，毫秒 |

### `Candlestick`

表示單根 OHLCV K 線。

| 欄位 | 型別 | 預設值 | 說明 |
| :--- | :--- | :--- | :--- |
| `product_id` | `str` | *必填* | 產品標識符（已驗證） |
| `timeframe` | `str` | *必填* | 例如 `1m`、`5m`、`1h` |
| `timestamp` | `int` | *必填* | Unix 時間戳，毫秒（開盤時間） |
| `open` | `Decimal` | *必填* | 開盤價 |
| `high` | `Decimal` | *必填* | 最高價 |
| `low` | `Decimal` | *必填* | 最低價 |
| `close` | `Decimal` | *必填* | 收盤價 |
| `volume` | `Decimal` | *必填* | 基礎資產的成交量 |

### `Signal`

策略決策邏輯的輸出。包含訂單建立所需的所有參數，包括風險管理（SL/TP/Trailing）。

| 欄位 | 型別 | 預設值 | 說明 |
| :--- | :--- | :--- | :--- |
| `strategy_id` | `str` | *必填* | 產生此訊號的策略 |
| `product_id` | `str` | *必填* | 目標產品（已驗證） |
| `timeframe` | `str` | *必填* | 觸發此訊號的 K 線時間週期 |
| `timestamp` | `int` | *必填* | 建立時間戳，毫秒 |
| `type` | `SignalType` | *必填* | 執行動作（LONG、SHORT、EXIT_LONG、EXIT_SHORT、NO_SIGNAL） |
| `value` | `Optional[Decimal]` | `None` | 用於記錄的指標值。當 `price` 未設定時，也作為限價單價格的備用值。 |
| `quantity` | `Optional[Decimal]` | `None` | 部位大小。若為 `None`，由執行/風控層決定。 |
| `price` | `Optional[Decimal]` | `None` | 明確的進場價格（限價單）。優先於 `value`。 |
| `stop_loss` | `Optional[Decimal]` | `None` | 停損價格水位 |
| `take_profit` | `Optional[Decimal]` | `None` | 止盈價格水位 |
| `trailing_distance` | `Optional[Decimal]` | `None` | 追蹤停損與價格的距離 |
| `metadata` | `Optional[dict]` | `None` | 用於除錯或記錄的鍵值對 |

### `Position`

表示某策略在某產品上的當前持倉狀態。

| 欄位 | 型別 | 預設值 | 說明 |
| :--- | :--- | :--- | :--- |
| `strategy_id` | `str` | *必填* | 擁有者策略 |
| `product_id` | `str` | *必填* | 產品標識符（已驗證） |
| `side` | `PositionSide` | *必填* | `LONG` 或 `SHORT`（列舉型別） |
| `quantity` | `Decimal` | *必填* | 絕對部位大小（正數） |
| `entry_price` | `Decimal` | *必填* | 平均進場價格 |
| `unrealized_pnl` | `Decimal` | *必填* | 預估未實現損益（快照） |

---

## 分析模型 (Analytics Models)

**模組：** `src.core.analytics`

### `ClosedTrade` (dataclass)

已完成的往返交易（Round-Trip Trade），包含進出場詳情，由原始成交記錄的 FIFO 淨額計算建構。使用 `@dataclass(slots=True)` 以提升記憶體效率。

| 欄位 | 型別 | 說明 |
| :--- | :--- | :--- |
| `entry_time` | `int` | 進場時間戳，毫秒 |
| `exit_time` | `int` | 出場時間戳，毫秒 |
| `entry_price` | `Decimal` | 平均進場價格 |
| `exit_price` | `Decimal` | 平均出場價格 |
| `side` | `PositionSide` | `LONG` 或 `SHORT` |
| `quantity` | `Decimal` | 交易數量 |
| `pnl` | `Decimal` | 已實現損益 |
