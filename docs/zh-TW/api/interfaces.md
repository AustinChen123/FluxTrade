# 介面 (Interfaces) API 參考

FluxTrade 定義了三個核心介面（抽象基底類別，Abstract Base Classes），將策略引擎與具體實作解耦。這使得相同的策略程式碼可以在實盤交易所、模擬回測或任何自訂後端上執行。

---

## IExchangeAdapter

**模組：** `src.core.interfaces.exchange`

交易所適配器介面（實盤與模擬）。將訂單執行與特定交易所實作解耦。

### 例外 (Exceptions)

| 例外 | 父類別 | 說明 |
| :--- | :--- | :--- |
| `ExchangeError` | `Exception` | 所有交易所相關錯誤的基礎例外 |
| `InsufficientFundsError` | `ExchangeError` | 當帳戶餘額不足以執行訂單時拋出 |
| `NetworkError` | `ExchangeError` | 當與交易所的網路連線出現問題時拋出 |

### 抽象方法 (Abstract Methods)

#### `place_order(order: Order) -> str`

在交易所下單。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `order` | `orm_models.Order` | 包含所有細節的內部 Order 物件（ORM 模型） |

**回傳值：** `str` -- 交易所的訂單 ID。

**拋出：** 訂單失敗時拋出 `ExchangeError`。

---

#### `cancel_order(order_id: str, product_id: str) -> bool`

取消現有訂單。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `order_id` | `str` | 交易所的訂單 ID（非內部資料庫 ID） |
| `product_id` | `str` | 產品/交易對標識符（例如 `BINANCE:BTCUSDT-PERP`） |

**回傳值：** `bool` -- 取消成功回傳 `True`，否則回傳 `False`。

---

#### `get_balance(asset: str) -> Decimal`

取得特定資產的可用餘額。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `asset` | `str` | 資產代號（例如 `USDT`、`BTC`） |

**回傳值：** `Decimal` -- 可用餘額。

---

#### `get_position(product_id: str) -> Optional[Position]`

取得某產品當前的未平倉部位。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `product_id` | `str` | 產品/交易對標識符 |

**回傳值：** `Optional[Position]` -- 部位詳情（Pydantic `models.Position`），若無部位則回傳 `None`。

### 具體方法 (Concrete Methods)

#### `on_market_data(candle: Candlestick) -> List[Dict]`

處理市場資料以檢查模擬訂單成交。在模擬適配器中覆寫此方法以實作搓合邏輯。實盤適配器回傳空列表（交易所原生管理 SL/TP）。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `candle` | `models.Candlestick` | 最新的 K 線資料 |

**回傳值：** `List[Dict]` -- 成交字典列表，包含鍵值：`order`、`price`、`quantity`、`fee`、`fill_type`。預設回傳 `[]`。

### 實作 (Implementations)

| 類別 | 模組 | 說明 |
| :--- | :--- | :--- |
| `SimulatedAdapter` | `src.core.adapters.simulated` | 委託 Rust `PyMatchingEngine` 進行回測搓合 |
| `CcxtExchangeAdapter` | `src.core.adapters.ccxt_adapter` | 基於 CCXT 的通用實盤交易所適配器 |
| `LiveBinanceAdapter` | `src.core.adapters.live_binance` | 擴展 `CcxtExchangeAdapter`，加入 WebSocket 快速通道 |

---

## IDataSource

**模組：** `src.core.interfaces.data_source`

K 線資料的抽象資料來源。各實作透過統一介面從不同後端提供 K 線資料。

### 抽象方法 (Abstract Methods)

#### `get_candles(product_id: str, timeframe: str, start: int, end: int) -> Generator[Candlestick, None, None]`

以時間戳升序產出 (yield) `Candlestick` 物件。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `product_id` | `str` | 產品標識符（例如 `BINANCE:BTCUSDT-PERP`） |
| `timeframe` | `str` | K 線時間週期（例如 `1m`、`5m`、`15m`） |
| `start` | `int` | 起始時間戳，毫秒（包含） |
| `end` | `int` | 結束時間戳，毫秒（包含） |

**回傳值：** `Generator[Candlestick, None, None]`

---

#### `get_candles_df(product_id: str, timeframe: str, start: int, end: int) -> pd.DataFrame`

回傳以時間戳為索引、包含 OHLCV 欄位的 DataFrame。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `product_id` | `str` | 產品標識符 |
| `timeframe` | `str` | K 線時間週期 |
| `start` | `int` | 起始時間戳，毫秒（包含） |
| `end` | `int` | 結束時間戳，毫秒（包含） |

**回傳值：** `pd.DataFrame` -- 欄位：`open`、`high`、`low`、`close`、`volume`（float）。索引：`timestamp`（int，毫秒）。

---

#### `get_available_range(product_id: str, timeframe: str) -> Optional[tuple[int, int]]`

回傳可用的資料範圍。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `product_id` | `str` | 產品標識符 |
| `timeframe` | `str` | K 線時間週期 |

**回傳值：** `Optional[tuple[int, int]]` -- `(min_timestamp, max_timestamp)`，若無資料則回傳 `None`。

### 具體方法 (Concrete Methods)

#### `validate() -> bool`

檢查資料來源是否可存取且包含有效資料。預設回傳 `True`。

### 實作 (Implementations)

| 類別 | 模組 | 說明 |
| :--- | :--- | :--- |
| `DatabaseDataSource` | `src.core.data_sources` | 透過 ORM 從 PostgreSQL 讀取 |
| `CsvDataSource` | `src.core.data_sources` | 從 CSV 檔案讀取 |
| `YahooFinanceDataSource` | `src.core.data_sources` | 從 Yahoo Finance API 擷取 |
| `MemoryDataSource` | `src.core.data_sources` | 記憶體內資料，用於測試 |

---

## IOrderRepository

**模組：** `src.core.interfaces.repository`

訂單、成交與部位的持久化介面。此處引用的所有型別為 `src.core.orm_models` 中的 ORM 模型，而非 `src.core.models` 中的 Pydantic 模型。

### 抽象方法 (Abstract Methods)

#### `add_order(order: Order) -> None`

持久化一筆新訂單。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `order` | `orm_models.Order` | 要持久化的訂單 |

---

#### `update_order(order: Order) -> None`

更新現有訂單的欄位（例如狀態、已成交數量）。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `order` | `orm_models.Order` | 包含更新欄位的訂單 |

---

#### `update_order_exchange_id(order: Order, exchange_order_id: str) -> None`

在下單後設定交易所分配的訂單 ID。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `order` | `orm_models.Order` | 要更新的訂單 |
| `exchange_order_id` | `str` | 交易所分配的訂單 ID |

---

#### `add_trade(trade: Trade) -> None`

持久化一筆成交記錄。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `trade` | `orm_models.Trade` | 要持久化的成交記錄 |

---

#### `update_position(strategy_id: str, product_id: str, side: str, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None`

在成交後更新或建立部位記錄。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `strategy_id` | `str` | 擁有此部位的策略 |
| `product_id` | `str` | 產品/交易對標識符 |
| `side` | `str` | 訂單方向（`"buy"` 或 `"sell"`） |
| `fill_quantity` | `Decimal` | 成交數量 |
| `fill_price` | `Decimal` | 成交價格 |
| `position_side` | `str` | 部位方向（`"LONG"` 或 `"SHORT"`） |

---

#### `get_position(strategy_id: str, product_id: str, side: str) -> Optional[Position]`

根據策略、產品和方向取得部位。

| 參數 | 型別 | 說明 |
| :--- | :--- | :--- |
| `strategy_id` | `str` | 策略標識符 |
| `product_id` | `str` | 產品/交易對標識符 |
| `side` | `str` | 部位方向（`"LONG"` 或 `"SHORT"`） |

**回傳值：** `Optional[orm_models.Position]` -- 部位資訊，若未找到則回傳 `None`。

### 實作 (Implementations)

| 類別 | 模組 | 說明 |
| :--- | :--- | :--- |
| `LiveOrderRepository` | `src.core.repositories` | 基於 PostgreSQL 的持久化，透過 SQLAlchemy |
| `BacktestOrderRepository` | `src.core.repositories` | 記憶體內儲存，用於回測 |
