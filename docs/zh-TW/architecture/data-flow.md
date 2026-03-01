# 資料流 (Data Flow)

本文件追蹤 FluxTrade 在每種運行模式下的完整資料流：實盤交易、回測和監控。

## 實盤交易路徑

```
Exchange WebSocket
       |
       v
connector/*.rs          Parse exchange-specific JSON into unified candle format
       |
       v
aggregator/mod.rs       Bucket 1m candles into 5m/15m/1h
       |
       v
publisher/mod.rs        Publish completed candles via bounded mpsc channel
       |
       v
Redis Stream            stream:market:{exchange}:{symbol}:{tf}
       |
       v
consumer.py             XREADGROUP with consumer group, parse candle from stream
       |
       v
engine.py               Dispatch candle to registered strategies (thread-safe copy)
       |
       v
strategy.on_candle()    Strategy evaluates indicators, may emit Signal
       |
       v
execution.py            Signal -> Order creation (with SL/TP/Trailing conditionals)
       |
       v
risk_manager.py         Pre-trade validation: balance, position limits, exposure
       |
       v
ccxt_adapter.py         Convert LONG/SHORT -> buy/sell, call exchange REST API
       |
       v
Exchange API            Order placed on exchange
```

### 逐步詳解

**1. Exchange WebSocket -> 連接器 (Connector)**

每個連接器（`binance.rs`、`bybit.rs`、`backpack.rs`）維護一個持久的 WebSocket 連線。傳入的交易/K 線訊息從交易所特定的 JSON 解析為統一的內部 K 線表示。連接器處理：

- 搭配指數退避 (Exponential Backoff) 的重連機制
- 透過分割 WebSocket 寫入端的 ping/pong 保活
- 交易資料去重（Binance 特定）

**2. 連接器 -> 聚合器 (Aggregator)**

聚合器接收 1 分鐘 K 線，並為每個已設定的高時間框架維護滾動 OHLCV 桶。當時間框架邊界被跨越時（例如第 5 分鐘完成一根 5m K 線），聚合器發出已完成的 K 線。所有算術使用 `Decimal` 以防止浮點漂移 (Floating-point Drift)。OHLC 不變式檢查（`low <= open,close <= high`）驗證每根發出的 K 線。

**3. 聚合器 -> 發布器 (Publisher) -> Redis Stream**

已完成的 K 線透過有界 `mpsc` 通道（容量：10,000 則訊息）發送至發布器。發布器將每根 K 線寫入 Redis Stream，鍵值編碼完整的路由上下文：

```
stream:market:binance:BTCUSDT:5m
stream:market:bybit:ETHUSDT:1h
```

此無鎖通道架構取代了早期的 `Arc<Mutex<RedisPublisher>>` 設計。

**4. Redis Stream -> 消費者 (Consumer)**

Python `consumer.py` 使用 `XREADGROUP` 作為消費者群組的一部分從 Redis Stream 消費。每個消費者實例追蹤自己的偏移量，實現：

- 斷線後恢復（無訊息遺失）
- 同一 Stream 上的多個獨立消費者
- 透過可設定逾時的阻塞讀取實現背壓 (Backpressure)

消費者將 Redis 雜湊解析為 `Candlestick` Pydantic 模型（所有欄位皆為 Decimal）。

**5. 消費者 -> 引擎 (Engine) -> 策略 (Strategy)**

`engine.py` 事件迴圈接收解析後的 K 線並分派給所有已註冊的策略。分派前：

- 建立策略列表的執行緒安全複本（防止迭代期間的變異）
- 時間框架安全防護過濾與策略宣告時間框架不匹配的 K 線（深度防禦 (Defense-in-Depth)；Stream 鍵已提供隔離）

**6. 策略 -> 執行 (Execution) -> 交易所**

當策略從 `on_candle()` 返回 `Signal` 時，執行管線：

1. 從訊號的進場參數建立主要 `Order`
2. 如果指定了 SL、TP 和 Trailing Stop，則建立條件單
3. 將每筆訂單通過 `risk_manager.py` 進行交易前驗證
4. 呼叫 `adapter.place_order()`，路由至 `CcxtExchangeAdapter`（或 `LiveBinanceAdapter`）
5. 適配器將 `LONG/SHORT` 轉換為 `buy/sell` 並呼叫交易所 API

執行延遲透過 `time.monotonic()` 測量，並記錄至 Prometheus 直方圖 (Histogram)。

## 回測路徑

```
IDataSource                 CSV, Database, Yahoo Finance, or Memory
       |
       v
backtest_runner.py          Iterate candles with circuit breaker
       |
       v
engine.py                  Same event loop as live mode
       |
       v
strategy.on_candle()       Same strategy code as live mode
       |
       v
execution.py               Same Signal -> Order pipeline
       |
       v
simulated.py               SimulatedAdapter (Python)
       |
       v
PyMatchingEngine           Rust matching engine via PyO3
  (matcher.rs)             Market/Limit/SL/TP/Trailing/OCO + fees
       |
       v
analytics.py               Sharpe, Sortino, Calmar, monthly returns
                           FIFO trade pairing, all Decimal
```

### 與實盤路徑的關鍵差異

| 面向 | 實盤 | 回測 |
|------|------|------|
| 資料來源 | Redis Stream（即時） | IDataSource（歷史） |
| 適配器 | CcxtExchangeAdapter | SimulatedAdapter |
| 搓合 | 交易所搓合引擎 | Rust PyMatchingEngine |
| 訂單路由 | 交易所 REST/WS API | 行程內 Rust 呼叫 |
| 延遲 | 網路限制 (50-500ms) | CPU 限制 (~11us/K 線) |
| 後續分析 | 即時儀表板 | analytics.py 報告 |

### IDataSource 實作

`IDataSource` 介面（`src/core/interfaces/data_source.py`）抽象化歷史資料檢索：

```python
class IDataSource(ABC):
    @abstractmethod
    def get_candles(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> Generator[Candlestick, None, None]: ...

    @abstractmethod
    def get_candles_df(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> pd.DataFrame: ...

    @abstractmethod
    def get_available_range(
        self, product_id: str, timeframe: str
    ) -> Optional[tuple[int, int]]: ...
```

注意：`get_candles()` 返回一個 **Generator**（非列表），按時間戳升序產出 `Candlestick` 物件。`start` 和 `end` 參數為毫秒時間戳（int），`get_available_range()` 返回 `Optional[tuple[int, int]]`（最小/最大毫秒時間戳）或 `None`（若無資料）。

可用實作：

| 實作 | 來源 | 使用場景 |
|------|------|----------|
| `DatabaseDataSource` | PostgreSQL | 使用儲存資料的正式回測 |
| `CsvDataSource` | CSV 檔案 | 本地開發、可重現測試 |
| `YahooFinanceDataSource` | Yahoo Finance API | 使用免費資料的快速原型設計 |
| `MemoryDataSource` | 記憶體列表 | 單元測試、合成資料 |

### BacktestRunner 執行迴圈

```python
# Simplified backtest loop (synchronous, not async)
stop_threshold = initial_balance * (1 - max_drawdown_limit)

for candle in data_source.get_candles(product_id, tf, start, end):
    # Update simulation clock
    self.clock.set_time(candle.timestamp / 1000)

    # Engine dispatches to strategies + adapter processes fills
    self.engine.on_market_data(candle)

    # Circuit breaker: halt if balance drops below threshold
    current_balance = mock_account.get_balance()
    if current_balance < stop_threshold:
        break
```

此迴圈是**同步的**（無 `await`）。它僅呼叫 `engine.on_market_data(candle)`，內部分派至策略和適配器。熔斷器 (Circuit Breaker) 將當前餘額與預先計算的 `stop_threshold`（初始餘額乘以一減最大回撤限制）進行比較 — 沒有 `.should_halt()` 方法。

`BacktestRunner` 將此迴圈封裝進度追蹤、效能測量和報告生成。迴圈完成後，`analytics.py` 計算交易層級和投資組合層級的指標。

### Rust 搓合引擎 (Matching Engine)

`SimulatedAdapter` 將所有搓合委託給 Rust 的 `PyMatchingEngine`。對於每根 K 線，引擎按優先順序處理未結訂單（Market > SL/TP/Trailing > Limit），套用手續費、管理持倉，並返回成交事件。

!!! note "效能"
    包含完整訂單搓合和手續費計算，約 89,000 根 K 線/秒。100K 根 K 線的回測大約在 1.12 秒內完成。

搓合邏輯、訂單類型和持倉管理的詳細說明，請參閱 [Rust 搓合引擎](rust-engine.md)。

## 監控路徑

```
engine.py                  Heartbeat loop (periodic)
       |
       v
Redis                      Publish heartbeat + status metrics
       |
       v
dashboard/data_provider.py Read heartbeat, positions, orders from Redis
       |
       v
app.py (Streamlit)         Render real-time dashboard
```

### 心跳資料 (Heartbeat Data)

引擎的 `_start_heartbeat()` 執行一個背景執行緒，每秒執行三項操作：

1. **Redis 鍵**：設定 `heartbeat:python`，TTL 為 3 秒（`self.redis_client.setex("heartbeat:python", 3, "1")`）
2. **Prometheus 量表 (Gauge)**：以當前帳戶餘額更新 `BALANCE_USDT`
3. **資料庫 last_heartbeat**：以當前時間戳更新所有活躍策略的 `StrategyState.last_heartbeat`

心跳**不會**發布策略數量、消費者延遲或訊號計數器。

### Prometheus 指標

六個指標在埠號 9090 上暴露供 Prometheus 抓取：

| 指標 | 類型 | 說明 |
|------|------|------|
| `SIGNALS_TOTAL` | Counter | 策略發出的訊號總數 |
| `ORDERS_TOTAL` | Counter | 下單總數（成功/失敗） |
| `EXECUTION_LATENCY` | Histogram | 從訊號到下單的時間 |
| `BALANCE_USDT` | Gauge | 當前帳戶餘額 |
| `CONSUMER_LAG_MS` | Gauge | 每個 Stream 的 Redis 消費者延遲 |
| `ACTIVE_STRATEGIES` | Gauge | 執行中的策略數量 |

## Redis Stream 鍵格式

所有跨服務通訊使用具有結構化鍵格式的 Redis Streams：

```
stream:market:{exchange}:{symbol}:{timeframe}
```

範例：

```
stream:market:binance:BTCUSDT:1m
stream:market:binance:BTCUSDT:5m
stream:market:binance:ETHUSDT:15m
stream:market:bybit:BTCUSDT:1h
```

### 時間框架通道隔離 (Timeframe Channel Isolation)

每個策略宣告其運行的時間框架。系統在兩個層級強制隔離：

**第一層 — Stream 訂閱（主要）**

消費者僅訂閱與策略宣告時間框架匹配的 Stream。設定為 `5m` K 線的策略訂閱 `stream:market:*:*:5m`，永遠不會看到 1m 或 1h 資料。

**第二層 — 引擎防護（深度防禦）**

即使 K 線以某種方式以不匹配的時間框架到達引擎，引擎在分派前會檢查 K 線的時間框架是否與策略的宣告一致。這是安全防護，而非主要過濾機制。

```
Strategy declares: timeframe = "5m"

Stream subscription:  stream:market:binance:BTCUSDT:5m  -- only 5m data
Engine guard:         candle.timeframe == "5m"?          -- defense-in-depth
Strategy receives:    only 5m candles, guaranteed
```

!!! tip "為何需要兩個層級？"
    Stream 層級的隔離很高效（無不必要的網路流量或解析）。引擎防護能防範設定錯誤或未來重構可能不小心混合時間框架的情況。深度防禦是 FluxTrade 的核心可靠性原則。

## 整條管線的資料完整性

管線的每個階段都對財務值強制使用 `Decimal` 算術：

| 階段 | 類型 | 邊界處理 |
|------|------|----------|
| Rust 連接器 | `rust_decimal::Decimal` | 從交易所 JSON 字串解析 |
| Rust 聚合器 | `rust_decimal::Decimal` | 原生 Decimal 算術 |
| Redis Stream | `String` | 序列化為字串，無精度損失 |
| Python 消費者 | `decimal.Decimal` | 從 Redis 字串值解析 |
| Python 模型 | `decimal.Decimal` | Pydantic 模型搭配 Decimal 欄位 |
| Rust 搓合引擎 | `rust_decimal::Decimal` | PyO3 邊界使用 String |
| Python 分析 | `decimal.Decimal` | 所有指標計算使用 Decimal |

`float` **絕不**用於管線任何階段的任何貨幣值。
