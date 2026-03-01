# 實盤交易 (Live Trading)

本指南涵蓋將 FluxTrade 策略部署到實盤交易所的流程。在回測中執行的相同策略程式碼可以在實盤模式中不做任何修改地執行——適配器模式 (Adapter Pattern) 確保你的 `on_candle()` 邏輯永遠不知道自己處於哪種模式。

---

## 前提條件

在上線之前，請確保你已具備：

1. **交易所 API 金鑰**，具有交易權限（從測試網/沙盒金鑰開始）
2. **基礎設施**：PostgreSQL 15、Redis，以及正在執行的 Rust 資料服務
3. **經過回測的策略**，具有可接受的績效指標
4. **`.env` 檔案**已填入所有必要變數（參見[配置](configuration.md)）

---

## 架構：實盤管線 (Live Pipeline)

在實盤交易中，資料流經完整的微服務管線：

```
Exchange WebSocket
    |
    v
[Rust Data Service]         -- Connects to exchange WS, aggregates raw trades into candles
    |
    v (Redis Streams)
    |                       -- stream:market:{exchange}:{symbol}:{timeframe}
    v
[DataConsumer]              -- XREADGROUP with consumer groups, conflation logic
    |
    v
[StrategyEngine]            -- Routes candles to registered strategies
    |
    v
[Strategy.on_candle()]      -- Your strategy code, returns a Signal
    |
    v
[ExecutionEngine]           -- Converts Signal to Order, applies risk checks
    |
    v
[IExchangeAdapter]          -- CcxtExchangeAdapter or LiveBinanceAdapter
    |
    v
Exchange API                -- REST (or WebSocket for Binance market orders)
```

### 關鍵元件

**Rust Data Service** (`rust-data-service/`)：連接到交易所 WebSocket 資料流（Binance、Bybit、Backpack），將原始成交資料聚合為多時間框架的 OHLCV K 線（1m、5m、15m、1h 等），並發布到 Redis Streams。Stream 鍵名包含時間框架：`stream:market:{exchange}:{symbol}:{tf}`。

**DataConsumer** (`src/core/consumer.py`)：一個 Python Redis Streams 消費者，透過 `XREADGROUP` 搭配消費者群組 (Consumer Groups) 讀取 K 線。它實作了：

- **重連機制**，指數退避（最多 10 次重試，最大退避 300 秒）
- **聚合 (Conflation)**：當消費者延遲超過 100ms，將一批訊息合成為單根 K 線以追趕進度，同時保持 OHLC 不變式
- **消費者群組**：多個 Python 實例可以共享工作負載

**StrategyEngine** (`src/core/engine.py`)：事件驅動核心，管理策略生命週期、訊號處理、風險檢查和審計軌跡。它提供：

- 透過檔案系統掃描的熱插拔策略發現
- 策略生命週期管理 (DISCOVERED, READY, ACTIVE, STOPPED, ERROR)
- 具備執行緒鎖的並行安全策略註冊
- Redis 心跳（1 秒間隔）用於健康監控
- 透過 Redis Pub/Sub 的指令監聽器用於遠端控制

---

## 設定實盤適配器配置

### 通用 CCXT 適配器（任何交易所）

```python
adapter_config = {
    "mode": "live",
    "exchange": "binance",     # any CCXT-supported exchange
    "api_key": "your_key",     # or set EXCHANGE_API_KEY env var
    "secret": "your_secret",   # or set EXCHANGE_SECRET env var
    "testnet": True,           # always start with testnet
}
```

`CcxtExchangeAdapter` 支援 CCXT 函式庫中的任何交易所。它：

- 預設啟用速率限制
- 為永續合約設定 `defaultType: "swap"`
- 當配置中未提供憑證時，回退至 `EXCHANGE_API_KEY` 和 `EXCHANGE_SECRET` 環境變數

### Binance WebSocket 快速通道

針對 Binance，你可以啟用 WebSocket 快速通道來執行市價單：

```python
adapter_config = {
    "mode": "live",
    "exchange": "binance",
    "testnet": True,
    "enable_ws": True,         # enables WS market order fast path
}
```

`LiveBinanceAdapter` 繼承 `CcxtExchangeAdapter`，嘗試透過 WebSocket 發送市價單以獲得更低延遲。若 WebSocket 初始化失敗或 WS 委託失敗，會透明地回退至 REST。

---

## 策略註冊與部署

### 方法 1：熱插拔策略（建議方式）

將策略檔案放置在 `strategies_hot/` 目錄中。引擎在啟動時及收到 `SCAN` 指令時會掃描此目錄：

```
python-strategy/strategies_hot/
    my_strategy.py
    another_strategy.py
```

每個檔案必須包含一個繼承 `BaseStrategy` 的類別。引擎會自動發現、實例化並管理其生命週期。

**策略生命週期狀態：**

| 狀態 | 意義 |
|------|------|
| `DISCOVERED` | 找到檔案，類別載入成功 |
| `READY` | 資料可用性檢查通過 |
| `WARNING` | 歷史資料不足（仍可手動啟動） |
| `ACTIVE` | 執行中並處理行情資料 |
| `STOPPED` | 手動停止 |
| `ERROR` | 載入失敗或執行時錯誤 |

**透過 Redis Pub/Sub 遠端控制：**

引擎在 `cmd:strategy:control` 頻道上監聽指令：

```python
import redis, json

r = redis.Redis()

# Scan for new strategy files
r.publish("cmd:strategy:control", json.dumps({"command": "SCAN"}))

# Test-run a strategy (check data availability)
r.publish("cmd:strategy:control", json.dumps({
    "command": "TEST_RUN",
    "params": {"id": "my_strategy", "days": 1},
}))

# Start a strategy
r.publish("cmd:strategy:control", json.dumps({
    "command": "START",
    "params": {"id": "my_strategy"},
}))

# Stop a strategy
r.publish("cmd:strategy:control", json.dumps({
    "command": "STOP",
    "params": {"id": "my_strategy"},
}))
```

### 方法 2：靜態註冊（傳統方式）

對於較簡單的設置，你可以透過 `add_strategy()` 直接註冊策略：

```python
from src.core.engine import StrategyEngine
from src.core.clock import Clock
from src.core.db import SessionLocal

db_session = SessionLocal()
clock = Clock()

engine = StrategyEngine(
    db_session,
    clock,
    adapter_config={
        "mode": "live",
        "exchange": "binance",
        "testnet": True,
    },
)

# Register strategy instances
from src.strategies.golden_cross import GoldenCrossStrategy

strategy = GoldenCrossStrategy(
    strategy_id="golden_cross_btc",
    product_id="BINANCE:BTCUSDT-PERP",
)
engine.add_strategy(strategy)

# Start engine (heartbeat, command listener, strategy scanner)
engine.startup()
```

### 連接消費者

引擎啟動後，透過 `DataConsumer` 將其連接到 Redis Stream：

```python
from src.core.consumer import DataConsumer

# Build stream keys from registered strategy requirements
channels = engine.build_stream_channels()
# e.g., ["stream:market:binance:btcusdt:1h"]

consumer = DataConsumer(
    channels=channels,
    on_message_callback=engine.on_market_data,
)

# This blocks and processes messages until stopped
consumer.start()
```

消費者會自動建立消費者群組、處理重連，並在延遲時套用聚合。

---

## 監控與健康檢查

### Redis 心跳

`StrategyEngine` 每秒向 Redis 發送一次心跳：

```
Key: heartbeat:python
Value: "1"
TTL: 3 seconds
```

若此鍵過期，Rust 資料服務的看門狗 (Watchdog) 可以觸發警報或重連。

### 系統狀態鎖

引擎在啟動時檢查 `system:state`。若設定為 `LOCKDOWN`，引擎會進入暫停迴圈直到狀態被清除：

```bash
# Emergency stop (via Redis CLI)
redis-cli SET system:state LOCKDOWN

# Resume operations
redis-cli DEL system:state
```

### Prometheus 指標

當 `METRICS_ENABLED=true` 時，Python 策略服務在配置的埠號上暴露指標（預設 9090）：

| 指標 | 型別 | 說明 |
|------|------|------|
| `fluxtrade_signals_total` | Counter | 已發出的訊號，按策略、類型、風險狀態標籤 |
| `fluxtrade_orders_total` | Counter | 已提交的委託單，按類型和狀態標籤 |
| `fluxtrade_execution_latency_seconds` | Histogram | 適配器 `place_order()` 延遲 |
| `fluxtrade_balance_usdt` | Gauge | 目前 USDT 帳戶餘額 |
| `fluxtrade_consumer_lag_ms` | Gauge | 每個 Stream 鍵的 Redis Stream 消費者延遲 |
| `fluxtrade_active_strategies` | Gauge | 目前活躍策略數量 |

### Grafana 儀表板

監控堆疊（Prometheus + Grafana）包含在 `docker-compose.prod.yml` 中：

- **Prometheus**：`http://localhost:9091` -- 從 Python 策略服務抓取指標
- **Grafana**：`http://localhost:3000` -- 餘額、訊號、延遲、消費者延遲的儀表板

---

## 安全注意事項

### 從測試網開始

開發時務必在適配器配置中設定 `testnet: True`。這會連接到使用模擬資金進行交易的交易所沙盒。只有在經過徹底的回測和測試網驗證後，才切換為 `testnet: False`。

### 風險管理 (Risk Management)

`RiskManager` 在執行前檢查每個訊號：

- **餘額檢查**：確保有足夠的資金下單
- **持倉限制**：強制執行最大持倉大小
- **曝險計算**：使用 `current_price`（K 線收盤價）進行精確的風險評估

未通過風險檢查的訊號會在審計軌跡中記錄為 `risk_status="REJECT"`，但不會被執行。

### 熔斷機制（僅限回測）

`max_drawdown_limit` 熔斷機制是 `BacktestRunner` 的功能，不適用於實盤交易。對於實盤風險管理，請依賴：

- 交易所層級的停損委託（在回測中由撮合引擎管理，在實盤中由交易所管理）
- `RiskManager` 訊號驗證
- `system:state LOCKDOWN` 緊急停止

### 訊號審計軌跡

引擎處理的每個訊號都會記錄在 `signal_audits` 資料庫表中，包含：

- 時間戳、策略 ID、交易對 ID、訊號類型
- 風險檢查結果 (PASS/REJECT) 及原因
- 關聯的委託單 ID（若已執行）
- 完整的 K 線和訊號元資料（JSON 格式）

### 優雅關機 (Graceful Shutdown)

呼叫 `engine.shutdown()` 來乾淨地停止引擎：

```python
engine.shutdown(timeout=30.0)
```

這會停止心跳和指令監聽器執行緒、排空執行緒池執行器，並關閉 Redis 連線。

---

## Docker 部署

對於正式環境部署，使用提供的 Docker Compose 配置：

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 2. Start all services
docker-compose -f docker-compose.prod.yml up -d

# 3. Verify health
docker-compose -f docker-compose.prod.yml ps

# 4. Check logs
docker-compose -f docker-compose.prod.yml logs -f python-strategy
docker-compose -f docker-compose.prod.yml logs -f rust-data
```

Python 策略服務將 `./python-strategy/strategies_hot` 掛載為 Volume，因此你可以在不重建容器的情況下新增或更新策略檔案。變更後使用 `SCAN` Redis 指令重新載入。

完整的 Docker 服務參考和資源限制請參見[配置](configuration.md)。

---

## 下一步

- [撰寫策略](writing-strategies.md) -- 使用 `BaseStrategy` 建立自訂策略
- [回測](backtesting.md) -- 上線前驗證策略
- [配置](configuration.md) -- 完整的環境和適配器配置參考
