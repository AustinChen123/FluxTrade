# FluxTrade 架構概覽

## 系統架構圖

```
                         +---------------------+
                         |   Exchange APIs      |
                         | Binance/Bybit/Backpack|
                         +----------+----------+
                                    |
                          WebSocket | REST (backfill)
                                    |
                         +----------v----------+
                         |  Rust Data Service   |
                         |                      |
                         |  connector/*.rs      |  WebSocket handlers
                         |  aggregator/mod.rs   |  1m -> 5m/15m/1h bucketing
                         |  publisher/mod.rs    |  Redis Stream publish
                         |  historical/mod.rs   |  REST candle backfill
                         |  watchdog.rs         |  Heartbeat monitoring
                         +----------+-----------+
                                    |
                           Redis Streams (ordered, persistent)
                           stream:market:{exchange}:{symbol}:{tf}
                                    |
                         +----------v-----------+
                         | Python Strategy Svc   |
                         |                       |
                         |  consumer.py          |  XREADGROUP consumer
                         |  engine.py            |  Event-driven coordinator
                         |  strategy_state_*     |  Strategy lifecycle guard
                         |  execution.py         |  Signal -> Order -> Fill
                         |  risk_manager.py      |  Rule-based risk checks
                         |  backtest_runner.py   |  Backtesting framework
                         |  analytics.py         |  Sharpe/Sortino/Calmar
                         +---+-------------+-----+
                             |             |
                    +--------v---+   +-----v--------+
                    | PostgreSQL |   | Exchange API  |
                    | (persist)  |   | (live orders) |
                    +--------+---+   +--------------+
                             |
                    +--------v-----------+
                    | Streamlit Dashboard |
                    | (monitoring & viz)  |
                    +--------------------+
```

## Rust 資料服務 (`rust-data-service/`)

Rust 服務負責所有即時市場資料的接收，並透過 PyO3 作為回測的高效能搓合引擎 (Matching Engine)。

### 模組分解

| 模組 | 檔案 | 職責 |
|------|------|------|
| **進入點** | `src/main.rs` | Tokio runtime、訊號處理器、優雅關閉 (Graceful Shutdown) |
| **PyO3 橋接** | `src/lib.rs` | 匯出 `fluxtrade_core` Python 模組 |
| **搓合引擎** | `src/binding/matcher.rs` | Market/Limit/SL/TP/Trailing/OCO 搓合，全 Decimal 算術 |
| **資料模型** | `src/binding/models.rs` | PyO3 模型，String 邊界、Decimal 內部 |
| **連接器** | `src/connector/*.rs` | 交易所 WebSocket 處理器 (Binance, Bybit, Backpack) |
| **聚合器** | `src/aggregator/mod.rs` | K 線桶式聚合 (Bucketing)，從 1 分鐘 K 線聚合為 5m/15m/1h |
| **發布器** | `src/publisher/mod.rs` | 透過有界 mpsc 通道發布至 Redis Stream |
| **歷史資料** | `src/historical/mod.rs` | REST 歷史 K 線回補，可設定並行數 |
| **看門狗** | `src/watchdog.rs` | Python 心跳監控、交易所重連觸發 |

### 連接器 (Connector) 架構

每個交易所連接器都實作共同模式：

1. 建立與交易所的 WebSocket 連線
2. 訂閱已設定交易對的交易/K 線資料流
3. 將交易所特定的 JSON 解析為統一的內部 K 線格式
4. 將解析後的資料轉發至聚合器

連接器負責處理重連、ping/pong 保活機制（透過分割 WebSocket 的寫入端），以及交易資料去重。

### 聚合器 (Aggregator)（K 線桶式聚合）

聚合器從連接器接收 1 分鐘 K 線，並維護各高時間框架的滾動桶：

```
1m candle in -> update 5m bucket
                update 15m bucket
                update 1h bucket

when bucket boundary crossed -> emit completed higher-TF candle
```

每個桶使用 Decimal 算術追蹤 OHLCV 資料。OHLC 不變式檢查 (Invariant Check) 確保在發出任何 K 線之前 `low <= open,close <= high`。

### 發布器 (Publisher)（Redis Stream）

發布器透過有界 `mpsc` 通道（`PublishMessage` 列舉，容量 10K）接收已完成的 K 線，並寫入 Redis Stream。Stream 鍵 (Key) 編碼完整上下文：

```
stream:market:{exchange}:{symbol}:{timeframe}
```

此設計以無鎖通道架構取代了早期的 `Arc<Mutex<RedisPublisher>>` 設計。

### 任務監督 (Task Supervision)

所有非同步任務（連接器、聚合器、發布器、看門狗）都在 `JoinSet` 監督器下運行：

- `TaskId` 列舉用於識別
- `TaskFailureTracker` 搭配指數退避 (Exponential Backoff)
- 連續 3 次失敗觸發優雅關閉
- 任何任務 panic 則立即關閉

## Python 策略服務 (`python-strategy/`)

Python 服務包含交易邏輯、執行管線、風險管理和回測基礎設施。

### 核心引擎 (`src/core/`)

| 模組 | 職責 |
|------|------|
| `engine.py` | 事件驅動協調器：接收市場資料、分派至策略、負責生命週期 wiring |
| `strategy_registry.py` | 探索、載入並保存執行期策略實例 |
| `command_router.py` | 處理 start、stop、resume、force recover 等操作命令 |
| `signal_processor.py` | 在執行訊號前套用策略狀態防護 |
| `strategy_state_manager.py` | 管理生命週期轉換、樂觀版本控制、Redis 快取更新與轉換歷史 |
| `execution.py` | 訊號到訂單到成交管線 (Signal-to-Order-to-Fill Pipeline)：從訊號建立 SL/TP/Trailing 條件單 |
| `risk_manager.py` | 交易前驗證協調：餘額、名目金額限制、價格合理性、速率限制、每日虧損熔斷 |
| `risk_rules/` | `RiskManager` 使用的可測試規則模組 |
| `daily_nav_snapshot.py` | 讀取或初始化每日虧損檢查使用的日初 NAV |
| `audit_service.py` | Signal、系統事件、意圖、結果稽核 helper，payload 保持 JSONB 安全 |
| `client_order_id.py` | 產生決定性的 client order ID，用於冪等下單與重啟 reconcile |
| `order_manager.py` | 訂單生命週期管理，搭配 Redis Lua 原子操作 |
| `consumer.py` | Redis Stream XREADGROUP 消費者、K 線解析與時間框架合成 |
| `backtest_runner.py` | 回測框架：IDataSource -> K 線迴圈 -> 熔斷器 (Circuit Breaker) -> 報告 |
| `analytics.py` | 交易後分析：Sharpe、Sortino、Calmar、月報酬、平均成本 closed-trade 配對 |
| `models.py` | Pydantic 資料模型（Candlestick、Order、Trade、Signal、Position），全 Decimal |
| `journal.py` | 結構化交易事件記錄至 JSONL |

### 介面 (`src/core/interfaces/`)

三個核心抽象層 (Abstraction) 解耦系統：

**IExchangeAdapter** (`interfaces/exchange.py`)：

```python
class IExchangeAdapter(ABC):
    @abstractmethod
    def place_order(self, order: Order) -> str: ...
    @abstractmethod
    def cancel_order(self, order_id: str, product_id: str) -> bool: ...
    @abstractmethod
    def get_balance(self, asset: str) -> Decimal: ...
    @abstractmethod
    def get_position(self, product_id: str) -> Optional[Position]: ...
    def on_market_data(self, candle: Candlestick) -> List[Dict]: ...
```

**IDataSource** (`interfaces/data_source.py`)：

```python
class IDataSource(ABC):
    @abstractmethod
    def get_candles(self, product_id: str, timeframe: str, start: int, end: int) -> Generator[Candlestick, None, None]: ...
    @abstractmethod
    def get_candles_df(self, product_id: str, timeframe: str, start: int, end: int) -> pd.DataFrame: ...
    @abstractmethod
    def get_available_range(self, product_id: str, timeframe: str) -> Optional[tuple[int, int]]: ...
```

**IOrderRepository** (`interfaces/repository.py`)：

```python
class IOrderRepository(ABC):
    @abstractmethod
    def add_order(self, order: Order) -> None: ...
    @abstractmethod
    def update_order(self, order: Order) -> None: ...
    @abstractmethod
    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None: ...
    @abstractmethod
    def add_trade(self, trade: Trade) -> None: ...
    @abstractmethod
    def update_position(self, strategy_id: str, product_id: str, side: str, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None: ...
    @abstractmethod
    def get_position(self, strategy_id: str, product_id: str, side: str) -> Optional[Position]: ...
```

### 策略 (`src/strategies/`)

所有策略都繼承 `BaseStrategy` 並實作 `on_candle()`。它們發出包含進場參數、停損 (Stop-Loss)、停利 (Take-Profit) 和追蹤停損 (Trailing Stop) 設定的 `Signal` 物件。策略永遠不會直接管理訂單生命週期。

可用策略：`golden_cross`、`rsi_scalper`、`bb_reversion`、`macd_momentum`、`market_structure_strategy`、`smc_strategy`、`callable_strategy`、`csv_signal_strategy`。

熱插拔策略 (Hot-Pluggable Strategy) 可在執行期從 `strategies_hot/` 目錄載入，無需重啟系統。

## 關鍵設計決策

### 為何使用 Rust 作為搓合引擎

搓合引擎在回測中會對每根 K 線處理所有未結訂單。在 100K 根 K 線的規模下，這是系統中最緊密的迴圈。Rust 實作達到 **約 89K 根 K 線/秒**的效能，包含完整訂單搓合（Market、Limit、SL、TP、Trailing Stop、OCO）和手續費計算 — 全部使用 Decimal 算術。

PyO3 橋接將 `PyMatchingEngine` 暴露給 Python，使熱路徑 (Hot Path) 保留在 Rust 中，而策略邏輯則留在 Python 以便快速迭代。

!!! note "編譯方式"
    Rust 函式庫編譯為共享物件 (`fluxtrade_core.so`)，由 Python 直接載入。請**勿**使用 `maturin develop`，因為 edition2024 傳遞性依賴問題。請改用 `cargo build --lib --release` 編譯，並將 `.dylib` 複製到 Python 原始碼目錄。

### 為何使用適配器模式 (Adapter Pattern)

FluxTrade 的核心承諾是**實盤交易 = 回測**。相同的策略程式碼在兩種模式下無需修改即可運行。這透過 `IExchangeAdapter` 實現：

- **實盤**：`CcxtExchangeAdapter` 呼叫真實交易所 API
- **回測**：`SimulatedAdapter` 委託至 Rust `PyMatchingEngine`

策略呼叫 `adapter.place_order()` 時，完全不知道自己處於哪種模式。詳見 [適配器模式](adapter-pattern.md)。

### 為何使用 Redis Streams

選擇 Redis Streams 而非 Pub/Sub 或訊息佇列 (Message Queue) 有三個原因：

1. **有序性 (Ordered)**：訊息按 Stream ID 嚴格排序，對 K 線序列至關重要
2. **持久性 (Persistent)**：訊息在消費者斷線後仍然保留；消費者可透過消費者群組 (Consumer Group)（`XREADGROUP`）從上次讀取位置恢復
3. **消費者群組 (Consumer Groups)**：多個策略實例可獨立消費同一個 Stream，各自追蹤偏移量 (Offset)

Stream 鍵編碼了交易所、交易對和時間框架，實現**時間框架通道隔離 (Timeframe Channel Isolation)**：每個策略只接收與其宣告時間框架匹配的 K 線，引擎提供安全防護作為深度防禦 (Defense-in-Depth)。

### 資料完整性：全面使用 Decimal

所有財務計算使用 `Decimal`（Python）或 `rust_decimal::Decimal`（Rust）。浮點數 (Float) **禁止**用於貨幣值。PyO3 邊界使用 `String` 序列化以在語言邊界間保持精度。

### P0 架構強化摘要

目前架構已包含 migrations 5-8 與 Engine/Risk 拆分後的 P0 強化結果：

| 關注點 | 擁有者 | 說明 |
|--------|--------|------|
| 策略執行期生命週期 | `StrategyStateManager` | Active/stopped/error 轉換、version guard、Redis state-change channel、transition history |
| 操作命令處理 | `CommandRouter` + `StrategyEngine` | 命令透過 engine lifecycle methods 執行，並保留 actor/reason metadata |
| 訊號防護 | `SignalProcessor` | stopped/error 策略的訊號會在執行前被阻擋 |
| 風控執行 | `RiskManager` + `risk_rules/` | 規則模組回傳 structured status/reason；違規會拒單，不會靜默調整倉位 |
| 每日虧損熔斷 | `RiskManager` + `DailyNavSnapshotService` | 可從 `daily_nav_snapshots` 取得日初 NAV，觸發時可將策略狀態轉為 ERROR |
| 冪等訂單恢復 | `client_order_id.py`、`execution.py`、adapter snapshots | Client order ID 支援 duplicate suppression 與 startup reconciliation |
| 回測帳戶狀態 | `SimulatedAdapter` + Rust matcher | Balance/position 透過 `BacktestAccountService` 從 matcher-backed adapter 讀取 |

Schema 強化摘要：

| Migration | 主要變更 |
|-----------|----------|
| 5 | Order intent/outcome audit 欄位、JSONB audit payload、`system_events`、client-order idempotency 支援 |
| 6 | Strategy-state lifecycle metadata、transition history、daily NAV snapshots、lifecycle CHECK constraints |
| 7 | `evolution_epochs` 與 `gene_records`，用於基因參數搜尋 lineage |
| 8 | Gene registry 之後的 optional performance indexes |

慣例：

- 新 audit-style payload 使用 JSONB；Decimal 在邊界以字串序列化，進 Python 內部用 `Decimal(str(value))` 還原。
- 新 lifecycle/audit timestamp 使用 migration 引入的 timezone-aware database timestamp；舊的毫秒整數欄位除非有 migration，不做回填式改造。
- 回測持倉使用明確的 `side + absolute quantity`；SHORT 由 `side == SHORT` 表示，不用負 quantity 表示。
- Realized PnL 目前使用平均成本 netting，與 Rust matcher 和 `analytics._build_closed_trades()` 一致。

## 測試覆蓋率

- Python CI 會執行 Ruff、明確的 invariant tests (`tests/test_invariant_*.py`) 與帶 coverage gate 的 non-integration tests (`--cov-fail-under=77`)。
- P0 強化期間最新本地 non-integration 結果：`924 passed, 75 deselected`。
- Invariant coverage 包含 matcher/account position consistency、RiskManager position-source consistency、side-boundary conversion、realized PnL recomputation against matcher balance。
- 關鍵測試模式：工廠式 fixtures、基於 `spec` 的 mocks、透過 `MockExchangeAdapter` 的選擇性故障注入。
