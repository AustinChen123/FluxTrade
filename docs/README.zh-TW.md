# FluxTrade

基於微服務架構的加密貨幣交易系統，使用 Rust 與 Python 建構。Rust 負責即時市場資料接收；Python 負責策略邏輯、風險管理與回測。服務間透過 Redis Streams 通訊。

**文件**: [English](../../README.md) | [Developer Guide (EN)](en/developer_guide.md) | [開發者指南 (中文)](zh-TW/developer_guide.md) | [User Guide](user_guide.md)

## 架構

```
Exchange WebSocket
        │
        ▼
┌──────────────────────┐
│  Rust Data Service   │
│  WebSocket → Candle  │
│  Aggregator → Redis  │
└──────────────────────┘
        │ Redis Streams (per product × timeframe)
        ▼
┌──────────────────────────────────────┐
│  Python Strategy Service             │
│                                      │
│  Consumer → StrategyEngine           │
│    ├─ Strategy.on_candle() → Signal  │
│    ├─ RiskManager → validation       │
│    ├─ ExecutionEngine → Adapter      │
│    └─ OrderManager → persistence     │
│                                      │
│  Adapters:                           │
│    ├─ LiveBinanceAdapter (CCXT)      │
│    └─ SimulatedAdapter (backtest)    │
└──────────────────────────────────────┘
        │
        ▼
┌──────────────────────┐
│  Streamlit Dashboard │
│  PnL / Positions /   │
│  Strategy Status     │
└──────────────────────┘
```

### 組件說明

**Rust Data Service** (`rust-data-service/`)

- 與 Binance、Bybit、Backpack 建立 WebSocket 連線
- 多時間框架 K 線聚合（1m → 5m/15m，透過 bucketing 實現）
- 發布至按時間框架區分的 Redis Streams
- PyO3 綁定提供 `fluxtrade_core` 撮合引擎供回測使用

**Python Strategy Service** (`python-strategy/`)

- 事件驅動的 `StrategyEngine` 協調所有組件
- Adapter 模式：`IExchangeAdapter` 介面，支援實盤與模擬實作
- `IDataSource` 介面提供可插拔的資料後端（PostgreSQL、CSV、記憶體）
- 從 `strategies_hot/` 目錄熱插拔策略（無需重啟）
- 風險管理器提供餘額檢查與持倉限制

**Dashboard** (`dashboard/`)

- 基於 Streamlit 的即時監控介面
- PnL 追蹤、持倉視覺化、策略狀態

### 回測機制

回測管線重用與實盤交易相同的 `StrategyEngine` 和策略程式碼。唯一差異在於 adapter 和資料來源：

|      | 實盤                                      | 回測                                |
| ---- | ----------------------------------------- | ----------------------------------- |
| 資料 | Redis Streams（即時）                     | `IDataSource`（DB / CSV / 記憶體）  |
| 執行 | `LiveBinanceAdapter`（CCXT → 交易所 API） | `SimulatedAdapter`（Rust 撮合引擎） |
| 時鐘 | 系統時鐘                                  | `BacktestClock`（模擬時間）         |

`SimulatedAdapter` 使用透過 PyO3 編譯為 Python 擴充模組的 Rust 撮合引擎。逐根 K 線進行訂單撮合（市價單以開盤價成交，限價單在價格觸及 high/low 範圍時成交）。

## Benchmark：撮合引擎效能

所有引擎執行相同的 SMA(10/30) 交叉策略，使用相同的合成資料（Ornstein-Uhlenbeck 價格模型，seed=42，無手續費）。

| 框架                           | 類型                   | 10K 根 K 線 | 100K 根 K 線 | 500K 根 K 線 |
| ------------------------------ | ---------------------- | ----------- | ------------ | ------------ |
| **fluxtrade_core (Rust/PyO3)** | 事件驅動，逐 bar       | ~0.003s     | ~0.025s      | ~0.13s       |
| **backtesting.py**             | 事件驅動，逐 bar       | ~0.02s      | ~0.18s       | ~0.9s        |
| **vectorbt**                   | 向量化（NumPy/pandas） | ~0.04s      | ~0.06s       | ~0.15s       |
| **Pure Python**                | 事件驅動，逐 bar       | ~0.01s      | ~0.10s       | ~0.50s       |

_時間為近似值，依硬體而異。執行 `tools/benchmark_matching_engine.py` 可重現。_

**Benchmark 量測範圍**：僅限訂單撮合吞吐量 — 將 K 線送入撮合引擎並處理成交。這是回測最內層的迴圈。

**未量測範圍**：完整回測管線開銷（資料載入、策略邏輯、持久化、分析計算）。

### 比較說明

**vs. vectorbt**：vectorbt 使用 NumPy 向量化一次處理整段價格序列，適合簡單的訊號策略。FluxTrade 的 Rust 引擎採用事件驅動（逐 bar），支援有狀態的邏輯如追蹤停損、部分成交、持倉淨額計算 — 這些操作難以用純向量化形式表達。在小資料量時 vectorbt 的常數開銷佔主導；在大資料量時兩者趨近。

**vs. backtesting.py**：兩者都是事件驅動的逐 bar 引擎。backtesting.py 是純 Python 框架，內建繪圖與參數最佳化功能。FluxTrade 的 Rust 引擎處理相同的撮合邏輯，但透過 PyO3 執行編譯後的程式碼，因此每根 bar 的開銷較低。

**vs. Freqtrade / Jesse / Hummingbot**：這些是完整的交易平台，擁有各自的策略 DSL、交易所整合與 CLI/UI。FluxTrade 在架構上的差異：

- **語言分工**：Rust 負責資料接收與撮合；Python 負責策略邏輯。其他平台為純 Python（Freqtrade、Jesse）或 C++/Python 混合（Hummingbot）。
- **微服務部署**：各服務獨立運行，透過 Redis 通訊。其他平台通常以單一行程運行。
- **策略介面**：策略實作 `BaseStrategy.on_candle()` 並接收已聚合的 K 線。沒有自訂 DSL 或基於裝飾器的訊號系統。
- **回測/實盤一致性**：相同的 `StrategyEngine` 程式碼路徑在兩種模式下運行，僅切換 adapter 和資料來源。部分平台的回測與實盤引擎是分開的。

FluxTrade 不包含成熟平台提供的功能：策略管理 Web UI、超參數最佳化、策略市集、內建技術指標庫、多交易所套利。專注於資料管線與執行路徑。

## 快速啟動

### Docker（建議方式）

```bash
cp .env.example .env
# 編輯 .env，填入資料庫、Redis 與交易所憑證

docker-compose -f docker-compose.prod.yml up -d

# Dashboard 位於 http://localhost:8501
```

### 手動建置

環境需求：Python 3.12+、Rust stable、PostgreSQL 15、Redis

```bash
# Rust Data Service
cd rust-data-service
cargo build --release

# Python Strategy Service
cd python-strategy
uv sync
uv run maturin develop  # 建置 PyO3 擴充模組

# Database
cd database
alembic upgrade head
```

## 環境設定

複製 `.env.example` 為 `.env` 並設定：

| 變數                                         | 說明                           |
| -------------------------------------------- | ------------------------------ |
| `POSTGRES_USER` / `PASSWORD` / `DB` / `HOST` | PostgreSQL 連線設定            |
| `REDIS_HOST`                                 | Redis 連線設定                 |
| `EXCHANGE_ID`                                | 目標交易所（例如 `binance`）   |
| `EXCHANGE_API_KEY` / `SECRET`                | API 憑證                       |
| `EXCHANGE_TESTNET`                           | 使用測試網（`true` / `false`） |

## 開發

```bash
# Python — lint 與測試
cd python-strategy
uv run ruff check .
uv run pytest

# Rust — 格式化、lint、測試
cd rust-data-service
cargo fmt
cargo clippy -- -D warnings
cargo test

# Benchmark（從 repo 根目錄）
cd python-strategy
uv run python ../tools/benchmark_matching_engine.py
```

## 專案結構

```
FluxTrade/
├── rust-data-service/       # Rust: WebSocket、聚合、PyO3 綁定
│   └── src/
│       ├── connector/       # 交易所 WebSocket 客戶端
│       ├── aggregator/      # 多時間框架 K 線聚合
│       ├── publisher/       # Redis stream 發布器
│       ├── binding/         # PyO3 撮合引擎供 Python 使用
│       └── model/           # 共用資料模型
├── python-strategy/         # Python: 策略引擎、回測
│   └── src/
│       ├── core/
│       │   ├── engine.py            # StrategyEngine 協調器
│       │   ├── risk_manager.py      # 風險檢查
│       │   ├── order_manager.py     # 訂單生命週期
│       │   ├── execution_engine.py  # 訊號 → 訂單執行
│       │   ├── consumer.py          # Redis stream 消費者
│       │   ├── backtest_runner.py   # 回測協調器
│       │   ├── adapters/            # IExchangeAdapter 實作
│       │   ├── interfaces/          # 抽象介面
│       │   └── data_sources/        # IDataSource 實作
│       └── strategies/              # 策略實作
├── dashboard/               # Streamlit 監控介面
├── database/                # Alembic migrations
└── tools/                   # Benchmark、資料生成、工具
```
