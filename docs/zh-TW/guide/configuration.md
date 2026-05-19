# 配置 (Configuration)

本指南涵蓋 FluxTrade 的所有配置選項，包括環境變數、適配器選擇 (Adapter Selection)、回測設定，以及 Docker 部署。

---

## 環境變數

FluxTrade 使用專案根目錄下的 `.env` 檔案進行所有服務配置。複製範例檔案即可開始：

```bash
cp .env.example .env
```

### 資料庫 (PostgreSQL)

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `POSTGRES_USER` | `fluxtrade` | PostgreSQL 使用者名稱 |
| `POSTGRES_PASSWORD` | *（必填）* | PostgreSQL 密碼 |
| `POSTGRES_DB` | `fluxtrade` | 資料庫名稱 |
| `POSTGRES_HOST` | `localhost` | 資料庫主機（Docker 中為 `db`） |
| `POSTGRES_PORT` | `5432` | 資料庫埠號 |

Python 策略服務會自動建構連線 URL：

```
postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}
```

資料庫引擎在首次使用時才懶惰建立（具備雙重檢查鎖定的執行緒安全機制）。

### Redis

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `REDIS_HOST` | `localhost` | Redis 主機（Docker 中為 `redis`） |
| `REDIS_PORT` | `6379` | Redis 埠號 |
| `REDIS_PASSWORD` | *（空）* | Redis 密碼（本機開發時留空） |

當 `REDIS_PASSWORD` 為空或未設定時，客戶端不使用認證連線。

### 交易所憑證

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `EXCHANGE_ID` | `binance` | CCXT 交易所識別碼 |
| `EXCHANGE_API_KEY` | *（空）* | 交易所 API 金鑰 |
| `EXCHANGE_SECRET` | *（空）* | 交易所 API 密鑰 |
| `EXCHANGE_TESTNET` | `true` | 使用交易所測試網（沙盒）模式 |

針對使用 WebSocket 快速通道的 Binance 特定實盤交易：

| 變數 | 說明 |
|------|------|
| `BINANCE_API_KEY` | Binance 專用 API 金鑰 |
| `BINANCE_SECRET` | Binance 專用 API 密鑰 |

### 策略服務

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `HOT_STRATEGIES_PATH` | `/app/strategies_hot` | 監視 `.py` 策略檔案的目錄；在此放入檔案即可無需重啟服務載入 |
| `METRICS_ENABLED` | `false` | 啟用 Prometheus 指標 HTTP 伺服器 |
| `METRICS_PORT` | `9090` | Prometheus 指標端點的埠號 |
| `LOG_FORMAT` | *（text）* | 設為 `json` 以使用結構化 JSON 日誌 |

### 儀表板 (Dashboard)

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `DASHBOARD_PASSWORD` | *（空）* | 儀表板登入密碼（留空則停用認證） |

### 監控 (Monitoring)

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `GRAFANA_PASSWORD` | *（必填）* | Grafana 管理員密碼 |

### 完整 `.env.example`

```bash
POSTGRES_USER=fluxtrade
POSTGRES_PASSWORD=fluxtrade_password
POSTGRES_DB=fluxtrade
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

REDIS_HOST=localhost
REDIS_PORT=6379
# Redis auth (leave empty for no password in local dev)
REDIS_PASSWORD=

HOT_STRATEGIES_PATH=/app/strategies_hot

# Exchange Config (Optional for Mock Mode)
EXCHANGE_ID=binance
EXCHANGE_API_KEY=
EXCHANGE_SECRET=
EXCHANGE_TESTNET=true

# Dashboard auth (leave empty to disable login)
DASHBOARD_PASSWORD=
```

---

## 適配器配置 (Adapter Configuration)

FluxTrade 使用適配器模式 (Adapter Pattern) 將交易所互動隔離在 `IExchangeAdapter` 介面之後。`src.core.adapters` 中的 `create_adapter(config)` 工廠函式根據配置字典選擇正確的實作。

### 配置字典

```python
from src.core.adapters import create_adapter

adapter = create_adapter({
    "mode": "simulated",       # "simulated" | "live"
    "exchange": "binance",     # CCXT exchange id (live only)
    "api_key": "...",          # API key (falls back to EXCHANGE_API_KEY env var)
    "secret": "...",           # API secret (falls back to EXCHANGE_SECRET env var)
    "testnet": True,           # Use sandbox mode (default: True)
    "balance": 100000,         # Initial simulated balance (simulated only)
    "maker_fee": 0.0002,       # Maker fee rate (simulated only)
    "taker_fee": 0.0006,       # Taker fee rate (simulated only)
    "enable_ws": False,        # Enable WebSocket fast path (live Binance only)
    "extra_config": {},        # Extra CCXT configuration dict
})
```

### 適配器選擇邏輯

```
mode == "simulated"?
  └── Yes → SimulatedAdapter (Rust matching engine, no network)
  └── No (mode == "live")
        └── exchange == "binance" AND enable_ws == True?
              └── Yes → LiveBinanceAdapter (CCXT + WebSocket fast path)
              └── No  → CcxtExchangeAdapter (universal CCXT REST)
```

| `mode` | `exchange` | `enable_ws` | 建立的適配器 |
|--------|-----------|-------------|-------------|
| `simulated` | *（忽略）* | *（忽略）* | `SimulatedAdapter`（Rust 撮合引擎） |
| `live` | `binance` | `True` | `LiveBinanceAdapter`（CCXT + WebSocket） |
| `live` | `binance` | `False` | `CcxtExchangeAdapter`（僅 REST） |
| `live` | 其他 | *（忽略）* | `CcxtExchangeAdapter`（通用 CCXT） |

### 模擬模式（回測）

`SimulatedAdapter` 透過 PyO3 將所有訂單撮合委派給 Rust `PyMatchingEngine`。支援市價單 (Market)、限價單 (Limit)、停損 (Stop Loss)、停利 (Take Profit)、移動停損 (Trailing Stop) 和 OCO 委託，並完整計算手續費：

```python
adapter = create_adapter({
    "mode": "simulated",
    "balance": 10000,
    "maker_fee": 0.0002,
    "taker_fee": 0.0006,
})
```

### 實盤模式

實盤適配器透過 CCXT 連接真實交易所。若配置中未提供 API 憑證，會回退至環境變數：

```python
adapter = create_adapter({
    "mode": "live",
    "exchange": "binance",
    "testnet": True,        # always start with testnet
    "enable_ws": True,      # optional WebSocket for market orders
})
```

`LiveBinanceAdapter` 繼承 `CcxtExchangeAdapter`，附加選用的 WebSocket 快速通道用於市價單。若 WebSocket 初始化失敗，會靜默回退至 REST。

---

## 回測配置

### BacktestRunner 參數

```python
from src.core.backtest_runner import BacktestRunner

runner = BacktestRunner(
    start_time=1700000000000,            # Unix ms (required)
    end_time=1700500000000,              # Unix ms (required)
    product_id="BINANCE:BTCUSDT-PERP",   # required
    timeframe="15m",                      # required
    initial_balance=10000.0,              # starting balance in USD
    max_drawdown_limit=0.20,              # circuit breaker: stop at 20% drawdown
    data_source=ds,                       # IDataSource (None = use PostgreSQL)
    fee_config={                          # maker/taker fee rates
        "maker": 0.0002,
        "taker": 0.0006,
    },
    report_config={                       # output file toggles
        "csv_trades": True,
        "equity_curve": True,
        "markdown_report": True,
        "journal_export": True,
        "output_dir": "backtest_output/",
    },
)
```

### 手續費配置

手續費以 `Decimal` 值傳遞給 Rust 撮合引擎，在每次訂單成交時套用：

| 鍵 | 說明 | 典型值 |
|----|------|--------|
| `maker` | 限價單手續費率 | `0.0002` (0.02%) |
| `taker` | 市價單、SL/TP 觸發的手續費率 | `0.0006` (0.06%) |

常見交易所手續費率：

| 交易所 | Maker | Taker |
|--------|-------|-------|
| Binance Futures | 0.0002 | 0.0005 |
| Bybit | 0.0001 | 0.0006 |
| Backpack | 0.0002 | 0.0006 |

### 熔斷機制

`max_drawdown_limit` 在帳戶餘額跌破以下值時停止回測：

```
stop_threshold = initial_balance * (1 - max_drawdown_limit)
```

例如，`initial_balance=10000.0` 且 `max_drawdown_limit=0.20` 時，若餘額跌破 8000 則回測停止。

### 資料來源選擇

| 資料來源 | 匯入路徑 | 使用場景 |
|----------|----------|----------|
| `CsvDataSource` | `from src.core.data_sources.csv_source import CsvDataSource` | CSV 檔案（TradingView、Yahoo Finance 等） |
| `MemoryDataSource` | `from src.core.data_sources.memory import MemoryDataSource` | 單元測試、合成資料 |
| `DatabaseDataSource` | `from src.core.data_sources.database import DatabaseDataSource` | 正式環境（經由 Rust 資料服務匯入） |
| `YahooFinanceDataSource` | `from src.core.data_sources.yahoo import YahooFinanceDataSource` | 傳統資產快速原型開發 |

當 `data_source=None` 時，`BacktestRunner` 回退至使用 `get_candles_generator()` 的 PostgreSQL 資料庫。

### 報告配置

| 鍵 | 型別 | 預設值 | 說明 |
|----|------|--------|------|
| `csv_trades` | `bool` | `True` | 寫入 `trades.csv`，包含所有已平倉交易 |
| `equity_curve` | `bool` | `True` | 寫入 `equity_curve.csv`，包含累計損益 |
| `markdown_report` | `bool` | `True` | 寫入 `report.md` 績效摘要 |
| `journal_export` | `bool` | `True` | 寫入 `journal.jsonl` 結構化事件記錄 |
| `output_dir` | `str` | `"backtest_output/"` | 輸出目錄路徑 |

---

## Docker 部署配置

FluxTrade 透過 `docker-compose.prod.yml` 以多容器應用程式方式執行。所有服務從相同的 `.env` 檔案讀取。

### 服務概覽

| 服務 | 容器 | 埠號 | 說明 |
|------|------|------|------|
| `redis` | `fluxtrade-redis` | 6379 | 訊息代理 (Redis Streams) |
| `db` | `fluxtrade-db` | 5432 | PostgreSQL 資料庫 |
| `rust-data` | `fluxtrade-rust` | -- | 行情資料匯入（WebSocket + 聚合） |
| `python-strategy` | `fluxtrade-python` | 9090 | 策略引擎 + Prometheus 指標 |
| `dashboard` | `fluxtrade-dashboard` | 8501 | Streamlit 監控儀表板 |
| `prometheus` | `fluxtrade-prometheus` | 9091 | 指標收集 |
| `grafana` | `fluxtrade-grafana` | 3000 | 指標視覺化 |

### 資源限制

| 服務 | 記憶體 | CPU |
|------|--------|-----|
| Redis | 256M | 0.5 |
| PostgreSQL | 512M | 1.0 |
| Rust Data Service | 512M | 1.0 |
| Python Strategy | 1G | 1.5 |
| Dashboard | 512M | 0.5 |
| Prometheus | 512M | 0.5 |
| Grafana | 256M | 0.5 |

### 啟動順序

服務以健康檢查依賴關係啟動：

1. **Redis** 和 **PostgreSQL** 最先啟動，附帶健康檢查
2. **Rust Data Service** 等待 Redis 和 DB 健康
3. **Python Strategy Service** 等待 Redis、DB（健康）及 Rust Data（已啟動）
4. **Dashboard** 等待 Redis 和 DB 健康
5. **Prometheus** 和 **Grafana** 獨立啟動

### Volume 掛載

| Volume | 用途 |
|--------|------|
| `postgres_data` | 持久化資料庫儲存 |
| `prometheus_data` | Prometheus 指標保留（30 天） |
| `grafana_data` | Grafana 儀表板和配置 |
| `./python-strategy/strategies_hot` | 熱插拔策略檔案（bind mount） |

### 啟動與停止

```bash
# Start all services
docker-compose -f docker-compose.prod.yml up -d

# View logs
docker-compose -f docker-compose.prod.yml logs -f

# View logs for a specific service
docker-compose -f docker-compose.prod.yml logs -f python-strategy

# Stop all services
docker-compose -f docker-compose.prod.yml down

# Stop and remove volumes (destructive)
docker-compose -f docker-compose.prod.yml down -v
```

---

## 下一步

- [撰寫策略](writing-strategies.md) -- 建立自訂交易策略
- [回測](backtesting.md) -- 執行包含完整指標和報告的回測
- [實盤交易](live-trading.md) -- 將策略部署到實盤交易所
