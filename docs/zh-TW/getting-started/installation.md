# 安裝指南

本指南涵蓋從原始碼安裝 FluxTrade 及其所有相依套件的步驟。

## 前置需求

| 相依項目     | 版本      | 說明                                       |
|--------------|-----------|--------------------------------------------|
| Python       | 3.12+     | `pyproject.toml` 中指定                    |
| Rust         | 1.82.0    | 由 `rust-toolchain.toml` 鎖定             |
| PostgreSQL   | 15        | 用於交易紀錄與回測結果儲存                 |
| Redis        | Latest    | 服務間的 Pub/Sub 訊息匯流排               |
| uv           | Latest    | Python 套件管理工具                        |

## 複製儲存庫

```bash
git clone https://github.com/your-org/FluxTrade.git
cd FluxTrade
```

## 環境設定

複製範例環境檔並填入你的憑證：

```bash
cp .env.example .env
```

`.env` 檔案包含以下內容：

```ini
POSTGRES_USER=fluxtrade
POSTGRES_PASSWORD=fluxtrade_password
POSTGRES_DB=fluxtrade
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

EXCHANGE_ID=binance
EXCHANGE_API_KEY=
EXCHANGE_SECRET=
EXCHANGE_TESTNET=true

DASHBOARD_PASSWORD=
```

若僅用於本地開發與回測，交易所 API 金鑰為選填。

## Python 策略服務

```bash
cd python-strategy
uv sync
```

此命令會安裝 `pyproject.toml` 中定義的所有運行時與開發相依套件，包括：

- **運行時**：`ccxt`、`redis`、`sqlalchemy`、`pydantic`、`pandas`、`pandas-ta`、`structlog`、`prometheus-client`
- **開發**：`pytest`、`ruff`、`pyright`、`maturin`、`pytest-cov`

## Rust 資料服務

確認已安裝 Rust 1.82.0。專案根目錄的 `rust-toolchain.toml` 會自動鎖定版本：

```bash
cd rust-data-service
cargo build
```

此命令會編譯資料服務的執行檔及 `fluxtrade_core` PyO3 函式庫。

## PyO3 擴充（Python 使用的 Rust 搓合引擎）

Python 回測引擎仰賴 `fluxtrade_core.so`，這是一個 Rust 編譯的擴充模組，提供搓合引擎 (`PyMatchingEngine`)。你必須手動編譯它：

```bash
cd rust-data-service

RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup" \
  cargo build --lib --release
```

接著將編譯好的函式庫複製到 Python 原始碼目錄：

```bash
# macOS
cp target/release/libfluxtrade_core.dylib ../python-strategy/src/fluxtrade_core.so

# Linux
cp target/release/libfluxtrade_core.so ../python-strategy/src/fluxtrade_core.so
```

!!! warning "請勿使用 `maturin develop`"
    `uv run maturin develop` 指令會因 `edition2024` 傳遞相依問題而失敗。請一律使用上述手動 `cargo build --lib --release` 的工作流程。

!!! note "`.so` 檔案不會提交至 git"
    每次修改 Rust 程式碼後，你都必須重新建置 `fluxtrade_core.so`。此檔案已被 `.gitignore` 忽略。

驗證擴充模組是否正確載入：

```bash
cd ../python-strategy
python -c "from fluxtrade_core import PyMatchingEngine; print('PyO3 extension loaded successfully')"
```

## 資料庫設定

啟動 PostgreSQL 與 Redis（若未使用 Docker）：

```bash
# macOS (Homebrew)
brew services start postgresql@15
brew services start redis

# Linux (systemd)
sudo systemctl start postgresql redis
```

PostgreSQL 運行後，執行資料庫遷移：

```bash
cd database
alembic upgrade head
```

## Docker 設定（完整系統）

透過 Docker 執行所有服務（Redis、PostgreSQL、Rust 資料服務、Python 策略服務、Dashboard、Prometheus、Grafana）：

```bash
docker-compose -f docker-compose.prod.yml up -d
```

停止所有服務：

```bash
docker-compose -f docker-compose.prod.yml down
```

Docker Compose 設定檔要求在 `.env` 中設定 `POSTGRES_PASSWORD` 與 `GRAFANA_PASSWORD`。

## 驗證

### 執行 Rust 測試

```bash
cd rust-data-service
cargo test --no-default-features
```

`--no-default-features` 旗標會停用 `extension-module` 功能，此功能僅在為 Python 建置 `.so` 時需要。

### 執行 Python 測試

```bash
cd python-strategy
uv run pytest
```

若只想執行單元測試（排除需要 Docker 服務的整合測試）：

```bash
uv run pytest -m "not integration"
```

### 程式碼檢查

```bash
# Python
cd python-strategy
uv run ruff check .

# Rust
cd rust-data-service
cargo fmt --check
cargo clippy -- -D warnings
```

若所有測試通過且程式碼檢查無誤，安裝即完成。請繼續閱讀[快速開始](quickstart.md)指南。
