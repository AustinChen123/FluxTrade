# FluxTrade

FluxTrade 是一個高效能、基於微服務架構的加密貨幣自動交易系統。設計目標為實現低延遲數據處理、靈活的策略執行以及即時的風險控管。

🌍 **文件**: [English README](README.md) | [繁體中文開發者指南](docs/zh-TW/developer_guide.md) | [快速上手 (User Guide)](docs/user_guide.md)

## ✨ 核心特色

*   **🚀 Rust 核心數據引擎**: 極低延遲的 WebSocket 數據攝取與 K 線聚合，確保策略永遠運行在最新數據上。
*   **🐍 Python 熱插拔策略**: 使用熟悉的 Python 語法編寫策略，支援動態載入與即時回填 (Backfill)，無需重啟系統。
*   **🛡️ 安全優先風控**: 內建強制市價單防護與即時餘額檢查 (Redis-backed Risk Manager)，保障資金安全。
*   **📊 完整可觀測性**: 整合 Streamlit 儀表板，提供即時 PnL、持倉監控與策略狀態視覺化。

## 系統架構

系統由三個核心服務組成：

1.  **Rust Data Service**: 
    - 負責與交易所 (Binance, Bybit, Backpack) 建立 WebSocket 連線。
    - 即時標準化 Market Data (Trades, Candles) 並發布至 Redis Pub/Sub。
    - 實作高效能的 K 線聚合 (Aggregator)。

2.  **Python Strategy Service**:
    - 訂閱 Redis 數據流。
    - 執行交易策略 (Strategy Engine)。
    - 進行風險檢查 (Risk Manager) 與訂單管理 (Order Manager)。
    - 支援回測與模擬執行。

3.  **Dashboard (Python/Streamlit)**:
    - 提供即時市場概況、交易歷史與風險監控介面。
    - 用於驗證策略邏輯與視覺化數據。

## 環境設定 (Configuration)

本專案使用 `.env` 檔案進行環境變數管理。

1.  **複製範例檔案**：
    ```bash
    cp .env.example .env
    ```

2.  **設定變數說明**：
    打開 `.env` 檔案並填入您的配置：

    *   **資料庫設定 (PostgreSQL)**
        *   `POSTGRES_USER`: 資料庫使用者名稱 (預設: fluxtrade)
        *   `POSTGRES_PASSWORD`: 資料庫密碼
        *   `POSTGRES_DB`: 資料庫名稱
        *   `POSTGRES_HOST`: 主機 (本地開發使用 localhost)
    
    *   **快取設定 (Redis)**
        *   `REDIS_HOST`: Redis 主機 (預設: localhost)
        
    *   **交易所設定 (Exchange)**
        *   `EXCHANGE_ID`: 主要交易平台 (例如: binance)
        *   `EXCHANGE_API_KEY`: 您的 API Key
        *   `EXCHANGE_SECRET`: 您的 API Secret
        *   `EXCHANGE_TESTNET`: 是否使用測試網 (true/false)

## 快速啟動 (Quick Start)

我們推薦使用 Docker Compose 進行一鍵部署。

1.  **啟動所有服務**
    ```bash
    docker-compose -f docker-compose.prod.yml up -d
    ```

2.  **訪問儀表板**
    打開瀏覽器前往 [http://localhost:8501](http://localhost:8501)

3.  **停止服務**
    ```bash
    docker-compose -f docker-compose.prod.yml down
    ```

若需手動個別啟動服務，請參考 [使用者指南](docs/user_guide.md)。
