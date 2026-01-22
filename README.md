# FluxTrade

FluxTrade 是一個高效能、基於微服務架構的加密貨幣自動交易系統。設計目標為實現低延遲數據處理、靈活的策略執行以及即時的風險控管。

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

1.  **啟動基礎設施 (Postgres & Redis)**
    ```bash
    docker compose up -d
    ```

2.  **啟動 Rust 資料服務**
    ```bash
    cd rust-data-service
    cargo run
    ```

3.  **啟動 Python 策略服務**
    ```bash
    cd python-strategy
    # 確保已安裝 uv
    uv run src/main.py
    ```

4.  **啟動儀表板**
    ```bash
    cd dashboard
    streamlit run app.py
    ```
