# FluxTrade 使用者指南 (User Guide)

歡迎使用 FluxTrade！這是一份針對使用者的詳細操作指南，旨在協助您快速上手並掌握系統的核心功能。

## 目錄 (Table of Contents)

1.  [系統概覽 (Overview)](#1-系統概覽-overview)
2.  [環境準備 (Prerequisites)](#2-環境準備-prerequisites)
3.  [快速啟動 (Quick Start)](#3-快速啟動-quick-start)
4.  [策略管理 (Strategy Management)](#4-策略管理-strategy-management)
5.  [風險控管 (Risk Management)](#5-風險控管-risk-management)
6.  [監控儀表板 (Dashboard)](#6-監控儀表板-dashboard)
7.  [常見問題排查 (Troubleshooting)](#7-常見問題排查-troubleshooting)

---

## 1. 系統概覽 (Overview)

FluxTrade 是一個專為加密貨幣永續合約 (Perpetual Futures) 設計的自動化交易系統。它結合了 Rust 的高效能數據處理與 Python 的靈活策略開發能力。

**核心優勢：**
*   **低延遲數據**：Rust 服務直接與交易所 WebSocket 連線，即時聚合 K 線。
*   **即時風控**：在每一筆訂單發送前，系統會根據即時帳戶餘額進行強制檢查，防止超額下單。
*   **安全防護**：具備斷線保護與強制市價單防護機制，確保在網路不穩時不會發生意外成交。

---

## 2. 環境準備 (Prerequisites)

在使用 FluxTrade 之前，請確保您的系統已安裝以下工具：

*   **Docker & Docker Compose**: 用於運行資料庫與基礎服務。
*   **Python 3.10+**: 用於運行策略引擎與儀表板。
*   **Rust (Cargo)** (選用): 如果您需要編譯數據服務，否則可直接使用 Docker 映像檔。
*   **uv**: 推薦使用的 Python 套件管理器 (比 pip 更快)。

### API Key 申請
您需要申請交易所 (Binance Futures, Backpack 等) 的 API Key。
*   **建議權限**：僅開啟「合約交易 (Enable Futures)」與「讀取資訊 (Reading)」，**切勿**開啟提幣權限。

---

## 3. 快速啟動 (Quick Start)

### 步驟 1: 設定環境變數
複製範例檔案並填入您的 API Key：
```bash
cp .env.example .env
# 編輯 .env 檔案，填入 EXCHANGE_API_KEY 與 EXCHANGE_SECRET
```

### 步驟 2: 啟動基礎服務
使用 Docker Compose 一鍵啟動所有後端服務：
```bash
docker-compose -f docker-compose.prod.yml up -d
```
這將啟動 Postgres, Redis, Rust Data Service, Python Strategy Engine 與 Dashboard。

### 步驟 3: 確認狀態
檢查容器是否正常運行：
```bash
docker ps
```
您應該能看到 `fluxtrade-rust`, `fluxtrade-python` 等容器狀態為 Up。

---

## 4. 策略管理 (Strategy Management)

FluxTrade 採用「熱插拔 (Hot-Pluggable)」的策略架構。您可以在不重啟系統的情況下新增或修改策略。

### 新增策略
1.  在 `python-strategy/src/strategies/` 目錄下建立新的 Python 檔案 (例如 `my_strategy.py`)。
2.  繼承 `BaseStrategy` 並實作 `on_candle` 方法。

```python
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class MyStrategy(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            lookback_window=20
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        # 簡單的範例：如果收盤價大於 50000 則做多
        if candle.close > 50000:
            return Signal(type=SignalType.LONG, ...)
        return Signal(type=SignalType.NO_SIGNAL)
```

### 策略生命週期
*   **DISCOVERED**: 系統偵測到新檔案。
*   **WARNING**: 歷史數據不足，正在自動回填 (Backfill)。
*   **READY**: 數據準備就緒，等待啟動。
*   **ACTIVE**: 策略正在運行並接收即時數據。

---

## 5. 風險控管 (Risk Management)

系統內建嚴格的風險檢查器 (Risk Manager)，位於 `python-strategy/src/core/risk_manager.py`。

**主要規則：**
1.  **餘額檢查 (Zero Balance Protection)**: 若 Redis 中的帳戶餘額 <= 0，拒絕所有開倉信號。
2.  **最大曝險限制 (Max Exposure)**: 限制單一交易對的最大持倉價值 (預設 50,000 USDT)。
3.  **價格防護**: 若 WebSocket 斷線且系統降級為 REST 下單，系統會強制檢查 Limit Price，防止意外以市價成交。

---

## 6. 監控儀表板 (Dashboard)

FluxTrade 提供一個基於 Streamlit 的網頁介面。

**訪問方式：**
打開瀏覽器，前往 `http://localhost:8501`。

**功能：**
*   **即時行情**: 查看最新的 K 線圖與指標。
*   **策略狀態**: 監控所有策略的運行狀態 (ACTIVE/STOPPED) 與 PnL。
*   **系統日誌**: 查看系統運行 Log，便於排錯。

---

## 7. 常見問題排查 (Troubleshooting)

**Q: 策略一直停留在 WARNING 狀態？**
A: 這通常表示歷史數據回填 (Backfill) 正在進行中，或者失敗了。請檢查 Rust 服務的 Log：
```bash
docker logs fluxtrade-rust
```

**Q: 下單失敗，顯示 "Insufficient Balance"？**
A: 請檢查您的交易所合約帳戶是否有足夠的 USDT。系統會即時同步餘額，若餘額不足將自動拒單。

**Q: 如何手動重置系統？**
A: 如果遇到狀態不一致，可以使用提供的測試工具進行重置（注意：這會清空當前狀態）：
```bash
python3 tools/smoke_test_hotplug.py
```
*(注意：此腳本主要用於開發測試，生產環境請謹慎使用)*
