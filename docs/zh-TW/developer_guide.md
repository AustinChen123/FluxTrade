# FluxTrade 開發者指南

歡迎閱讀 FluxTrade 開發者指南。本文檔將深入介紹如何開發自定義策略、理解系統架構，以及如何與系統暴露的接口進行交互。

## 1. 策略開發 (Strategy Development)

FluxTrade 採用「熱插拔 (Hot-Pluggable)」策略引擎，允許您在不重啟系統的情況下新增、修改或移除策略。

### `BaseStrategy` 介面

所有策略都必須繼承位於 `python-strategy/src/strategies/base.py` 的 `BaseStrategy` 類別。

```python
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class MyCustomStrategy(BaseStrategy):
    """
    一個自定義的趨勢跟隨策略。
    """

    @property
    def requirements(self) -> StrategyRequirements:
        """
        定義策略所需的數據要求。
        """
        return StrategyRequirements(
            product_id="BINANCE:BTCUSDT-PERP", # 交易所:交易對
            timeframe="1m",                    # K線週期 (1m, 5m, 1h 等)
            lookback_window=50                 # 所需的歷史 K 線數量
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        """
        每當有一根新的 K 線「收盤 (Closed)」時觸發。
        
        Args:
            candle: 最新收盤的 K 線數據。
            
        Returns:
            Signal: 交易決策 (LONG, SHORT, EXIT, 或 NO_SIGNAL)。
        """
        
        # 存取歷史數據 (由引擎自動管理)
        # self.candles 是一個 deque，包含了最近 `lookback_window` 根 K 線
        if len(self.candles) < self.requirements.lookback_window:
            return Signal(type=SignalType.NO_SIGNAL)

        # 範例邏輯：簡單移動平均線交叉
        # ... 計算邏輯 ...
        
        return Signal(
            type=SignalType.LONG,
            product_id=candle.product_id,
            strategy_id=self.id,
            value=50000.0  # 可選：限價單價格 (Limit Price)
        )
```

### 關鍵元件

*   **`requirements`**: 告訴系統您需要什麼數據。系統會在啟動策略前自動檢查並下載 (Backfill) 歷史數據。
*   **`on_candle`**: 核心邏輯層。只有在 K 線確認收盤後才會執行，避免信號閃爍。
*   **`Signal`**: 策略的輸出結果。
    *   `SignalType.LONG` / `SignalType.SHORT`: 開倉。
    *   `SignalType.EXIT_LONG` / `SignalType.EXIT_SHORT`: 平倉。
    *   `SignalType.NO_SIGNAL`: 觀望。

## 2. 系統接口與數據流 (System Interfaces & Data Flow)

FluxTrade 建立在基於 Redis 的發布/訂閱 (Pub/Sub) 架構之上。

### Redis 頻道 (Internal API)

雖然您通常透過 Python 類別進行開發，但了解底層 Redis 頻道有助於進階除錯或開發監控工具。

| 頻道 (Channel) | 方向 | 描述 |
| :--- | :--- | :--- |
| `market_data.BINANCE.BTCUSDT-PERP.1m` | Pub (Rust) -> Sub (Python) | 即時 K 線更新。 |
| `stream.user.updates` | Pub (Rust) -> Sub (Python) | 來自交易所的即時餘額與持倉變動。 |
| `cmd:strategy:control` | Pub (外部) -> Sub (Python) | 發送控制指令給策略引擎 (START, STOP, TEST_RUN)。 |
| `system:events` | Pub (Python) -> Sub (Dashboard) | 系統級事件與日誌。 |

### REST API (交易所適配器)

執行引擎 (`src/core/execution.py`) 封裝了 CCXT，提供統一的下單接口。它具備自動化處理能力：
*   **WebSocket 回退 (Fallback)**: 若即時下單連線失敗，自動切換至 REST API。
*   **安全檢查**: 強制檢查回退時的訂單類型，確保限價單不會變成市價單滑價。

## 3. 運維與生命週期 (Operations & Lifecycle)

### 部署策略
1.  **建立**: 將您的 `.py` 檔案存入 `python-strategy/src/strategies/`。
2.  **發現 (Discovery)**: 系統每分鐘掃描一次該目錄。
    *   狀態變更為: `DISCOVERED`
3.  **回填 (Backfill)**: 系統檢查 Postgres 中是否有足夠歷史數據。若不足，觸發 Rust 服務進行下載。
    *   狀態變更為: `WARNING` (缺數據) -> `READY` (數據就緒)
4.  **啟動 (Activation)**:
    *   透過 Redis 發送 `START` 指令。
    *   狀態變更為: `ACTIVE`

### 儀表板更新
儀表板 (`dashboard/app.py`) 直接讀取 Redis 與 Postgres。
*   **策略狀態**: 從 Postgres 的 `strategy_state` 表讀取 (由 Python 引擎更新)。
*   **即時指標**: 訂閱 Redis 頻道以獲取即時 PnL 更新。

## 4. 進階：客製化 Rust 數據服務

若您需要新增交易所或數據源：
1.  **Connector**: 在 `rust-data-service/src/connector/mod.rs` 實作 `ExchangeConnector` trait。
2.  **標準化**: 確保將原始 JSON 映射至內部的 `Candlestick` 與 `Trade` 結構。
3.  **User Stream**: 實作經過簽章驗證的 WebSocket 訂閱以獲取帳戶更新。
