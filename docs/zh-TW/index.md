# FluxTrade

**高效能加密貨幣交易系統——實盤交易 = 回測。**

## 核心承諾

相同的 Python 策略程式碼在實盤交易和回測中完全一致地運行——無需任何修改。這是透過以下機制實現的：

- **適配器模式 (Adapter Pattern)**：`IExchangeAdapter` 介面隔離了實盤與模擬執行的差異
- **Rust 搓合引擎 (Matching Engine)**：`PyMatchingEngine`（透過 PyO3）處理所有訂單搓合，支援逐根回放
- **訊號驅動架構 (Signal-Based Architecture)**：策略只需發出 Signal；系統負責完整的訂單生命週期管理

## 快速連結

- [快速開始](getting-started/quickstart.md) — 5 分鐘內執行你的第一次回測
- [撰寫策略](guide/writing-strategies.md) — 建立自訂策略
- [外部訊號](guide/external-signals.md) — 整合 ML 模型與外部訊號來源
- [架構總覽](architecture/overview.md) — 了解系統設計

## 系統架構

```
Exchange WebSocket → [Rust Data Service] → Redis Pub/Sub → [Python Strategy] → Exchange API
                                                                ↓
                                                        [PostgreSQL]
                                                                ↓
                                                        [Streamlit Dashboard]
```

## 功能特色

- **逐根回測 (Bar-by-bar backtesting)**，搭配 Rust 驅動的搓合引擎（約 89K 根/秒）
- **多策略管理**，支援資金分配與個別策略風險控制
- **訂單類型**：Market、Limit、Stop Loss、Take Profit、Trailing Stop、OCO
- **外部訊號整合**：`CallableStrategy` 用於 ML 模型，`CsvSignalStrategy` 用於訊號回放
- **Prometheus 指標**與 Grafana 儀表板
- **結構化日誌 (Structured logging)**，支援 trace_id 關聯追蹤
