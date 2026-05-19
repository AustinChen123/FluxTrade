# FluxTrade

**High-performance crypto trading system where Live Trading = Backtesting.**

## Core Promise

The same Python strategy code runs identically in both live trading and backtesting — no modifications needed. This is achieved through:

- **Adapter Pattern**: `IExchangeAdapter` interface isolates live/simulated execution
- **Rust Matching Engine**: `PyMatchingEngine` (via PyO3) handles all order matching with bar-by-bar replay
- **Signal-Based Architecture**: Strategies emit Signals; the system handles the entire order lifecycle

## Quick Links

- [Quick Start](getting-started/quickstart.md) — Run your first backtest in 5 minutes
- [Writing Strategies](guide/writing-strategies.md) — Create custom strategies
- [External Signals](guide/external-signals.md) — Integrate ML models and external signal sources
- [Architecture](architecture/overview.md) — Understand the system design

## System Architecture

```
Exchange WebSocket → [Rust Data Service] → Redis Pub/Sub → [Python Strategy] → Exchange API
                                                                ↓
                                                        [PostgreSQL]
                                                                ↓
                                                        [Streamlit Dashboard]
```

## Features

- **Bar-by-bar backtesting** with Rust-powered matching engine (~89K candles/sec)
- **Multi-strategy management** with capital allocation and per-strategy risk
- **Order types**: Market, Limit, Stop Loss, Take Profit, Trailing Stop, OCO
- **External signal integration**: `CallableStrategy` for ML models, `CsvSignalStrategy` for signal replay
- **Prometheus metrics** and Grafana dashboards
- **Structured logging** with trace_id correlation
