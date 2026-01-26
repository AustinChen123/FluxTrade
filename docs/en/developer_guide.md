# FluxTrade Developer Guide

Welcome to the FluxTrade Developer Guide. This document provides an in-depth look at how to develop custom strategies, understand the system architecture, and interact with the exposed interfaces.

## 1. Strategy Development

FluxTrade uses a "Hot-Pluggable" strategy engine, allowing you to add, modify, or remove strategies without restarting the entire system.

### The `BaseStrategy` Interface

All strategies must inherit from the `BaseStrategy` class located in `python-strategy/src/strategies/base.py`.

```python
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class MyCustomStrategy(BaseStrategy):
    """
    A custom trend-following strategy.
    """

    @property
    def requirements(self) -> StrategyRequirements:
        """
        Define the data requirements for your strategy.
        """
        return StrategyRequirements(
            product_id="BINANCE:BTCUSDT-PERP", # Exchange:Symbol
            timeframe="1m",                    # Candle timeframe (1m, 5m, 1h, etc.)
            lookback_window=50                 # Number of historical candles needed
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        """
        Called every time a new candle is closed.
        
        Args:
            candle: The latest closed candlestick data.
            
        Returns:
            Signal: The trading decision (LONG, SHORT, EXIT, or NO_SIGNAL).
        """
        
        # Access historical data (automatically managed by the engine)
        # self.candles is a deque containing the last `lookback_window` candles
        if len(self.candles) < self.requirements.lookback_window:
            return Signal(type=SignalType.NO_SIGNAL)

        # Example Logic: Simple Moving Average Crossover
        # ... calculation logic ...
        
        return Signal(
            type=SignalType.LONG,
            product_id=candle.product_id,
            strategy_id=self.id,
            value=50000.0  # Optional: Limit Price
        )
```

### Key Components

*   **`requirements`**: This property tells the system what data you need. The system will automatically fetch historical data (Backfill) before starting your strategy.
*   **`on_candle`**: The core logic. It is triggered only when a candle *closes*.
*   **`Signal`**: The output of your strategy.
    *   `SignalType.LONG` / `SignalType.SHORT`: Open a position.
    *   `SignalType.EXIT_LONG` / `SignalType.EXIT_SHORT`: Close a position.
    *   `SignalType.NO_SIGNAL`: Do nothing.

## 2. System Interfaces & Data Flow

FluxTrade is built on a Pub/Sub architecture using Redis.

### Redis Channels (Internal API)

While you typically interact via the Python classes, understanding the Redis channels is useful for advanced debugging or building custom tools.

| Channel | Direction | Description |
| :--- | :--- | :--- |
| `market_data.BINANCE.BTCUSDT-PERP.1m` | Pub (Rust) -> Sub (Python) | Live candlestick updates. |
| `stream.user.updates` | Pub (Rust) -> Sub (Python) | Real-time balance and position updates from the exchange. |
| `cmd:strategy:control` | Pub (External) -> Sub (Python) | Send commands to the strategy engine (START, STOP, TEST_RUN). |
| `system:events` | Pub (Python) -> Sub (Dashboard) | System-wide events and logs. |

### REST API (Exchange Adapter)

The Execution Engine (`src/core/execution.py`) wraps CCXT to provide a unified interface for order execution. It automatically handles:
*   **WebSocket Fallback**: If the real-time order connection fails, it falls back to REST API.
*   **Safety Checks**: Enforces `limit` order types on fallbacks to prevent slippage.

## 3. Operations & Lifecycle

### Deploying a Strategy
1.  **Create**: Save your `.py` file in `python-strategy/src/strategies/`.
2.  **Discovery**: The system automatically scans this directory every minute.
    *   Status: `DISCOVERED`
3.  **Backfill**: The system checks if enough historical data exists in Postgres. If not, it triggers the Rust service to fetch it.
    *   Status: `WARNING` (Data Missing) -> `READY` (Data Loaded)
4.  **Activation**:
    *   Send a `START` command via Redis or use the Dashboard (Future feature).
    *   Status: `ACTIVE`

### Dashboard Updates
The Dashboard (`dashboard/app.py`) reads directly from Redis and Postgres.
*   **Strategy State**: Read from the `strategy_state` table in Postgres (updated by Python Engine).
*   **Live Metrics**: Subscribes to Redis channels for real-time PnL updates.

## 4. Advanced: Customizing Rust Data Service

If you need to add a new exchange or data source:
1.  **Connector**: Implement the `ExchangeConnector` trait in `rust-data-service/src/connector/mod.rs`.
2.  **Normalization**: Ensure you map raw JSON to the internal `Candlestick` and `Trade` structs.
3.  **User Stream**: Implement the authenticated WebSocket subscription for account updates.
