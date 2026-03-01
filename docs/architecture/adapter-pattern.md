# Adapter Pattern -- Live/Backtest Parity

## Core Principle

FluxTrade's fundamental promise is that **the same Python strategy code runs identically in live trading and backtesting**. The Adapter Pattern is the mechanism that makes this possible.

Strategies interact with exchanges exclusively through the `IExchangeAdapter` interface. They never check which mode they are running in, never import mode-specific code, and never branch on configuration flags. The adapter is injected at engine startup, and the strategy is unaware of which implementation it received.

## IExchangeAdapter Interface

Defined in `src/core/interfaces/exchange.py`. The interface is **synchronous** (not async):

```python
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, List, Optional

from src.core.orm_models import Order
from src.core.models import Candlestick, Position


class IExchangeAdapter(ABC):
    """Unified interface for all exchange interactions."""

    @abstractmethod
    def place_order(self, order: Order) -> str:
        """Place an order. Takes an ORM Order object, returns exchange order ID string."""
        ...

    @abstractmethod
    def cancel_order(self, order_id: str, product_id: str) -> bool:
        """Cancel an open order. Returns True if successful."""
        ...

    @abstractmethod
    def get_balance(self, asset: str) -> Decimal:
        """Return available balance for a specific asset as Decimal."""
        ...

    @abstractmethod
    def get_position(self, product_id: str) -> Optional[Position]:
        """Return current open position for a product, or None."""
        ...

    def on_market_data(self, candle: Candlestick) -> List[Dict]:
        """Process market data to check for simulated order fills.

        Override in simulated adapters. Live adapters return empty list.
        Returns list of fill dicts: {order, price, quantity, fee, fill_type}.
        """
        return []
```

Every exchange interaction in the system flows through this interface. The `execution.py` module calls `adapter.place_order()` to convert signals into orders; the engine calls `adapter.on_market_data()` to feed candles; risk checks call `adapter.get_balance()` to verify available funds.

### Exception Hierarchy

The interface defines three exception types in the same module:

- `ExchangeError` -- base exception for all exchange-related errors
- `InsufficientFundsError(ExchangeError)` -- insufficient funds for the order
- `NetworkError(ExchangeError)` -- network connectivity issues

## Signal and Strategy Types

Strategies extend `BaseStrategy` and emit `Signal` objects:

```python
class BaseStrategy(ABC):
    def __init__(self, strategy_id: str, product_id: str):
        self.strategy_id = strategy_id
        self.product_id = product_id
        self.journal = StrategyJournal(strategy_id)

    @property
    @abstractmethod
    def requirements(self) -> StrategyRequirements: ...

    @abstractmethod
    def on_candle(self, candle: Candlestick) -> Signal: ...
```

Signals use the `SignalType` enum:

```python
class SignalType(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"
    NO_SIGNAL = "NO_SIGNAL"
```

Side enums are defined as:

- `OrderSide(str, Enum)` with values `BUY = "buy"` and `SELL = "sell"`
- `PositionSide(str, Enum)` with values `LONG = "LONG"` and `SHORT = "SHORT"`

The `str, Enum` base ensures backward compatibility with string comparisons.

## Adapter Implementations

### SimulatedAdapter (Backtest)

**File**: `src/core/adapters/simulated.py`

The `SimulatedAdapter` delegates all order matching to the Rust `PyMatchingEngine` via PyO3. It maintains no matching logic of its own -- every Market, Limit, Stop-Loss, Take-Profit, Trailing Stop, and OCO order is processed by Rust.

```
Strategy Signal
    -> execution.py creates Order
    -> SimulatedAdapter.place_order()
    -> PyMatchingEngine.submit_order()  [Rust]
    -> on_market_data() ticks the engine each candle
    -> PyMatchingEngine.on_candle()  [Rust]
    -> fills returned to Python as FillEvent objects
    -> SimulatedAdapter converts to fill dicts
```

Key responsibilities:

- **String/Decimal boundary**: Converts Python `Decimal` to `str` when crossing into Rust, and parses `str` back to `Decimal` on return
- **Side conversion**: Translates `buy/sell` (ORM OrderSide) to `LONG/SHORT` (Rust PositionSide) via `_side_to_rust()`. For conditional orders (SL/TP/Trailing), the side is inverted because Rust expects the position side being protected
- **Balance tracking**: Balance is managed entirely by the Rust engine; `get_balance()` reads `self._engine.balance`
- **Position lookup**: Uses composite keys `strategy_id:product_id` from the Rust engine's position HashMap, with fallback to product_id-only keys for backward compatibility
- **Fill propagation**: Returns fill results as dicts compatible with `ExecutionEngine`: `{"order": ORM Order, "price": Decimal, "quantity": Decimal, "fee": Decimal, "fill_type": str}`
- **OCO cleanup**: After fills, syncs `_order_map` by removing orders that Rust cancelled (e.g., OCO counterparts)

!!! warning "No Matching Logic in Python"
    The `SimulatedAdapter` must **never** contain order matching logic. All matching -- including SL/TP triggering, trailing stop adjustment, and OCO cancellation -- lives exclusively in the Rust `PyMatchingEngine`. This prevents divergence between simulated and live behavior.

### CcxtExchangeAdapter (Live -- Generic)

**File**: `src/core/adapters/ccxt_adapter.py`

Wraps the [CCXT](https://github.com/ccxt/ccxt) library to provide a unified interface to 100+ cryptocurrency exchanges. Handles:

- Order placement via `self.client.create_order()` with exchange-specific parameter mapping
- Balance queries via `self.client.fetch_balance()` returning `Decimal(str(free.get(asset, 0)))`
- Position queries via `self.client.fetch_positions()` with CCXT symbol conversion
- Order cancellation with `OrderNotFound` handling
- Rate limiting via CCXT's built-in throttler

```python
def place_order(self, order: Order) -> str:
    ccxt_symbol = to_ccxt_symbol(order.product_id)
    response = self.client.create_order(
        symbol=ccxt_symbol,
        type=order.type,
        side=order.side,          # 'buy' or 'sell' at CCXT boundary
        amount=str(order.quantity),
        price=str(order.price) if order.price else None,
        params=params,
    )
    return str(response["id"])
```

Constructor parameters:

```python
CcxtExchangeAdapter(
    exchange_id: str,          # CCXT exchange name (e.g., "binance", "bybit")
    api_key: str | None,       # Falls back to EXCHANGE_API_KEY env var
    secret: str | None,        # Falls back to EXCHANGE_SECRET env var
    testnet: bool = False,
    extra_config: dict | None = None,
)
```

!!! note "Decimal Discipline"
    The adapter passes `str(order.quantity)` and `str(order.price)` to CCXT, never `float()`. This preserves precision across the entire pipeline.

### LiveBinanceAdapter (Live -- Binance Optimized)

**File**: `src/core/adapters/live_binance.py`

Extends `CcxtExchangeAdapter` with a WebSocket fast path for market orders:

- Attempts WebSocket order entry for market orders via `WebSocketOrderConnector`
- Falls back to REST (parent class) if WebSocket is unavailable or the order fails
- Only activates when `enable_ws=True` and the WebSocket connection succeeds

```python
class LiveBinanceAdapter(CcxtExchangeAdapter):
    def __init__(
        self,
        api_key: str | None = None,
        secret: str | None = None,
        testnet: bool = True,
        enable_ws: bool = True,
    ): ...

    def place_order(self, order: Order) -> str:
        # Try WS fast path for market orders
        if self.ws_connector and order.type.lower() == "market":
            # ... attempt WebSocket order
        # REST fallback (parent class)
        return super().place_order(order)
```

All other methods (`cancel_order`, `get_balance`, `get_position`) are inherited from `CcxtExchangeAdapter`.

## Factory Function

**File**: `src/core/adapters/__init__.py`

```python
def create_adapter(config: dict) -> IExchangeAdapter:
    """
    Factory that selects the appropriate adapter based on configuration.

    Config keys:
        mode: "simulated" | "live"  (default: "simulated")
        exchange: CCXT exchange id  (required for live, default: "binance")
        api_key / secret: optional, falls back to env vars
        testnet: bool (default: True)
        balance: initial balance (simulated only, default: 100000)
        maker_fee / taker_fee: fee rates (simulated only, default: 0)
        enable_ws: bool (live only, default: False)
        extra_config: dict (extra CCXT config, optional)

    Selection logic:
    - mode == "simulated" -> SimulatedAdapter(balance, maker_fee, taker_fee)
    - mode == "live" and exchange == "binance" and enable_ws == True -> LiveBinanceAdapter
    - mode == "live" -> CcxtExchangeAdapter (generic CCXT)
    """
```

The factory is called once at engine startup. The returned adapter is injected into the engine and used for the entire session. Strategies never call this factory -- they receive the adapter through dependency injection.

## Side Naming Convention

FluxTrade uses a dual naming convention for order/position sides:

| Context | Long | Short |
|---------|------|-------|
| Internal (Python models, Rust engine) | `LONG` | `SHORT` |
| Exchange boundary (CCXT API calls) | `buy` | `sell` |

Side enums:

- `OrderSide(str, Enum)`: `BUY = "buy"`, `SELL = "sell"`
- `PositionSide(str, Enum)`: `LONG = "LONG"`, `SHORT = "SHORT"`

Conversion happens **only** at the adapter boundary:

- `SimulatedAdapter._side_to_rust()`: Converts `buy` -> `LONG` and `sell` -> `SHORT` before calling Rust
- `CcxtExchangeAdapter`: The ORM `Order` already has `side` as `buy`/`sell` (matching CCXT's expectation), so no conversion is needed

!!! warning "Never Convert in Strategies"
    Strategies emit `Signal` objects with `SignalType.LONG`/`SignalType.SHORT`. The execution pipeline and adapters handle all side conversions. If a strategy references `buy` or `sell` directly, it is violating the abstraction boundary.

## Same Strategy, Both Modes

The following example demonstrates how a strategy runs identically in live and backtest modes. The only difference is which adapter is injected:

```python
# --- Strategy code (unchanged between modes) ---
class GoldenCrossStrategy(BaseStrategy):
    def __init__(self):
        super().__init__(strategy_id="golden_cross", product_id="BINANCE:BTCUSDT-PERP")

    def on_candle(self, candle: Candlestick) -> Signal:
        self.update_indicators(candle)
        if self.fast_ma > self.slow_ma and not self.in_position:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=candle.product_id,
                timeframe=candle.timeframe,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                stop_loss=candle.close - Decimal("50"),
                take_profit=candle.close + Decimal("100"),
            )
        return Signal(
            strategy_id=self.strategy_id,
            product_id=candle.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )

# --- Backtest mode (most common) ---
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources import CsvDataSource

runner = BacktestRunner(
    start_time=1704067200000,   # 2024-01-01 UTC ms
    end_time=1706745600000,     # 2024-02-01 UTC ms
    product_id="BINANCE:BTC-PERP",
    timeframe="1h",
    initial_balance=100000.0,
    data_source=CsvDataSource("btc_1h.csv", product_id="BINANCE:BTC-PERP", timeframe="1h"),
    fee_config={"maker": 0.0002, "taker": 0.0004},
)
strategy = GoldenCrossStrategy("golden_cross_1", "BINANCE:BTC-PERP")
runner.add_strategy(strategy)
result = runner.run()  # Internally creates SimulatedAdapter -> Rust PyMatchingEngine

# --- Live mode (advanced) ---
# StrategyEngine requires db_session, clock, adapter, order_repository, etc.
# See the Live Trading Guide for full setup.
```

The strategy class is identical. It emits `Signal` objects; the execution pipeline and adapter handle the rest. The strategy never calls exchange APIs directly, never manages SL/TP lifecycle, and never checks whether it is running live or in backtest.

## Design Rules

1. **No exchange logic in strategies**: SL/TP/Trailing management belongs in the matching engine (Rust) or adapter, never in `on_candle()`
2. **Live/backtest parity is non-negotiable**: Any feature added to backtesting must produce behavior identical to real exchange execution
3. **Fees must be reflected**: Both `SimulatedAdapter` (via Rust) and `CcxtExchangeAdapter` (via exchange response) include maker/taker fees in fill results
4. **Signals are the only strategy output**: Strategies emit `Signal` objects with entry, SL, TP, and trailing parameters. The system handles the full order lifecycle from there
