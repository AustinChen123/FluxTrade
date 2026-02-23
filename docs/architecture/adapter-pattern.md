# Adapter Pattern — Live/Backtest Parity

## Core Principle

FluxTrade's fundamental promise is that **the same Python strategy code runs identically in live trading and backtesting**. The Adapter Pattern is the mechanism that makes this possible.

Strategies interact with exchanges exclusively through the `IExchangeAdapter` interface. They never check which mode they are running in, never import mode-specific code, and never branch on configuration flags. The adapter is injected at engine startup, and the strategy is unaware of which implementation it received.

## IExchangeAdapter Interface

Defined in `src/core/interfaces/exchange.py`:

```python
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

class IExchangeAdapter(ABC):
    """Unified interface for all exchange interactions."""

    @abstractmethod
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        **kwargs,
    ) -> dict:
        """Place an order on the exchange (or simulated matching engine)."""
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> dict:
        """Cancel an open order."""
        ...

    @abstractmethod
    async def get_balance(self) -> dict:
        """Return current account balance."""
        ...

    @abstractmethod
    async def on_market_data(self, candle) -> None:
        """Process incoming market data (used by SimulatedAdapter to tick the engine)."""
        ...
```

Every exchange interaction in the system flows through this interface. The `execution.py` module calls `adapter.place_order()` to convert signals into orders; the engine calls `adapter.on_market_data()` to feed candles; risk checks call `adapter.get_balance()` to verify available funds.

## Adapter Implementations

### SimulatedAdapter (Backtest)

**File**: `src/core/adapters/simulated.py`

The `SimulatedAdapter` delegates all order matching to the Rust `PyMatchingEngine` via PyO3. It maintains no matching logic of its own — every Market, Limit, Stop-Loss, Take-Profit, Trailing Stop, and OCO order is processed by Rust.

```
Strategy Signal
    -> execution.py creates Order
    -> SimulatedAdapter.place_order()
    -> PyMatchingEngine.place_order()  [Rust]
    -> on_market_data() ticks the engine each candle
    -> PyMatchingEngine.process_candle()  [Rust]
    -> fills returned to Python as dicts
    -> SimulatedAdapter converts to Fill objects
```

Key responsibilities:

- **String/Decimal boundary**: Converts Python `Decimal` to `str` when crossing into Rust, and parses `str` back to `Decimal` on return
- **Side conversion**: Translates `LONG/SHORT` (internal convention) to the format expected by the matching engine
- **Balance tracking**: Maintains a simulated account balance, deducting fees and reflecting PnL from fills
- **Fill propagation**: Returns fill results to the execution pipeline in the same format as live fills

!!! warning "No Matching Logic in Python"
    The `SimulatedAdapter` must **never** contain order matching logic. All matching — including SL/TP triggering, trailing stop adjustment, and OCO cancellation — lives exclusively in the Rust `PyMatchingEngine`. This prevents divergence between simulated and live behavior.

### CcxtExchangeAdapter (Live — Generic)

**File**: `src/core/adapters/ccxt_adapter.py`

Wraps the [CCXT](https://github.com/ccxt/ccxt) library to provide a unified interface to 100+ cryptocurrency exchanges. Handles:

- Order placement with exchange-specific parameter mapping
- Balance queries with currency normalization
- Order cancellation with error handling and retry
- Rate limiting via CCXT's built-in throttler

```python
# Simplified flow
async def place_order(self, symbol, side, order_type, quantity, price=None, **kwargs):
    # Convert Decimal -> str for CCXT (not float!)
    params = self._build_ccxt_params(order_type, **kwargs)
    result = await self.exchange.create_order(
        symbol=symbol,
        type=order_type,
        side=side,          # 'buy' or 'sell' at CCXT boundary
        amount=str(quantity),
        price=str(price) if price else None,
        params=params,
    )
    return self._normalize_order_result(result)
```

!!! note "Decimal Discipline"
    The adapter passes `str(quantity)` and `str(price)` to CCXT, never `float()`. This preserves precision across the entire pipeline.

### LiveBinanceAdapter (Live — Binance Optimized)

**File**: `src/core/adapters/live_binance.py`

Extends `CcxtExchangeAdapter` with a WebSocket fast path for Binance-specific features:

- **User Data Stream**: Subscribes to Binance's WebSocket for real-time order updates, balance changes, and position events
- **Faster fills**: Receives fill notifications via WebSocket before REST polling would detect them
- **Keepalive**: Manages the listenKey lifecycle (create, extend every 30min, recreate on disconnect)

Falls back to the parent `CcxtExchangeAdapter` REST methods for any operation not covered by the WebSocket path.

## Factory Function

**File**: `src/core/adapters/__init__.py`

```python
def create_adapter(config) -> IExchangeAdapter:
    """
    Factory that selects the appropriate adapter based on configuration.

    - mode == 'backtest'  -> SimulatedAdapter (Rust matching engine)
    - mode == 'live' and exchange == 'binance' -> LiveBinanceAdapter
    - mode == 'live' -> CcxtExchangeAdapter (generic CCXT)
    """
```

The factory is called once at engine startup. The returned adapter is injected into the engine and used for the entire session. Strategies never call this factory — they receive the adapter through dependency injection.

## Side Naming Convention

FluxTrade uses a dual naming convention for order/position sides:

| Context | Long | Short |
|---------|------|-------|
| Internal (Python models, Rust engine) | `LONG` | `SHORT` |
| Exchange boundary (CCXT API calls) | `buy` | `sell` |

Conversion happens **only** at the adapter boundary:

- `SimulatedAdapter`: Works with `LONG/SHORT` natively (Rust engine uses this convention)
- `CcxtExchangeAdapter`: Converts `LONG` -> `buy` and `SHORT` -> `sell` before calling CCXT

Python uses `PositionSide(str, Enum)` with values `LONG`/`SHORT` and `OrderSide(str, Enum)` with values `BUY`/`SELL`. The `str, Enum` base ensures backward compatibility with string comparisons.

!!! warning "Never Convert in Strategies"
    Strategies must use `LONG`/`SHORT` exclusively. The `buy`/`sell` conversion is the adapter's responsibility. If a strategy references `buy` or `sell` directly, it is violating the abstraction boundary.

## Same Strategy, Both Modes

The following example demonstrates how a strategy runs identically in live and backtest modes. The only difference is which adapter is injected:

```python
# --- Strategy code (unchanged between modes) ---
class GoldenCrossStrategy(BaseStrategy):
    def on_candle(self, candle: Candlestick) -> Optional[Signal]:
        self.update_indicators(candle)
        if self.fast_ma > self.slow_ma and not self.in_position:
            return Signal(
                symbol=candle.symbol,
                side="LONG",
                entry_price=candle.close,
                stop_loss=candle.close - Decimal("50"),
                take_profit=candle.close + Decimal("100"),
            )
        return None

# --- Live mode setup ---
adapter = create_adapter(Config(mode="live", exchange="binance"))
engine = StrategyEngine(adapter=adapter)
engine.add_strategy(GoldenCrossStrategy())
await engine.run()  # Connects to real exchange via LiveBinanceAdapter

# --- Backtest mode setup ---
adapter = create_adapter(Config(mode="backtest"))
engine = StrategyEngine(adapter=adapter)
engine.add_strategy(GoldenCrossStrategy())  # Same strategy instance
runner = BacktestRunner(engine=engine, data_source=CsvDataSource("btc_1h.csv"))
report = await runner.run()  # Uses SimulatedAdapter -> Rust PyMatchingEngine
```

The strategy class is identical. It emits `Signal` objects; the execution pipeline and adapter handle the rest. The strategy never calls exchange APIs directly, never manages SL/TP lifecycle, and never checks whether it is running live or in backtest.

## Design Rules

1. **No exchange logic in strategies**: SL/TP/Trailing management belongs in the matching engine (Rust) or adapter, never in `on_candle()`
2. **Live/backtest parity is non-negotiable**: Any feature added to backtesting must produce behavior identical to real exchange execution
3. **Fees must be reflected**: Both `SimulatedAdapter` (via Rust) and `CcxtExchangeAdapter` (via exchange response) include maker/taker fees in fill results
4. **Signals are the only strategy output**: Strategies emit `Signal` objects with entry, SL, TP, and trailing parameters. The system handles the full order lifecycle from there
