"""Integration tests for multi-strategy management using the real Rust matching engine.

Tests verify strategy_id flows correctly across the Python<->Rust boundary,
including independent position tracking per strategy, shared balance pool,
and correct fill event attribution.

Requires: compiled fluxtrade_core.so in src/
"""
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.models import Signal, SignalType, Candlestick
from src.core.adapters.simulated import SimulatedAdapter
from src.core.capital_allocator import CapitalAllocator
from src.strategies.base import BaseStrategy, StrategyRequirements
from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle_series

# Skip entire module if Rust .so is not available
try:
    from fluxtrade_core import PyMatchingEngine, Order as RustOrder, Candlestick as RustCandlestick
    HAS_RUST = True
except ImportError:
    HAS_RUST = False

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rust,
    pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def make_rust_order(
    order_id: str,
    side: str,
    order_type: str = "MARKET",
    price: str = "0",
    quantity: str = "0.1",
    strategy_id: str = "",
    product_id: str = PRODUCT_ID,
    trigger_price: str | None = None,
    trailing_distance: str | None = None,
    linked_order_id: str | None = None,
) -> "RustOrder":
    return RustOrder(
        id=order_id,
        product_id=product_id,
        side=side,
        order_type=order_type,
        price=price,
        quantity=quantity,
        timestamp=1_700_000_000_000,
        trigger_price=trigger_price,
        trailing_distance=trailing_distance,
        linked_order_id=linked_order_id,
        strategy_id=strategy_id,
    )


def make_rust_candle(
    open_: str = "50000",
    high: str = "50100",
    low: str = "49900",
    close: str = "50050",
    ts: int = 1_700_000_900_000,
    product_id: str = PRODUCT_ID,
) -> "RustCandlestick":
    return RustCandlestick(
        product_id=product_id,
        timeframe=TIMEFRAME,
        timestamp=ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume="100",
    )


def make_adapter(balance: str = "10000") -> SimulatedAdapter:
    """Create SimulatedAdapter with real PyMatchingEngine and zero fees."""
    return SimulatedAdapter(
        initial_balance=Decimal(balance),
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0"),
    )


def make_orm_order(
    order_id: str,
    strategy_id: str,
    side: str = "buy",
    order_type: str = "market",
    price: Decimal | None = None,
    quantity: Decimal = Decimal("0.1"),
    product_id: str = PRODUCT_ID,
    trigger_price: Decimal | None = None,
):
    """Create an ORM Order for use with SimulatedAdapter.place_order()."""
    from src.core.orm_models import Order
    return Order(
        id=order_id,
        exchange_order_id=None,
        strategy_id=strategy_id,
        product_id=product_id,
        exchange_id="BINANCE",
        type=order_type,
        side=side,
        price=price or Decimal("50000"),
        trigger_price=trigger_price,
        quantity=quantity,
        status="new",
        timestamp=1_700_000_000_000,
    )


# ---------------------------------------------------------------------------
# Test strategy classes for BacktestRunner integration
# ---------------------------------------------------------------------------
class StrategyA(BaseStrategy):
    """Goes LONG on candle 5, exits on candle 10."""

    def __init__(self, strategy_id: str = "strategy_a", product_id: str = PRODUCT_ID):
        super().__init__(strategy_id, product_id)
        self._count = 0

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=TIMEFRAME,
            lookback_window=3,
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        self._count += 1
        if self._count == 5:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=self.product_id,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                value=candle.close,
                quantity=Decimal("0.01"),
            )
        elif self._count == 10:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=self.product_id,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                value=candle.close,
                quantity=Decimal("0.01"),
            )
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
            value=candle.close,
        )


class StrategyB(BaseStrategy):
    """Goes SHORT on candle 7, exits on candle 14."""

    def __init__(self, strategy_id: str = "strategy_b", product_id: str = PRODUCT_ID):
        super().__init__(strategy_id, product_id)
        self._count = 0

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=TIMEFRAME,
            lookback_window=3,
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        self._count += 1
        if self._count == 7:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=self.product_id,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.SHORT,
                value=candle.close,
                quantity=Decimal("0.01"),
            )
        elif self._count == 14:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=self.product_id,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_SHORT,
                value=candle.close,
                quantity=Decimal("0.01"),
            )
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
            value=candle.close,
        )


# ===========================================================================
# Test 1: Two strategies, same product, independent positions
# ===========================================================================
class TestIndependentPositions:
    """Verify that two strategies can hold opposite positions on the same product."""

    def test_rust_engine_two_strategies_opposite_positions(self):
        """Directly exercise PyMatchingEngine: strategy A LONG, strategy B SHORT."""
        engine = PyMatchingEngine("10000", "0", "0")

        # Strategy A goes LONG
        order_a = make_rust_order("a1", "LONG", strategy_id="strategy_a")
        engine.submit_order(order_a)

        # Strategy B goes SHORT
        order_b = make_rust_order("b1", "SHORT", strategy_id="strategy_b")
        engine.submit_order(order_b)

        candle = make_rust_candle(open_="50000", high="50100", low="49900", close="50050")
        fills = engine.on_candle(candle)

        assert len(fills) == 2
        assert fills[0].strategy_id == "strategy_a"
        assert fills[1].strategy_id == "strategy_b"

        # Verify independent positions via composite keys
        positions = engine.positions
        key_a = "strategy_a:" + PRODUCT_ID
        key_b = "strategy_b:" + PRODUCT_ID
        assert key_a in positions
        assert key_b in positions

        pos_a = positions[key_a]
        assert pos_a.side == "LONG"
        assert Decimal(pos_a.quantity) == Decimal("0.1")

        pos_b = positions[key_b]
        assert pos_b.side == "SHORT"
        assert Decimal(pos_b.quantity) == Decimal("0.1")

    def test_simulated_adapter_two_strategies_independent_positions(self):
        """Via SimulatedAdapter: strategy A LONG, strategy B SHORT on same product."""
        adapter = make_adapter("100000")

        # Strategy A places a BUY (LONG)
        order_a = make_orm_order("ORD-A1", "strategy_a", side="buy", quantity=Decimal("0.1"))
        adapter.place_order(order_a)

        # Strategy B places a SELL (SHORT)
        order_b = make_orm_order("ORD-B1", "strategy_b", side="sell", quantity=Decimal("0.05"))
        adapter.place_order(order_b)

        # Feed candle to trigger fills
        candle = Candlestick(
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            timestamp=1_700_000_900_000,
            open=Decimal("50000"),
            high=Decimal("50100"),
            low=Decimal("49900"),
            close=Decimal("50050"),
            volume=Decimal("100"),
        )
        fills = adapter.on_market_data(candle)
        assert len(fills) == 2

        # Check positions via adapter.get_position
        pos_a = adapter.get_position(PRODUCT_ID, strategy_id="strategy_a")
        assert pos_a is not None
        assert pos_a.side == "LONG"
        assert pos_a.quantity == Decimal("0.1")

        pos_b = adapter.get_position(PRODUCT_ID, strategy_id="strategy_b")
        assert pos_b is not None
        assert pos_b.side == "SHORT"
        assert pos_b.quantity == Decimal("0.05")


# ===========================================================================
# Test 2: Closing one strategy's position doesn't affect the other
# ===========================================================================
class TestPositionIsolation:
    """Closing one strategy's position must not impact another strategy."""

    def test_close_strategy_a_preserves_strategy_b(self):
        """Close strategy A via opposite market order; strategy B remains unchanged."""
        engine = PyMatchingEngine("100000", "0", "0")

        # Both strategies open LONG positions
        engine.submit_order(make_rust_order("a1", "LONG", quantity="1", strategy_id="strategy_a"))
        engine.submit_order(make_rust_order("b1", "LONG", quantity="2", strategy_id="strategy_b"))

        c1 = make_rust_candle(open_="50000", high="50100", low="49900", close="50050")
        engine.on_candle(c1)

        key_a = "strategy_a:" + PRODUCT_ID
        key_b = "strategy_b:" + PRODUCT_ID
        assert key_a in engine.positions
        assert key_b in engine.positions

        # Strategy A closes with a SHORT market order (quantity matches)
        engine.submit_order(make_rust_order(
            "a_close", "SHORT", quantity="1", strategy_id="strategy_a",
        ))
        c2 = make_rust_candle(
            open_="50200", high="50300", low="50100", close="50250",
            ts=1_700_001_800_000,
        )
        fills = engine.on_candle(c2)

        assert len(fills) == 1
        assert fills[0].order_id == "a_close"
        assert fills[0].strategy_id == "strategy_a"

        # Strategy A position is closed
        assert key_a not in engine.positions

        # Strategy B position is untouched
        pos_b = engine.positions[key_b]
        assert pos_b.side == "LONG"
        assert Decimal(pos_b.quantity) == Decimal("2")
        assert Decimal(pos_b.entry_price) == Decimal("50000")

    def test_close_via_stop_loss_preserves_other(self):
        """Close strategy A via SL; strategy B remains unchanged."""
        engine = PyMatchingEngine("100000", "0", "0")

        # Open positions
        engine.submit_order(make_rust_order("a1", "LONG", quantity="1", strategy_id="strategy_a"))
        engine.submit_order(make_rust_order("b1", "LONG", quantity="1", strategy_id="strategy_b"))

        c1 = make_rust_candle(open_="50000", high="50100", low="49900", close="50050")
        engine.on_candle(c1)

        # Strategy A places SL
        sl = make_rust_order(
            "a_sl", "LONG", order_type="STOP_LOSS", quantity="1",
            strategy_id="strategy_a", trigger_price="49000",
        )
        engine.submit_order(sl)

        # Candle triggers SL
        c2 = make_rust_candle(
            open_="49500", high="49600", low="48500", close="48800",
            ts=1_700_001_800_000,
        )
        fills = engine.on_candle(c2)

        sl_fills = [f for f in fills if f.order_id == "a_sl"]
        assert len(sl_fills) == 1
        assert sl_fills[0].strategy_id == "strategy_a"

        key_a = "strategy_a:" + PRODUCT_ID
        key_b = "strategy_b:" + PRODUCT_ID
        assert key_a not in engine.positions

        pos_b = engine.positions[key_b]
        assert pos_b.side == "LONG"
        assert Decimal(pos_b.quantity) == Decimal("1")


# ===========================================================================
# Test 3: Shared balance — both strategies deduct from the same pool
# ===========================================================================
class TestSharedBalance:
    """Verify that all strategies share one balance pool."""

    def test_both_strategies_deduct_fees_from_shared_balance(self):
        """Opening positions from two strategies deducts fees from the same balance."""
        engine = PyMatchingEngine("10000", "0", "0.001")  # 0.1% taker

        initial = Decimal("10000")

        # Strategy A opens
        engine.submit_order(make_rust_order(
            "a1", "LONG", quantity="0.1", strategy_id="strategy_a",
        ))
        c1 = make_rust_candle(open_="50000", high="50100", low="49900", close="50050")
        fills_a = engine.on_candle(c1)
        assert len(fills_a) == 1

        balance_after_a = Decimal(engine.balance)
        fee_a = Decimal(fills_a[0].fee)
        assert fee_a == Decimal("50000") * Decimal("0.1") * Decimal("0.001")
        assert balance_after_a == initial - fee_a

        # Strategy B opens
        engine.submit_order(make_rust_order(
            "b1", "SHORT", quantity="0.2", strategy_id="strategy_b",
        ))
        c2 = make_rust_candle(
            open_="50100", high="50200", low="50000", close="50150",
            ts=1_700_001_800_000,
        )
        fills_b = engine.on_candle(c2)
        assert len(fills_b) == 1

        balance_after_b = Decimal(engine.balance)
        fee_b = Decimal(fills_b[0].fee)
        assert fee_b == Decimal("50100") * Decimal("0.2") * Decimal("0.001")

        # Total balance deduction = fee_a + fee_b from the SAME pool
        assert balance_after_b == initial - fee_a - fee_b

    def test_pnl_from_both_strategies_affects_shared_balance(self):
        """Realized PnL from closing both strategies hits the same balance."""
        engine = PyMatchingEngine("100000", "0", "0")

        # Strategy A: LONG 1 BTC @ 50000
        engine.submit_order(make_rust_order("a1", "LONG", quantity="1", strategy_id="strategy_a"))
        # Strategy B: SHORT 1 BTC @ 50000
        engine.submit_order(make_rust_order("b1", "SHORT", quantity="1", strategy_id="strategy_b"))

        c1 = make_rust_candle(open_="50000", high="50100", low="49900", close="50050")
        engine.on_candle(c1)

        initial_balance = Decimal(engine.balance)
        assert initial_balance == Decimal("100000")

        # Close A via opposite order at 52000 -> PnL +2000
        engine.submit_order(make_rust_order(
            "a_close", "SHORT", quantity="1", strategy_id="strategy_a",
        ))
        c2 = make_rust_candle(
            open_="52000", high="52100", low="51900", close="52050",
            ts=1_700_001_800_000,
        )
        engine.on_candle(c2)

        # A closed at 52000, entry 50000 -> +2000
        balance_after_a_close = Decimal(engine.balance)
        assert balance_after_a_close == Decimal("102000")

        # Close B via opposite order at 52000 -> PnL -2000 (short, price went up)
        engine.submit_order(make_rust_order(
            "b_close", "LONG", quantity="1", strategy_id="strategy_b",
        ))
        c3 = make_rust_candle(
            open_="52000", high="52100", low="51900", close="52050",
            ts=1_700_002_700_000,
        )
        engine.on_candle(c3)

        # B closed at 52000, entry 50000 -> -2000
        final_balance = Decimal(engine.balance)
        assert final_balance == Decimal("100000")  # net zero


# ===========================================================================
# Test 4: CapitalAllocator integration
# ===========================================================================
class TestCapitalAllocator:
    """Test CapitalAllocator with per-strategy allocation and usage tracking."""

    def test_allocate_and_check_available(self):
        """Basic allocation and availability checking."""
        allocator = CapitalAllocator(Decimal("10000"))

        allocator.allocate("strategy_a", Decimal("6000"))
        allocator.allocate("strategy_b", Decimal("3000"))

        assert allocator.get_available("strategy_a") == Decimal("6000")
        assert allocator.get_available("strategy_b") == Decimal("3000")
        assert allocator.get_unallocated() == Decimal("1000")

    def test_usage_tracking_deducts_from_available(self):
        """Recording usage reduces available capital."""
        allocator = CapitalAllocator(Decimal("10000"))

        allocator.allocate("strategy_a", Decimal("5000"))
        allocator.allocate("strategy_b", Decimal("5000"))

        allocator.record_usage("strategy_a", Decimal("3000"))
        assert allocator.get_available("strategy_a") == Decimal("2000")

        allocator.record_usage("strategy_b", Decimal("1000"))
        assert allocator.get_available("strategy_b") == Decimal("4000")

    def test_over_allocation_raises(self):
        """Cannot allocate more than total balance."""
        allocator = CapitalAllocator(Decimal("10000"))
        allocator.allocate("strategy_a", Decimal("8000"))

        with pytest.raises(ValueError, match="only.*unallocated"):
            allocator.allocate("strategy_b", Decimal("3000"))

    def test_over_usage_raises(self):
        """Cannot use more than allocated."""
        allocator = CapitalAllocator(Decimal("10000"))
        allocator.allocate("strategy_a", Decimal("5000"))

        with pytest.raises(ValueError, match="only.*available"):
            allocator.record_usage("strategy_a", Decimal("6000"))

    def test_release_and_deallocate(self):
        """Release usage then deallocate capital back to pool."""
        allocator = CapitalAllocator(Decimal("10000"))
        allocator.allocate("strategy_a", Decimal("5000"))
        allocator.record_usage("strategy_a", Decimal("3000"))

        allocator.release_usage("strategy_a", Decimal("3000"))
        assert allocator.get_available("strategy_a") == Decimal("5000")

        returned = allocator.deallocate("strategy_a")
        assert returned == Decimal("5000")
        assert allocator.get_unallocated() == Decimal("10000")


# ===========================================================================
# Test 5: Full pipeline — BacktestRunner with two strategies
# ===========================================================================
class TestBacktestMultiStrategy:
    """End-to-end backtest with two strategies and the real Rust engine."""

    @patch("src.core.backtest_runner.SessionLocal")
    def test_backtest_two_strategies_run_to_completion(self, mock_session_local):
        """BacktestRunner with two strategies completes and returns result."""
        from src.core.backtest_runner import BacktestRunner
        from src.core.data_sources.memory import MemoryDataSource

        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        mock_session_local.return_value = mock_session

        candle_data = make_candle_series(count=50)

        ds = MemoryDataSource()
        ds.add_candles(candle_data)

        runner = BacktestRunner(
            start_time=candle_data[0].timestamp,
            end_time=candle_data[-1].timestamp,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            initial_balance=10000.0,
            data_source=ds,
            fee_config={"maker": 0.0002, "taker": 0.0006},
            report_config={
                "csv_trades": False,
                "equity_curve": False,
                "markdown_report": False,
                "journal": False,
            },
        )

        strategy_a = StrategyA()
        strategy_b = StrategyB()
        runner.add_strategy(strategy_a)
        runner.add_strategy(strategy_b)

        result = runner.run()

        assert result is not None
        assert "total_pnl" in result
        assert "journal_count" in result
        # Journal should have events from both strategies
        assert result["journal_count"] > 0

    @patch("src.core.backtest_runner.SessionLocal")
    def test_backtest_journal_captures_both_strategies(self, mock_session_local):
        """Journal entries should contain events from both strategies."""
        from src.core.backtest_runner import BacktestRunner
        from src.core.data_sources.memory import MemoryDataSource

        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        mock_session_local.return_value = mock_session

        candle_data = make_candle_series(count=50)

        ds = MemoryDataSource()
        ds.add_candles(candle_data)

        runner = BacktestRunner(
            start_time=candle_data[0].timestamp,
            end_time=candle_data[-1].timestamp,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            initial_balance=10000.0,
            data_source=ds,
            fee_config={"maker": 0, "taker": 0},
            report_config={
                "csv_trades": False,
                "equity_curve": False,
                "markdown_report": False,
                "journal": False,
            },
        )

        runner.add_strategy(StrategyA())
        runner.add_strategy(StrategyB())

        result = runner.run()

        journal_entries = result.get("journal", [])
        # With 50 candles: StrategyA enters@5, exits@10; StrategyB enters@7, exits@14
        # That means at least 4 entry events + fill events
        entry_events = [e for e in journal_entries if e.get("tag") == "entry"]
        assert len(entry_events) >= 2, f"Expected >=2 entry events, got {len(entry_events)}"


# ===========================================================================
# Test 6: strategy_id propagation through FillEvent
# ===========================================================================
class TestStrategyIdPropagation:
    """Verify strategy_id is correctly propagated in FillEvent from Rust."""

    def test_fill_event_carries_strategy_id(self):
        """FillEvent returned by on_candle must contain the correct strategy_id."""
        engine = PyMatchingEngine("10000", "0", "0")

        engine.submit_order(make_rust_order(
            "ord_alpha", "LONG", quantity="0.1", strategy_id="alpha",
        ))
        engine.submit_order(make_rust_order(
            "ord_beta", "SHORT", quantity="0.2", strategy_id="beta",
        ))

        candle = make_rust_candle()
        fills = engine.on_candle(candle)

        assert len(fills) == 2
        fill_map = {f.order_id: f for f in fills}

        assert fill_map["ord_alpha"].strategy_id == "alpha"
        assert fill_map["ord_beta"].strategy_id == "beta"

    def test_get_position_by_strategy_id(self):
        """PyMatchingEngine.get_position(strategy_id, product_id) returns correct position."""
        engine = PyMatchingEngine("10000", "0", "0")

        engine.submit_order(make_rust_order(
            "a1", "LONG", quantity="0.5", strategy_id="strat_x",
        ))
        engine.submit_order(make_rust_order(
            "b1", "SHORT", quantity="0.3", strategy_id="strat_y",
        ))

        candle = make_rust_candle()
        engine.on_candle(candle)

        pos_x = engine.get_position("strat_x", PRODUCT_ID)
        assert pos_x is not None
        assert pos_x.side == "LONG"
        assert Decimal(pos_x.quantity) == Decimal("0.5")
        assert pos_x.strategy_id == "strat_x"

        pos_y = engine.get_position("strat_y", PRODUCT_ID)
        assert pos_y is not None
        assert pos_y.side == "SHORT"
        assert Decimal(pos_y.quantity) == Decimal("0.3")
        assert pos_y.strategy_id == "strat_y"

        # Non-existent strategy returns None
        assert engine.get_position("nonexistent", PRODUCT_ID) is None


# ===========================================================================
# Test 7: SimulatedAdapter strategy-aware position lookup
# ===========================================================================
class TestSimulatedAdapterStrategyAware:
    """Verify SimulatedAdapter correctly passes strategy_id to/from Rust."""

    def test_adapter_get_position_with_strategy_id(self):
        """get_position(product_id, strategy_id=...) returns correct position."""
        adapter = make_adapter("100000")

        # Strategy A buys
        order_a = make_orm_order("ORD-A", "strat_1", side="buy", quantity=Decimal("1"))
        adapter.place_order(order_a)

        # Strategy B sells
        order_b = make_orm_order("ORD-B", "strat_2", side="sell", quantity=Decimal("0.5"))
        adapter.place_order(order_b)

        candle = Candlestick(
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            timestamp=1_700_000_900_000,
            open=Decimal("50000"),
            high=Decimal("50100"),
            low=Decimal("49900"),
            close=Decimal("50050"),
            volume=Decimal("100"),
        )
        adapter.on_market_data(candle)

        pos_1 = adapter.get_position(PRODUCT_ID, strategy_id="strat_1")
        assert pos_1 is not None
        assert pos_1.side == "LONG"
        assert pos_1.quantity == Decimal("1")
        assert pos_1.strategy_id == "strat_1"

        pos_2 = adapter.get_position(PRODUCT_ID, strategy_id="strat_2")
        assert pos_2 is not None
        assert pos_2.side == "SHORT"
        assert pos_2.quantity == Decimal("0.5")
        assert pos_2.strategy_id == "strat_2"

    def test_adapter_fills_contain_correct_order_mapping(self):
        """on_market_data fills should map back to the correct ORM orders."""
        adapter = make_adapter("100000")

        order_a = make_orm_order("ORD-X", "alpha", side="buy", quantity=Decimal("0.1"))
        order_b = make_orm_order("ORD-Y", "beta", side="sell", quantity=Decimal("0.2"))

        adapter.place_order(order_a)
        adapter.place_order(order_b)

        candle = Candlestick(
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            timestamp=1_700_000_900_000,
            open=Decimal("50000"),
            high=Decimal("50100"),
            low=Decimal("49900"),
            close=Decimal("50050"),
            volume=Decimal("100"),
        )
        fills = adapter.on_market_data(candle)

        assert len(fills) == 2
        fill_orders = {f["order"].id: f for f in fills}
        assert "ORD-X" in fill_orders
        assert "ORD-Y" in fill_orders
        assert fill_orders["ORD-X"]["order"].strategy_id == "alpha"
        assert fill_orders["ORD-Y"]["order"].strategy_id == "beta"
