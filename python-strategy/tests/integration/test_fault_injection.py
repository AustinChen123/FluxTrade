"""Integration: Fault injection tests for system resilience.

Tests: partial fills, adapter failures, invalid data, duplicate signals.
Uses MockExchangeAdapter / RealisticMockAdapter (no Rust .so required).
"""
import pytest
from decimal import Decimal
from unittest.mock import MagicMock

from sqlalchemy.orm import Session
from src.core.models import Signal, SignalType
from src.core.execution import ExecutionEngine
from src.core.journal import StrategyJournal
from conftest import (
    MockExchangeAdapter,
    RealisticMockAdapter,
    MockOrderRepository,
    MockClock,
)
from integration.conftest import PRODUCT_ID, TIMEFRAME, INITIAL_BALANCE


@pytest.fixture
def clock():
    return MockClock(initial_time=1_700_000_000)


@pytest.fixture
def order_repo():
    return MockOrderRepository()


@pytest.fixture
def journal():
    return StrategyJournal("fault-test")


def _make_engine(clock, adapter, order_repo, journal):
    db_session = MagicMock(spec=Session)
    return ExecutionEngine(
        db_session=db_session,
        clock=clock,
        adapter=adapter,
        order_repository=order_repo,
        journal=journal,
    )


def _make_signal(ts: int = 1_700_000_000_000, sig_type=SignalType.LONG,
                 qty=Decimal("0.1"), **kwargs):
    return Signal(
        strategy_id="fault-test",
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        timestamp=ts,
        type=sig_type,
        quantity=qty,
        **kwargs,
    )


class TestFaultInjection:

    def test_partial_fill_order_placed(self, clock, order_repo, journal):
        """With fill_ratio < 1.0, order should still be placed successfully."""
        adapter = RealisticMockAdapter(
            initial_balance=INITIAL_BALANCE,
            fill_ratio=0.5,
            seed=42,
        )
        engine = _make_engine(clock, adapter, order_repo, journal)
        signal = _make_signal()

        order_id = engine.execute_signal(signal)
        assert order_id is not None
        assert len(adapter.open_orders) == 1

    def test_adapter_rejection_no_residual_state(self, clock, order_repo, journal):
        """When adapter rejects all orders, no open orders should remain."""
        adapter = RealisticMockAdapter(
            initial_balance=INITIAL_BALANCE,
            reject_probability=1.0,
            seed=42,
        )
        engine = _make_engine(clock, adapter, order_repo, journal)
        signal = _make_signal()

        result = engine.execute_signal(signal)
        assert result is None
        assert len(adapter.open_orders) == 0

    def test_adapter_failure_mid_sequence(self, clock, order_repo, journal):
        """First order succeeds, second fails -- first remains unaffected."""
        adapter = MockExchangeAdapter(initial_balance=INITIAL_BALANCE)
        engine = _make_engine(clock, adapter, order_repo, journal)

        sig1 = _make_signal(ts=1_700_000_000_000)
        order1 = engine.execute_signal(sig1)
        assert order1 is not None
        assert len(adapter.open_orders) == 1

        adapter.set_should_fail(True, "Simulated mid-trade failure")

        sig2 = _make_signal(ts=1_700_000_900_000)
        result2 = engine.execute_signal(sig2)
        assert result2 is None

        # First order still intact in adapter
        assert len(adapter.open_orders) == 1

    def test_duplicate_signal_creates_multiple_orders(self, clock, order_repo, journal):
        """Same signal sent twice creates two orders (execution layer doesn't deduplicate)."""
        adapter = MockExchangeAdapter(initial_balance=INITIAL_BALANCE)
        engine = _make_engine(clock, adapter, order_repo, journal)

        sig = _make_signal()
        id1 = engine.execute_signal(sig)
        id2 = engine.execute_signal(sig)

        assert id1 is not None
        assert id2 is not None
        assert len(adapter.open_orders) == 2

    def test_no_signal_type_no_order(self, clock, order_repo, journal):
        """NO_SIGNAL type should not place any order."""
        adapter = MockExchangeAdapter(initial_balance=INITIAL_BALANCE)
        engine = _make_engine(clock, adapter, order_repo, journal)

        sig = _make_signal(sig_type=SignalType.NO_SIGNAL)
        order_id = engine.execute_signal(sig)

        assert order_id is None
        assert len(adapter.open_orders) == 0

    def test_set_fail_on_order_types(self, clock, order_repo, journal):
        """Adapter failure on specific order types (e.g., stop_loss) should not crash."""
        adapter = MockExchangeAdapter(initial_balance=INITIAL_BALANCE)
        adapter.set_fail_on_order_types({"stop_loss"})
        engine = _make_engine(clock, adapter, order_repo, journal)

        sig = _make_signal(stop_loss=Decimal("49000"))
        order_id = engine.execute_signal(sig)

        # Market order should succeed even if SL conditional fails
        assert order_id is not None
        # Only the market order should be in open_orders (SL failed to place)
        assert len(adapter.open_orders) == 1
        assert adapter.open_orders[0].type == "market"
