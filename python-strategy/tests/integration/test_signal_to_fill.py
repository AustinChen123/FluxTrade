"""Integration test: Signal → RiskManager → ExecutionEngine → Adapter → Fill.

No external dependencies (no Redis, no Rust .so, no DB).
Uses MockExchangeAdapter for order execution.
"""
import pytest
from decimal import Decimal
from unittest.mock import MagicMock

from src.core.models import Signal, SignalType, Candlestick, Position
from src.core.execution import ExecutionEngine
from src.core.risk_manager import RiskManager
from src.core.journal import StrategyJournal
from conftest import (
    MockExchangeAdapter,
    MockOrderRepository,
    MockAccountService,
    MockClock,
)
from integration.conftest import PRODUCT_ID, TIMEFRAME, INITIAL_BALANCE, make_candle


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def clock():
    return MockClock(initial_time=1_700_000_000)


@pytest.fixture
def adapter():
    return MockExchangeAdapter(initial_balance=INITIAL_BALANCE)


@pytest.fixture
def order_repo():
    return MockOrderRepository()


@pytest.fixture
def account_service():
    svc = MockAccountService(balance=INITIAL_BALANCE)
    return svc


@pytest.fixture
def journal():
    return StrategyJournal("integration-test")


@pytest.fixture
def risk_manager(account_service):
    return RiskManager(account_service=account_service)


@pytest.fixture
def execution_engine(clock, adapter, order_repo, journal):
    db_session = MagicMock()
    return ExecutionEngine(
        db_session=db_session,
        clock=clock,
        adapter=adapter,
        order_repository=order_repo,
        journal=journal,
        is_backtest=True,
    )


def _make_signal(
    signal_type: SignalType,
    price: Decimal | None = None,
    quantity: Decimal = Decimal("0.1"),
    stop_loss: Decimal | None = None,
    take_profit: Decimal | None = None,
    trailing_distance: Decimal | None = None,
    timestamp: int = 1_700_000_000_000,
) -> Signal:
    return Signal(
        strategy_id="test-strategy",
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        timestamp=timestamp,
        type=signal_type,
        value=price or Decimal("50000"),
        quantity=quantity,
        price=price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        trailing_distance=trailing_distance,
    )


def _make_candle_at(close: Decimal, ts: int = 1_700_000_900_000) -> Candlestick:
    return make_candle(
        timestamp=ts,
        open=close - Decimal("10"),
        high=close + Decimal("50"),
        low=close - Decimal("50"),
        close=close,
    )


# ===========================================================================
# Test: Full Signal → Order → Fill pipeline
# ===========================================================================
class TestSignalToFillPipeline:
    """Test the complete signal-to-fill pipeline without external services."""

    def test_long_entry_creates_order(self, execution_engine, adapter):
        """LONG signal → place_order → adapter receives order."""
        signal = _make_signal(SignalType.LONG)
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None
        assert len(adapter.open_orders) == 1
        assert adapter.open_orders[0].side.lower() == "buy"

    def test_short_entry_creates_order(self, execution_engine, adapter):
        """SHORT signal → place_order → adapter receives order."""
        signal = _make_signal(SignalType.SHORT)
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None
        assert len(adapter.open_orders) == 1
        assert adapter.open_orders[0].side.lower() == "sell"

    def test_market_data_fills_open_order(self, execution_engine, adapter, order_repo):
        """Order placed → market data → adapter triggers fill → repo records trade."""
        signal = _make_signal(SignalType.LONG, quantity=Decimal("0.1"))
        execution_engine.execute_signal(signal)
        assert len(adapter.open_orders) == 1

        candle = _make_candle_at(Decimal("50100"))
        execution_engine.process_market_data(candle)

        assert len(adapter.open_orders) == 0
        assert len(adapter.filled_orders) == 1
        assert len(order_repo.trades) == 1

    def test_fill_records_correct_price(self, execution_engine, adapter, order_repo):
        """Fill price should match candle close (default MockExchangeAdapter behavior)."""
        signal = _make_signal(SignalType.LONG)
        execution_engine.execute_signal(signal)

        fill_price = Decimal("50250")
        adapter.set_next_fill_price(fill_price)
        candle = _make_candle_at(Decimal("50200"))
        execution_engine.process_market_data(candle)

        trade = order_repo.trades[0]
        assert trade.price == fill_price

    def test_no_signal_returns_none(self, execution_engine, adapter):
        """NO_SIGNAL type should not create any order."""
        signal = _make_signal(SignalType.NO_SIGNAL)
        order_id = execution_engine.execute_signal(signal)

        assert order_id is None
        assert len(adapter.open_orders) == 0


# ===========================================================================
# Test: Signal with SL/TP creates conditional orders
# ===========================================================================
class TestConditionalOrders:
    """Test SL/TP/Trailing conditional order creation via execute_signal."""

    def test_signal_with_sl_tp_creates_three_orders(self, execution_engine, adapter):
        """LONG + SL + TP → 3 orders: entry + SL + TP."""
        signal = _make_signal(
            SignalType.LONG,
            stop_loss=Decimal("49000"),
            take_profit=Decimal("52000"),
        )
        execution_engine.execute_signal(signal)

        # entry + SL + TP = 3 orders
        assert len(adapter.open_orders) == 3

    def test_signal_with_sl_only(self, execution_engine, adapter):
        """LONG + SL → 2 orders: entry + SL."""
        signal = _make_signal(
            SignalType.LONG,
            stop_loss=Decimal("49000"),
        )
        execution_engine.execute_signal(signal)
        assert len(adapter.open_orders) == 2

    def test_signal_with_trailing_stop(self, execution_engine, adapter):
        """LONG + trailing_distance → 2 orders: entry + trailing."""
        signal = _make_signal(
            SignalType.LONG,
            trailing_distance=Decimal("500"),
        )
        execution_engine.execute_signal(signal)
        assert len(adapter.open_orders) == 2


# ===========================================================================
# Test: Risk manager integration
# ===========================================================================
class TestRiskManagerIntegration:
    """Test risk manager gates within the signal flow."""

    def test_risk_check_passes_for_entry(self, risk_manager):
        """New entry signal should pass risk check with sufficient balance."""
        signal = _make_signal(SignalType.LONG, quantity=Decimal("0.1"))
        current_price = Decimal("50000")
        passed, msg = risk_manager.check_risk(signal, current_price)
        assert passed is True

    def test_risk_check_passes_for_exit(self, risk_manager, account_service):
        """Exit signal should always pass risk check."""
        account_service.set_position(
            Position(
                strategy_id="test-strategy",
                product_id=PRODUCT_ID,
                side="LONG",
                quantity=Decimal("0.1"),
                entry_price=Decimal("50000"),
                unrealized_pnl=Decimal("0"),
            ),
        )
        signal = _make_signal(SignalType.EXIT_LONG, quantity=Decimal("0.1"))
        current_price = Decimal("50000")
        passed, msg = risk_manager.check_risk(signal, current_price)
        assert passed is True

    def test_risk_blocks_over_exposure(self, risk_manager, account_service):
        """Signal exceeding max exposure should be rejected."""
        # Set a large existing position
        account_service.set_position(
            Position(
                strategy_id="test-strategy",
                product_id=PRODUCT_ID,
                side="LONG",
                quantity=Decimal("100"),
                entry_price=Decimal("50000"),
                unrealized_pnl=Decimal("0"),
            ),
        )
        signal = _make_signal(SignalType.LONG, quantity=Decimal("100"))
        current_price = Decimal("50000")
        passed, msg = risk_manager.check_risk(signal, current_price)
        assert passed is False


# ===========================================================================
# Test: Journal records events
# ===========================================================================
class TestJournalIntegration:
    """Test that journal captures events through the pipeline."""

    def test_entry_logged_in_journal(self, execution_engine, journal):
        """execute_signal should log an 'entry' event in the journal."""
        signal = _make_signal(SignalType.LONG)
        execution_engine.execute_signal(signal)

        events = journal.to_dicts()
        entry_events = [e for e in events if e.get("tag") == "entry"]
        assert len(entry_events) >= 1

    def test_fill_logged_in_journal(self, execution_engine, journal):
        """process_market_data fill should log in the journal."""
        signal = _make_signal(SignalType.LONG)
        execution_engine.execute_signal(signal)

        candle = _make_candle_at(Decimal("50100"))
        execution_engine.process_market_data(candle)

        events = journal.to_dicts()
        # Should have entry + fill events
        assert len(events) >= 2


# ===========================================================================
# Test: Adapter failure handling
# ===========================================================================
class TestAdapterFailure:
    """Test error handling when adapter rejects orders."""

    def test_adapter_failure_returns_none(self, execution_engine, adapter, order_repo):
        """When adapter raises, execute_signal returns None and marks order failed."""
        adapter.set_should_fail(True, "Insufficient margin")
        signal = _make_signal(SignalType.LONG)
        order_id = execution_engine.execute_signal(signal)

        assert order_id is None
        failed = [o for o in order_repo.orders.values() if o.status == "failed"]
        assert len(failed) == 1
