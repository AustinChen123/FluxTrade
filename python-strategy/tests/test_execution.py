"""
Tests for src/core/execution.py

Covers:
- Signal to order conversion
- Side determination (LONG/SHORT/EXIT)
- Order type detection (market/limit)
- Adapter delegation
- Error handling on execution failure
- Market data processing for simulated fills
"""

from contextlib import nullcontext

import pytest
from decimal import Decimal

from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeError
from src.core.models import OrderStatus, SignalType
from src.core.client_order_id import parse_client_order_id


@pytest.fixture
def execution_engine(mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo):
    """Provides an ExecutionEngine with mock dependencies."""
    return ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo
    )


class TestSideDetermination:
    """Tests for signal type to order side mapping."""

    def test_long_signal_becomes_buy(self, execution_engine, signal_factory):
        """LONG signal should produce a buy order."""
        signal = signal_factory(signal_type=SignalType.LONG, price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None

    def test_short_signal_becomes_sell(self, execution_engine, signal_factory):
        """SHORT signal should produce a sell order."""
        signal = signal_factory(signal_type=SignalType.SHORT, price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None

    def test_exit_long_becomes_sell(self, execution_engine, signal_factory):
        """EXIT_LONG signal should produce a sell order."""
        signal = signal_factory(signal_type=SignalType.EXIT_LONG, price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None

    def test_exit_short_becomes_buy(self, execution_engine, signal_factory):
        """EXIT_SHORT signal should produce a buy order."""
        signal = signal_factory(signal_type=SignalType.EXIT_SHORT, price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None

    def test_no_signal_returns_none(self, execution_engine, signal_factory):
        """NO_SIGNAL should return None (no order created)."""
        signal = signal_factory(signal_type=SignalType.NO_SIGNAL)
        order_id = execution_engine.execute_signal(signal)

        assert order_id is None


class TestOrderTypeDetection:
    """Tests for order type (market/limit) detection."""

    def test_signal_with_price_creates_limit(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Signal with price should create limit order."""
        signal = signal_factory(price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 1
        assert mock_exchange_adapter.open_orders[0].type == "limit"

    def test_signal_with_value_creates_limit(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Signal with value (legacy) should create limit order."""
        signal = signal_factory(price=None, value=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 1
        assert mock_exchange_adapter.open_orders[0].type == "limit"

    def test_signal_without_price_creates_market(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Signal without price should create market order."""
        signal = signal_factory(price=None, value=None)
        execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 1
        assert mock_exchange_adapter.open_orders[0].type == "market"


class TestQuantityHandling:
    """Tests for quantity determination."""

    def test_signal_quantity_used(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Signal's quantity should be used when provided."""
        signal = signal_factory(quantity=Decimal("0.5"), price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert mock_exchange_adapter.open_orders[0].quantity == Decimal("0.5")

    def test_default_quantity_when_none(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Default quantity should be used when signal quantity is None."""
        signal = signal_factory(quantity=None, price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert mock_exchange_adapter.open_orders[0].quantity == Decimal("0.01")

    def test_default_quantity_when_zero(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Default quantity should be used when signal quantity is zero."""
        signal = signal_factory(quantity=Decimal("0"), price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert mock_exchange_adapter.open_orders[0].quantity == Decimal("0.01")


class TestAdapterDelegation:
    """Tests for adapter order placement."""

    def test_order_sent_to_adapter(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Order should be sent to adapter for execution."""
        signal = signal_factory(price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 1

    def test_exchange_id_recorded(self, execution_engine, signal_factory, mock_order_repo):
        """Exchange order ID should be recorded after placement."""
        signal = signal_factory(price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        order = mock_order_repo.orders[order_id]
        assert order.exchange_order_id.startswith("MOCK-")

    def test_multiple_signals_create_multiple_orders(
        self, execution_engine, signal_factory, mock_exchange_adapter
    ):
        """Multiple signals should create independent orders."""
        for _ in range(3):
            signal = signal_factory(price=Decimal("42000"))
            execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 3


class TestExecutionErrorHandling:
    """Tests for error handling during execution."""

    def test_adapter_failure_marks_order_failed(
        self, execution_engine, signal_factory, mock_exchange_adapter, mock_order_repo
    ):
        """Adapter failure should mark order as failed."""
        mock_exchange_adapter.set_should_fail(True, "Connection timeout")

        signal = signal_factory(price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is None

        # Order should be in repo with failed status
        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 1

    def test_adapter_failure_returns_none(
        self, execution_engine, signal_factory, mock_exchange_adapter
    ):
        """Adapter failure should return None."""
        mock_exchange_adapter.set_should_fail(True, "Insufficient funds")

        signal = signal_factory(price=Decimal("42000"))
        result = execution_engine.execute_signal(signal)

        assert result is None


class TestAuditedExecution:
    """Tests for opt-in fail-stop audit execution path."""

    def test_requires_session_factory(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            audit_external_orders=True,
        )

        with pytest.raises(RuntimeError, match="requires db_session_factory"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

    def test_success_writes_intent_and_outcome(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
        )
        signal = signal_factory(price=Decimal("42000"), quantity=Decimal("0.25"))

        order_id = engine.execute_signal(signal)

        assert order_id is not None
        order = mock_order_repo.orders[order_id]
        coid = parse_client_order_id(order.client_order_id)
        assert coid.strategy_id == signal.strategy_id
        assert coid.instance_id == "execution"
        assert coid.action == "long"
        assert order.intent_payload["order"]["quantity"] == "0.25"
        assert order.intent_payload["order"]["price"] == "42000"
        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id.startswith("MOCK-")
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.client_order_id == order.client_order_id
        assert audit.intent_payload["order"]["client_order_id"] == order.client_order_id
        assert audit.outcome_payload["status"] == "placed"
        assert audit.outcome_payload["exchange_order_id"].startswith("MOCK-")
        assert audit.order_id == order.id
        assert audit_session.flush.call_count == 1
        assert audit_session.commit.call_count == 2

    def test_exchange_failure_writes_outcome_then_raises(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.set_should_fail(True, "Connection timeout")
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
        )

        with pytest.raises(ExchangeError, match="Connection timeout"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 1
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.order_id == failed_orders[0].id
        assert audit.outcome_payload == {
            "status": "failed",
            "error": "Connection timeout",
        }
        assert audit_session.flush.call_count == 1
        assert audit_session.commit.call_count == 2

    def test_intent_audit_failure_stops_before_external_order(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_db_session.flush.side_effect = RuntimeError("intent audit failed")
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )

        with pytest.raises(RuntimeError, match="intent audit failed"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        assert mock_exchange_adapter.open_orders == []
        mock_db_session.rollback.assert_called_once()

    def test_success_outcome_audit_failure_raises_after_external_order(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_db_session.commit.side_effect = [None, RuntimeError("outcome audit failed")]
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )

        with pytest.raises(RuntimeError, match="outcome audit failed"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        assert len(mock_exchange_adapter.open_orders) == 1
        audit = mock_db_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload["status"] == "placed"
        mock_db_session.rollback.assert_called_once()

    def test_exchange_failure_outcome_audit_failure_raises_audit_error(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.set_should_fail(True, "Connection timeout")
        mock_db_session.commit.side_effect = [None, RuntimeError("outcome audit failed")]
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )

        with pytest.raises(RuntimeError, match="outcome audit failed"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 1
        audit = mock_db_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload == {
            "status": "failed",
            "error": "Connection timeout",
        }
        mock_db_session.rollback.assert_called_once()


class TestCancelOrder:
    """Tests for execution-level cancellation."""

    def test_cancel_order_returns_false_when_order_missing(
        self, execution_engine, mock_exchange_adapter
    ):
        assert execution_engine.cancel_order("missing") is False
        assert mock_exchange_adapter.open_orders == []

    def test_cancel_order_calls_adapter_and_marks_cancelled(
        self, execution_engine, signal_factory, mock_order_repo, mock_exchange_adapter
    ):
        order_id = execution_engine.execute_signal(signal_factory(price=None, value=None))
        order = mock_order_repo.orders[order_id]

        result = execution_engine.cancel_order(order_id)

        assert result is True
        assert order.status == OrderStatus.CANCELLED.value
        assert mock_exchange_adapter.open_orders == []

    def test_cancel_order_is_idempotent_for_cancelled_order(
        self, execution_engine, signal_factory, mock_order_repo, mock_exchange_adapter
    ):
        order_id = execution_engine.execute_signal(signal_factory(price=None, value=None))
        order = mock_order_repo.orders[order_id]
        order.status = OrderStatus.CANCELLED.value

        result = execution_engine.cancel_order(order_id)

        assert result is True
        assert len(mock_exchange_adapter.open_orders) == 1


class TestMarketDataProcessing:
    """Tests for process_market_data (simulated fills)."""

    def test_market_data_triggers_fills(
        self, mock_db_session, mock_clock, mock_exchange_adapter, signal_factory, candlestick_factory
    ):
        """Market data should trigger fills for pending orders."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=backtest_repo
        )

        signal = signal_factory(price=Decimal("42000"))
        engine.execute_signal(signal)
        assert len(mock_exchange_adapter.open_orders) == 1

        candle = candlestick_factory(close=Decimal("42100"))
        engine.process_market_data(candle)

        # Order should be filled
        assert len(mock_exchange_adapter.open_orders) == 0

    def test_no_orders_no_fills(self, execution_engine, candlestick_factory):
        """No fills when no pending orders."""
        candle = candlestick_factory()
        # Should not raise
        execution_engine.process_market_data(candle)


class TestConditionalOrderErrorHandling:
    """Tests for SL/TP/Trailing Stop order placement error paths."""

    def test_sl_order_failure_logs_error(
        self, execution_engine, signal_factory, mock_exchange_adapter, caplog
    ):
        """SL order placement failure should log error but not fail main order."""
        mock_exchange_adapter.set_fail_on_order_types({"stop_loss"})

        signal = signal_factory(
            price=Decimal("42000"),
            stop_loss=Decimal("41000"),
        )
        order_id = execution_engine.execute_signal(signal)

        # Main order should succeed
        assert order_id is not None
        # Entry order placed, SL order failed
        assert len(mock_exchange_adapter.open_orders) == 1
        assert "Failed to place SL order" in caplog.text

    def test_tp_order_failure_logs_error(
        self, execution_engine, signal_factory, mock_exchange_adapter, caplog
    ):
        """TP order placement failure should log error but not fail main order."""
        mock_exchange_adapter.set_fail_on_order_types({"take_profit"})

        signal = signal_factory(
            price=Decimal("42000"),
            take_profit=Decimal("45000"),
        )
        order_id = execution_engine.execute_signal(signal)

        # Main order should succeed
        assert order_id is not None
        # Entry order placed, TP order failed
        assert len(mock_exchange_adapter.open_orders) == 1
        assert "Failed to place TP order" in caplog.text

    def test_trailing_stop_failure_logs_error(
        self, execution_engine, signal_factory, mock_exchange_adapter, caplog
    ):
        """Trailing stop order placement failure should log error."""
        mock_exchange_adapter.set_fail_on_order_types({"trailing_stop"})

        signal = signal_factory(
            price=Decimal("42000"),
            stop_loss=Decimal("41000"),
            trailing_distance=Decimal("500"),
        )
        order_id = execution_engine.execute_signal(signal)

        # Main order should succeed
        assert order_id is not None
        assert "Failed to place trailing stop order" in caplog.text

    def test_all_conditional_orders_fail_main_succeeds(
        self, execution_engine, signal_factory, mock_exchange_adapter, caplog
    ):
        """All conditional orders can fail while main order succeeds."""
        mock_exchange_adapter.set_fail_on_order_types(
            {"stop_loss", "take_profit", "trailing_stop"}
        )

        signal = signal_factory(
            price=Decimal("42000"),
            stop_loss=Decimal("41000"),
            take_profit=Decimal("45000"),
            trailing_distance=Decimal("500"),
        )
        order_id = execution_engine.execute_signal(signal)

        # Main order should still succeed
        assert order_id is not None
        assert len(mock_exchange_adapter.open_orders) == 1
        # All conditional order errors logged
        assert "Failed to place SL order" in caplog.text
        assert "Failed to place TP order" in caplog.text
        assert "Failed to place trailing stop order" in caplog.text
