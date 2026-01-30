"""
Tests for src/core/repositories.py

Covers:
- BacktestOrderRepository (primary focus - no DB required)
  - Trade logging via add_trade
  - No-op methods: add_order, update_order, update_position
  - get_position returns None (position state delegated to Rust engine)
  - update_order_exchange_id sets exchange_order_id on ORM Order

Note: Position netting, balance tracking, and PnL realization were removed
from BacktestOrderRepository in Phase 4.5. These responsibilities are now
handled by the Rust PyMatchingEngine via SimulatedAdapter. See
test_adapters_simulated.py for coverage of those behaviours.
"""

from decimal import Decimal

from src.core.repositories import BacktestOrderRepository
from src.core.orm_models import Trade


class TestBacktestOrderRepositoryBasics:
    """Basic tests for BacktestOrderRepository."""

    def test_initialization(self, mock_db_session):
        """Should initialize with correct defaults."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        assert repo.session_id == 1
        assert repo.balance == Decimal("10000")

    def test_initialization_custom_balance(self, mock_db_session):
        """Should accept custom initial balance."""
        repo = BacktestOrderRepository(
            mock_db_session,
            session_id=1,
            initial_balance=Decimal("50000")
        )

        assert repo.balance == Decimal("50000")

    def test_add_order_is_noop(self, mock_db_session, order_factory):
        """add_order should be no-op in backtest (orders not persisted)."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order = order_factory()

        # Should not raise
        repo.add_order(order)

    def test_update_order_is_noop(self, mock_db_session, order_factory):
        """update_order should be no-op in backtest."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order = order_factory()

        # Should not raise
        repo.update_order(order)


class TestBacktestPositionDelegation:
    """Position/balance operations are delegated to Rust engine."""

    def test_update_position_is_noop(self, mock_db_session):
        """update_position should be no-op (Rust engine handles positions)."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        # Should not raise and should not change balance
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy",
                             Decimal("1.0"), Decimal("42000"))

        assert repo.balance == Decimal("10000")

    def test_get_position_returns_none(self, mock_db_session):
        """get_position should return None (position state in Rust engine)."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        assert pos is None

    def test_get_position_with_side_returns_none(self, mock_db_session):
        """get_position should return None regardless of side argument."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        assert repo.get_position("test", "BINANCE:BTCUSDT-PERP", "LONG") is None
        assert repo.get_position("test", "BINANCE:BTCUSDT-PERP", "SHORT") is None


class TestBacktestTradeLogging:
    """Tests for trade logging in BacktestOrderRepository."""

    def test_add_trade_calls_db(self, mock_db_session, order_factory):
        """add_trade should create BacktestTradeLog and commit."""
        repo = BacktestOrderRepository(mock_db_session, session_id=42)

        trade = Trade(
            order_id=order_factory().id,
            exchange_trade_id="sim-trade-001",
            product_id="BINANCE:BTCUSDT-PERP",
            side="buy",
            price=Decimal("42000"),
            quantity=Decimal("1.0"),
            fee=Decimal("2.52"),
            fee_asset="USDT",
            timestamp=1704067200000,
        )
        repo.add_trade(trade)

        assert mock_db_session.add.called
        assert mock_db_session.commit.called

    def test_update_order_exchange_id(self, mock_db_session, order_factory):
        """update_order_exchange_id should set exchange_order_id on order."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order = order_factory()

        repo.update_order_exchange_id(order, "SIM-abc123")

        assert order.exchange_order_id == "SIM-abc123"
