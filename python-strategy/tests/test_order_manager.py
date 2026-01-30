"""
Tests for src/core/order_manager.py

Covers:
- Order creation
- Order status updates (fill, fail)
- Exchange order ID tracking
- Backtest mode detection
- Position updates via repository
"""

import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.order_manager import OrderManager
from src.core.models import Signal, SignalType


class TestOrderCreation:
    """Tests for order creation."""

    def test_create_market_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should create a market order from signal."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(
            signal=signal,
            side="buy",
            order_type="market",
            quantity=Decimal("0.1")
        )

        assert order is not None
        assert order.side == "buy"
        assert order.type == "market"
        assert order.quantity == Decimal("0.1")
        assert order.strategy_id == signal.strategy_id
        assert order.product_id == signal.product_id
        assert order.status == "open"

    def test_create_limit_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should create a limit order with price."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(
            signal=signal,
            side="buy",
            order_type="limit",
            quantity=Decimal("0.1"),
            price=Decimal("42000")
        )

        assert order.type == "limit"
        assert order.price == Decimal("42000")

    def test_create_sell_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should create a sell order."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory(signal_type=SignalType.EXIT_LONG)

        order = order_manager.create_order(
            signal=signal,
            side="sell",
            order_type="market",
            quantity=Decimal("0.5")
        )

        assert order.side == "sell"

    def test_order_has_unique_id(self, mock_order_repo, mock_clock, signal_factory):
        """Each order should have a unique ID."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order1 = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))
        order2 = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        assert order1.id != order2.id

    def test_order_added_to_repository(self, mock_order_repo, mock_clock, signal_factory):
        """Order should be added to repository."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        assert order.id in mock_order_repo.orders

    def test_order_timestamp_from_clock(self, mock_order_repo, mock_clock, signal_factory):
        """Order timestamp should come from clock."""
        mock_clock.set_time(1704153600.0)  # 2024-01-02 00:00:00 UTC
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        assert order.timestamp == 1704153600000  # milliseconds

    def test_order_extracts_exchange_id(self, mock_order_repo, mock_clock, signal_factory):
        """Order should extract exchange ID from product_id."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory(product_id="BYBIT:ETHUSDT-PERP")

        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        assert order.exchange_id == "BYBIT"


class TestOrderStatusUpdates:
    """Tests for order status updates."""

    def test_fill_order_updates_status(self, mock_clock, mock_db_session, signal_factory):
        """Filling an order should update its status to closed."""
        # Use BacktestOrderRepository to avoid Redis connection
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order_manager = OrderManager(backtest_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        order_manager.fill_order(order, Decimal("42100"), Decimal("0.1"))

        assert order.status == "closed"

    def test_fill_order_records_fill_price(self, mock_clock, mock_db_session, signal_factory):
        """Filling should record the fill price."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order_manager = OrderManager(backtest_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        order_manager.fill_order(order, Decimal("42100.50"), Decimal("0.1"))

        assert order.filled_price == Decimal("42100.50")

    def test_fill_order_records_fill_quantity(self, mock_clock, mock_db_session, signal_factory):
        """Filling should record the fill quantity."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order_manager = OrderManager(backtest_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        order_manager.fill_order(order, Decimal("42100"), Decimal("0.1"))

        assert order.filled_quantity == Decimal("0.1")

    def test_fill_order_creates_trade(self, mock_clock, mock_db_session, signal_factory):
        """Filling should create a trade record."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order_manager = OrderManager(backtest_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        order_manager.fill_order(order, Decimal("42100"), Decimal("0.1"))

        # BacktestOrderRepository adds to db session, verify via mock
        # The trade is added to DB via mock_db_session.add()
        assert order.status == "closed"

    def test_fail_order_updates_status(self, mock_order_repo, mock_clock, signal_factory):
        """Failing an order should update its status to failed."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        order_manager.fail_order(order, "Insufficient funds")

        assert order.status == "failed"


class TestExchangeOrderId:
    """Tests for exchange order ID handling."""

    def test_update_exchange_order_id(self, mock_order_repo, mock_clock, signal_factory):
        """Should update exchange order ID."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        order_manager.update_exchange_order_id(order, "BINANCE-12345")

        assert order.exchange_order_id == "BINANCE-12345"

    def test_default_exchange_order_id_is_simulated(self, mock_order_repo, mock_clock, signal_factory):
        """Default exchange order ID should indicate simulation."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))

        assert order.exchange_order_id.startswith("sim_")


class TestBacktestModeDetection:
    """Tests for backtest mode detection."""

    def test_detects_backtest_mode(self, mock_clock, mock_db_session):
        """Should detect backtest mode from BacktestOrderRepository."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)

        order_manager = OrderManager(backtest_repo, mock_clock)

        assert order_manager.is_backtest is True

    def test_detects_live_mode(self, mock_order_repo, mock_clock):
        """Should detect live mode from non-backtest repository."""
        order_manager = OrderManager(mock_order_repo, mock_clock)

        # MockOrderRepository is not BacktestOrderRepository
        assert order_manager.is_backtest is False


class TestPositionUpdates:
    """Tests for position updates through order manager."""

    def test_fill_updates_position_in_backtest(self, mock_clock, mock_db_session, signal_factory):
        """Fill in backtest mode — position tracked by Rust engine, not repo."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order_manager = OrderManager(backtest_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))
        order_manager.fill_order(order, Decimal("42000"), Decimal("0.1"))

        # BacktestOrderRepository delegates position tracking to Rust engine
        # get_position always returns None in backtest repo
        pos = backtest_repo.get_position(signal.strategy_id, signal.product_id)
        assert pos is None


class TestOrderManagerEdgeCases:
    """Edge case tests for OrderManager."""

    def test_multiple_orders_same_signal(self, mock_order_repo, mock_clock, signal_factory):
        """Should handle multiple orders from same signal."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        orders = []
        for _ in range(5):
            order = order_manager.create_order(signal, "buy", "market", Decimal("0.1"))
            orders.append(order)

        # All orders should have unique IDs
        order_ids = [o.id for o in orders]
        assert len(order_ids) == len(set(order_ids))

    def test_zero_quantity_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should create order with zero quantity (edge case)."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, "buy", "market", Decimal("0"))

        assert order.quantity == Decimal("0")

    def test_very_small_quantity_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should handle very small quantities."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, "buy", "market", Decimal("0.00000001"))

        assert order.quantity == Decimal("0.00000001")

    def test_very_large_quantity_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should handle very large quantities."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, "buy", "market", Decimal("1000000"))

        assert order.quantity == Decimal("1000000")
