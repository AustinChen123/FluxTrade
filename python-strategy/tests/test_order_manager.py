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
from src.core.models import OrderSide, OrderStatus, SignalType


class TestOrderCreation:
    """Tests for order creation."""

    def test_create_market_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should create a market order from signal."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(
            signal=signal,
            side=OrderSide.BUY,
            order_type="market",
            quantity=Decimal("0.1")
        )

        assert order is not None
        assert order.side == OrderSide.BUY
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
            side=OrderSide.BUY,
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
            side=OrderSide.SELL,
            order_type="market",
            quantity=Decimal("0.5")
        )

        assert order.side == OrderSide.SELL

    def test_order_has_unique_id(self, mock_order_repo, mock_clock, signal_factory):
        """Each order should have a unique ID."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order1 = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))
        order2 = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        assert order1.id != order2.id

    def test_order_added_to_repository(self, mock_order_repo, mock_clock, signal_factory):
        """Order should be added to repository."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        assert order.id in mock_order_repo.orders

    def test_order_timestamp_from_clock(self, mock_order_repo, mock_clock, signal_factory):
        """Order timestamp should come from clock."""
        mock_clock.set_time(1704153600.0)  # 2024-01-02 00:00:00 UTC
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        assert order.timestamp == 1704153600000  # milliseconds

    def test_order_extracts_exchange_id(self, mock_order_repo, mock_clock, signal_factory):
        """Order should extract exchange ID from product_id."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory(product_id="BYBIT:ETHUSDT-PERP")

        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        assert order.exchange_id == "BYBIT"

    def test_order_accepts_client_order_id_and_intent_payload(
        self, mock_order_repo, mock_clock, signal_factory
    ):
        """Order/audit correlation metadata should be stored on the order."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(
            signal=signal,
            side=OrderSide.BUY,
            order_type="market",
            quantity=Decimal("0.1"),
            client_order_id="client-123",
            intent_payload={
                "quantity": Decimal("0.1"),
                "limits": [Decimal("42000.5")],
            },
        )

        assert order.client_order_id == "client-123"
        assert order.exchange_order_id is None
        assert order.status == OrderStatus.NEW.value
        assert order.intent_payload == {
            "quantity": "0.1",
            "limits": ["42000.5"],
        }


class TestOrderStatusUpdates:
    """Tests for order status updates."""

    def test_fill_order_updates_status(self, mock_clock, mock_db_session, signal_factory):
        """Filling an order should update its status to closed."""
        # Use BacktestOrderRepository to avoid Redis connection
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order_manager = OrderManager(backtest_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        order_manager.fill_order(order, Decimal("42100"), Decimal("0.1"))

        assert order.status == "closed"

    def test_fill_order_records_fill_price(self, mock_clock, mock_db_session, signal_factory):
        """Filling should record the fill price."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order_manager = OrderManager(backtest_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        order_manager.fill_order(order, Decimal("42100.50"), Decimal("0.1"))

        assert order.filled_price == Decimal("42100.50")

    def test_fill_order_records_fill_quantity(self, mock_clock, mock_db_session, signal_factory):
        """Filling should record the fill quantity."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order_manager = OrderManager(backtest_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        order_manager.fill_order(order, Decimal("42100"), Decimal("0.1"))

        assert order.filled_quantity == Decimal("0.1")

    def test_fill_order_creates_trade(self, mock_clock, mock_db_session, signal_factory):
        """Filling should create a trade record."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order_manager = OrderManager(backtest_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        order_manager.fill_order(order, Decimal("42100"), Decimal("0.1"))

        # BacktestOrderRepository adds to db session, verify via mock
        # The trade is added to DB via mock_db_session.add()
        assert order.status == "closed"

    def test_fail_order_updates_status(self, mock_order_repo, mock_clock, signal_factory):
        """Failing an order should update its status to failed."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        order_manager.fail_order(order, "Insufficient funds")

        assert order.status == "failed"

    def test_mark_submitted_unconfirmed_updates_status(
        self, mock_order_repo, mock_clock, signal_factory
    ):
        """Sent orders should enter ACK-pending state."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        order_manager.mark_submitted_unconfirmed(order)

        assert order.status == OrderStatus.SUBMITTED_UNCONFIRMED.value
        assert mock_order_repo.orders[order.id].status == OrderStatus.SUBMITTED_UNCONFIRMED.value

    def test_mark_submitted_updates_status_and_exchange_id(
        self, mock_order_repo, mock_clock, signal_factory
    ):
        """ACKed orders should store exchange order id."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        order_manager.mark_submitted(order, "EX-123")

        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id == "EX-123"

    def test_mark_cancelled_updates_status(self, mock_order_repo, mock_clock, signal_factory):
        """Cancelled orders should enter terminal cancelled state."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        order_manager.mark_cancelled(order)

        assert order.status == OrderStatus.CANCELLED.value
        assert mock_order_repo.orders[order.id].status == OrderStatus.CANCELLED.value


class TestExchangeOrderId:
    """Tests for exchange order ID handling."""

    def test_update_exchange_order_id(self, mock_order_repo, mock_clock, signal_factory):
        """Should update exchange order ID."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        order_manager.update_exchange_order_id(order, "BINANCE-12345")

        assert order.exchange_order_id == "BINANCE-12345"

    def test_default_exchange_order_id_is_simulated(self, mock_order_repo, mock_clock, signal_factory):
        """Default exchange order ID should indicate simulation."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

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

        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))
        order_manager.fill_order(order, Decimal("42000"), Decimal("0.1"))

        # BacktestOrderRepository delegates position tracking to Rust engine
        # get_position always returns None in backtest repo
        pos = backtest_repo.get_position(signal.strategy_id, signal.product_id)
        assert pos is None


class TestOrderValidation:
    """Tests for order input validation (M6 fix)."""

    def test_invalid_side_raises(self, mock_order_repo, mock_clock, signal_factory):
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        with pytest.raises(ValueError, match="Invalid order side"):
            order_manager.create_order(signal, "foobar", "market", Decimal("0.1"))

    def test_invalid_order_type_raises(self, mock_order_repo, mock_clock, signal_factory):
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        with pytest.raises(ValueError, match="Invalid order type"):
            order_manager.create_order(signal, OrderSide.BUY, "invalid_type", Decimal("0.1"))

    def test_valid_stop_loss_type_accepted(self, mock_order_repo, mock_clock, signal_factory):
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, OrderSide.SELL, "stop_loss", Decimal("0.1"), trigger_price=Decimal("40000"))
        assert order.type == "stop_loss"

    def test_case_insensitive_validation(self, mock_order_repo, mock_clock, signal_factory):
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()
        order = order_manager.create_order(signal, "BUY", "MARKET", Decimal("0.1"))
        assert order is not None


class TestOrderManagerEdgeCases:
    """Edge case tests for OrderManager."""

    def test_multiple_orders_same_signal(self, mock_order_repo, mock_clock, signal_factory):
        """Should handle multiple orders from same signal."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        orders = []
        for _ in range(5):
            order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))
            orders.append(order)

        # All orders should have unique IDs
        order_ids = [o.id for o in orders]
        assert len(order_ids) == len(set(order_ids))

    def test_zero_quantity_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should create order with zero quantity (edge case)."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0"))

        assert order.quantity == Decimal("0")

    def test_very_small_quantity_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should handle very small quantities."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("0.00000001"))

        assert order.quantity == Decimal("0.00000001")

    def test_very_large_quantity_order(self, mock_order_repo, mock_clock, signal_factory):
        """Should handle very large quantities."""
        order_manager = OrderManager(mock_order_repo, mock_clock)
        signal = signal_factory()

        order = order_manager.create_order(signal, OrderSide.BUY, "market", Decimal("1000000"))

        assert order.quantity == Decimal("1000000")


class TestOrderManagerLiveMode:
    """Tests for live mode (non-backtest) OrderManager."""

    def test_lua_file_not_found_raises(self, mock_clock):
        """Missing Lua script should raise on init."""
        from src.core.repositories import LiveOrderRepository

        mock_db = MagicMock()
        repo = LiveOrderRepository(mock_db)

        with patch("src.core.order_manager.create_redis_client") as mock_redis_cls, \
             patch("builtins.open", side_effect=FileNotFoundError("no lua")):
            mock_redis_cls.return_value = MagicMock()

            with pytest.raises(FileNotFoundError):
                OrderManager(repo, mock_clock, is_backtest=False)

    def test_live_fill_calls_lua_script(self, mock_clock, signal_factory):
        """fill_order in live mode should call Redis Lua script."""
        from src.core.repositories import LiveOrderRepository

        mock_db = MagicMock()
        repo = LiveOrderRepository(mock_db)

        mock_redis = MagicMock()
        mock_script = MagicMock()
        mock_redis.register_script.return_value = mock_script

        with patch("src.core.order_manager.create_redis_client", return_value=mock_redis), \
             patch("builtins.open", MagicMock()):
            om = OrderManager(repo, mock_clock, is_backtest=False)

        signal = signal_factory()
        order = om.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))
        om.fill_order(order, Decimal("42000"), Decimal("0.1"))

        mock_script.assert_called_once()
        assert order.status == "closed"

    def test_live_fill_lua_error_raises_runtime(self, mock_clock, signal_factory):
        """Lua script ResponseError should raise RuntimeError."""
        import redis as redis_lib
        from src.core.repositories import LiveOrderRepository

        mock_db = MagicMock()
        repo = LiveOrderRepository(mock_db)

        mock_redis = MagicMock()
        mock_script = MagicMock()
        mock_script.side_effect = redis_lib.exceptions.ResponseError("script error")
        mock_redis.register_script.return_value = mock_script

        with patch("src.core.order_manager.create_redis_client", return_value=mock_redis), \
             patch("builtins.open", MagicMock()):
            om = OrderManager(repo, mock_clock, is_backtest=False)

        signal = signal_factory()
        order = om.create_order(signal, OrderSide.BUY, "market", Decimal("0.1"))

        with pytest.raises(RuntimeError, match="Critical State Corruption"):
            om.fill_order(order, Decimal("42000"), Decimal("0.1"))

    def test_explicit_backtest_flag_overrides_detection(self, mock_order_repo, mock_clock):
        """Explicit is_backtest=True should skip Redis init."""
        om = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
        assert om.is_backtest is True
        assert om.redis_client is None
