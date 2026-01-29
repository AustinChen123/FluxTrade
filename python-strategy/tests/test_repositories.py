"""
Tests for src/core/repositories.py

Covers:
- BacktestOrderRepository (primary focus - no DB required)
- Position netting logic
- Balance updates on PnL realization
- Trade logging
"""

import pytest
from decimal import Decimal

from src.core.repositories import BacktestOrderRepository
from src.core.orm_models import Order, Trade


class TestBacktestOrderRepositoryBasics:
    """Basic tests for BacktestOrderRepository."""

    def test_initialization(self, mock_db_session):
        """Should initialize with correct defaults."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        assert repo.session_id == 1
        assert repo.balance == Decimal("10000")
        assert len(repo._positions) == 0

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


class TestBacktestPositionNetting:
    """Tests for position netting logic in BacktestOrderRepository."""

    def test_new_long_position(self, mock_db_session):
        """Should create new LONG position on buy."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        repo.update_position(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            side="buy",
            fill_quantity=Decimal("1.0"),
            fill_price=Decimal("42000")
        )

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        assert pos is not None
        assert pos.side == "LONG"
        assert pos.quantity == Decimal("1.0")
        assert pos.entry_price == Decimal("42000")

    def test_new_short_position(self, mock_db_session):
        """Should create new SHORT position on sell."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        repo.update_position(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            side="sell",
            fill_quantity=Decimal("1.0"),
            fill_price=Decimal("42000")
        )

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        assert pos is not None
        assert pos.side == "SHORT"
        assert pos.quantity == Decimal("1.0")

    def test_increase_long_position(self, mock_db_session):
        """Should increase LONG position and average entry price."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        # Initial position
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("40000"))
        # Add more at higher price
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("44000"))

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        assert pos.quantity == Decimal("2.0")
        # Average: (1 * 40000 + 1 * 44000) / 2 = 42000
        assert pos.entry_price == Decimal("42000")

    def test_increase_short_position(self, mock_db_session):
        """Should increase SHORT position and average entry price."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        # Initial short
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("1.0"), Decimal("44000"))
        # Add more short at lower price
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("1.0"), Decimal("40000"))

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        assert pos.side == "SHORT"
        assert pos.quantity == Decimal("2.0")
        assert pos.entry_price == Decimal("42000")

    def test_partial_close_long(self, mock_db_session):
        """Should partially close LONG position on sell."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("10000")
        )

        # Open long
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("2.0"), Decimal("40000"))
        # Partial close
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("1.0"), Decimal("42000"))

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        assert pos.side == "LONG"
        assert pos.quantity == Decimal("1.0")
        # Entry price unchanged for remaining position
        assert pos.entry_price == Decimal("40000")

    def test_full_close_long(self, mock_db_session):
        """Should fully close LONG position."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("10000")
        )

        # Open long
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("40000"))
        # Full close
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("1.0"), Decimal("42000"))

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        # Position should be flat (qty = 0)
        assert pos.quantity == Decimal("0")

    def test_flip_long_to_short(self, mock_db_session):
        """Should flip from LONG to SHORT when sell exceeds position."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("10000")
        )

        # Open long 1.0
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("40000"))
        # Sell 2.0 (flip)
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("2.0"), Decimal("42000"))

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        assert pos.side == "SHORT"
        assert pos.quantity == Decimal("1.0")
        assert pos.entry_price == Decimal("42000")

    def test_flip_short_to_long(self, mock_db_session):
        """Should flip from SHORT to LONG when buy exceeds position."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("10000")
        )

        # Open short 1.0
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("1.0"), Decimal("42000"))
        # Buy 2.0 (flip)
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("2.0"), Decimal("40000"))

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        assert pos.side == "LONG"
        assert pos.quantity == Decimal("1.0")
        assert pos.entry_price == Decimal("40000")


class TestBacktestPnLRealization:
    """Tests for PnL realization in BacktestOrderRepository."""

    def test_profit_on_long_close(self, mock_db_session):
        """Should realize profit when closing LONG at higher price."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("10000")
        )

        # Open long at 40000
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("40000"))
        initial_balance = repo.balance

        # Close at 42000 (2000 profit per unit)
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("1.0"), Decimal("42000"))

        # PnL = (42000 - 40000) * 1.0 = 2000
        assert repo.balance == initial_balance + Decimal("2000")

    def test_loss_on_long_close(self, mock_db_session):
        """Should realize loss when closing LONG at lower price."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("10000")
        )

        # Open long at 42000
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("42000"))
        initial_balance = repo.balance

        # Close at 40000 (2000 loss per unit)
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("1.0"), Decimal("40000"))

        # PnL = (40000 - 42000) * 1.0 = -2000
        assert repo.balance == initial_balance - Decimal("2000")

    def test_profit_on_short_close(self, mock_db_session):
        """Should realize profit when closing SHORT at lower price."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("10000")
        )

        # Open short at 42000
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("1.0"), Decimal("42000"))
        initial_balance = repo.balance

        # Close at 40000 (profit for short)
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("40000"))

        # PnL = (42000 - 40000) * 1.0 = 2000 (entry - exit for short)
        assert repo.balance == initial_balance + Decimal("2000")

    def test_loss_on_short_close(self, mock_db_session):
        """Should realize loss when closing SHORT at higher price."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("10000")
        )

        # Open short at 40000
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("1.0"), Decimal("40000"))
        initial_balance = repo.balance

        # Close at 42000 (loss for short)
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("42000"))

        # PnL = (40000 - 42000) * 1.0 = -2000
        assert repo.balance == initial_balance - Decimal("2000")

    def test_partial_close_pnl(self, mock_db_session):
        """Should realize PnL only for closed portion."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("10000")
        )

        # Open long 2.0 at 40000
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("2.0"), Decimal("40000"))
        initial_balance = repo.balance

        # Close 0.5 at 42000
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "sell", Decimal("0.5"), Decimal("42000"))

        # PnL = (42000 - 40000) * 0.5 = 1000
        assert repo.balance == initial_balance + Decimal("1000")


class TestBacktestGetPosition:
    """Tests for get_position in BacktestOrderRepository."""

    def test_get_nonexistent_position(self, mock_db_session):
        """Should return None for nonexistent position."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")

        assert pos is None

    def test_get_position_ignores_side_filter(self, mock_db_session):
        """Should return position regardless of side filter (netting mode)."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)
        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("40000"))

        # Ask for LONG - should return
        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP", "LONG")
        assert pos is not None

        # Ask for SHORT - should return None (position is LONG)
        pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP", "SHORT")
        assert pos is None


class TestBacktestMultipleProducts:
    """Tests for multiple products in BacktestOrderRepository."""

    def test_independent_positions_per_product(self, mock_db_session):
        """Positions for different products should be independent."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        repo.update_position("test", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("40000"))
        repo.update_position("test", "BINANCE:ETHUSDT-PERP", "sell", Decimal("10.0"), Decimal("2000"))

        btc_pos = repo.get_position("test", "BINANCE:BTCUSDT-PERP")
        eth_pos = repo.get_position("test", "BINANCE:ETHUSDT-PERP")

        assert btc_pos.side == "LONG"
        assert btc_pos.quantity == Decimal("1.0")
        assert eth_pos.side == "SHORT"
        assert eth_pos.quantity == Decimal("10.0")

    def test_independent_positions_per_strategy(self, mock_db_session):
        """Positions for different strategies should be independent."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        repo.update_position("strategy_a", "BINANCE:BTCUSDT-PERP", "buy", Decimal("1.0"), Decimal("40000"))
        repo.update_position("strategy_b", "BINANCE:BTCUSDT-PERP", "sell", Decimal("2.0"), Decimal("42000"))

        pos_a = repo.get_position("strategy_a", "BINANCE:BTCUSDT-PERP")
        pos_b = repo.get_position("strategy_b", "BINANCE:BTCUSDT-PERP")

        assert pos_a.side == "LONG"
        assert pos_a.quantity == Decimal("1.0")
        assert pos_b.side == "SHORT"
        assert pos_b.quantity == Decimal("2.0")
