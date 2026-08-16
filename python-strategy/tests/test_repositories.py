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

from contextlib import nullcontext
from decimal import Decimal
import inspect

import pytest

from src.core.interfaces.repository import IOrderRepository
from src.core.models import OrderStatus
from src.core.repositories import BacktestOrderRepository, LiveOrderRepository
from src.core.orm_models import Order, Position, Trade


class TestOrderRepositoryInterface:
    """Contract checks for order repository session lifecycle."""

    def test_interface_documents_session_factory_constructor(self):
        params = inspect.signature(IOrderRepository.__init__).parameters

        assert "db_session_factory" in params

    def test_live_repository_accepts_session_factory_parameter(self):
        params = inspect.signature(LiveOrderRepository.__init__).parameters

        assert "db_session_factory" in params

    def test_backtest_repository_accepts_session_factory_parameter(self):
        params = inspect.signature(BacktestOrderRepository.__init__).parameters

        assert "db_session_factory" in params


class TestBacktestOrderRepositoryBasics:
    """Basic tests for BacktestOrderRepository."""

    def test_initialization(self, mock_db_session):
        """Should initialize with correct defaults."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        assert repo.session_id == 1
        assert repo.balance == Decimal("10000")
        assert not hasattr(repo, "db")

    def test_initialization_custom_balance(self, mock_db_session):
        """Should accept custom initial balance."""
        repo = BacktestOrderRepository(
            mock_db_session, session_id=1, initial_balance=Decimal("50000")
        )

        assert repo.balance == Decimal("50000")

    def test_add_order_is_noop(self, mock_db_session, order_factory):
        """add_order should be no-op in backtest (orders not persisted)."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order = order_factory()

        repo.add_order(order)

        assert repo._order_strategy_map[order.id] == order.strategy_id
        mock_db_session.add.assert_not_called()
        mock_db_session.commit.assert_not_called()
        mock_db_session.refresh.assert_not_called()

    def test_update_order_is_noop(self, mock_db_session, order_factory):
        """update_order should be no-op in backtest."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order = order_factory()

        repo.update_order(order)

        mock_db_session.add.assert_not_called()
        mock_db_session.commit.assert_not_called()
        mock_db_session.refresh.assert_not_called()

    def test_get_order_returns_none(self, mock_db_session):
        """Backtest repo does not persist orders locally."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        assert repo.get_order("order-1") is None

    def test_get_order_by_client_order_id_returns_none(self, mock_db_session):
        """Backtest repo does not persist client order IDs locally."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        assert repo.get_order_by_client_order_id("client-1") is None

    def test_list_client_orders_by_statuses_returns_empty(self, mock_db_session):
        """Backtest repo does not persist recoverable client orders locally."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        assert repo.list_client_orders_by_statuses({"NEW", "SUBMITTED"}) == []


class TestBacktestPositionDelegation:
    """Position/balance operations are delegated to Rust engine."""

    def test_update_position_is_noop(self, mock_db_session):
        """update_position should be no-op (Rust engine handles positions)."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)

        # Should not raise and should not change balance
        repo.update_position(
            "test",
            "BINANCE:BTCUSDT-PERP",
            "buy",
            Decimal("1.0"),
            Decimal("42000"),
            "BUY",
        )

        assert repo.balance == Decimal("10000")
        mock_db_session.add.assert_not_called()
        mock_db_session.commit.assert_not_called()

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

    def test_accepts_session_factory(self, mock_db_session):
        """Backtest trade logging should use an injected session factory."""
        repo = BacktestOrderRepository(
            None,
            session_id=42,
            db_session_factory=lambda: nullcontext(mock_db_session),
        )
        trade = Trade(
            order_id="order-1",
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

        mock_db_session.add.assert_called()
        mock_db_session.commit.assert_called()

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

    def test_add_trade_assigns_monotonic_fill_sequence(
        self,
        mock_db_session,
        order_factory,
    ):
        repo = BacktestOrderRepository(mock_db_session, session_id=42)
        order_id = order_factory().id

        for trade_id in ("trade-1", "trade-2"):
            repo.add_trade(
                Trade(
                    id=trade_id,
                    order_id=order_id,
                    exchange_trade_id=f"sim-{trade_id}",
                    product_id="BINANCE:BTCUSDT-PERP",
                    side="buy",
                    price=Decimal("42000"),
                    quantity=Decimal("1"),
                    fee=Decimal("0"),
                    fee_asset="USDT",
                    timestamp=1704067200000,
                )
            )

        persisted = [call.args[0] for call in mock_db_session.add.call_args_list]
        assert [trade.fill_sequence for trade in persisted] == [0, 1]

    def test_failed_commit_does_not_consume_fill_sequence(
        self,
        mock_db_session,
        order_factory,
    ):
        repo = BacktestOrderRepository(mock_db_session, session_id=42)
        order_id = order_factory().id
        mock_db_session.commit.side_effect = [RuntimeError("commit failed"), None]

        def trade(trade_id):
            return Trade(
                id=trade_id,
                order_id=order_id,
                exchange_trade_id=f"sim-{trade_id}",
                product_id="BINANCE:BTCUSDT-PERP",
                side="buy",
                price=Decimal("42000"),
                quantity=Decimal("1"),
                fee=Decimal("0"),
                fee_asset="USDT",
                timestamp=1704067200000,
            )

        with pytest.raises(RuntimeError, match="commit failed"):
            repo.add_trade(trade("failed"))
        repo.add_trade(trade("retry"))

        persisted = [call.args[0] for call in mock_db_session.add.call_args_list]
        assert [item.fill_sequence for item in persisted] == [0, 0]

    def test_update_order_exchange_id(self, mock_db_session, order_factory):
        """update_order_exchange_id should set exchange_order_id on order."""
        repo = BacktestOrderRepository(mock_db_session, session_id=1)
        order = order_factory()

        repo.update_order_exchange_id(order, "SIM-abc123")

        assert order.exchange_order_id == "SIM-abc123"


# =============================================================================
# LiveOrderRepository
# =============================================================================


class TestLiveOrderRepositoryBasics:
    def test_missing_session_fails_only_when_database_work_begins(self):
        repo = LiveOrderRepository()

        with pytest.raises(RuntimeError, match="^database session is required$"):
            repo.get_order("order-1")

    def test_accepts_session_factory(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        """Live repository should use an injected session factory."""
        repo = LiveOrderRepository(
            db_session_factory=sqlite_order_session_factory,
        )
        order = order_factory()

        repo.add_order(order)

        assert not hasattr(repo, "db")
        with sqlite_order_session_factory() as session:
            assert session.get(Order, order.id) is not None

    def test_add_order_commits(self, sqlite_order_session_factory, order_factory):
        """add_order should add to session and commit."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        order = order_factory()

        repo.add_order(order)

        with sqlite_order_session_factory() as session:
            persisted = session.get(Order, order.id)
            assert persisted is not None
            assert persisted.product_id == "BINANCE:BTCUSDT-PERP"

    def test_update_order_commits(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        """update_order should add to session and commit."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        order = order_factory()
        repo.add_order(order)
        order.status = "closed"

        repo.update_order(order)

        assert order.status == "closed"
        with sqlite_order_session_factory() as session:
            persisted = session.get(Order, order.id)
            assert persisted is not None
            assert persisted.status == "closed"

    def test_get_order_by_id(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        """get_order should return the persisted order with the requested ID."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        order = order_factory(order_id="order-1")
        repo.add_order(order)

        result = repo.get_order("order-1")

        assert result is not None
        assert result.id == "order-1"

    def test_cancellation_snapshot_and_command_are_account_scoped(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        unbound = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        current_order = order_factory(
            order_id="current",
            account_profile="binance",
            account_id="ACCOUNT-A",
            product_id="BINANCE:BTCUSDT-PERP",
            client_order_id="shared-client",
            exchange_order_id="shared-exchange",
            status=OrderStatus.SUBMITTED.value,
            filled_quantity=Decimal("0.25"),
        )
        foreign_order = order_factory(
            order_id="foreign",
            account_profile="binance",
            account_id="ACCOUNT-B",
            product_id="BINANCE:BTCUSDT-PERP",
            client_order_id="shared-client",
            exchange_order_id="shared-exchange",
            status=OrderStatus.SUBMITTED.value,
        )
        unbound.add_order(current_order)
        unbound.add_order(foreign_order)
        current = LiveOrderRepository(
            db_session_factory=sqlite_order_session_factory,
            account_profile="binance",
            account_id="ACCOUNT-A",
        )

        snapshot = current.get_order_for_cancellation("current")

        assert snapshot is not None
        assert (
            snapshot.id,
            snapshot.product_id,
            snapshot.type,
            snapshot.status,
            snapshot.filled_quantity,
            snapshot.client_order_id,
            snapshot.exchange_order_id,
        ) == (
            "current",
            "BINANCE:BTCUSDT-PERP",
            current_order.type,
            OrderStatus.SUBMITTED.value,
            Decimal("0.25"),
            "shared-client",
            "shared-exchange",
        )
        assert current.get_order_for_cancellation("foreign") is None

        current.mark_order_cancelled("current")

        assert current.get_order("current").status == OrderStatus.CANCELLED.value
        assert unbound.get_order("foreign").status == OrderStatus.SUBMITTED.value
        with pytest.raises(RuntimeError, match="^cancellation_order_not_found$"):
            current.mark_order_cancelled("foreign")

    def test_conditional_order_commands_are_account_scoped(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        unbound = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        current_order = order_factory(
            order_id="current-conditional",
            account_profile="binance",
            account_id="ACCOUNT-A",
            exchange_id="BINANCE",
            status=OrderStatus.NEW.value,
        )
        foreign_order = order_factory(
            order_id="foreign-conditional",
            account_profile="binance",
            account_id="ACCOUNT-B",
            exchange_id="BINANCE",
            status=OrderStatus.NEW.value,
        )
        unbound.add_order(current_order)
        unbound.add_order(foreign_order)
        current = LiveOrderRepository(
            db_session_factory=sqlite_order_session_factory,
            account_profile="binance",
            account_id="ACCOUNT-A",
        )

        loaded = current.get_conditional_order("current-conditional")
        assert loaded is not None
        assert current.get_conditional_order("foreign-conditional") is None
        assert [
            order.id
            for order in current.list_conditional_orders_by_statuses(
                {OrderStatus.NEW.value},
                exchange_id="binance",
            )
        ] == ["current-conditional"]

        loaded.intent_payload = {"placement_mode": "place-after-fill"}
        current.persist_conditional_order(loaded)

        assert current.get_order("current-conditional").intent_payload == {
            "placement_mode": "place-after-fill"
        }
        assert unbound.get_order("foreign-conditional").intent_payload is None

    def test_backtest_conditional_port_retains_noop_semantics(
        self,
        mock_db_session,
        order_factory,
    ):
        repository = BacktestOrderRepository(mock_db_session, session_id=1)
        order = order_factory()

        repository.persist_conditional_order(order)

        assert repository.get_conditional_order(order.id) is None
        assert (
            repository.list_conditional_orders_by_statuses({OrderStatus.NEW.value})
            == []
        )
        mock_db_session.add.assert_not_called()
        mock_db_session.commit.assert_not_called()

    def test_backtest_cancellation_port_has_no_persisted_order(
        self,
        mock_db_session,
    ):
        repository = BacktestOrderRepository(mock_db_session, session_id=1)

        assert repository.get_order_for_cancellation("missing") is None
        with pytest.raises(RuntimeError, match="^cancellation_order_not_found$"):
            repository.mark_order_cancelled("missing")

    def test_get_order_by_client_order_id(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        """get_order_by_client_order_id should return the matching order."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        order = order_factory(order_id="order-1", client_order_id="client-1")
        repo.add_order(order)

        result = repo.get_order_by_client_order_id("client-1")

        assert result is not None
        assert result.id == "order-1"

    def test_bound_repository_scopes_same_provider_ids_by_account(
        self,
        sqlite_order_session_factory,
        order_factory,
    ) -> None:
        unbound = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        for account_id in ("ACCOUNT-A", "ACCOUNT-B"):
            unbound.add_order(
                order_factory(
                    order_id=f"order-{account_id}",
                    client_order_id="shared-client",
                    exchange_order_id="shared-exchange",
                    account_profile="binance",
                    account_id=account_id,
                    status="SUBMITTED",
                )
            )
        current = LiveOrderRepository(
            db_session_factory=sqlite_order_session_factory,
            account_profile="binance",
            account_id="ACCOUNT-A",
        )

        assert current.get_order("order-ACCOUNT-B") is None
        assert current.get_order_by_client_order_id("shared-client").id == (
            "order-ACCOUNT-A"
        )
        assert (
            current.get_order_by_exchange_order_id(
                "shared-exchange",
                exchange_id="BINANCE",
            ).id
            == "order-ACCOUNT-A"
        )
        assert [
            order.id
            for order in current.list_client_orders_by_statuses(
                {"SUBMITTED"}, exchange_id="BINANCE"
            )
        ] == ["order-ACCOUNT-A"]

    def test_bound_repository_rejects_legacy_identifier_collision(
        self,
        sqlite_order_session_factory,
        order_factory,
    ) -> None:
        unbound = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        unbound.add_order(
            order_factory(
                order_id="legacy",
                client_order_id="shared-client",
                exchange_order_id="shared-exchange",
            )
        )
        current = LiveOrderRepository(
            db_session_factory=sqlite_order_session_factory,
            account_profile="binance",
            account_id="ACCOUNT-A",
        )

        with pytest.raises(
            RuntimeError,
            match="^order_account_identity_legacy_collision$",
        ):
            current.add_order(
                order_factory(
                    order_id="identified",
                    client_order_id="shared-client",
                    exchange_order_id="shared-exchange",
                )
            )

        assert current.list_legacy_orders_by_statuses({"open"})[0].id == "legacy"
        assert unbound.list_legacy_orders_by_statuses({"open"}) == []

    def test_list_client_orders_by_statuses_filters_status_and_client_id(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        """Recoverable order query should require target status and client ID."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        repo.add_order(
            order_factory(
                order_id="recoverable",
                exchange_order_id=None,
                client_order_id="client-1",
                status="SUBMITTED",
            )
        )
        repo.add_order(
            order_factory(
                order_id="missing-client-id",
                exchange_order_id="EX-2",
                client_order_id=None,
                status="SUBMITTED",
            )
        )
        repo.add_order(
            order_factory(
                order_id="terminal",
                exchange_order_id=None,
                client_order_id="client-2",
                status="FILLED",
            )
        )

        result = repo.list_client_orders_by_statuses({"NEW", "SUBMITTED"})

        assert [order.id for order in result] == ["recoverable"]

    def test_list_client_orders_by_statuses_skips_empty_statuses(
        self,
        sqlite_order_session_factory,
    ):
        """Empty status sets should return no orders."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)

        assert repo.list_client_orders_by_statuses(set()) == []

    @pytest.mark.parametrize(
        ("exchange_id", "expected_ids"),
        [
            ("binance", ["current"]),
            ("BINANCE", ["current"]),
            ("bin", []),
            ("binance ", []),
            ("", []),
            (None, ["current", "foreign"]),
        ],
    )
    @pytest.mark.parametrize(
        "status",
        ["NEW", "SUBMITTED_UNCONFIRMED", "SUBMITTED", "PARTIALLY_FILLED"],
    )
    def test_order_status_queries_apply_exact_optional_venue_scope(
        self,
        sqlite_order_session_factory,
        order_factory,
        exchange_id,
        expected_ids,
        status,
    ):
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        repo.add_order(
            order_factory(
                order_id="current",
                exchange_id="BINANCE",
                client_order_id="current-client",
                status=status,
            )
        )
        repo.add_order(
            order_factory(
                order_id="foreign",
                exchange_id="BYBIT",
                product_id="BYBIT:BTCUSDT-PERP",
                client_order_id="foreign-client",
                status=status,
            )
        )

        client_orders = repo.list_client_orders_by_statuses(
            {status},
            exchange_id=exchange_id,
        )
        all_orders = repo.list_orders_by_statuses(
            {status},
            exchange_id=exchange_id,
        )

        assert [order.id for order in client_orders] == expected_ids
        assert [order.id for order in all_orders] == expected_ids

    @pytest.mark.parametrize(
        "status",
        ["NEW", "SUBMITTED_UNCONFIRMED", "SUBMITTED", "PARTIALLY_FILLED"],
    )
    def test_in_memory_status_queries_apply_the_same_venue_scope(
        self,
        mock_order_repo,
        order_factory,
        status,
    ):
        mock_order_repo.add_order(
            order_factory(
                order_id="current",
                exchange_id="BINANCE",
                client_order_id="current-client",
                status=status,
            )
        )
        mock_order_repo.add_order(
            order_factory(
                order_id="foreign",
                exchange_id="BYBIT",
                product_id="BYBIT:BTCUSDT-PERP",
                client_order_id="foreign-client",
                status=status,
            )
        )

        assert [
            order.id
            for order in mock_order_repo.list_client_orders_by_statuses(
                {status},
                exchange_id="binance",
            )
        ] == ["current"]
        assert [
            order.id
            for order in mock_order_repo.list_orders_by_statuses(
                {status},
                exchange_id="BINANCE",
            )
        ] == ["current"]

    def test_add_trade_commits(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        """add_trade should add to session and commit."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        order = order_factory(order_id="o1")
        repo.add_order(order)
        trade = Trade(
            id="t1",
            order_id="o1",
            exchange_trade_id="t1",
            product_id="BINANCE:BTCUSDT-PERP",
            side="buy",
            price=Decimal("42000"),
            quantity=Decimal("1.0"),
            fee=Decimal("2.52"),
            fee_asset="USDT",
            timestamp=1704067200000,
        )

        repo.add_trade(trade)

        with sqlite_order_session_factory() as session:
            assert session.get(Trade, "t1") is not None

    def test_persist_fill_commits_order_and_trade_together(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        order = order_factory(order_id="atomic-order", status="open")
        repo.add_order(order)
        order.status = "closed"
        order.filled_quantity = Decimal("1")
        order.filled_price = Decimal("42000")
        trade = Trade(
            id="atomic-trade",
            order_id=order.id,
            exchange_trade_id="atomic-trade",
            product_id=order.product_id,
            side=order.side,
            price=Decimal("42000"),
            quantity=Decimal("1"),
            fee=Decimal("2.52"),
            fee_asset="USDT",
            timestamp=1704067200000,
        )
        trade_id = trade.id

        repo.persist_fill(order, trade)

        with sqlite_order_session_factory() as session:
            persisted_order = session.get(Order, order.id)
            persisted_trade = session.get(Trade, trade_id)

        assert persisted_order is not None
        assert persisted_order.status == "closed"
        assert persisted_order.filled_quantity == Decimal("1")
        assert persisted_trade is not None
        assert persisted_trade.order_id == order.id

    def test_update_order_exchange_id_commits(
        self,
        sqlite_order_session_factory,
        order_factory,
    ):
        """update_order_exchange_id should set ID and commit."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        order = order_factory()
        repo.add_order(order)

        repo.update_order_exchange_id(order, "EX-999")

        assert order.exchange_order_id == "EX-999"
        with sqlite_order_session_factory() as session:
            persisted = session.get(Order, order.id)
            assert persisted is not None
            assert persisted.exchange_order_id == "EX-999"


class TestLiveOrderRepositoryPositionUpdate:
    def test_buy_creates_new_position(self, sqlite_order_session_factory):
        """Buying when no position exists should create a new position."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)

        repo.update_position(
            strategy_id="test_strategy",
            product_id="BINANCE:BTCUSDT-PERP",
            side="buy",
            fill_quantity=Decimal("0.5"),
            fill_price=Decimal("42000"),
            position_side="LONG",
        )

        with sqlite_order_session_factory() as session:
            position = session.get(
                Position,
                ("test_strategy", "BINANCE:BTCUSDT-PERP", "LONG"),
            )
            assert position is not None
            assert position.quantity == Decimal("0.5")
            assert position.entry_price == Decimal("42000")

    def test_sell_without_position_is_noop(self, sqlite_order_session_factory):
        """Selling when no position exists should commit and return."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)

        repo.update_position(
            strategy_id="test_strategy",
            product_id="BINANCE:BTCUSDT-PERP",
            side="sell",
            fill_quantity=Decimal("0.5"),
            fill_price=Decimal("42000"),
            position_side="LONG",
        )

        with sqlite_order_session_factory() as session:
            assert (
                session.get(
                    Position,
                    ("test_strategy", "BINANCE:BTCUSDT-PERP", "LONG"),
                )
                is None
            )

    def test_buy_updates_existing_position(self, sqlite_order_session_factory):
        """Buying into existing position should average entry price."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        repo.update_position(
            strategy_id="test_strategy",
            product_id="BINANCE:BTCUSDT-PERP",
            side="buy",
            fill_quantity=Decimal("0.5"),
            fill_price=Decimal("40000"),
            position_side="LONG",
        )

        repo.update_position(
            strategy_id="test_strategy",
            product_id="BINANCE:BTCUSDT-PERP",
            side="buy",
            fill_quantity=Decimal("0.5"),
            fill_price=Decimal("44000"),
            position_side="LONG",
        )

        # New avg entry: (0.5*40000 + 0.5*44000) / 1.0 = 42000
        with sqlite_order_session_factory() as session:
            position = session.get(
                Position,
                ("test_strategy", "BINANCE:BTCUSDT-PERP", "LONG"),
            )
            assert position is not None
            assert position.entry_price == Decimal("42000")
            assert position.quantity == Decimal("1.0")

    def test_sell_reduces_position(self, sqlite_order_session_factory):
        """Selling should reduce position quantity."""
        repo = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
        repo.update_position(
            strategy_id="test_strategy",
            product_id="BINANCE:BTCUSDT-PERP",
            side="buy",
            fill_quantity=Decimal("1.0"),
            fill_price=Decimal("42000"),
            position_side="LONG",
        )

        repo.update_position(
            strategy_id="test_strategy",
            product_id="BINANCE:BTCUSDT-PERP",
            side="sell",
            fill_quantity=Decimal("0.3"),
            fill_price=Decimal("43000"),
            position_side="LONG",
        )

        with sqlite_order_session_factory() as session:
            position = session.get(
                Position,
                ("test_strategy", "BINANCE:BTCUSDT-PERP", "LONG"),
            )
            assert position is not None
            assert position.quantity == Decimal("0.7")
