"""RED test matrix for L6 live-ops safety features.

Covers:
A. OpsSafetyService.kill_switch — cancel scope, ordering, isolation, idempotency, audit
B. ExecutionEngine.flatten_position — LONG/SHORT dispatch, adapter failure
C. StrategyEngine._handle_command — KILL_SWITCH routing, unknown-command regression

All tests in this file must FAIL (NotImplementedError or AssertionError) until
the implementer fills in the stubs.  Do NOT modify these tests.
"""

from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import NetworkError
from src.core.models import Candlestick, OrderStatus, Position, PositionSide, Signal, SignalType
from src.core.ops_safety import OpsSafetyService
from src.core.orm_models import Exchange, Order, Product, Strategy
from src.core.repositories import LiveOrderRepository


# =============================================================================
# Helpers / fakes
# =============================================================================

PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
STRATEGY_ID = "strat_alpha"


def _make_order(
    order_id: str,
    status: str,
    strategy_id: str = STRATEGY_ID,
    product_id: str = PRODUCT_ID,
    side: str = "BUY",
) -> Order:
    o = Order()
    o.id = order_id
    o.strategy_id = strategy_id
    o.product_id = product_id
    o.exchange_id = "BINANCE"
    o.type = "market"
    o.side = side
    o.quantity = Decimal("1.0")
    o.status = status
    o.timestamp = 1_700_000_000_000
    o.exchange_order_id = None if status == OrderStatus.NEW.value else f"EX-{order_id}"
    o.client_order_id = f"cli-{order_id}"
    return o


def _make_position(
    strategy_id: str = STRATEGY_ID,
    product_id: str = PRODUCT_ID,
    side: PositionSide = PositionSide.LONG,
    quantity: Decimal = Decimal("2.0"),
) -> Position:
    return Position(
        strategy_id=strategy_id,
        product_id=product_id,
        side=side,
        quantity=quantity,
        entry_price=Decimal("50000"),
        unrealized_pnl=Decimal("0"),
    )


class FakeAccountService:
    """Minimal fake that exposes get_all_positions() for ops-safety tests."""

    def __init__(
        self,
        positions: list[Position] | None = None,
        balance: Decimal = Decimal("100000"),
    ) -> None:
        self._positions: list[Position] = positions or []
        self._balance = balance

    def get_balance(self) -> Decimal:
        return self._balance

    def get_all_positions(self) -> list[Position]:
        return list(self._positions)


class RecordingExecutionEngine:
    """Fake ExecutionEngine that records calls in order for sequencing tests."""

    def __init__(
        self,
        cancel_results: dict[str, bool] | None = None,
        flatten_results: dict[tuple[str, str], str | None] | None = None,
        orders_by_status: dict[str, list[Order]] | None = None,
    ) -> None:
        self.calls: list[tuple] = []
        self._cancel_results = cancel_results or {}
        self._flatten_results = flatten_results or {}
        self.order_manager = _FakeOrderManager(orders_by_status or {})
        self.adapter = None
        self.flatten_reference_prices: list[Decimal | None] = []

    def cancel_order(self, order_id: str) -> bool:
        self.calls.append(("cancel_order", order_id))
        return self._cancel_results.get(order_id, True)

    def flatten_position(
        self,
        strategy_id: str,
        product_id: str,
        side: str,
        quantity: Decimal,
        reference_price: Decimal | None = None,
    ) -> str | None:
        self.calls.append(("flatten_position", strategy_id, product_id))
        self.flatten_reference_prices.append(reference_price)
        return self._flatten_results.get((strategy_id, product_id), "flat-order-id")


class _FakeOrderManager:
    """Minimal order_manager fake for OpsSafetyService tests."""

    def __init__(self, orders_by_status: dict[str, list[Order]]) -> None:
        self._orders_by_status = orders_by_status
        self.failed_orders: list[tuple[Order, str]] = []

        # Build flat orders list for repo.list_orders_by_statuses
        all_orders: list[Order] = []
        for orders in orders_by_status.values():
            all_orders.extend(orders)
        self.repo = _FakeOrderRepo(all_orders)

    def fail_order(self, order: Order, reason: str) -> None:
        self.failed_orders.append((order, reason))


class _FakeOrderRepo:
    def __init__(self, orders: list[Order]) -> None:
        self._orders = orders

    def list_orders_by_statuses(self, statuses: set[str]) -> list[Order]:
        return [o for o in self._orders if o.status in statuses]


def _make_null_db_session_factory():
    """Returns a context-manager factory that yields a MagicMock session."""

    @contextmanager
    def factory():
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)
        yield session

    return factory


def _make_service(
    *,
    orders: list[Order] | None = None,
    positions: list[Position] | None = None,
    cancel_results: dict[str, bool] | None = None,
    flatten_results: dict[tuple[str, str], str | None] | None = None,
) -> tuple[OpsSafetyService, RecordingExecutionEngine, FakeAccountService]:
    orders_by_status: dict[str, list[Order]] = {}
    for o in orders or []:
        orders_by_status.setdefault(o.status, []).append(o)

    fake_engine = RecordingExecutionEngine(
        cancel_results=cancel_results,
        flatten_results=flatten_results,
        orders_by_status=orders_by_status,
    )
    fake_account = FakeAccountService(positions=positions)
    db_factory = _make_null_db_session_factory()
    service = OpsSafetyService(fake_engine, fake_account, db_factory)
    return service, fake_engine, fake_account


class FakeExchangePositionAdapter:
    def __init__(
        self,
        positions: list[Position] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._positions = positions or []
        self._error = error

    def get_all_positions(self) -> list[Position]:
        if self._error is not None:
            raise self._error
        return list(self._positions)


# =============================================================================
# A. OpsSafetyService.kill_switch tests
# =============================================================================


class TestKillSwitchIdempotency:
    """Matrix item 6: no open orders + no positions → already_flat=True."""

    def test_already_flat_returns_correct_shape(self):
        service, _, _ = _make_service()

        result = service.kill_switch(actor="ops", reason="drill")

        assert result["already_flat"] is True
        assert result["cancelled_orders"] == 0
        assert result["cancel_failures"] == []
        assert result["flattened_positions"] == 0
        assert result["flatten_failures"] == []

    def test_local_empty_exchange_position_is_flattened(self):
        """Kill switch must not treat an empty local cache as flat."""
        exchange_pos = _make_position(strategy_id="LIVE", quantity=Decimal("0.75"))
        service, engine, _ = _make_service(positions=[])
        engine.adapter = FakeExchangePositionAdapter(positions=[exchange_pos])

        result = service.kill_switch(actor="ops", reason="stale_redis")

        assert result["already_flat"] is False
        assert result["flattened_positions"] == 1
        assert (
            "flatten_position",
            "LIVE",
            PRODUCT_ID,
        ) in engine.calls

    def test_exchange_position_uses_local_owner_when_available(self):
        """Exchange snapshots should flatten against the local strategy owner when known."""
        local_pos = _make_position(strategy_id=STRATEGY_ID, quantity=Decimal("0.75"))
        exchange_pos = _make_position(strategy_id="LIVE", quantity=Decimal("0.75"))
        service, engine, _ = _make_service(positions=[local_pos])
        engine.adapter = FakeExchangePositionAdapter(positions=[exchange_pos])

        service.kill_switch(actor="ops", reason="stale_redis")

        assert (
            "flatten_position",
            STRATEGY_ID,
            PRODUCT_ID,
        ) in engine.calls

    def test_exchange_position_enumeration_failure_is_not_reported_flat(self):
        """If live exposure cannot be enumerated, kill switch must fail closed."""
        service, engine, _ = _make_service(positions=[])
        engine.adapter = FakeExchangePositionAdapter(
            error=RuntimeError("exchange unavailable")
        )

        result = service.kill_switch(actor="ops", reason="drill")

        assert result["already_flat"] is False
        assert result["flattened_positions"] == 0
        assert result["flatten_failures"]
        assert "exchange unavailable" in result["flatten_failures"][0]["reason"]

    def test_already_flat_audit_event_still_written(self):
        """Audit event must be written even when already flat."""
        db_factory = _make_null_db_session_factory()
        fake_engine = RecordingExecutionEngine()
        fake_account = FakeAccountService()
        service = OpsSafetyService(fake_engine, fake_account, db_factory)

        with patch("src.core.ops_safety.write_system_event") as mock_write:
            service.kill_switch(actor="ops", reason=None)
            mock_write.assert_called_once()
            kwargs = mock_write.call_args.kwargs
            assert kwargs["event_type"] == "ops"
            assert kwargs["event_subtype"] == "kill_switch"


class TestKillSwitchCancelScope:
    """Matrix items 2 & 9: cancel scope covers correct statuses."""

    def test_submitted_order_cancelled_via_engine(self):
        order = _make_order("o1", OrderStatus.SUBMITTED.value)
        service, engine, _ = _make_service(orders=[order])

        service.kill_switch(actor="ops")

        assert any(c == ("cancel_order", "o1") for c in engine.calls)

    def test_submitted_unconfirmed_cancelled_via_engine(self):
        order = _make_order("o2", OrderStatus.SUBMITTED_UNCONFIRMED.value)
        service, engine, _ = _make_service(orders=[order])

        service.kill_switch(actor="ops")

        assert any(c == ("cancel_order", "o2") for c in engine.calls)

    def test_partially_filled_cancelled_via_engine(self):
        order = _make_order("o3", OrderStatus.PARTIALLY_FILLED.value)
        service, engine, _ = _make_service(orders=[order])

        service.kill_switch(actor="ops")

        assert any(c == ("cancel_order", "o3") for c in engine.calls)

    def test_new_order_failed_locally_not_via_cancel(self):
        """NEW orders are failed locally via order_manager.fail_order."""
        order = _make_order("o-new", OrderStatus.NEW.value)
        service, engine, _ = _make_service(orders=[order])

        service.kill_switch(actor="ops")

        # fail_order must be called, cancel_order must NOT be called for NEW
        failed_ids = [o.id for o, _ in engine.order_manager.failed_orders]
        assert "o-new" in failed_ids
        assert all(c != ("cancel_order", "o-new") for c in engine.calls)

    def test_new_order_fail_reason_is_kill_switch(self):
        order = _make_order("o-new2", OrderStatus.NEW.value)
        service, engine, _ = _make_service(orders=[order])

        service.kill_switch(actor="ops")

        reasons = [reason for o, reason in engine.order_manager.failed_orders if o.id == "o-new2"]
        assert reasons == ["kill_switch"]

    def test_filled_orders_not_cancelled(self):
        """FILLED orders are terminal and must be ignored."""
        order = _make_order("o-filled", OrderStatus.FILLED.value)
        service, engine, _ = _make_service(orders=[order])

        service.kill_switch(actor="ops")

        assert all(c[1] != "o-filled" for c in engine.calls if c[0] == "cancel_order")

    def test_cancelled_orders_not_re_cancelled(self):
        order = _make_order("o-already-cancelled", OrderStatus.CANCELLED.value)
        service, engine, _ = _make_service(orders=[order])

        service.kill_switch(actor="ops")

        assert all(c[1] != "o-already-cancelled" for c in engine.calls if c[0] == "cancel_order")

    def test_result_counts_only_successful_cancels(self):
        o1 = _make_order("o-ok", OrderStatus.SUBMITTED.value)
        o2 = _make_order("o-fail", OrderStatus.SUBMITTED.value)
        service, _, _ = _make_service(
            orders=[o1, o2],
            cancel_results={"o-ok": True, "o-fail": False},
        )

        result = service.kill_switch(actor="ops")

        assert result["cancelled_orders"] == 1


class TestKillSwitchOrdering:
    """Matrix item 1: all cancellations complete before any flatten."""

    def test_all_cancels_precede_all_flattens(self):
        order = _make_order("ord-1", OrderStatus.SUBMITTED.value)
        pos = _make_position()
        service, engine, _ = _make_service(orders=[order], positions=[pos])

        service.kill_switch(actor="ops")

        cancel_indices = [i for i, c in enumerate(engine.calls) if c[0] == "cancel_order"]
        flatten_indices = [i for i, c in enumerate(engine.calls) if c[0] == "flatten_position"]

        assert cancel_indices, "no cancel_order calls recorded"
        assert flatten_indices, "no flatten_position calls recorded"
        assert max(cancel_indices) < min(flatten_indices), (
            "at least one flatten_position call occurred before all cancel_order calls"
        )


class TestKillSwitchCancelFailureIsolation:
    """Matrix item 3: one cancel failure → others attempted, flatten proceeds."""

    def test_cancel_failure_recorded_in_result(self):
        o1 = _make_order("o-good", OrderStatus.SUBMITTED.value)
        o2 = _make_order("o-bad", OrderStatus.SUBMITTED.value)
        service, _, _ = _make_service(
            orders=[o1, o2],
            cancel_results={"o-good": True, "o-bad": False},
        )

        result = service.kill_switch(actor="ops")

        failed_ids = [f["order_id"] for f in result["cancel_failures"]]
        assert "o-bad" in failed_ids

    def test_cancel_failure_does_not_stop_remaining_cancels(self):
        o1 = _make_order("o-bad", OrderStatus.SUBMITTED.value)
        o2 = _make_order("o-good", OrderStatus.SUBMITTED.value)
        service, engine, _ = _make_service(
            orders=[o1, o2],
            cancel_results={"o-bad": False, "o-good": True},
        )

        service.kill_switch(actor="ops")

        cancel_targets = {c[1] for c in engine.calls if c[0] == "cancel_order"}
        assert "o-good" in cancel_targets

    def test_cancel_exception_recorded_and_flatten_proceeds(self):
        """cancel_order raising an exception must not abort flatten."""
        order = _make_order("o-exc", OrderStatus.SUBMITTED.value)
        pos = _make_position()
        service, engine, _ = _make_service(orders=[order], positions=[pos])
        engine.cancel_order = lambda oid: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[assignment]

        # Monkeypatch so cancel raises but flatten is a proper recording
        flatten_calls: list = []
        engine.flatten_position = lambda sid, pid, side, qty: flatten_calls.append((sid, pid)) or "flat-id"  # type: ignore[assignment]

        result = service.kill_switch(actor="ops")

        assert len(result["cancel_failures"]) >= 1
        assert len(flatten_calls) >= 1, "flatten must proceed even after cancel exception"

    def test_cancel_failure_includes_reason(self):
        """cancel_failures entries must include both order_id and reason."""
        order = _make_order("o-fail", OrderStatus.SUBMITTED.value)
        service, engine, _ = _make_service(
            orders=[order], cancel_results={"o-fail": False}
        )

        result = service.kill_switch(actor="ops")

        entry = next((f for f in result["cancel_failures"] if f["order_id"] == "o-fail"), None)
        assert entry is not None
        assert "reason" in entry


class TestKillSwitchFlattenFailureIsolation:
    """Matrix item 5: one flatten failure → others attempted."""

    def test_flatten_failure_recorded_in_result(self):
        pos1 = _make_position(strategy_id="strat_a", product_id=PRODUCT_ID)
        pos2 = _make_position(
            strategy_id="strat_b",
            product_id="BINANCE:ETHUSDT-PERP",
        )
        service, _, _ = _make_service(
            positions=[pos1, pos2],
            flatten_results={("strat_b", "BINANCE:ETHUSDT-PERP"): None},
        )

        result = service.kill_switch(actor="ops")

        failed = result["flatten_failures"]
        assert any(f["strategy_id"] == "strat_b" for f in failed)

    def test_flatten_failure_does_not_stop_other_flattens(self):
        pos1 = _make_position(strategy_id="strat_a", product_id=PRODUCT_ID)
        pos2 = _make_position(
            strategy_id="strat_b",
            product_id="BINANCE:ETHUSDT-PERP",
        )
        service, engine, _ = _make_service(
            positions=[pos1, pos2],
            flatten_results={("strat_a", PRODUCT_ID): None},
        )

        service.kill_switch(actor="ops")

        flatten_calls = [(c[1], c[2]) for c in engine.calls if c[0] == "flatten_position"]
        assert ("strat_b", "BINANCE:ETHUSDT-PERP") in flatten_calls

    def test_flatten_failure_result_includes_strategy_id_and_product_id(self):
        pos = _make_position()
        service, _, _ = _make_service(
            positions=[pos],
            flatten_results={(STRATEGY_ID, PRODUCT_ID): None},
        )

        result = service.kill_switch(actor="ops")

        entry = next(
            (f for f in result["flatten_failures"] if f["strategy_id"] == STRATEGY_ID),
            None,
        )
        assert entry is not None
        assert entry["product_id"] == PRODUCT_ID
        assert "reason" in entry


class TestKillSwitchAuditAlways:
    """Matrix item 7: audit event written ALWAYS."""

    def test_audit_written_on_partial_cancel_failure(self):
        order = _make_order("o-fail", OrderStatus.SUBMITTED.value)
        db_factory = _make_null_db_session_factory()
        fake_engine = RecordingExecutionEngine(
            cancel_results={"o-fail": False},
            orders_by_status={OrderStatus.SUBMITTED.value: [order]},
        )
        fake_account = FakeAccountService()
        service = OpsSafetyService(fake_engine, fake_account, db_factory)

        with patch("src.core.ops_safety.write_system_event") as mock_write:
            service.kill_switch(actor="ops")
            mock_write.assert_called_once()
            kwargs = mock_write.call_args.kwargs
            assert kwargs["event_type"] == "ops"
            assert kwargs["event_subtype"] == "kill_switch"

    def test_audit_payload_includes_actor_and_reason(self):
        db_factory = _make_null_db_session_factory()
        fake_engine = RecordingExecutionEngine()
        fake_account = FakeAccountService()
        service = OpsSafetyService(fake_engine, fake_account, db_factory)

        with patch("src.core.ops_safety.write_system_event") as mock_write:
            service.kill_switch(actor="compliance_team", reason="eod_drill")
            kwargs = mock_write.call_args.kwargs
            payload = kwargs["payload"]
            assert payload["actor"] == "compliance_team"
            assert payload["reason"] == "eod_drill"

    def test_audit_payload_includes_full_result(self):
        db_factory = _make_null_db_session_factory()
        fake_engine = RecordingExecutionEngine()
        fake_account = FakeAccountService()
        service = OpsSafetyService(fake_engine, fake_account, db_factory)

        with patch("src.core.ops_safety.write_system_event") as mock_write:
            service.kill_switch(actor="ops")
            payload = mock_write.call_args.kwargs["payload"]
            for key in ("cancelled_orders", "cancel_failures", "flattened_positions",
                        "flatten_failures", "already_flat"):
                assert key in payload, f"payload missing key: {key}"


class TestKillSwitchFlattenPositions:
    """Matrix item 4: flatten places opposite-side call per position."""

    def test_long_position_triggers_flatten(self):
        pos = _make_position(side=PositionSide.LONG, quantity=Decimal("3.0"))
        service, engine, _ = _make_service(positions=[pos])

        service.kill_switch(actor="ops")

        flatten_calls = [c for c in engine.calls if c[0] == "flatten_position"]
        assert len(flatten_calls) == 1
        assert flatten_calls[0][1] == STRATEGY_ID
        assert flatten_calls[0][2] == PRODUCT_ID

    def test_multiple_positions_all_flattened(self):
        pos1 = _make_position(strategy_id="s1", product_id=PRODUCT_ID)
        pos2 = _make_position(strategy_id="s2", product_id="BINANCE:ETHUSDT-PERP")
        service, engine, _ = _make_service(positions=[pos1, pos2])

        service.kill_switch(actor="ops")

        flatten_calls = [(c[1], c[2]) for c in engine.calls if c[0] == "flatten_position"]
        assert ("s1", PRODUCT_ID) in flatten_calls
        assert ("s2", "BINANCE:ETHUSDT-PERP") in flatten_calls

    def test_result_counts_successful_flattens(self):
        pos = _make_position()
        service, _, _ = _make_service(positions=[pos])

        result = service.kill_switch(actor="ops")

        assert result["flattened_positions"] == 1

    def test_flatten_uses_position_entry_price_as_reference_price(self):
        pos = _make_position(quantity=Decimal("0.75"))
        service, engine, _ = _make_service(positions=[pos])

        service.kill_switch(actor="ops")

        assert engine.flatten_reference_prices == [pos.entry_price]


# =============================================================================
# B. ExecutionEngine.flatten_position tests
# =============================================================================


class TestFlattenPosition:
    """Matrix items for ExecutionEngine.flatten_position."""

    @pytest.fixture()
    def eng(self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo):
        return ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
        )

    def test_long_position_places_sell_market_order(
        self, eng, mock_exchange_adapter, mock_order_repo
    ):
        """LONG position → BUY order on adapter (sell direction), type=market."""
        result = eng.flatten_position(
            STRATEGY_ID, PRODUCT_ID, "LONG", Decimal("2.5")
        )

        # Must return an order id (non-None)
        assert result is not None

        # Adapter must have received exactly one order
        placed = mock_exchange_adapter.filled_orders or mock_exchange_adapter.open_orders
        assert len(placed) >= 1

        # The placed order must be a market SELL (flatten LONG = sell)
        placed_order = (mock_exchange_adapter.open_orders + mock_exchange_adapter.filled_orders)[-1]
        order_obj = placed_order if isinstance(placed_order, Order) else placed_order.get("order")
        assert order_obj is not None
        assert order_obj.type == "market"
        # Side convention: flatten LONG → place SELL (buy/sell convention from adapter boundary)
        assert order_obj.side.lower() in ("sell", "short")

    def test_short_position_places_buy_market_order(
        self, eng, mock_exchange_adapter
    ):
        """SHORT position → market BUY (flatten direction)."""
        result = eng.flatten_position(
            STRATEGY_ID, PRODUCT_ID, "SHORT", Decimal("1.0")
        )

        assert result is not None
        placed_order = (mock_exchange_adapter.open_orders + mock_exchange_adapter.filled_orders)[-1]
        order_obj = placed_order if isinstance(placed_order, Order) else placed_order.get("order")
        assert order_obj is not None
        assert order_obj.type == "market"
        assert order_obj.side.lower() in ("buy", "long")

    def test_order_persisted_in_order_repo(self, eng, mock_order_repo):
        """Order must be recorded via order_manager before adapter placement."""
        eng.flatten_position(STRATEGY_ID, PRODUCT_ID, "LONG", Decimal("1.0"))
        assert len(mock_order_repo.orders) >= 1

    def test_reference_price_attached_to_flatten_order(
        self, eng, mock_exchange_adapter
    ):
        """Market flatten orders need a min-notional reference price."""
        eng.flatten_position(
            STRATEGY_ID,
            PRODUCT_ID,
            "LONG",
            Decimal("1.0"),
            reference_price=Decimal("50000"),
        )

        placed_order = (
            mock_exchange_adapter.open_orders + mock_exchange_adapter.filled_orders
        )[-1]
        order_obj = placed_order if isinstance(placed_order, Order) else placed_order.get("order")
        assert order_obj.min_notional_reference_price == Decimal("50000")

    def test_adapter_error_returns_none(self, eng, mock_exchange_adapter):
        """Adapter exception → returns None, does not propagate."""
        mock_exchange_adapter.set_should_fail(True, reason="exchange offline")

        result = eng.flatten_position(STRATEGY_ID, PRODUCT_ID, "LONG", Decimal("1.0"))

        assert result is None

    def test_adapter_error_local_order_marked_failed(
        self, eng, mock_exchange_adapter, mock_order_repo
    ):
        """When adapter raises, the local order must be in a terminal (FAILED/CANCELLED) state."""
        mock_exchange_adapter.set_should_fail(True, reason="exchange offline")

        eng.flatten_position(STRATEGY_ID, PRODUCT_ID, "LONG", Decimal("1.0"))

        terminal_statuses = {OrderStatus.FAILED.value, OrderStatus.CANCELLED.value, "failed"}
        orders_in_terminal = [
            o for o in mock_order_repo.orders.values() if o.status in terminal_statuses
        ]
        assert len(orders_in_terminal) >= 1

    def test_ambiguous_submit_leaves_flatten_order_recoverable(
        self, eng, mock_order_repo
    ):
        """Timeout-like flatten errors must not mark the local order failed."""

        class TimeoutAdapter:
            def place_order(self, order):
                raise NetworkError("timeout after submit")

            def get_order_by_client_id(self, client_order_id, product_id, *, order_type=None):
                return None

        eng.adapter = TimeoutAdapter()

        result = eng.flatten_position(
            STRATEGY_ID,
            PRODUCT_ID,
            "LONG",
            Decimal("1.0"),
            reference_price=Decimal("50000"),
        )

        assert result is None
        order = next(iter(mock_order_repo.orders.values()))
        assert order.client_order_id is not None
        assert order.status == OrderStatus.SUBMITTED_UNCONFIRMED.value

    def test_live_position_uses_reserved_ops_strategy_with_real_fk(
        self,
        tmp_path,
        mock_clock,
    ):
        """Exchange-only LIVE positions must persist flatten orders in a real DB."""
        engine = create_engine(f"sqlite:///{tmp_path / 'ops_flatten.db'}")

        @event.listens_for(engine, "connect")
        def _enable_fk(dbapi_connection, _connection_record):
            dbapi_connection.execute("PRAGMA foreign_keys=ON")

        for table in [Exchange.__table__, Product.__table__, Strategy.__table__]:
            table.create(engine, checkfirst=True)
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE "order" (
                        id VARCHAR PRIMARY KEY,
                        exchange_order_id VARCHAR,
                        strategy_id VARCHAR NOT NULL REFERENCES strategy(id),
                        product_id VARCHAR NOT NULL REFERENCES product(id),
                        exchange_id VARCHAR NOT NULL REFERENCES exchange(id),
                        type VARCHAR NOT NULL,
                        side VARCHAR NOT NULL,
                        price NUMERIC,
                        trigger_price NUMERIC,
                        quantity NUMERIC NOT NULL,
                        status VARCHAR NOT NULL,
                        timestamp BIGINT NOT NULL,
                        filled_quantity NUMERIC,
                        filled_price NUMERIC,
                        client_order_id VARCHAR(128),
                        intent_payload TEXT,
                        submitted_at DATETIME,
                        acked_at DATETIME,
                        last_reconciled_at DATETIME,
                        UNIQUE(exchange_order_id, exchange_id)
                    )
                    """
                )
            )
        Session = sessionmaker(bind=engine)
        with Session() as session:
            session.add(Exchange(id="BINANCE", name="Binance"))
            session.add(
                Product(
                    id=PRODUCT_ID,
                    exchange_id="BINANCE",
                    base_asset="BTC",
                    quote_asset="USDT",
                )
            )
            session.commit()

        class AcceptingAdapter:
            def place_order(self, order):
                return "EX-FLAT"

        session_factory = sessionmaker(bind=engine)
        eng = ExecutionEngine(
            db_session=Session(),
            clock=mock_clock,
            adapter=AcceptingAdapter(),
            order_repository=LiveOrderRepository(db_session_factory=session_factory),
            is_backtest=True,
        )

        order_id = eng.flatten_position("LIVE", PRODUCT_ID, "LONG", Decimal("1.0"))

        assert order_id is not None
        with Session() as session:
            strategy = session.get(Strategy, "__ops_kill_switch__")
            assert strategy is not None
            order = session.get(Order, order_id)
        assert order.strategy_id == "__ops_kill_switch__"


class TestKillSwitchHalting:
    def test_kill_switch_halts_followup_strategy_signals(self, engine_factory):
        engine = engine_factory()
        engine.ops_safety.kill_switch = MagicMock(
            return_value={
                "cancelled_orders": 0,
                "cancel_failures": [],
                "flattened_positions": 0,
                "flatten_failures": [],
                "already_flat": True,
            }
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "ok"))
        engine.execution_engine.execute_signal = MagicMock(return_value="order-1")

        engine._handle_command({"command": "KILL_SWITCH", "params": {"actor": "ops"}})
        engine.process_signal(
            Signal(
                strategy_id=STRATEGY_ID,
                product_id=PRODUCT_ID,
                timeframe="1m",
                timestamp=1_700_000_000_000,
                type=SignalType.LONG,
                quantity=Decimal("0.1"),
            ),
            Candlestick(
                product_id=PRODUCT_ID,
                timeframe="1m",
                timestamp=1_700_000_000_000,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100"),
                volume=Decimal("1"),
            ),
        )

        engine.risk_manager.check_risk.assert_not_called()
        engine.execution_engine.execute_signal.assert_not_called()


# =============================================================================
# C. StrategyEngine._handle_command — KILL_SWITCH routing
# =============================================================================


class TestEngineKillSwitchCommand:
    """Matrix items for engine._handle_command routing of KILL_SWITCH."""

    def test_kill_switch_command_routes_to_ops_safety(self, engine_factory):
        """KILL_SWITCH dispatches to self.ops_safety.kill_switch with correct kwargs."""
        engine = engine_factory()
        mock_ops_safety = MagicMock()
        mock_ops_safety.kill_switch.return_value = {
            "cancelled_orders": 0,
            "cancel_failures": [],
            "flattened_positions": 0,
            "flatten_failures": [],
            "already_flat": True,
        }
        engine.ops_safety = mock_ops_safety

        engine._handle_command(
            {"command": "KILL_SWITCH", "params": {"actor": "ops", "reason": "drill"}}
        )

        mock_ops_safety.kill_switch.assert_called_once_with(actor="ops", reason="drill")

    def test_kill_switch_default_actor_when_not_provided(self, engine_factory):
        """When actor absent, default to 'operator'."""
        engine = engine_factory()
        mock_ops_safety = MagicMock()
        mock_ops_safety.kill_switch.return_value = {
            "cancelled_orders": 0, "cancel_failures": [],
            "flattened_positions": 0, "flatten_failures": [], "already_flat": True,
        }
        engine.ops_safety = mock_ops_safety

        engine._handle_command({"command": "KILL_SWITCH", "params": {}})

        mock_ops_safety.kill_switch.assert_called_once_with(actor="operator", reason=None)

    def test_unknown_command_delegated_to_command_router(self, engine_factory):
        """Regression: unknown commands must still reach _command_router.handle()."""
        engine = engine_factory()
        engine._command_router.handle = MagicMock(
            return_value=MagicMock(success=True, message="ok")
        )

        data = {"command": "SOME_FUTURE_COMMAND"}
        engine._handle_command(data)

        engine._command_router.handle.assert_called_once_with(data)


# =============================================================================
# D. Fix 1 — _positions() local fetch failure isolation
# =============================================================================


class FakeFailingAccountService:
    """Account service whose get_all_positions always raises."""

    def __init__(self, exc: Exception | None = None) -> None:
        self._exc = exc or RuntimeError("Redis connection refused")

    def get_balance(self) -> Decimal:
        return Decimal("0")

    def get_all_positions(self) -> list:
        raise self._exc


def _make_service_with_failing_account(
    *,
    adapter: object | None = None,
    exc: Exception | None = None,
) -> tuple[OpsSafetyService, RecordingExecutionEngine]:
    fake_engine = RecordingExecutionEngine()
    fake_engine.adapter = adapter
    account = FakeFailingAccountService(exc=exc)
    db_factory = _make_null_db_session_factory()
    service = OpsSafetyService(fake_engine, account, db_factory)
    return service, fake_engine


class TestLocalFetchFailureIsolation:
    """Fix 1: local position fetch failure must not suppress adapter flatten."""

    def test_local_raises_adapter_has_positions_all_flattened(self):
        """account_service raises + adapter has positions → every exchange position is flattened."""
        exc = RuntimeError("Redis down")
        exchange_pos = _make_position(strategy_id="LIVE", quantity=Decimal("1.5"))
        service, engine = _make_service_with_failing_account(
            adapter=FakeExchangePositionAdapter(positions=[exchange_pos]),
            exc=exc,
        )

        result = service.kill_switch(actor="ops")

        assert result["flattened_positions"] == 1
        assert any(c[0] == "flatten_position" for c in engine.calls)

    def test_local_raises_adapter_has_positions_flatten_failures_contains_unavailable_entry(self):
        """flatten_failures must include a local_positions_unavailable entry."""
        exc = RuntimeError("Redis down")
        exchange_pos = _make_position(strategy_id="LIVE", quantity=Decimal("1.5"))
        service, engine = _make_service_with_failing_account(
            adapter=FakeExchangePositionAdapter(positions=[exchange_pos]),
            exc=exc,
        )

        result = service.kill_switch(actor="ops")

        unavailable = [
            f for f in result["flatten_failures"]
            if "local_positions_unavailable" in f.get("reason", "")
        ]
        assert unavailable, (
            "Expected a local_positions_unavailable entry in flatten_failures"
        )

    def test_local_raises_adapter_has_positions_already_flat_false(self):
        """already_flat must be False when local fetch failed (cannot confirm flat state)."""
        exchange_pos = _make_position(strategy_id="LIVE", quantity=Decimal("1.5"))
        service, _ = _make_service_with_failing_account(
            adapter=FakeExchangePositionAdapter(positions=[exchange_pos]),
        )

        result = service.kill_switch(actor="ops")

        assert result["already_flat"] is False

    def test_local_raises_adapter_no_enumeration_failure_recorded(self):
        """account_service raises + adapter has no enumeration method → failure recorded."""

        class NoEnumAdapter:
            """Adapter without get_all_positions or list_positions."""

        service, _ = _make_service_with_failing_account(adapter=NoEnumAdapter())

        result = service.kill_switch(actor="ops")

        assert result["flatten_failures"], (
            "Expected a flatten_failure entry when both local and adapter enumeration fail"
        )

    def test_local_raises_adapter_no_enumeration_already_flat_false(self):
        """already_flat must be False when enumeration is completely unavailable."""

        class NoEnumAdapter:
            pass

        service, _ = _make_service_with_failing_account(adapter=NoEnumAdapter())

        result = service.kill_switch(actor="ops")

        assert result["already_flat"] is False

    def test_local_raises_adapter_no_enumeration_no_flatten_attempted(self):
        """No flatten should be attempted when the position list cannot be determined."""

        class NoEnumAdapter:
            pass

        service, engine = _make_service_with_failing_account(adapter=NoEnumAdapter())

        result = service.kill_switch(actor="ops")

        assert result["flattened_positions"] == 0
        assert not any(c[0] == "flatten_position" for c in engine.calls)


@pytest.mark.parametrize(
    "local_ok, adapter_has_enum, expected_flattened, expect_local_error_entry",
    [
        # local ok + adapter has get_all_positions → attributed flatten, no local error entry
        (True, True, 1, False),
        # local ok + no adapter → uses local positions, no local error entry
        (True, False, 1, False),
        # local error + adapter has get_all_positions → adapter flatten, local_error entry
        (False, True, 1, True),
        # local error + no adapter → no flatten, failure recorded, no "local_positions_unavailable" entry
        #   (the re-raised exc is caught as a generic enumeration failure)
        (False, False, 0, False),
    ],
    ids=["ok/enum", "ok/none", "error/enum", "error/none"],
)
def test_positions_fetch_matrix(
    local_ok: bool,
    adapter_has_enum: bool,
    expected_flattened: int,
    expect_local_error_entry: bool,
):
    """2×2 matrix: local {ok, error} × adapter {has get_all_positions, has neither}."""
    exchange_pos = _make_position(strategy_id="LIVE", quantity=Decimal("1.0"))
    local_pos = _make_position(strategy_id=STRATEGY_ID, quantity=Decimal("1.0"))

    if adapter_has_enum:
        adapter: object = FakeExchangePositionAdapter(positions=[exchange_pos])
    else:
        # No adapter attached — positions come from local only (or fail entirely)
        adapter = None

    if local_ok:
        account = FakeAccountService(positions=[local_pos])
        fake_engine = RecordingExecutionEngine()
        fake_engine.adapter = adapter
        service = OpsSafetyService(fake_engine, account, _make_null_db_session_factory())
    else:
        fake_engine = RecordingExecutionEngine()
        fake_engine.adapter = adapter
        account = FakeFailingAccountService()
        service = OpsSafetyService(fake_engine, account, _make_null_db_session_factory())

    result = service.kill_switch(actor="ops")

    assert result["flattened_positions"] == expected_flattened, (
        f"Expected flattened={expected_flattened}, got {result['flattened_positions']}"
    )

    if expect_local_error_entry:
        unavailable = [
            f for f in result["flatten_failures"]
            if "local_positions_unavailable" in f.get("reason", "")
        ]
        assert unavailable, "Expected local_positions_unavailable entry in flatten_failures"
    else:
        unavailable = [
            f for f in result["flatten_failures"]
            if "local_positions_unavailable" in f.get("reason", "")
        ]
        assert not unavailable, (
            "Did not expect local_positions_unavailable entry in flatten_failures"
        )


# =============================================================================
# E. Fix 2 — Submission drain gate
# =============================================================================

import threading  # noqa: E402 — stdlib, safe to import here


class BlockingAdapter:
    """Adapter whose place_order blocks until released."""

    def __init__(self) -> None:
        self._block = threading.Event()
        self._placed = threading.Event()

    def release(self) -> None:
        self._block.set()

    def place_order(self, order) -> str:
        self._placed.set()
        self._block.wait()
        order.exchange_order_id = "BLOCKED-EX-001"
        return "BLOCKED-EX-001"

    def get_order_by_client_id(self, client_order_id, product_id, *, order_type=None):
        return None


def _make_drain_engine(adapter=None, mock_db_session=None, mock_clock=None):
    """Build a real ExecutionEngine with an in-memory repo for drain tests."""
    from tests.conftest import MockOrderRepository, MockClock
    from unittest.mock import MagicMock

    repo = MockOrderRepository()
    clk = mock_clock if mock_clock is not None else MockClock()
    db = mock_db_session if mock_db_session is not None else MagicMock()
    adp = adapter if adapter is not None else MagicMock()
    return ExecutionEngine(
        db_session=db,
        clock=clk,
        adapter=adp,
        order_repository=repo,
    )


def _make_signal(
    strategy_id: str = STRATEGY_ID,
    product_id: str = PRODUCT_ID,
    quantity: Decimal = Decimal("0.1"),
) -> Signal:
    return Signal(
        strategy_id=strategy_id,
        product_id=product_id,
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.LONG,
        quantity=quantity,
    )


class TestSubmissionDrainGate:
    """Fix 2: drain gate prevents TOCTOU between execute_signal and kill_switch."""

    def test_post_halt_execute_signal_returns_none_without_calling_adapter(self):
        """After halt_and_drain, execute_signal must return None immediately."""
        from unittest.mock import patch

        eng = _make_drain_engine()
        eng.halt_and_drain(timeout=0.01)

        signal = _make_signal()
        with patch.object(eng, "_execute_signal_core", wraps=eng._execute_signal_core) as mock_core:
            result = eng.execute_signal(signal)

        assert result is None
        mock_core.assert_not_called()

    def test_inflight_drain_waits_for_order_before_snapshot(self):
        """kill_switch waits until an in-flight execute_signal completes."""
        blocking_adapter = BlockingAdapter()
        eng = _make_drain_engine(adapter=blocking_adapter)

        signal = _make_signal()

        # Start execute_signal in a background thread.
        submit_thread = threading.Thread(target=eng.execute_signal, args=(signal,), daemon=True)
        submit_thread.start()

        # Wait until the adapter's place_order is actually blocking.
        placed = blocking_adapter._placed.wait(timeout=2.0)
        assert placed, "place_order was never called — test setup issue"

        # Call halt_and_drain; it must block until we release the adapter.
        drain_done = threading.Event()
        drain_result: list[bool] = []

        def do_drain():
            drain_result.append(eng.halt_and_drain(timeout=5.0))
            drain_done.set()

        drain_thread = threading.Thread(target=do_drain, daemon=True)
        drain_thread.start()

        # Give drain a moment to start waiting.
        import time
        time.sleep(0.05)
        # Drain must still be waiting (in_flight > 0).
        assert not drain_done.is_set(), "halt_and_drain returned before order was placed"

        # Release the adapter; the submit thread completes; drain returns.
        blocking_adapter.release()
        submit_thread.join(timeout=2.0)
        drain_done.wait(timeout=2.0)

        assert drain_result == [True], "Expected drained=True after releasing the adapter"

        # The order should exist in the repo now.
        assert eng.order_manager.repo.orders, "Order must be in the repo after drain"

    def test_drain_timeout_result_has_drain_timeout_true(self):
        """When place_order never returns, kill_switch records drain_timeout=True."""
        blocking_adapter = BlockingAdapter()
        eng = _make_drain_engine(adapter=blocking_adapter)

        signal = _make_signal()

        submit_thread = threading.Thread(target=eng.execute_signal, args=(signal,), daemon=True)
        submit_thread.start()

        # Wait until place_order is blocking.
        blocking_adapter._placed.wait(timeout=2.0)

        fake_account = FakeAccountService(positions=[])
        service = OpsSafetyService(
            eng, fake_account, _make_null_db_session_factory(), drain_timeout=0.1
        )
        result = service.kill_switch(actor="ops")

        assert result.get("drain_timeout") is True, (
            f"Expected drain_timeout=True in result, got: {result}"
        )

        # Cleanup: release so the thread can exit.
        blocking_adapter.release()
        submit_thread.join(timeout=2.0)

    def test_gated_conditional_placement_after_halt_returns_halted_failure(self):
        """_place_pending_conditional_orders_for_entry must report kill_switch_halted after halt."""
        eng = _make_drain_engine()
        eng.halt_and_drain(timeout=0.01)

        entry = Order()
        entry.id = "entry-001"
        entry.type = "market"
        entry.filled_quantity = Decimal("1.0")

        failures = eng._place_pending_conditional_orders_for_entry(entry)

        assert any(f.get("reason") == "kill_switch_halted" for f in failures), (
            f"Expected kill_switch_halted failure entry, got: {failures}"
        )

    def test_invariant_no_cancellable_orders_after_clean_drain(self):
        """After a kill_switch with a clean drain, no non-ops orders remain in cancellable statuses."""
        repo_eng = _make_drain_engine()

        # Pre-populate repo with a submitted order as if a previous execute_signal ran.
        submitted_order = _make_order("submitted-001", OrderStatus.SUBMITTED.value)
        repo_eng.order_manager.repo.orders[submitted_order.id] = submitted_order

        fake_account = FakeAccountService(positions=[])
        service = OpsSafetyService(
            repo_eng, fake_account, _make_null_db_session_factory(), drain_timeout=5.0
        )

        # Patch cancel_order on the engine (it's called by kill_switch for SUBMITTED orders).
        cancelled: list[str] = []

        def fake_cancel(order_id: str) -> bool:
            submitted_order.status = OrderStatus.CANCELLED.value
            repo_eng.order_manager.repo.orders[submitted_order.id] = submitted_order
            cancelled.append(order_id)
            return True

        repo_eng.cancel_order = fake_cancel

        result = service.kill_switch(actor="ops")

        assert result.get("drain_timeout") is False or "drain_timeout" not in result or not result["drain_timeout"]

        cancellable = {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
        ops_strategy = "__ops_kill_switch__"
        remaining = [
            o for o in repo_eng.order_manager.repo.orders.values()
            if o.status in cancellable and o.strategy_id != ops_strategy
        ]
        assert not remaining, (
            f"Non-ops orders in cancellable statuses after kill_switch: {[o.id for o in remaining]}"
        )
