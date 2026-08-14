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
import inspect
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.execution import ExecutionEngine, FlattenPending
from src.core.interfaces.exchange import ExchangeError, NetworkError
from src.core.models import (
    Candlestick,
    OrderStatus,
    Position,
    PositionSide,
    Signal,
    SignalType,
)
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
        self.authoritative_exits: list[tuple[str, str]] = []

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

    def exit_authoritative_position(self, product_id: str, *, account_id: str) -> bool:
        self.calls.append(("exit_authoritative_position", product_id))
        self.authoritative_exits.append((product_id, account_id))
        return True


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


class FailingEnumerationScopedPositionAdapter(FakeExchangePositionAdapter):
    def __init__(self, scoped_result) -> None:
        super().__init__(error=RuntimeError("exchange enumeration unavailable"))
        self._scoped_result = scoped_result

    def get_position(self, product_id: str) -> Position | None:
        if isinstance(self._scoped_result, Exception):
            raise self._scoped_result
        return self._scoped_result


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

    def test_exchange_enumeration_failure_does_not_flatten_local_positions(self):
        """Local exposure is not authoritative enough to place a flatten order."""
        local_position = _make_position(quantity=Decimal("0.75"))
        service, engine, _ = _make_service(positions=[local_position])
        engine.adapter = FakeExchangePositionAdapter(
            error=RuntimeError("exchange unavailable")
        )

        result = service.kill_switch(actor="ops", reason="degraded_exchange")

        assert result["already_flat"] is False
        assert result["flattened_positions"] == 0
        assert not any(call[0] == "flatten_position" for call in engine.calls)
        assert any(
            "exchange_positions_unavailable" in failure["reason"]
            for failure in result["flatten_failures"]
        )

    @pytest.mark.parametrize(
        ("scoped_result", "expected_flattened"),
        [
            (_make_position(strategy_id="LIVE", quantity=Decimal("0.5")), 1),
            (None, 0),
            (RuntimeError("scoped lookup unavailable"), 0),
        ],
        ids=["position", "flat", "error"],
    )
    def test_exchange_enumeration_failure_uses_authoritative_scoped_lookup(
        self, scoped_result, expected_flattened
    ):
        local_position = _make_position(quantity=Decimal("0.75"))
        service, engine, _ = _make_service(positions=[local_position])
        engine.adapter = FailingEnumerationScopedPositionAdapter(scoped_result)

        result = service.kill_switch(actor="ops", reason="degraded_exchange")

        assert result["flattened_positions"] == expected_flattened
        assert any(
            "exchange_positions_unavailable" in failure["reason"]
            for failure in result["flatten_failures"]
        )

    def test_scoped_unified_short_places_one_reduce_only_buy_after_bulk_failure(
        self,
        mock_clock,
        mock_db_session,
        mock_order_repo,
    ):
        import ccxt as ccxt_lib

        client = MagicMock()
        client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "contract": True,
                "linear": True,
                "inverse": False,
                "contractSize": "1",
            }
        }

        def fetch_positions(symbols=None):
            if symbols is None:
                raise ccxt_lib.ExchangeError("bulk position lookup unavailable")
            return [
                {
                    "symbol": "BTC/USDT:USDT",
                    "contracts": 2,
                    "side": "short",
                    "entryPrice": 70000,
                    "unrealizedPnl": -50,
                }
            ]

        client.fetch_positions.side_effect = fetch_positions
        client.create_order.return_value = {"id": "EX-FLAT"}
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_ccxt.binance = MagicMock(return_value=client)
            adapter = CcxtExchangeAdapter(
                exchange_id="binance",
                api_key="test-key",
                secret="test-secret",
            )

        execution_engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
        )
        service = OpsSafetyService(
            execution_engine,
            FakeAccountService(positions=[_make_position()]),
            _make_null_db_session_factory(),
        )

        result = service.kill_switch(actor="ops", reason="degraded_exchange")

        assert result["flattened_positions"] == 1
        client.create_order.assert_called_once()
        call_kwargs = client.create_order.call_args.kwargs
        assert call_kwargs["side"] == "buy"
        assert call_kwargs["params"]["reduceOnly"] is True

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


class TestKillSwitchClear:
    def test_recovery_pending_cannot_clear(self):
        service, engine, _ = _make_service()
        service._recovery_pending = True
        engine._submissions_halted = True
        persist = MagicMock()

        result = service.clear_kill_switch(persist_clear=persist)

        assert result == {"cleared": False, "reason": "recovery_pending"}
        persist.assert_not_called()
        assert engine._submissions_halted is True

    def test_open_position_cannot_clear(self):
        service, engine, _ = _make_service(positions=[_make_position()])
        engine._submissions_halted = True
        persist = MagicMock()

        result = service.clear_kill_switch(persist_clear=persist)

        assert result == {"cleared": False, "reason": "exposure_not_flat"}
        persist.assert_not_called()
        assert engine._submissions_halted is True

    def test_verified_flat_persists_before_resume(self):
        service, engine, _ = _make_service(positions=[])
        engine._submissions_halted = True
        calls = []
        persist = MagicMock(side_effect=lambda: calls.append("persist"))
        engine.resume_submissions = MagicMock(side_effect=lambda: calls.append("resume"))

        result = service.clear_kill_switch(persist_clear=persist)

        assert result == {"cleared": True, "reason": None}
        assert calls == ["persist", "resume"]


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
            service.kill_switch(
                actor="compliance_team",
                reason="eod_drill",
                operation_id="mobile-lockdown-1",
            )
            kwargs = mock_write.call_args.kwargs
            payload = kwargs["payload"]
            assert payload["actor"] == "compliance_team"
            assert payload["reason"] == "eod_drill"
            assert payload["operation_id"] == "mobile-lockdown-1"

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

    def test_ambiguous_flatten_is_reported_pending_not_successful(self):
        pending = FlattenPending("flatten-order", "verification_blocked")
        position = _make_position()
        service, _, _ = _make_service(
            positions=[position],
            flatten_results={(position.strategy_id, position.product_id): pending},
        )

        result = service.kill_switch(actor="ops")

        assert result["flattened_positions"] == 0
        assert result["flatten_pending"] == [
            {
                "strategy_id": position.strategy_id,
                "product_id": position.product_id,
                "order_id": "flatten-order",
                "reason": "verification_blocked",
            }
        ]

    def test_flatten_does_not_use_position_entry_price_as_reference_price(self):
        pos = _make_position(quantity=Decimal("0.75"))
        service, engine, _ = _make_service(positions=[pos])

        service.kill_switch(actor="ops")

        assert engine.flatten_reference_prices == [None]

    def test_authoritative_loader_and_account_are_forwarded_together(self):
        position = _make_position(strategy_id="LIVE")
        service, engine, _ = _make_service(positions=[])
        service._write_event_best_effort = MagicMock()

        result = service.kill_switch_with_authoritative_positions(
            actor="ops",
            reason="drill",
            position_loader=lambda: [position],
            account_id="ACCOUNT",
        )

        assert result["flattened_positions"] == 1
        assert ("exit_authoritative_position", PRODUCT_ID) in engine.calls
        assert engine.authoritative_exits == [(PRODUCT_ID, "ACCOUNT")]
        service._write_event_best_effort.assert_not_called()

        service.record_kill_switch_result(
            actor="ops",
            reason="drill",
            result={**result, "authoritative_flatten_verified": True},
            operation_id="mobile-lockdown-1",
        )

        service._write_event_best_effort.assert_called_once_with(
            actor="ops",
            reason="drill",
            result={**result, "authoritative_flatten_verified": True},
            operation_id="mobile-lockdown-1",
        )


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

    def test_authoritative_exit_uses_server_side_adapter_operation(
        self,
        eng,
        mock_exchange_adapter,
        mock_order_repo,
    ):
        calls = []
        eng._operation_guard = MagicMock(side_effect=lambda: calls.append("guard"))
        mock_exchange_adapter.exit_authoritative_position = MagicMock(
            side_effect=lambda *_args, **_kwargs: calls.append("adapter") or True
        )

        assert (
            eng.exit_authoritative_position(
                PRODUCT_ID,
                account_id="ACCOUNT",
            )
            is True
        )

        mock_exchange_adapter.exit_authoritative_position.assert_called_once_with(
            PRODUCT_ID,
            account_id="ACCOUNT",
        )
        assert calls == ["guard", "adapter"]
        assert mock_order_repo.orders == {}

    def test_authoritative_exit_checks_operation_fence_before_adapter(
        self,
        eng,
        mock_exchange_adapter,
        mock_order_repo,
    ):
        eng._operation_guard = MagicMock(side_effect=RuntimeError("lease_lost"))
        mock_exchange_adapter.exit_authoritative_position = MagicMock()

        with pytest.raises(ExchangeError, match="external_operation_fenced"):
            eng.exit_authoritative_position(PRODUCT_ID, account_id="ACCOUNT")

        assert mock_order_repo.orders == {}
        mock_exchange_adapter.exit_authoritative_position.assert_not_called()

    def test_authoritative_exit_requires_explicit_adapter_capability(
        self,
        eng,
        mock_exchange_adapter,
        mock_order_repo,
    ):
        mock_exchange_adapter.account_id = "ACCOUNT"

        with pytest.raises(ExchangeError, match="unsupported"):
            eng.exit_authoritative_position(PRODUCT_ID, account_id="ACCOUNT")

        assert mock_order_repo.orders == {}

    def test_authoritative_exit_facade_contains_no_provider_authority_marker(self):
        source = inspect.getsource(ExecutionEngine.exit_authoritative_position)

        assert "authoritative_position_exit_authority" not in source
        assert "rithmic_exit_position" not in source

    def test_flatten_persists_and_submits_validated_quantity(
        self, eng, mock_order_repo
    ):
        adapter = MagicMock()
        adapter.place_order.return_value = "exchange-flatten-id"

        def quantize(order):
            order.quantity = Decimal("0.9")

        adapter.validate_order.side_effect = quantize
        eng.adapter = adapter

        result = eng.flatten_position(
            STRATEGY_ID,
            PRODUCT_ID,
            "LONG",
            Decimal("1.0"),
            reference_price=Decimal("50000"),
        )

        assert result is not None
        submitted_order = adapter.place_order.call_args.args[0]
        persisted_order = mock_order_repo.get_order(result)
        assert submitted_order.quantity == Decimal("0.9")
        assert persisted_order.quantity == submitted_order.quantity

    def test_flatten_validation_rejection_never_submits_and_fails_order(
        self, eng, mock_order_repo
    ):
        adapter = MagicMock()
        adapter.validate_order.side_effect = ExchangeError("min_notional_not_met")
        eng.adapter = adapter

        result = eng.flatten_position(
            STRATEGY_ID,
            PRODUCT_ID,
            "LONG",
            Decimal("0.0001"),
            reference_price=Decimal("50000"),
        )

        assert result is None
        adapter.place_order.assert_not_called()
        order = next(iter(mock_order_repo.orders.values()))
        assert order.status in {OrderStatus.FAILED.value, "failed"}

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
        """Timeout-like flatten errors remain recoverable and idempotent."""

        class TimeoutAdapter:
            def __init__(self):
                self.place_calls = 0

            def place_order(self, order):
                self.place_calls += 1
                raise NetworkError("timeout after submit")

            def get_order_by_client_id(self, client_order_id, product_id, *, order_type=None):
                return None

        adapter = TimeoutAdapter()
        eng.adapter = adapter

        first_result = eng.flatten_position(
            STRATEGY_ID,
            PRODUCT_ID,
            "LONG",
            Decimal("1.0"),
            reference_price=Decimal("50000"),
        )
        second_result = eng.flatten_position(
            STRATEGY_ID,
            PRODUCT_ID,
            "LONG",
            Decimal("1.0"),
            reference_price=Decimal("50000"),
        )

        assert isinstance(first_result, FlattenPending)
        assert isinstance(second_result, FlattenPending)
        assert second_result.order_id == first_result.order_id
        assert adapter.place_calls == 1
        assert len(mock_order_repo.orders) == 1
        order = next(iter(mock_order_repo.orders.values()))
        assert order.client_order_id is not None
        assert order.status == OrderStatus.SUBMITTED_UNCONFIRMED.value

    @pytest.mark.parametrize(
        "status, expected_reused",
        [
            (OrderStatus.SUBMITTED_UNCONFIRMED.value, True),
            (OrderStatus.SUBMITTED.value, True),
            (OrderStatus.PARTIALLY_FILLED.value, True),
            (OrderStatus.CANCELLED.value, False),
            (OrderStatus.FAILED.value, False),
            (OrderStatus.FILLED.value, False),
        ],
    )
    def test_flatten_reuses_only_active_ops_orders(
        self,
        eng,
        mock_exchange_adapter,
        mock_order_repo,
        status,
        expected_reused,
    ):
        existing = _make_order("existing-flatten", status, product_id=PRODUCT_ID)
        existing.intent_payload = {"reduce_only": True, "source": "kill_switch"}
        mock_order_repo.orders[existing.id] = existing

        result = eng.flatten_position(
            STRATEGY_ID,
            PRODUCT_ID,
            "LONG",
            Decimal("1.0"),
            reference_price=Decimal("50000"),
        )

        if status == OrderStatus.SUBMITTED_UNCONFIRMED.value:
            assert isinstance(result, FlattenPending)
            assert result.order_id == existing.id
        else:
            assert (result == existing.id) is expected_reused
        placed = mock_exchange_adapter.open_orders + mock_exchange_adapter.filled_orders
        assert bool(placed) is (not expected_reused)

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
                        account_profile VARCHAR(128),
                        account_id VARCHAR(128),
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
        engine.redis_client.set.assert_any_call(
            engine._system_state_key,
            "LOCKDOWN",
        )

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

    def test_kill_switch_stays_halted_when_persistence_fails(self, engine_factory):
        engine = engine_factory()
        engine.redis_client.set.side_effect = RuntimeError("redis unavailable")
        engine.ops_safety.kill_switch = MagicMock(return_value={})
        engine.ops_safety.persist_kill_switch_state = MagicMock()

        engine._handle_command({"command": "KILL_SWITCH", "params": {}})

        assert engine._kill_switch_halted is True
        assert engine.execution_engine._submissions_halted is True
        engine.ops_safety.persist_kill_switch_state.assert_called_once_with(
            "LOCKDOWN",
            actor="operator",
            reason=None,
        )
        engine.ops_safety.kill_switch.assert_called_once()

    def test_clear_kill_switch_persists_before_resuming(self, engine_factory):
        engine = engine_factory()
        engine._kill_switch_halted = True
        assert engine.execution_engine.halt_and_drain(timeout=0) is True
        engine.ops_safety.persist_kill_switch_state = MagicMock()

        engine._handle_command({"command": "CLEAR_KILL_SWITCH", "params": {}})

        engine.ops_safety.persist_kill_switch_state.assert_called_once_with(
            "OK",
            actor="operator",
            reason=None,
        )
        engine.redis_client.set.assert_any_call(engine._system_state_key, "OK")
        assert engine._kill_switch_halted is False
        assert engine.execution_engine._submissions_halted is False

    def test_clear_kill_switch_does_not_resume_when_persistence_fails(
        self, engine_factory
    ):
        engine = engine_factory()
        engine._kill_switch_halted = True
        assert engine.execution_engine.halt_and_drain(timeout=0) is True
        engine.redis_client.set.side_effect = RuntimeError("redis unavailable")

        engine._handle_command({"command": "CLEAR_KILL_SWITCH", "params": {}})

        assert engine._kill_switch_halted is True
        assert engine.execution_engine._submissions_halted is True

    def test_clear_waits_for_in_progress_kill_switch(self, engine_factory):
        engine = engine_factory()
        entered = threading.Event()
        release = threading.Event()

        def blocking_kill_switch(**_kwargs):
            entered.set()
            release.wait(timeout=2)
            return {}

        engine.ops_safety.kill_switch = blocking_kill_switch
        engine.ops_safety.clear_kill_switch = MagicMock(
            return_value={"cleared": True, "reason": None}
        )
        kill_thread = threading.Thread(
            target=engine._handle_command,
            args=({"command": "KILL_SWITCH", "params": {}},),
        )
        clear_thread = threading.Thread(
            target=engine._handle_command,
            args=({"command": "CLEAR_KILL_SWITCH", "params": {}},),
        )

        kill_thread.start()
        assert entered.wait(timeout=1)
        clear_thread.start()
        time.sleep(0.05)

        assert clear_thread.is_alive()
        assert engine._kill_switch_halted is True
        release.set()
        kill_thread.join(timeout=1)
        clear_thread.join(timeout=1)

        assert not kill_thread.is_alive()
        assert not clear_thread.is_alive()

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

    def test_non_iterable_adapter_positions_are_classified_as_unavailable(self):
        class InvalidAdapter:
            def get_all_positions(self) -> int:
                return 7

        service, _ = _make_service_with_failing_account(adapter=InvalidAdapter())

        result = service.kill_switch(actor="ops")

        assert result["flattened_positions"] == 0
        assert any(
            "exchange position enumeration must return an iterable"
            in failure.get("reason", "")
            for failure in result["flatten_failures"]
        )


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


class SequencedDrainEngine(RecordingExecutionEngine):
    def __init__(
        self,
        drain_results: list[bool],
        *,
        orders: list[Order] | None = None,
        late_order: Order | None = None,
    ) -> None:
        orders_by_status: dict[str, list[Order]] = {}
        for order in orders or []:
            orders_by_status.setdefault(order.status, []).append(order)
        super().__init__(orders_by_status=orders_by_status)
        self._drain_results = iter(drain_results)
        self._late_order = late_order
        self.drain_calls = 0
        self.drain_callback = None

    def halt_and_drain(self, timeout: float) -> bool:
        self.drain_calls += 1
        drained = next(self._drain_results)
        if self.drain_calls == 2 and self._late_order is not None:
            self.order_manager.repo._orders.append(self._late_order)
        return drained

    def cancel_order(self, order_id: str) -> bool:
        self.calls.append(("cancel_order", order_id))
        for order in self.order_manager.repo._orders:
            if str(order.id) == order_id:
                order.status = OrderStatus.CANCELLED.value
                return True
        return False

    def run_when_submissions_drained(self, callback) -> None:
        self.drain_callback = callback

    def complete_submission(self) -> None:
        callback = self.drain_callback
        self.drain_callback = None
        callback()


class BlockingFirstAccountService(FakeAccountService):
    def __init__(self) -> None:
        super().__init__(positions=[])
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0
        self._calls_lock = threading.Lock()

    def get_all_positions(self) -> list[Position]:
        with self._calls_lock:
            self.calls += 1
            call_number = self.calls
        if call_number == 1:
            self.entered.set()
            self.release.wait(timeout=2.0)
        return []


def _make_drain_engine(
    adapter=None,
    mock_db_session=None,
    mock_clock=None,
    *,
    audit_external_orders=False,
):
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
        db_session_factory=_make_null_db_session_factory(),
        audit_external_orders=audit_external_orders,
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

    def test_kill_switch_drains_inflight_order_before_snapshot_and_cancel(self):
        """Kill switch snapshots and cancels only after an in-flight submit completes."""
        blocking_adapter = BlockingAdapter()
        eng = _make_drain_engine(
            adapter=blocking_adapter,
            audit_external_orders=True,
        )

        signal = _make_signal()
        submit_thread = threading.Thread(target=eng.execute_signal, args=(signal,), daemon=True)
        submit_thread.start()
        assert blocking_adapter._placed.wait(timeout=2.0), "place_order was never called"

        cancelled: list[str] = []

        def cancel_order(order_id: str) -> bool:
            cancelled.append(order_id)
            order = eng.order_manager.repo.get_order(order_id)
            eng.order_manager.mark_cancelled(order)
            return True

        eng.cancel_order = cancel_order
        service = OpsSafetyService(
            eng,
            FakeAccountService(positions=[]),
            _make_null_db_session_factory(),
            drain_timeout=5.0,
        )
        result: list[dict] = []
        kill_thread = threading.Thread(
            target=lambda: result.append(service.kill_switch(actor="ops")),
            daemon=True,
        )
        kill_thread.start()

        time.sleep(0.05)
        assert kill_thread.is_alive(), "kill_switch returned before submit completed"
        assert cancelled == []

        blocking_adapter.release()
        submit_thread.join(timeout=2.0)
        kill_thread.join(timeout=2.0)

        assert not kill_thread.is_alive()
        assert result[0]["drain_timeout"] is False
        assert len(cancelled) == 1
        assert eng.order_manager.repo.get_order(cancelled[0]).status == OrderStatus.CANCELLED.value

    def test_execution_engine_runs_callback_only_after_submission_drains(self):
        blocking_adapter = BlockingAdapter()
        eng = _make_drain_engine(adapter=blocking_adapter)
        submit_thread = threading.Thread(
            target=eng.execute_signal,
            args=(_make_signal(),),
            daemon=True,
        )
        submit_thread.start()
        assert blocking_adapter._placed.wait(timeout=1.0)

        callback_ran = threading.Event()
        eng.run_when_submissions_drained(callback_ran.set)
        assert not callback_ran.is_set()

        blocking_adapter.release()
        submit_thread.join(timeout=1.0)
        assert callback_ran.wait(timeout=1.0)

    def test_drain_timeout_returns_bounded_then_callback_finishes_cleanup(self):
        """Late submissions are cleaned by the one-shot drain callback."""
        order = _make_order("known-order", OrderStatus.SUBMITTED.value)
        late_order = _make_order("late-order", OrderStatus.SUBMITTED.value)
        position = _make_position()
        eng = SequencedDrainEngine(
            [False, True],
            orders=[order],
            late_order=late_order,
        )
        service = OpsSafetyService(
            eng,
            FakeAccountService(positions=[position]),
            _make_null_db_session_factory(),
            drain_timeout=0.1,
        )

        audit_events: list[tuple[str, dict]] = []
        with patch.object(
            service,
            "_write_event",
            side_effect=lambda **kwargs: audit_events.append(
                (
                    kwargs.get("event_subtype", "kill_switch"),
                    dict(kwargs["result"]),
                )
            ),
        ):
            result = service.kill_switch(actor="ops")

            assert result["drain_timeout"] is True
            assert eng.drain_calls == 1
            assert [subtype for subtype, _ in audit_events] == [
                "kill_switch_pending",
            ]
            repeated_result = service.kill_switch(actor="ops")
            assert repeated_result == result
            assert eng.drain_calls == 1
            assert result["cancelled_orders"] == 0
            assert result["flattened_positions"] == 0
            assert not any(call[0] == "cancel_order" for call in eng.calls)
            assert not any(call[0] == "flatten_position" for call in eng.calls)

            eng.complete_submission()

        assert result["drain_timeout"] is True
        assert result["already_flat"] is False
        assert result["cancelled_orders"] == 0
        assert result["flattened_positions"] == 0
        assert ("cancel_order", "late-order") in eng.calls
        assert eng.drain_calls == 2
        assert [subtype for subtype, _ in audit_events] == [
            "kill_switch_pending",
            "kill_switch",
        ]
        assert audit_events[0][1]["drain_timeout"] is True
        assert audit_events[0][1]["flattened_positions"] == 0
        assert audit_events[1][1]["cancelled_orders"] == 2
        assert audit_events[1][1]["flattened_positions"] == 1
        assert audit_events[1][1]["drain_timeout"] is True

    def test_pending_audit_failure_does_not_block_timeout_mitigation(self):
        order = _make_order("known-order", OrderStatus.SUBMITTED.value)
        eng = SequencedDrainEngine([False], orders=[order])
        service = OpsSafetyService(
            eng,
            FakeAccountService(positions=[_make_position()]),
            _make_null_db_session_factory(),
            drain_timeout=0.1,
        )

        with patch.object(
            service,
            "_write_event",
            side_effect=RuntimeError("database unavailable"),
        ):
            result = service.kill_switch(actor="ops")

        assert result["drain_timeout"] is True
        assert result["cancelled_orders"] == 0
        assert result["flattened_positions"] == 0
        assert eng.drain_callback is not None

    def test_retry_after_timeout_catches_late_order_before_success(self):
        """A converged retry runs a second pass that catches the late submission."""
        late_order = _make_order("late-order", OrderStatus.SUBMITTED.value)
        eng = SequencedDrainEngine([False, True], late_order=late_order)
        service = OpsSafetyService(
            eng,
            FakeAccountService(positions=[]),
            _make_null_db_session_factory(),
            drain_timeout=0.1,
        )

        result = service.kill_switch(actor="ops")

        assert result["drain_timeout"] is True
        assert result["already_flat"] is False
        assert result["cancelled_orders"] == 0
        eng.complete_submission()
        assert ("cancel_order", "late-order") in eng.calls
        assert eng.drain_calls == 2

    @pytest.mark.parametrize(
        "drain_results",
        [[True], [False, True]],
        ids=["drained", "recovery-pending"],
    )
    def test_each_position_is_flattened_once_per_invocation(self, drain_results):
        eng = SequencedDrainEngine(drain_results)
        service = OpsSafetyService(
            eng,
            FakeAccountService(positions=[_make_position()]),
            _make_null_db_session_factory(),
            drain_timeout=0.1,
        )

        service.kill_switch(actor="ops")

        if drain_results[0] is False:
            eng.complete_submission()

        flatten_calls = [call for call in eng.calls if call[0] == "flatten_position"]
        assert len(flatten_calls) == 1

    @pytest.mark.parametrize("registration_mode", ["missing", "raises"])
    def test_drain_callback_registration_failure_stays_pending(
        self, registration_mode
    ):
        eng = SequencedDrainEngine([False])
        if registration_mode == "missing":
            eng.run_when_submissions_drained = None
        else:
            eng.run_when_submissions_drained = MagicMock(
                side_effect=RuntimeError("callback registration failed")
            )
        service = OpsSafetyService(
            eng,
            FakeAccountService(positions=[_make_position()]),
            _make_null_db_session_factory(),
            drain_timeout=0.1,
        )

        result = service.kill_switch(actor="ops")

        assert result["already_flat"] is False
        assert result["recovery_failures"]
        assert service._recovery_pending is True
        assert not any(call[0] == "flatten_position" for call in eng.calls)

    def test_concurrent_kill_switch_calls_are_serialized(self):
        account = BlockingFirstAccountService()
        service = OpsSafetyService(
            RecordingExecutionEngine(),
            account,
            _make_null_db_session_factory(),
        )
        first = threading.Thread(
            target=service.kill_switch,
            kwargs={"actor": "first"},
            daemon=True,
        )
        second = threading.Thread(
            target=service.kill_switch,
            kwargs={"actor": "second"},
            daemon=True,
        )

        first.start()
        assert account.entered.wait(timeout=1.0)
        second.start()
        time.sleep(0.05)
        assert account.calls == 1

        account.release.set()
        first.join(timeout=1.0)
        second.join(timeout=1.0)
        assert account.calls == 2

    def test_audit_failure_does_not_block_emergency_mitigation(self):
        order = _make_order("open-order", OrderStatus.SUBMITTED.value)
        service, engine, _ = _make_service(
            orders=[order],
            positions=[_make_position()],
        )
        with patch.object(
            service,
            "_write_event",
            side_effect=RuntimeError("audit failed"),
        ):
            result = service.kill_switch(actor="ops")

        assert result["cancelled_orders"] == 1
        assert result["flattened_positions"] == 1
        assert ("cancel_order", "open-order") in engine.calls

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
