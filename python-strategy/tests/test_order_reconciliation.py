import inspect
import logging
from contextlib import contextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.core.adapters.rithmic_owned_order_reconciliation import (
    RithmicOwnedOrderReconciler,
)
from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeError, IExchangeAdapter
from src.core.models import OrderStatus
from src.core.order_reconciliation import OrderReconciler
from src.core.repositories import LiveOrderRepository


class _UnsupportedAdapter(IExchangeAdapter):
    def place_order(self, order):
        raise AssertionError("unsupported adapter must not place orders")

    def cancel_order(self, order_id, product_id, *, order_type=None):
        raise AssertionError("unsupported adapter must not cancel orders")

    def get_balance(self, asset):
        return Decimal("0")

    def get_position(self, product_id):
        return None


def _generic_reconciler(adapter: IExchangeAdapter) -> OrderReconciler:
    return OrderReconciler(
        adapter=adapter,
        order_manager=SimpleNamespace(
            repo=SimpleNamespace(list_client_orders_by_statuses=lambda _statuses: [])
        ),
        clock=SimpleNamespace(now=lambda: 1_700_000_000),
        db_session_factory=None,
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        logger=logging.getLogger("test.order_reconciliation"),
    )


def test_generic_owned_order_reconciliation_boundary_is_venue_neutral() -> None:
    generic_source = inspect.getsource(OrderReconciler).lower()
    execution_source = inspect.getsource(ExecutionEngine.reconcile_owned_orders).lower()

    assert "rithmic" not in generic_source
    assert (
        "profile"
        not in inspect.signature(ExecutionEngine.reconcile_owned_orders).parameters
    )
    assert (
        "account_id"
        not in inspect.signature(ExecutionEngine.reconcile_owned_orders).parameters
    )
    assert "rithmic" not in execution_source


def test_rithmic_adapter_owns_the_reconciliation_capability_factory() -> None:
    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments={
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
                "multiplier": "20",
            }
        },
    )
    reconciler = _generic_reconciler(adapter)._owned_order_reconciler

    assert isinstance(reconciler, RithmicOwnedOrderReconciler)
    assert reconciler.profile == "test"
    assert reconciler.account_id == "ACCOUNT"
    adapter.profile = "misleading-profile"
    adapter.account_id = "MISLEADING-ACCOUNT"
    assert reconciler.profile == "test"
    assert reconciler.account_id == "ACCOUNT"


def test_unsupported_adapter_rejects_owned_order_reconciliation() -> None:
    reconciler = _generic_reconciler(_UnsupportedAdapter())

    with pytest.raises(ExchangeError, match="owned_order_reconciliation_unsupported"):
        reconciler.reconcile_owned_orders()


def test_generic_reconciler_delegates_once_without_venue_identity() -> None:
    adapter = _UnsupportedAdapter()
    capability = Mock()
    capability.reconcile.return_value = {"auto_resume_safe": True}
    adapter.create_owned_order_reconciler = Mock(return_value=capability)
    reconciler = _generic_reconciler(adapter)
    snapshot_loader = Mock()

    result = reconciler.reconcile_owned_orders(snapshot_loader=snapshot_loader)

    assert result == {"auto_resume_safe": True}
    adapter.create_owned_order_reconciler.assert_called_once()
    capability.reconcile.assert_called_once_with(snapshot_loader=snapshot_loader)


def test_live_ccxt_recoverable_scan_passes_exact_adapter_venue_scope() -> None:
    adapter = _UnsupportedAdapter()
    adapter.exchange_id = "binance"
    reconciler = _generic_reconciler(adapter)
    listing = Mock(return_value=[])
    reconciler.order_manager.repo.list_client_orders_by_statuses = listing

    assert reconciler.list_recoverable_client_orders() == []

    listing.assert_called_once_with(
        {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        },
        exchange_id="binance",
    )


def test_legacy_account_identity_blocks_reconciliation_before_provider_io() -> None:
    adapter = _UnsupportedAdapter()
    adapter.exchange_id = "binance"
    adapter.get_order_by_client_id = Mock(
        side_effect=AssertionError("provider I/O forbidden")
    )
    legacy = SimpleNamespace(status=OrderStatus.SUBMITTED.value)
    repository = SimpleNamespace(
        list_legacy_orders_by_statuses=Mock(return_value=[legacy]),
        list_client_orders_by_statuses=Mock(
            side_effect=AssertionError("identified scan forbidden")
        ),
    )
    db = MagicMock()

    @contextmanager
    def db_session_factory():
        yield db

    reconciler = OrderReconciler(
        adapter=adapter,
        order_manager=SimpleNamespace(repo=repository),
        clock=SimpleNamespace(now=lambda: 1_700_000_000),
        db_session_factory=db_session_factory,
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
    )

    with patch("src.core.order_reconciliation.write_system_event") as write_event:
        result = reconciler.reconcile_recoverable_client_orders()

        db.reset_mock()
        write_event.side_effect = RuntimeError("audit-failure")
        with pytest.raises(RuntimeError, match="^audit-failure$"):
            reconciler.reconcile_recoverable_client_orders()

    assert result["recoverable_count"] == 1
    assert result["unresolved_count"] == 1
    assert result["results"] == [
        {
            "action": "unresolved_legacy_account_identity",
            "verification_blocked": False,
            "unresolved": True,
        }
    ]
    adapter.get_order_by_client_id.assert_not_called()
    assert write_event.call_count == 2
    db.rollback.assert_called_once_with()
    db.commit.assert_not_called()


def test_live_ccxt_reconciliation_excludes_foreign_venue_from_money_path(
    mock_order_repo,
    order_factory,
) -> None:
    current = order_factory(
        order_id="current",
        exchange_id="BINANCE",
        client_order_id="current-client",
        status=OrderStatus.NEW.value,
    )
    foreign = order_factory(
        order_id="foreign",
        exchange_id="BYBIT",
        product_id="BYBIT:BTCUSDT-PERP",
        client_order_id="foreign-client",
        status=OrderStatus.NEW.value,
    )
    mock_order_repo.add_order(current)
    mock_order_repo.add_order(foreign)
    order_manager = SimpleNamespace(repo=mock_order_repo, fail_order=Mock())
    adapter = SimpleNamespace(
        exchange_id="binance",
        get_order_by_client_id=Mock(return_value=None),
    )
    db = MagicMock()

    @contextmanager
    def db_session_factory():
        yield db

    reconciler = OrderReconciler(
        adapter=adapter,
        order_manager=order_manager,
        clock=SimpleNamespace(now=lambda: 1_700_000_000),
        db_session_factory=db_session_factory,
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(return_value={"failures": []}),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
    )

    with patch("src.core.order_reconciliation.write_system_event") as write_event:
        result = reconciler.reconcile_recoverable_client_orders()

    adapter.get_order_by_client_id.assert_called_once_with(
        "current-client",
        current.product_id,
        order_type=current.type,
    )
    order_manager.fail_order.assert_called_once_with(
        current,
        "startup reconciliation: local order not found on exchange",
    )
    assert result["recoverable_count"] == 1
    assert [row["order_id"] for row in result["results"]] == ["current"]
    audit_payload = write_event.call_args.kwargs["payload"]
    assert audit_payload["recoverable_count"] == 1
    assert [row["order_id"] for row in audit_payload["results"]] == ["current"]
    assert foreign.status == OrderStatus.NEW.value
    assert foreign.last_reconciled_at is None


def test_actual_reconciliation_scopes_same_venue_collision_to_current_account(
    sqlite_order_session_factory,
    order_factory,
) -> None:
    repositories = {
        account_id: LiveOrderRepository(
            db_session_factory=sqlite_order_session_factory,
            account_profile="ccxt:binance:live",
            account_id=account_id,
        )
        for account_id in ("ACCOUNT-A", "ACCOUNT-B")
    }
    for account_id, repository in repositories.items():
        repository.add_order(
            order_factory(
                order_id=f"order-{account_id}",
                exchange_id="BINANCE",
                client_order_id="shared-client",
                exchange_order_id="shared-exchange",
                status=OrderStatus.SUBMITTED.value,
            )
        )
    order_manager = SimpleNamespace(
        repo=repositories["ACCOUNT-A"],
        fail_order=Mock(),
    )
    adapter = SimpleNamespace(
        exchange_id="binance",
        get_order_by_client_id=Mock(return_value=None),
    )
    db = MagicMock()

    @contextmanager
    def db_session_factory():
        yield db

    reconciler = OrderReconciler(
        adapter=adapter,
        order_manager=order_manager,
        clock=SimpleNamespace(now=lambda: 1_700_000_000),
        db_session_factory=db_session_factory,
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(return_value={"failures": []}),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
    )

    with patch("src.core.order_reconciliation.write_system_event"):
        result = reconciler.reconcile_recoverable_client_orders()

    adapter.get_order_by_client_id.assert_called_once_with(
        "shared-client",
        "BINANCE:BTCUSDT-PERP",
        order_type="market",
    )
    assert result["recoverable_count"] == 1
    assert result["verification_blocked_count"] == 1
    order_manager.fail_order.assert_not_called()
    assert repositories["ACCOUNT-B"].get_order("order-ACCOUNT-B").status == (
        OrderStatus.SUBMITTED.value
    )


@pytest.mark.parametrize(
    ("local_status", "exchange_status", "expected"),
    [
        (OrderStatus.NEW.value, None, "local_only"),
        (OrderStatus.SUBMITTED.value, None, "exchange_unknown"),
        (OrderStatus.SUBMITTED.value, "open", "exchange_open"),
        (OrderStatus.SUBMITTED.value, "partially_filled", "exchange_open"),
        (OrderStatus.SUBMITTED.value, "closed", "exchange_closed"),
        (OrderStatus.SUBMITTED.value, "cancelled", "exchange_closed"),
        (OrderStatus.SUBMITTED.value, "expired", "exchange_closed"),
        (OrderStatus.SUBMITTED.value, "weird_status", "exchange_unknown"),
    ],
)
def test_reconcile_decision_categories(local_status, exchange_status, expected):
    assert (
        OrderReconciler._reconcile_decision(local_status, exchange_status) == expected
    )


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        ("unknown_order", True),
        ("unknown_status", True),
        ("verification_blocked_order_lookup_failed", False),
        ("unresolved_missing_fill_price", False),
        ("applied", False),
    ],
)
def test_resync_action_verification_blocked(action, expected):
    assert OrderReconciler._resync_action_verification_blocked(action) is expected
