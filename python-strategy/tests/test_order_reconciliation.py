import inspect
import logging
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.core.adapters.rithmic_owned_order_reconciliation import (
    RithmicOwnedOrderReconciler,
)
from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeError, IExchangeAdapter
from src.core.models import OrderStatus
from src.core.order_reconciliation import OrderReconciler


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
    assert OrderReconciler._reconcile_decision(local_status, exchange_status) == expected


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
