import pytest

from src.core.models import OrderStatus
from src.core.order_reconciliation import OrderReconciler


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
