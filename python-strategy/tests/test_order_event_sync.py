from decimal import Decimal
from unittest.mock import Mock

import pytest

from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.models import OrderStatus
from src.core.order_event_sync import OrderEventApplier
from src.core.order_manager import OrderManager


def _applier(order_manager: OrderManager) -> OrderEventApplier:
    return OrderEventApplier(
        order_manager=order_manager,
        journal_fill=None,
        fail_pending_conditionals_for_terminal_entry=lambda _order: None,
        protective_terminal_without_fill_failure=lambda _order: None,
        write_conditional_warning=lambda **_kwargs: None,
        place_pending_conditionals_for_entry=lambda _order: [],
        protective_partial_fill_requires_resize=lambda _order, _state: None,
        cancel_linked_conditional_for_protection_fill=lambda _order: None,
    )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("open", "open"),
        ("partially_filled", "partial"),
        ("closed", "filled"),
        ("cancelled", "cancelled"),
        ("rejected", "rejected"),
        ("expired", "expired"),
        ("failed", "failed"),
        ("force_closed", "liquidated"),
        ("weird_status", "unknown"),
    ],
)
def test_classify_exchange_order_event_status(status, expected):
    assert OrderEventApplier._classify_exchange_order_event_status(status) == expected


def test_client_order_id_collision_for_different_product_is_not_applied(
    mock_clock,
    mock_order_repo,
    order_factory,
):
    order_manager = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
    applier = _applier(order_manager)
    order = order_factory(
        client_order_id="shared-client-id",
        exchange_order_id=None,
        status=OrderStatus.SUBMITTED.value,
        quantity=Decimal("1"),
    )
    mock_order_repo.add_order(order)
    state_before = (
        order.exchange_order_id,
        order.status,
        order.filled_quantity,
        order.filled_price,
    )

    result = applier.process_exchange_order_event(
        ExchangeOrderEvent(
            status="filled",
            product_id="RITHMIC:ES-202609",
            client_order_id=order.client_order_id,
            exchange_order_id="EX-FOREIGN",
            cumulative_filled_quantity=Decimal("1"),
            cumulative_average_price=Decimal("6500.25"),
        )
    )

    assert result["action"] == "unknown_order"
    assert (
        order.exchange_order_id,
        order.status,
        order.filled_quantity,
        order.filled_price,
    ) == state_before
    assert mock_order_repo.trades == []


def test_process_exchange_order_event_recomputes_catch_up_delta_price(
    mock_clock,
    mock_order_repo,
    order_factory,
):
    order_manager = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
    applier = _applier(order_manager)
    order = order_factory(
        exchange_order_id="EX-catch-up",
        status=OrderStatus.PARTIALLY_FILLED.value,
        quantity=Decimal("0.10"),
        filled_quantity=Decimal("0.04"),
        filled_price=Decimal("100"),
    )
    mock_order_repo.add_order(order)

    result = applier.process_exchange_order_event(
        ExchangeOrderEvent(
            status="filled",
            product_id=order.product_id,
            exchange_order_id="EX-catch-up",
            cumulative_filled_quantity=Decimal("0.10"),
            cumulative_average_price=Decimal("102.4"),
        )
    )

    assert result["action"] == "applied"
    assert order.status == OrderStatus.FILLED.value
    assert order.filled_quantity == Decimal("0.10")
    assert order.filled_price == Decimal("102.4")
    assert len(mock_order_repo.trades) == 1
    assert mock_order_repo.trades[0].quantity == Decimal("0.06")
    assert mock_order_repo.trades[0].price == Decimal("104")


@pytest.mark.parametrize(
    ("remote_follow_up_required", "expected_action"),
    [(True, "unresolved_remote_actions_suppressed"), (False, "applied")],
)
def test_recovery_applies_fill_without_executing_remote_follow_up(
    mock_clock,
    mock_order_repo,
    order_factory,
    remote_follow_up_required,
    expected_action,
):
    order_manager = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
    place_protection = Mock(return_value=[])
    resize_protection = Mock(return_value=None)
    cancel_sibling = Mock(return_value=None)
    applier = OrderEventApplier(
        order_manager=order_manager,
        journal_fill=None,
        fail_pending_conditionals_for_terminal_entry=lambda _order: None,
        protective_terminal_without_fill_failure=lambda _order: None,
        write_conditional_warning=lambda **_kwargs: None,
        place_pending_conditionals_for_entry=place_protection,
        protective_partial_fill_requires_resize=resize_protection,
        cancel_linked_conditional_for_protection_fill=cancel_sibling,
        remote_follow_up_required=lambda _order, _state: remote_follow_up_required,
    )
    order = order_factory(
        exchange_order_id="BASKET-1",
        status=OrderStatus.SUBMITTED.value,
        quantity=Decimal("1"),
    )
    mock_order_repo.add_order(order)

    result = applier.process_exchange_order_event(
        ExchangeOrderEvent(
            status="filled",
            product_id=order.product_id,
            exchange_order_id="BASKET-1",
            cumulative_filled_quantity=Decimal("1"),
            cumulative_average_price=Decimal("20000.25"),
        ),
        allow_remote_side_effects=False,
    )

    assert result["action"] == expected_action
    assert order.status == OrderStatus.FILLED.value
    assert order.filled_quantity == Decimal("1")
    place_protection.assert_not_called()
    resize_protection.assert_not_called()
    cancel_sibling.assert_not_called()
