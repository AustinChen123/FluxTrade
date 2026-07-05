from decimal import Decimal

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
