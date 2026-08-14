from decimal import Context, Decimal, ROUND_DOWN, ROUND_HALF_UP, localcontext
from unittest.mock import Mock

import pytest

from src.core.interfaces.exchange import ExchangeOrderEvent, ExchangeOrderSnapshot
from src.core.models import OrderStatus
from src.core.order_event_sync import (
    OrderEventApplier,
    exchange_snapshot_to_order_event,
    snapshot_fill_fee_rejection,
)
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
    (
        "local_filled",
        "fill_quantity",
        "fee",
        "fee_asset",
        "expected",
    ),
    [
        (Decimal("0"), Decimal("0"), None, None, None),
        (Decimal("0.25"), Decimal("0"), None, None, None),
        (
            Decimal("0.25"),
            Decimal("0.25"),
            Decimal("0.08"),
            "USDC",
            "unresolved_snapshot_cumulative_fee_not_delta",
        ),
        (
            Decimal("0"),
            Decimal("0.25"),
            None,
            "USDC",
            "unresolved_snapshot_fill_fee_identity_incomplete",
        ),
        (
            Decimal("0"),
            Decimal("0.25"),
            Decimal("0.08"),
            None,
            "unresolved_snapshot_fill_fee_identity_incomplete",
        ),
        (
            Decimal("0"),
            Decimal("0.25"),
            Decimal("0.08"),
            "",
            "unresolved_snapshot_fill_fee_identity_incomplete",
        ),
        (
            Decimal("0"),
            Decimal("0.25"),
            None,
            None,
            "unresolved_snapshot_fill_fee_identity_incomplete",
        ),
        (Decimal("0"), Decimal("0.25"), Decimal("0"), "USDC", None),
        (Decimal("0"), Decimal("0.25"), Decimal("0.08"), "USDC", None),
    ],
)
def test_snapshot_fill_fee_classifier_is_exact(
    local_filled,
    fill_quantity,
    fee,
    fee_asset,
    expected,
):
    assert (
        snapshot_fill_fee_rejection(
            local_filled=local_filled,
            fill_quantity=fill_quantity,
            fee=fee,
            fee_asset=fee_asset,
        )
        == expected
    )


@pytest.mark.parametrize("status", ["open", "filled"])
@pytest.mark.parametrize(
    ("fee", "fee_asset"),
    [
        (None, "USDC"),
        (Decimal("0.08"), None),
        (None, None),
        (Decimal("0.08"), ""),
    ],
)
def test_snapshot_delta_with_incomplete_fee_never_mutates_money_state(
    status,
    fee,
    fee_asset,
    mock_clock,
    mock_order_repo,
    order_factory,
):
    order_manager = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
    applier = _applier(order_manager)
    order = order_factory(
        client_order_id="client-snapshot-fee",
        exchange_order_id="EX-snapshot-fee",
        status=OrderStatus.SUBMITTED.value,
        quantity=Decimal("0.25") if status == "filled" else Decimal("0.50"),
        filled_quantity=Decimal("0"),
    )
    mock_order_repo.add_order(order)
    event = exchange_snapshot_to_order_event(
        order.product_id,
        ExchangeOrderSnapshot(
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            status=status,
            filled_quantity=Decimal("0.25"),
            average_price=Decimal("160"),
            fee=fee,
            fee_asset=fee_asset,
        ),
    )

    result = applier.process_exchange_order_event(event)

    assert result["action"] == "unresolved_snapshot_fill_fee_identity_incomplete"
    assert order.filled_quantity == Decimal("0")
    assert mock_order_repo.trades == []


@pytest.mark.parametrize("status", ["open", "filled"])
@pytest.mark.parametrize("fee", [Decimal("0"), Decimal("0.08")])
def test_snapshot_first_delta_with_complete_fee_records_exact_asset(
    status,
    fee,
    mock_clock,
    mock_order_repo,
    order_factory,
):
    order_manager = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
    applier = _applier(order_manager)
    order = order_factory(
        client_order_id="client-snapshot-complete",
        exchange_order_id="EX-snapshot-complete",
        status=OrderStatus.SUBMITTED.value,
        quantity=Decimal("0.25") if status == "filled" else Decimal("0.50"),
        filled_quantity=Decimal("0"),
    )
    mock_order_repo.add_order(order)

    result = applier.process_exchange_order_event(
        exchange_snapshot_to_order_event(
            order.product_id,
            ExchangeOrderSnapshot(
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                status=status,
                filled_quantity=Decimal("0.25"),
                average_price=Decimal("160"),
                fee=fee,
                fee_asset="USDC",
            ),
        )
    )

    assert result["action"] == "applied"
    assert len(mock_order_repo.trades) == 1
    assert mock_order_repo.trades[0].fee == fee
    assert mock_order_repo.trades[0].fee_asset == "USDC"


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
        ("modify_rejected", "modify_rejected"),
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


def test_modify_rejection_preserves_existing_protection_state(
    mock_clock,
    mock_order_repo,
    order_factory,
):
    order_manager = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
    applier = _applier(order_manager)
    order = order_factory(
        exchange_order_id="CHILD-1",
        status=OrderStatus.SUBMITTED.value,
        trigger_price=Decimal("19998.25"),
    )
    mock_order_repo.add_order(order)

    result = applier.process_exchange_order_event(
        ExchangeOrderEvent(
            status="modify_rejected",
            product_id=order.product_id,
            exchange_order_id="CHILD-1",
        )
    )

    assert result["action"] == "applied"
    assert result["state"] == "modify_rejected"
    assert order.status == OrderStatus.SUBMITTED.value
    assert order.trigger_price == Decimal("19998.25")


def test_zero_fill_terminal_cleans_children_before_parent_becomes_terminal(
    mock_clock,
    mock_order_repo,
    order_factory,
):
    order_manager = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
    cleanup = Mock(side_effect=RuntimeError("simulated crash"))
    applier = OrderEventApplier(
        order_manager=order_manager,
        journal_fill=None,
        fail_pending_conditionals_for_terminal_entry=cleanup,
        protective_terminal_without_fill_failure=lambda _order: None,
        write_conditional_warning=lambda **_kwargs: None,
        place_pending_conditionals_for_entry=lambda _order: [],
        protective_partial_fill_requires_resize=lambda _order, _state: None,
        cancel_linked_conditional_for_protection_fill=lambda _order: None,
    )
    order = order_factory(
        exchange_order_id="EX-CANCEL",
        status=OrderStatus.SUBMITTED.value,
        quantity=Decimal("2"),
        filled_quantity=Decimal("0"),
    )
    mock_order_repo.add_order(order)

    with pytest.raises(RuntimeError, match="simulated crash"):
        applier.process_exchange_order_event(
            ExchangeOrderEvent(
                status="cancelled",
                product_id=order.product_id,
                exchange_order_id="EX-CANCEL",
                cumulative_filled_quantity=Decimal("0"),
            )
        )

    cleanup.assert_called_once_with(order)
    assert order.status == OrderStatus.SUBMITTED.value


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
    ("precision", "rounding"),
    [(6, ROUND_DOWN), (60, ROUND_HALF_UP)],
)
def test_order_event_derives_context_independent_cumulative_average_from_deltas(
    precision,
    rounding,
    mock_clock,
    mock_order_repo,
    order_factory,
):
    order_manager = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
    applier = _applier(order_manager)
    order = order_factory(
        exchange_order_id="EX-weighted-average",
        status=OrderStatus.SUBMITTED.value,
        quantity=Decimal("0.10"),
        filled_quantity=Decimal("0"),
        filled_price=Decimal("0"),
    )
    mock_order_repo.add_order(order)

    with localcontext(Context(prec=precision, rounding=rounding)):
        partial = applier.process_exchange_order_event(
            ExchangeOrderEvent(
                status="partial",
                product_id=order.product_id,
                exchange_order_id="EX-weighted-average",
                cumulative_filled_quantity=Decimal("0.04"),
                last_fill_quantity=Decimal("0.04"),
                last_fill_price=Decimal("101"),
            )
        )
        final = applier.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=order.product_id,
                exchange_order_id="EX-weighted-average",
                cumulative_filled_quantity=Decimal("0.10"),
                last_fill_quantity=Decimal("0.06"),
                last_fill_price=Decimal("103"),
            )
        )

    assert partial["action"] == "applied"
    assert final["action"] == "applied"
    assert order.filled_quantity == Decimal("0.10")
    assert order.filled_price == Decimal("102.2")
    assert [trade.price for trade in mock_order_repo.trades] == [
        Decimal("101"),
        Decimal("103"),
    ]


@pytest.mark.parametrize(
    ("precision", "rounding"),
    [(6, ROUND_DOWN), (60, ROUND_HALF_UP)],
)
def test_nonterminating_cumulative_average_uses_fixed_half_even_context(
    precision,
    rounding,
    mock_clock,
    mock_order_repo,
    order_factory,
):
    order_manager = OrderManager(mock_order_repo, mock_clock, is_backtest=True)
    applier = _applier(order_manager)
    order = order_factory(
        exchange_order_id="EX-repeating-average",
        status=OrderStatus.SUBMITTED.value,
        quantity=Decimal("0.03"),
        filled_quantity=Decimal("0"),
        filled_price=Decimal("0"),
    )
    mock_order_repo.add_order(order)

    with localcontext(Context(prec=precision, rounding=rounding)):
        applier.process_exchange_order_event(
            ExchangeOrderEvent(
                status="partial",
                product_id=order.product_id,
                exchange_order_id="EX-repeating-average",
                cumulative_filled_quantity=Decimal("0.01"),
                last_fill_quantity=Decimal("0.01"),
                last_fill_price=Decimal("1"),
            )
        )
        result = applier.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=order.product_id,
                exchange_order_id="EX-repeating-average",
                cumulative_filled_quantity=Decimal("0.03"),
                last_fill_quantity=Decimal("0.02"),
                last_fill_price=Decimal("2"),
            )
        )

    assert result["action"] == "applied"
    assert order.filled_price == Decimal("1.666666666666666666666666667")


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
