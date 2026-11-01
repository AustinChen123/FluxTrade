"""Causal shared-applier coverage across generic and Rithmic callers."""

from contextlib import nullcontext
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest
from sqlalchemy.orm import Session
from typing import Callable, ContextManager, cast
from test_execution import TestAuditedExecution as Setup
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.models import PositionSide, OrderStatus


@pytest.fixture
def family(
    mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
):
    engine, entry, sl, tp = Setup()._setup_engine_with_filled_entry(
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        signal_factory,
    )
    entry.intent_payload["system_placed_protection"] = "SL"
    entry.intent_payload["system_placed_protection_revision"] = "r7"
    sink = MagicMock()
    engine._order_event_applier.notification_sink = sink
    position = SimpleNamespace(
        strategy_id=entry.strategy_id,
        product_id=entry.product_id,
        side=PositionSide.LONG,
        quantity=entry.filled_quantity,
    )
    engine._position_loader = lambda *_: position
    return engine, entry, sl, tp, sink, position


def event_for(child, **kw):
    return ExchangeOrderEvent(
        product_id=child.product_id,
        client_order_id=child.client_order_id,
        exchange_order_id=child.exchange_order_id,
        cumulative_filled_quantity=Decimal("0"),
        **kw,
    )


@pytest.mark.parametrize(
    ("local_filled", "remote_filled", "expected_status"),
    [
        (Decimal("0"), Decimal("1"), OrderStatus.PARTIALLY_FILLED.value),
        (Decimal("1"), Decimal("2"), OrderStatus.FILLED.value),
    ],
)
@pytest.mark.parametrize("provenance", ["live_event", "repair_snapshot"])
def test_positive_fill_application_is_invariant_to_alert_provenance(
    family, local_filled, remote_filled, expected_status, provenance
):
    from src.core.adapters.rithmic_recovery import (
        build_rithmic_recovery_plan,
    )
    from test_rithmic_recovery import local_order, remote_order, snapshot

    engine, entry, sl, tp, sink, _ = family
    entry.product_id = "RITHMIC:NQ-202609"
    entry.quantity = Decimal("2")
    sl.status = OrderStatus.CANCELLED.value
    tp.status = OrderStatus.CANCELLED.value
    entry.filled_quantity = local_filled
    entry.status = (
        OrderStatus.PARTIALLY_FILLED.value
        if local_filled
        else OrderStatus.SUBMITTED.value
    )
    local = local_order(
        id=str(entry.id),
        client_order_id=entry.client_order_id,
        exchange_order_id=entry.exchange_order_id,
        product_id=entry.product_id,
        quantity=entry.quantity,
        status=entry.status,
        filled_quantity=local_filled,
    )
    remote = remote_order(
        client_order_id=entry.client_order_id,
        exchange_order_id=entry.exchange_order_id,
        basket_id=entry.exchange_order_id,
        status="OPEN" if remote_filled < entry.quantity else "COMPLETE",
        notification_type="FILL",
        filled_quantity=str(remote_filled),
        unfilled_quantity=str(entry.quantity - remote_filled),
        average_fill_price="42000",
    )
    plan, external = build_rithmic_recovery_plan(
        [local], snapshot(orders=[remote])
    )
    assert external == []
    assert plan[0].event is not None
    event = replace(plan[0].event, protection_alert_source=provenance)
    assert event.is_snapshot_projection is False

    result = engine.process_exchange_order_event(event)

    assert result["action"] == "applied", result
    assert entry.filled_quantity == remote_filled
    assert entry.status == expected_status
    assert result["fill_quantity"] == remote_filled - local_filled
    sink.emit.assert_not_called()










@pytest.mark.parametrize(
    ("local_filled", "remote_filled", "expected_status"),
    [
        (Decimal("0"), Decimal("1"), OrderStatus.PARTIALLY_FILLED.value),
        (Decimal("1"), Decimal("2"), OrderStatus.FILLED.value),
    ],
)
def test_real_rithmic_owned_runtime_bootstrap_applies_positive_fill(
    family,
    mock_clock,
    mock_db_session,
    local_filled,
    remote_filled,
    expected_status,
):
    from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
    from src.core.adapters.rithmic_runtime_composition import RithmicRuntimeBootstrap
    from src.core.runtime_capabilities import OrderAccountIdentity
    from src.core.order_reconciliation import OrderReconciler
    from test_rithmic_recovery import remote_order, snapshot, LedgerPositionFixture

    engine, entry, sl, tp, sink, position = family
    product = "RITHMIC:NQ-202609"
    entry.product_id = product
    entry.exchange_id = "RITHMIC"
    entry.account_profile = "test"
    entry.account_id = "ACCOUNT"
    entry.quantity = Decimal("2")
    entry.filled_quantity = local_filled
    entry.status = (
        OrderStatus.PARTIALLY_FILLED.value
        if local_filled
        else OrderStatus.SUBMITTED.value
    )
    entry.side = str(getattr(entry.side, "value", entry.side))
    sl.status = OrderStatus.CANCELLED.value
    tp.status = OrderStatus.CANCELLED.value
    position.product_id = product
    position.quantity = remote_filled

    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments={
            product: {"exchange": "CME", "quantity_step": "1", "price_tick": "0.25"}
        },
        client_factory=MagicMock(),
    )
    bootstrap = RithmicRuntimeBootstrap(
        order_account_identity=OrderAccountIdentity(
            account_profile="test", account_id="ACCOUNT"
        ),
        is_rithmic_runtime=True,
    )
    engine._order_event_processor = bootstrap.process_order_event
    reconciler = OrderReconciler(
        adapter=adapter,
        order_manager=engine.order_manager,
        clock=mock_clock,
        db_session_factory=cast(
            Callable[[], ContextManager[Session]],
            lambda: nullcontext(mock_db_session),
        ),
        process_exchange_order_event=engine.process_exchange_order_event,
        place_pending_protection_for_filled_entries=MagicMock(),
        fail_pending_conditionals_for_terminal_entry=engine._fail_pending_conditional_orders_for_terminal_entry,
        protective_terminal_without_fill_failure=engine._protective_terminal_without_fill_failure,
        cancel_protective_order_when_sibling_closed=MagicMock(),
        cancel_linked_conditional_for_protection_fill=MagicMock(),
        local_positions_loader=lambda: [position],
    )
    # Keep the real owned reconciler and adapter path while restricting this
    # recovery snapshot to the entry whose positive fill is under test.
    owned = reconciler._owned_order_reconciler
    assert owned is not None
    remote = remote_order(
        client_order_id=entry.client_order_id,
        basket_id=entry.exchange_order_id,
        status="OPEN" if remote_filled < entry.quantity else "COMPLETE",
        notification_type="FILL",
        transaction_type="BUY",
        filled_quantity=str(remote_filled),
        unfilled_quantity=str(entry.quantity - remote_filled),
        average_fill_price="42000",
    )
    ledger = snapshot(
        orders=[remote],
        positions=[
            LedgerPositionFixture(symbol="NQU6", net_quantity=str(remote_filled))
        ],
    )
    # The context callback is the production list owner; narrow its output to
    # one real persisted engine order for this causal regression.
    engine.order_manager.repo.list_client_orders_by_statuses = (
        lambda *args, **kwargs: [entry]
    )

    result = reconciler.reconcile_owned_orders(
        snapshot_loader=lambda *args, **kwargs: ledger
    )

    results = cast(list[dict[str, object]], result["results"])
    assert results[0]["repair_action"] == "applied", result
    assert result["unresolved_count"] == 0
    assert result["verification_blocked_count"] == 0
    assert result["auto_resume_safe"] is True
    assert entry.filled_quantity == remote_filled
    assert entry.status == expected_status
    assert sink.emit.call_count == 0


