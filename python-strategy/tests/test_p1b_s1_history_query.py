import ast
import inspect
from itertools import product
import logging
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from src.core.models import OrderStatus
from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.interfaces.exchange import OwnedOrderReconciliationContext
from src.core.order_reconciliation import OrderReconciler
from src.core.repositories import LiveOrderRepository


_HISTORY_STATUSES = [
    OrderStatus.NEW.value,
    OrderStatus.SUBMITTED_UNCONFIRMED.value,
    OrderStatus.SUBMITTED.value,
    OrderStatus.PARTIALLY_FILLED.value,
    OrderStatus.FILLED.value,
    OrderStatus.CANCELLED.value,
    OrderStatus.FAILED.value,
    OrderStatus.LIQUIDATED.value,
    "UNKNOWN_STATUS",
]


@pytest.mark.parametrize("parent_status,child_status", list(product(_HISTORY_STATUSES, repeat=2)))
@pytest.mark.parametrize("declaration", ["NONE", "SL", "TP", "SL+TP"])
def test_repository_history_query_includes_all_status_parents_and_linked_children(
    sqlite_order_session_factory,
    order_factory,
    parent_status,
    child_status,
    declaration,
):
    repository = LiveOrderRepository(
        db_session_factory=sqlite_order_session_factory,
        account_profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_001",
    )
    parent = order_factory(
        order_id=f"parent-{parent_status}-{child_status}-{declaration}",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        status=parent_status,
        client_order_id=f"parent-client-{parent_status}-{child_status}-{declaration}",
    )
    parent.intent_payload = {
        "system_placed_protection": declaration,
        "system_placed_protection_revision": "entry-revision",
    }
    repository.add_order(parent)
    child = order_factory(
        order_id=f"child-{parent_status}-{child_status}-{declaration}",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        order_type="stop_loss",
        status=child_status,
        client_order_id=f"child-client-{parent_status}-{child_status}-{declaration}",
    )
    child.intent_payload = {"pending_entry_order_id": parent.id}
    repository.add_order(child)
    native_child = order_factory(
        order_id=f"native-child-{parent_status}-{child_status}-{declaration}",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        order_type="take_profit",
        status=child_status,
        client_order_id=f"native-client-{parent_status}-{child_status}-{declaration}",
    )
    native_child.intent_payload = {
        "native_parent_client_order_id": parent.client_order_id
    }
    repository.add_order(native_child)
    legacy = order_factory(
        order_id=f"legacy-{parent_status}-{child_status}-{declaration}",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        status=parent_status,
    )
    repository.add_order(legacy)
    invalid_declaration = order_factory(
        order_id=f"invalid-declaration-{parent_status}-{child_status}-{declaration}",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        status=parent_status,
    )
    invalid_declaration.intent_payload = {
        "system_placed_protection": "INVALID",
        "system_placed_protection_revision": "invalid-revision",
    }
    repository.add_order(invalid_declaration)

    foreign_repository = LiveOrderRepository(
        db_session_factory=sqlite_order_session_factory,
        account_profile="rithmic:other",
        account_id="TEST_ACCOUNT_002",
    )
    foreign = order_factory(
        order_id=f"foreign-{parent_status}-{child_status}-{declaration}",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        status=parent_status,
    )
    foreign.intent_payload = {
        "system_placed_protection": "SL",
        "system_placed_protection_revision": "other-revision",
    }
    foreign_repository.add_order(foreign)
    other_exchange = order_factory(
        order_id=f"other-exchange-{parent_status}-{child_status}-{declaration}",
        exchange_id="BINANCE",
        product_id="BINANCE:BTCUSDT-PERP",
        status=parent_status,
    )
    other_exchange.intent_payload = {
        "system_placed_protection": "SL",
        "system_placed_protection_revision": "binance-revision",
    }
    repository.add_order(other_exchange)

    result = repository.list_declared_protection_history_orders(
        account_profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_001",
        exchange_id="RITHMIC",
    )

    assert {order.id for order in result} == {parent.id, child.id, native_child.id}
    assert parent.status == parent_status
    assert all(order.status == child_status for order in (child, native_child))


def test_core_interfaces_do_not_import_venue_adapters():
    from src.core.interfaces import exchange, repository

    for module in (exchange, repository):
        tree = ast.parse(inspect.getsource(module))
        imports = [
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        ]
        assert not any(name.startswith("src.core.adapters") for name in imports)


def test_repository_history_query_isolates_each_account_scope_dimension(
    sqlite_order_session_factory, order_factory
):
    target = LiveOrderRepository(
        db_session_factory=sqlite_order_session_factory,
        account_profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_001",
    )
    unscoped = LiveOrderRepository(db_session_factory=sqlite_order_session_factory)
    same_profile_other_account = order_factory(
        order_id="same-profile-other-account",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        status=OrderStatus.FILLED.value,
        account_profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_002",
    )
    same_profile_other_account.intent_payload = {
        "system_placed_protection": "SL",
        "system_placed_protection_revision": "revision-a",
    }
    unscoped.add_order(same_profile_other_account)

    same_account_other_profile = order_factory(
        order_id="same-account-other-profile",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        status=OrderStatus.CANCELLED.value,
        account_profile="rithmic:other",
        account_id="TEST_ACCOUNT_001",
    )
    same_account_other_profile.intent_payload = {
        "system_placed_protection": "TP",
        "system_placed_protection_revision": "revision-b",
    }
    unscoped.add_order(same_account_other_profile)

    result = target.list_declared_protection_history_orders(
        account_profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_001",
        exchange_id="RITHMIC",
    )

    assert result == []


def test_production_context_exposes_scoped_terminal_history_without_provider_io(
    sqlite_order_session_factory, order_factory, monkeypatch
):
    adapter = RithmicExchangeAdapter(
        profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_001",
        instruments={
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
    )
    repository = LiveOrderRepository(
        db_session_factory=sqlite_order_session_factory,
        account_profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_001",
    )
    parent = order_factory(
        order_id="terminal-parent",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        timestamp=1_700_000_123_000,
        status=OrderStatus.CANCELLED.value,
        account_profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_001",
    )
    parent.intent_payload = {
        "system_placed_protection": "SL+TP",
        "system_placed_protection_revision": "entry-revision",
    }
    repository.add_order(parent)
    child = order_factory(
        order_id="terminal-child",
        exchange_id="RITHMIC",
        product_id="RITHMIC:NQ-202609",
        timestamp=1_700_000_124_000,
        status=OrderStatus.CANCELLED.value,
        account_profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_001",
    )
    child.intent_payload = {"pending_entry_order_id": parent.id}
    repository.add_order(child)
    history_query = Mock(wraps=repository.list_declared_protection_history_orders)
    repository.list_declared_protection_history_orders = history_query
    snapshot_loader = Mock(
        return_value=SimpleNamespace(
            account_id="TEST_ACCOUNT_001",
            account_currency="USD",
            account_summary=SimpleNamespace(
                account_balance="1",
                cash_on_hand="1",
                available_buying_power="1",
                day_pnl="0",
                net_quantity="0",
                timestamp_ms=1,
            ),
            orders=[],
            order_history=[],
            fills=[],
            positions=[],
        )
    )
    reconciler = OrderReconciler(
        adapter=adapter,
        order_manager=SimpleNamespace(repo=repository),
        clock=SimpleNamespace(now=lambda: 1_800_000_000),
        db_session_factory=sqlite_order_session_factory,
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
    )
    capability = reconciler._owned_order_reconciler
    assert capability is not None
    monkeypatch.setattr(
        capability,
        "_write_recovery_audit",
        lambda payload, *, phase="completed": payload,
    )

    result = capability.reconcile(snapshot_loader=snapshot_loader)

    snapshot_loader.assert_called_once()
    history_query.assert_not_called()
    assert "declaration_history_evidence" not in result
    assert result["recoverable_count"] == 0
    assert [
        order.id for order in reconciler.list_declared_protection_history_orders()
    ] == [parent.id, child.id]
    history_query.assert_called_once_with(
        account_profile="rithmic:lucid",
        account_id="TEST_ACCOUNT_001",
        exchange_id="rithmic",
    )


def test_recovery_candidates_preserve_base_56ae1da7_order_and_full_payload(
    monkeypatch,
):
    from src.core.adapters import rithmic_owned_order_reconciliation as module
    from src.core.adapters.rithmic_owned_order_reconciliation import (
        RithmicOwnedOrderReconciler,
    )

    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments={
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
    )
    later = SimpleNamespace(
        id="later-row",
        exchange_id="rithmic",
        timestamp=1_700_000_200_000,
        account_profile="test",
        account_id="ACCOUNT",
        client_order_id="client-later",
        exchange_order_id="basket-later",
        product_id="RITHMIC:NQ-202609",
        side="buy",
        quantity=1,
        status="SUBMITTED",
        filled_quantity=0,
        filled_price=0,
        type="market",
        intent_payload=None,
    )
    earlier = SimpleNamespace(
        id="earlier-row",
        exchange_id="rithmic",
        timestamp=1_700_000_100_000,
        account_profile="test",
        account_id="ACCOUNT",
        client_order_id="client-earlier",
        exchange_order_id="basket-earlier",
        product_id="RITHMIC:NQ-202609",
        side="buy",
        quantity=1,
        status="SUBMITTED",
        filled_quantity=0,
        filled_price=0,
        type="market",
        intent_payload=None,
    )
    candidate_rows = [later, earlier]
    restored_rows = []
    snapshot_rows = []
    event_processor = Mock(return_value={"action": "applied"})
    context = SimpleNamespace(
        db_session_factory=object(),
        list_recoverable_client_orders=lambda: candidate_rows,
        list_declared_protection_history_orders=lambda: pytest.fail(
            "S1 recovery must not query declaration history"
        ),
        process_exchange_order_event=event_processor,
        now_seconds=lambda: 1_800_000_000,
        local_positions_loader=lambda: [],
        logger=logging.getLogger("test.history.base-order"),
    )
    capability = RithmicOwnedOrderReconciler(
        adapter=adapter,
        profile="test",
        account_id="ACCOUNT",
        validate_snapshot_identities=lambda _snapshot: None,
        context=cast(OwnedOrderReconciliationContext, context),
    )
    snapshot = SimpleNamespace(
        account_id="ACCOUNT",
        account_currency="USD",
        orders=[],
        order_history=[
            SimpleNamespace(
                basket_id="basket-later",
                client_order_id="client-later",
                symbol="NQU6",
                exchange="CME",
                transaction_type="BUY",
                quantity="1",
                price_type=None,
                bracket_type=None,
                original_basket_id=None,
                status="CANCELLED",
                notification_type="CANCEL",
                filled_quantity="0",
                unfilled_quantity="1",
                average_fill_price=None,
                timestamp_ms=1_700_000_201_000,
            ),
            SimpleNamespace(
                basket_id="basket-earlier",
                client_order_id="client-earlier",
                symbol="NQU6",
                exchange="CME",
                transaction_type="BUY",
                quantity="1",
                price_type=None,
                bracket_type=None,
                original_basket_id=None,
                status="CANCELLED",
                notification_type="CANCEL",
                filled_quantity="0",
                unfilled_quantity="1",
                average_fill_price=None,
                timestamp_ms=1_700_000_101_000,
            ),
        ],
        fills=[],
        positions=[],
        account_summary=None,
    )

    def restore_order_groups(orders):
        restored_rows.extend(orders)

    def load_snapshot(_profile, _account_id, orders, *_args, **_kwargs):
        snapshot_rows.extend(orders)
        return snapshot

    monkeypatch.setattr(adapter, "restore_order_groups", restore_order_groups)
    monkeypatch.setattr(module, "load_rithmic_recovery_snapshot", load_snapshot)
    monkeypatch.setattr(
        capability,
        "_verify_ledger",
        lambda *_args, **_kwargs: {"verification_blocked": False},
    )
    monkeypatch.setattr(
        capability,
        "_write_recovery_audit",
        lambda payload, *, phase="completed": payload,
    )

    result = capability.reconcile()

    # Base 56ae1da7 preserved callback order through restoration, snapshot
    # acquisition, real planning, event application, and the decision payload.
    expected_order = [later, earlier]
    assert restored_rows == expected_order
    assert snapshot_rows == expected_order
    assert [call.args[0].client_order_id for call in event_processor.call_args_list] == [
        "client-later",
        "client-earlier",
    ]
    assert result == {
        "recoverable_count": 2,
        "matched_count": 0,
        "repaired_count": 2,
        "external_count": 0,
        "unresolved_count": 0,
        "verification_blocked_count": 0,
        "auto_resume_safe": True,
        "results": [
            {
                "order_id": "later-row",
                "client_order_id": "client-later",
                "classification": "repaired",
                "reason": None,
                "repair_action": "applied",
                "verification_blocked": False,
                "unresolved": False,
            },
            {
                "order_id": "earlier-row",
                "client_order_id": "client-earlier",
                "classification": "repaired",
                "reason": None,
                "repair_action": "applied",
                "verification_blocked": False,
                "unresolved": False,
            },
        ],
        "external_orders": [],
        "ledger_verification": {"verification_blocked": False},
    }
