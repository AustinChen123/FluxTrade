import logging
from contextlib import nullcontext
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from src.core.adapters.rithmic_recovery import (
    build_rithmic_recovery_plan,
    compare_rithmic_positions,
    load_rithmic_recovery_snapshot,
    rithmic_order_may_be_working,
)
from src.core.adapters.rithmic_owned_order_reconciliation import (
    RithmicOwnedOrderReconciler,
)
from src.core.interfaces.exchange import OwnedOrderReconciliationContext


SAFE_SNAPSHOT_FAILURES = [
    ("profile_lease", "profile_lease_failed", "profile lease failed"),
    (
        "runtime_initialization",
        "runtime_initialization_failed",
        "runtime initialization failed",
    ),
    (
        "request_validation",
        "invalid_ledger_snapshot_request",
        "ledger snapshot request validation failed",
    ),
    ("order_config", "order_config_failed", "ORDER config failed"),
    ("order_connect", "order_connect_failed", "ORDER connect failed"),
    ("order_heartbeat", "order_heartbeat_failed", "ORDER heartbeat failed"),
    ("order_login_info", "order_login_info_failed", "ORDER login info failed"),
    ("order_account_list", "order_account_list_failed", "ORDER account list failed"),
    ("order_snapshot", "order_snapshot_failed", "ORDER snapshot failed"),
    ("order_history", "order_history_failed", "ORDER history failed"),
    ("fill_history", "fill_history_failed", "fill history failed"),
    ("pnl_config", "pnl_config_failed", "PNL config failed"),
    ("pnl_connect", "pnl_connect_failed", "PNL connect failed"),
    ("pnl_heartbeat", "pnl_heartbeat_failed", "PNL heartbeat failed"),
    ("pnl_request", "pnl_request_failed", "PNL request failed"),
    ("pnl_snapshot", "pnl_snapshot_failed", "PNL snapshot failed"),
    (
        "unclassified_internal",
        "unclassified_ledger_snapshot_failure",
        "ledger snapshot failed before safe classification",
    ),
]
SNAPSHOT_DIAGNOSTIC_KEYS = (
    "snapshot_error_type",
    "snapshot_error_stage",
    "snapshot_error_code",
    "snapshot_error_cause",
)
RAW_SENTINELS = (
    "PROVIDER_SECRET_123 ACCOUNT_ID_SECRET_123 BASKET_ID_SECRET_123 "
    "STATUS_SECRET_123 FCM_ID_SECRET_123 IB_ID_SECRET_123 PROFILE_SECRET_123 "
    "URL_SECRET_123 USER_SECRET_123"
).split()


def owned_reconciler(
    *,
    adapter,
    order_manager,
    clock,
    db_session_factory,
    process_exchange_order_event,
    local_positions_loader=None,
    logger=None,
    profile="test",
    account_id="ACCOUNT",
    **_unused,
):
    return RithmicOwnedOrderReconciler(
        adapter=adapter,
        profile=profile,
        account_id=account_id,
        context=OwnedOrderReconciliationContext(
            list_recoverable_client_orders=lambda: (
                order_manager.repo.list_client_orders_by_statuses(
                    {"NEW", "SUBMITTED_UNCONFIRMED", "SUBMITTED", "PARTIALLY_FILLED"}
                )
            ),
            process_exchange_order_event=process_exchange_order_event,
            now_seconds=lambda: float(clock.now()),
            db_session_factory=db_session_factory,
            local_positions_loader=local_positions_loader,
            logger=logger or logging.getLogger("OrderReconciler"),
        ),
    )


def snapshot_failure(exception_type=RuntimeError, **attributes):
    raw = " ".join(RAW_SENTINELS)
    error = exception_type(raw)
    for name, value in attributes.items():
        setattr(error, name, value)
    try:
        raise ValueError(raw)
    except ValueError as source:
        try:
            raise error from source
        except Exception as chained:
            return chained


def assert_no_raw_sentinels(value):
    pending = [value]
    while pending:
        item = pending.pop()
        if type(item) is str:
            assert all(sentinel not in item for sentinel in RAW_SENTINELS)
        elif type(item) is dict:
            pending.extend((*item.keys(), *item.values()))
        elif type(item) in (list, tuple, set, frozenset):
            pending.extend(item)


def assert_snapshot_diagnostics(result, event_payload, records, expected):
    diagnostics = [
        record
        for record in records
        if record.getMessage() == "Rithmic ledger snapshot acquisition failed"
    ]
    assert len(diagnostics) == 1
    surfaces = (
        tuple(result[key] for key in SNAPSHOT_DIAGNOSTIC_KEYS),
        tuple(event_payload[key] for key in SNAPSHOT_DIAGNOSTIC_KEYS),
        tuple(getattr(diagnostics[0], key) for key in SNAPSHOT_DIAGNOSTIC_KEYS),
    )
    assert surfaces == (expected, expected, expected)
    assert_no_raw_sentinels((result, event_payload, vars(diagnostics[0])))


def local_order(**overrides):
    values = {
        "id": "local-1",
        "client_order_id": "flux-1",
        "exchange_order_id": "basket-1",
        "exchange_id": "rithmic",
        "account_profile": "test",
        "account_id": "ACCOUNT",
        "product_id": "RITHMIC:NQ-202609",
        "side": "buy",
        "quantity": Decimal("2"),
        "status": "SUBMITTED",
        "filled_quantity": Decimal("0"),
        "filled_price": Decimal("0"),
        "timestamp": 1_700_000_123_000,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def remote_order(**overrides):
    values = {
        "client_order_id": "flux-1",
        "exchange_order_id": "exchange-1",
        "basket_id": "basket-1",
        "symbol": "NQU6",
        "status": "OPEN",
        "notification_type": "OPEN",
        "transaction_type": "BUY",
        "quantity": "2",
        "filled_quantity": "0",
        "unfilled_quantity": "2",
        "average_fill_price": None,
        "timestamp_ms": 1_700_000_124_000,
        "original_basket_id": None,
        "price_type": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def remote_fill(**overrides):
    values = {
        "basket_id": "basket-1",
        "exchange_order_id": "exchange-1",
        "fill_id": "fill-1",
        "exchange": "CME",
        "symbol": "NQU6",
        "transaction_type": "BUY",
        "fill_quantity": "1",
        "fill_price": "20000.25",
        "timestamp_ms": 1_700_000_124_000,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def snapshot(*, orders=(), order_history=(), fills=(), positions=(), account_summary=True):
    return SimpleNamespace(
        account_id="ACCOUNT",
        account_currency="USD",
        orders=list(orders),
        order_history=list(order_history),
        fills=list(fills),
        positions=list(positions),
        account_summary=(
            SimpleNamespace(account_balance="100000") if account_summary else None
        ),
    )


@pytest.mark.parametrize(
    ("remote", "expected"),
    [
        (remote_order(), True),
        (
            remote_order(
                status="complete",
                notification_type="COMPLETE",
                filled_quantity=None,
                completion_reason="FA",
            ),
            False,
        ),
        (
            remote_order(
                status="complete",
                notification_type="CANCEL",
                filled_quantity="2",
            ),
            True,
        ),
        (
            remote_order(
                status="complete",
                notification_type="REJECT",
                filled_quantity="0",
            ),
            False,
        ),
        (
            remote_order(
                status="OPEN",
                notification_type="FILL",
                filled_quantity="2",
            ),
            False,
        ),
        (
            remote_order(
                status="OPEN",
                notification_type="OPEN",
                filled_quantity="2",
            ),
            True,
        ),
    ],
)
def test_rithmic_order_working_classifier_is_fail_closed(remote, expected):
    assert rithmic_order_may_be_working(remote) is expected


@pytest.mark.parametrize(
    ("working", "history", "fills", "classification", "status", "unresolved"),
    [
        ([remote_order()], [], [], "matched", "open", False),
        (
            [remote_order(status="OPEN", filled_quantity="1", average_fill_price="20000.25")],
            [],
            [],
            "repaired",
            "partially_filled",
            False,
        ),
        ([], [remote_order(status="CANCELLED")], [], "repaired", "cancelled", False),
        (
            [],
            [],
            [remote_fill(), remote_fill(fill_id="fill-2", fill_price="20000.75")],
            "repaired",
            "filled",
            False,
        ),
        ([], [], [remote_fill()], "repaired_partial", "partially_filled", True),
        (
            [],
            [remote_order(status="COMPLETE", filled_quantity="2", average_fill_price="20000.50")],
            [],
            "repaired",
            "filled",
            False,
        ),
        (
            [],
            [
                remote_order(
                    status="complete",
                    notification_type="CANCEL",
                    filled_quantity="0",
                    unfilled_quantity="2",
                )
            ],
            [],
            "repaired",
            "cancelled",
            False,
        ),
        (
            [],
            [
                remote_order(
                    status="complete",
                    notification_type="REJECT",
                    filled_quantity="0",
                    unfilled_quantity="2",
                )
            ],
            [],
            "repaired",
            "failed",
            False,
        ),
    ],
)
def test_owned_order_recovery_state_matrix(
    working,
    history,
    fills,
    classification,
    status,
    unresolved,
):
    plan, external = build_rithmic_recovery_plan(
        [local_order()],
        snapshot(orders=working, order_history=history, fills=fills),
    )

    assert external == []
    assert plan[0].classification == classification
    assert plan[0].event.status == status
    assert plan[0].unresolved is unresolved


def test_recovery_uses_latest_available_fill_timestamp_when_one_is_missing():
    plan, external = build_rithmic_recovery_plan(
        [local_order()],
        snapshot(
            fills=[
                remote_fill(timestamp_ms=None),
                remote_fill(fill_id="fill-2", timestamp_ms=1_700_000_125_000),
            ]
        ),
    )

    assert external == []
    assert plan[0].classification == "repaired"
    assert plan[0].event is not None
    assert plan[0].event.status == "filled"
    assert plan[0].event.event_timestamp == 1_700_000_125_000


def test_recovery_matches_cloned_python_snapshot_rows_by_stable_identity():
    class CloningSnapshot:
        account_id = "ACCOUNT"
        account_currency = "USD"
        order_history = []
        fills = []
        positions = []
        account_summary = SimpleNamespace(account_balance="100000")

        @property
        def orders(self):
            return [
                remote_order(
                    status="complete",
                    notification_type="CANCEL",
                    filled_quantity="0",
                    unfilled_quantity="2",
                )
            ]

    plan, external = build_rithmic_recovery_plan(
        [local_order()],
        CloningSnapshot(),
    )

    assert external == []
    assert plan[0].classification == "repaired"
    assert plan[0].event.status == "cancelled"


@pytest.mark.parametrize(
    ("remote_snapshot", "reason"),
    [
        (snapshot(), "no_authoritative_remote_evidence"),
        (
            snapshot(order_history=[remote_order(status="COMPLETE", filled_quantity="1")]),
            "unknown_rithmic_order_status",
        ),
        (
            snapshot(
                order_history=[
                    remote_order(
                        status="COMPLETE",
                        notification_type="COMPLETE",
                        filled_quantity="0",
                        unfilled_quantity="2",
                    )
                ]
            ),
            "unknown_rithmic_order_status",
        ),
        (
            snapshot(orders=[remote_order(), remote_order(exchange_order_id="exchange-2")]),
            "duplicate_remote_identity",
        ),
        (
            snapshot(orders=[remote_order(symbol="ESU6")]),
            "product_symbol_mismatch",
        ),
        (snapshot(orders=[remote_order(quantity="3")]), "order_quantity_mismatch"),
        (snapshot(orders=[remote_order(transaction_type="SELL")]), "order_side_mismatch"),
        (
            snapshot(
                order_history=[
                    remote_order(status="OPEN", timestamp_ms=None),
                    remote_order(status="CANCELLED", timestamp_ms=None),
                ]
            ),
            "ambiguous_order_history_ordering",
        ),
        (
            snapshot(
                order_history=[remote_order(status="CANCELLED", filled_quantity="0")],
                fills=[remote_fill()],
            ),
            "order_and_fill_history_quantity_mismatch",
        ),
        (
            snapshot(fills=[remote_fill(symbol="ESU6")]),
            "fill_product_symbol_mismatch",
        ),
        (
            snapshot(fills=[remote_fill(transaction_type="SELL")]),
            "fill_side_mismatch",
        ),
        (
            snapshot(fills=[remote_fill(transaction_type="unknown")]),
            "unknown_fill_transaction_type",
        ),
        (
            snapshot(
                orders=[
                    remote_order(
                        status="OPEN",
                        filled_quantity="2",
                        average_fill_price="20000.25",
                    )
                ]
            ),
            "unknown_rithmic_order_status",
        ),
        (
            snapshot(orders=[remote_order(status="PARTIAL", filled_quantity="0")]),
            "unknown_rithmic_order_status",
        ),
        (
            snapshot(
                order_history=[
                    remote_order(
                        status="FILLED",
                        filled_quantity="1",
                        average_fill_price="20000.25",
                    )
                ]
            ),
            "unknown_rithmic_order_status",
        ),
    ],
)
def test_owned_order_recovery_fails_closed_without_unambiguous_evidence(
    remote_snapshot,
    reason,
):
    plan, _ = build_rithmic_recovery_plan([local_order()], remote_snapshot)

    assert plan[0].classification == "unresolved"
    assert plan[0].event is None
    assert plan[0].reason == reason


def test_duplicate_identical_fills_are_idempotent():
    fill = remote_fill()
    plan, _ = build_rithmic_recovery_plan(
        [local_order()],
        snapshot(fills=[fill, SimpleNamespace(**vars(fill))]),
    )

    assert plan[0].event.cumulative_filled_quantity == Decimal("1")


def test_duplicate_local_order_identity_fails_closed_before_repair():
    plan, _ = build_rithmic_recovery_plan(
        [local_order(), local_order(id="local-2")],
        snapshot(orders=[remote_order()]),
    )

    assert [item.reason for item in plan] == [
        "duplicate_local_identity",
        "duplicate_local_identity",
    ]


def test_matching_working_partial_order_is_not_counted_as_repaired():
    order = local_order(
        status="PARTIALLY_FILLED",
        filled_quantity=Decimal("1"),
        filled_price=Decimal("20000.25"),
    )
    plan, _ = build_rithmic_recovery_plan(
        [order],
        snapshot(
            orders=[
                remote_order(
                    status="OPEN",
                    filled_quantity="1",
                    average_fill_price="20000.25",
                )
            ]
        ),
    )

    assert plan[0].classification == "matched"


def test_remote_only_working_order_is_reported_without_adoption():
    plan, external = build_rithmic_recovery_plan(
        [local_order()],
        snapshot(
            orders=[
                remote_order(),
                remote_order(
                    basket_id="manual-1",
                    client_order_id=None,
                    status="OPEN",
                ),
            ]
        ),
    )

    assert plan[0].classification == "matched"
    assert external == [
        {"basket_id": "manual-1", "client_order_id": None, "status": "OPEN"}
    ]


def test_remote_only_terminal_orders_do_not_trigger_external_order_lockdown():
    _, external = build_rithmic_recovery_plan(
        [local_order()],
        snapshot(
            orders=[
                remote_order(),
                remote_order(
                    basket_id="cancelled-1",
                    client_order_id=None,
                    status="complete",
                    notification_type="CANCEL",
                    filled_quantity="0",
                ),
                remote_order(
                    basket_id="rejected-1",
                    client_order_id=None,
                    status="complete",
                    notification_type="REJECT",
                    filled_quantity="0",
                ),
                remote_order(
                    basket_id="filled-1",
                    client_order_id=None,
                    status="complete",
                    notification_type="FILL",
                    filled_quantity="2",
                ),
                remote_order(
                    basket_id="status-cancelled-1",
                    client_order_id=None,
                    status="cancelled",
                    notification_type="STATUS",
                    filled_quantity="0",
                ),
                remote_order(
                    basket_id="inconsistent-filled-1",
                    client_order_id=None,
                    status="filled",
                    notification_type="STATUS",
                    filled_quantity="0",
                ),
                remote_order(
                    basket_id="inconsistent-open-full-1",
                    client_order_id=None,
                    status="OPEN",
                    notification_type="STATUS",
                    filled_quantity="2",
                ),
                remote_order(
                    basket_id="inconsistent-cancel-full-1",
                    client_order_id=None,
                    status="complete",
                    notification_type="CANCEL",
                    filled_quantity="2",
                ),
                remote_order(
                    basket_id="inconsistent-reject-full-1",
                    client_order_id=None,
                    status="complete",
                    notification_type="REJECT",
                    filled_quantity="2",
                ),
            ]
        ),
    )

    assert external == [
        {
            "basket_id": "inconsistent-filled-1",
            "client_order_id": None,
            "status": "filled",
        },
        {
            "basket_id": "inconsistent-open-full-1",
            "client_order_id": None,
            "status": "OPEN",
        },
        {
            "basket_id": "inconsistent-cancel-full-1",
            "client_order_id": None,
            "status": "complete",
        },
        {
            "basket_id": "inconsistent-reject-full-1",
            "client_order_id": None,
            "status": "complete",
        },
    ]


@pytest.mark.parametrize("status", ["expired", "failed"])
@pytest.mark.parametrize(
    ("filled_quantity", "is_external"),
    [("0", False), ("1", False), ("2", True)],
)
def test_remote_only_failed_terminal_fill_matrix(
    status,
    filled_quantity,
    is_external,
):
    _, external = build_rithmic_recovery_plan(
        [local_order()],
        snapshot(
            orders=[
                remote_order(),
                remote_order(
                    basket_id=f"{status}-{filled_quantity}",
                    client_order_id=None,
                    status=status,
                    notification_type="STATUS",
                    filled_quantity=filled_quantity,
                ),
            ]
        ),
    )

    assert bool(external) is is_external


def test_snapshot_loader_is_called_once_with_bounded_owned_window():
    loader = Mock(return_value=snapshot())
    orders = [
        local_order(exchange_order_id="basket-2", timestamp=1_700_000_125_000),
        local_order(id="local-2", exchange_order_id="basket-1"),
    ]

    result = load_rithmic_recovery_snapshot(
        "test",
        "ACCOUNT",
        orders,
        1_700_000_200,
        loader,
    )

    assert result.account_id == "ACCOUNT"
    loader.assert_called_once_with(
        "test",
        "ACCOUNT",
        recovery_basket_ids=["basket-1", "basket-2"],
        fill_start_index=1_700_000_122,
        fill_finish_index=1_700_000_201,
    )


def test_default_snapshot_loader_resolves_the_native_extension_export(monkeypatch):
    loader = Mock(return_value=snapshot())
    module_loader = Mock(
        return_value=SimpleNamespace(
            rithmic_ledger_snapshot=loader,
        )
    )
    monkeypatch.setattr(
        "src.core.adapters.rithmic_recovery.import_module",
        module_loader,
    )

    result = load_rithmic_recovery_snapshot(
        "test",
        "ACCOUNT",
        [],
        1_700_000_200,
    )

    assert result.account_id == "ACCOUNT"
    module_loader.assert_called_once_with("fluxtrade_core")
    loader.assert_called_once_with("test", "ACCOUNT")


def test_snapshot_loader_includes_persisted_native_parent_basket():
    loader = Mock(return_value=snapshot())
    child = local_order(
        exchange_order_id="child-stop-1",
        intent_payload={"native_parent_basket_id": "parent-1"},
    )

    load_rithmic_recovery_snapshot(
        "test",
        "ACCOUNT",
        [child],
        1_700_000_200,
        loader,
    )

    assert loader.call_args.kwargs["recovery_basket_ids"] == [
        "child-stop-1",
        "parent-1",
    ]


def test_native_child_is_recovered_without_parent_in_local_active_set():
    child = local_order(
        id="stop-1",
        client_order_id="strategy-execution-sl-123",
        exchange_order_id="child-stop-1",
        side="sell",
        quantity=Decimal("1"),
        type="stop_loss",
        intent_payload={
            "placement_mode": "attach-at-entry",
            "native_leg_type": "stop_loss",
            "native_parent_basket_id": "parent-1",
            "native_parent_client_order_id": "strategy-execution-long-123",
        },
    )
    remote = remote_order(
        client_order_id="strategy-execution-long-123",
        basket_id="child-stop-1",
        original_basket_id="parent-1",
        price_type="stop_market",
        transaction_type="SELL",
        quantity="1",
        trigger_price="19998.25",
        bracket_type="stop_only_static",
    )

    plan, external = build_rithmic_recovery_plan([child], snapshot(orders=[remote]))

    assert external == []
    assert plan[0].classification == "matched"
    assert plan[0].event.raw["trigger_price"] == "19998.25"
    assert plan[0].event.raw["price_type"] == "stop_market"


def test_native_child_with_wrong_parent_is_blocked_and_reported_external():
    child = local_order(
        id="stop-1",
        client_order_id="strategy-execution-sl-123",
        exchange_order_id="child-stop-1",
        side="sell",
        quantity=Decimal("1"),
        type="stop_loss",
        intent_payload={
            "placement_mode": "attach-at-entry",
            "native_parent_basket_id": "parent-1",
            "native_parent_client_order_id": "strategy-execution-long-123",
        },
    )
    remote = remote_order(
        client_order_id="strategy-execution-long-123",
        basket_id="child-stop-1",
        original_basket_id="other-parent",
        price_type="stop_market",
        transaction_type="SELL",
        quantity="1",
    )

    plan, external = build_rithmic_recovery_plan([child], snapshot(orders=[remote]))

    assert plan[0].reason == "native_parent_basket_id_mismatch"
    assert external == [
        {
            "basket_id": "child-stop-1",
            "client_order_id": "strategy-execution-long-123",
            "status": "OPEN",
        }
    ]


@pytest.mark.parametrize(
    ("original_basket_id", "price_type", "reason"),
    [
        ("other-parent", "stop_market", "native_parent_basket_id_mismatch"),
        ("parent-1", "limit", "native_bracket_leg_mismatch"),
    ],
)
def test_terminal_native_child_history_requires_parent_and_leg_identity(
    original_basket_id,
    price_type,
    reason,
):
    child = local_order(
        id="stop-1",
        client_order_id="strategy-execution-sl-123",
        exchange_order_id="child-stop-1",
        side="sell",
        quantity=Decimal("1"),
        type="stop_loss",
        intent_payload={
            "placement_mode": "attach-at-entry",
            "native_parent_basket_id": "parent-1",
            "native_parent_client_order_id": "strategy-execution-long-123",
        },
    )
    terminal = remote_order(
        client_order_id="strategy-execution-long-123",
        basket_id="child-stop-1",
        original_basket_id=original_basket_id,
        price_type=price_type,
        transaction_type="SELL",
        quantity="1",
        status="CANCELLED",
    )

    plan, _ = build_rithmic_recovery_plan(
        [child], snapshot(order_history=[terminal])
    )

    assert plan[0].reason == reason


def test_unexpected_extra_native_leg_is_reported_external():
    parent_client_id = "strategy-execution-long-123"
    parent = local_order(
        client_order_id=parent_client_id,
        exchange_order_id="parent-1",
        quantity=Decimal("1"),
        intent_payload={
            "native_protection": {
                "legs": {"stop_loss": {"client_order_id": "strategy-execution-sl-123"}}
            }
        },
    )
    remotes = [
        remote_order(client_order_id=parent_client_id, basket_id="parent-1", quantity="1"),
        remote_order(
            client_order_id=parent_client_id,
            basket_id="child-stop-1",
            original_basket_id="parent-1",
            price_type="stop_market",
            transaction_type="SELL",
            quantity="1",
        ),
        remote_order(
            client_order_id=parent_client_id,
            basket_id="child-target-1",
            original_basket_id="parent-1",
            price_type="limit",
            transaction_type="SELL",
            quantity="1",
        ),
    ]

    _, external = build_rithmic_recovery_plan([parent], snapshot(orders=remotes))

    assert external == [
        {
            "basket_id": "child-target-1",
            "client_order_id": parent_client_id,
            "status": "OPEN",
        }
    ]


def test_native_children_sharing_parent_user_tag_do_not_duplicate_parent_identity():
    parent_client_id = "strategy-execution-long-123"
    parent = local_order(
        client_order_id=parent_client_id,
        exchange_order_id="parent-1",
        quantity=Decimal("1"),
        intent_payload={
            "native_protection": {
                "legs": {
                    "stop_loss": {"client_order_id": "strategy-execution-sl-123"},
                    "take_profit": {"client_order_id": "strategy-execution-tp-123"},
                }
            }
        }
    )
    remotes = [
        remote_order(
            client_order_id=parent_client_id,
            basket_id="parent-1",
            quantity="1",
        ),
        remote_order(
            client_order_id=parent_client_id,
            basket_id="child-stop-1",
            original_basket_id="parent-1",
            price_type="stop_market",
            transaction_type="SELL",
            quantity="1",
        ),
        remote_order(
            client_order_id=parent_client_id,
            basket_id="child-target-1",
            original_basket_id="parent-1",
            price_type="limit",
            transaction_type="SELL",
            quantity="1",
        ),
    ]

    plan, external = build_rithmic_recovery_plan([parent], snapshot(orders=remotes))

    assert external == []
    assert plan[0].classification == "matched"


def test_position_comparison_covers_recovered_and_locally_held_products():
    drifts = compare_rithmic_positions(
        [local_order()],
        [
            SimpleNamespace(
                product_id="RITHMIC:NQ-202609",
                side="LONG",
                quantity=Decimal("1"),
            ),
            SimpleNamespace(
                product_id="RITHMIC:ES-202609",
                side="LONG",
                quantity=Decimal("9"),
            ),
        ],
        [
            SimpleNamespace(symbol="NQU6", net_quantity="2"),
            SimpleNamespace(symbol="ESU6", net_quantity="0"),
        ],
    )

    assert drifts == [
        {
            "product_id": "RITHMIC:ES-202609",
            "local_quantity": "9",
            "remote_quantity": "0",
        },
        {
            "product_id": "RITHMIC:NQ-202609",
            "local_quantity": "1",
            "remote_quantity": "2",
        },
    ]


@pytest.mark.parametrize(
    "applied_action",
    ["applied", "applied_position_cache_failed"],
)
def test_reconciler_applies_owned_event_without_remote_side_effects_and_audits(
    applied_action,
):
    order = local_order(status="SUBMITTED")
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = [order]
    order_manager = SimpleNamespace(repo=repo)
    processor = Mock(return_value={"action": applied_action})
    db = MagicMock()
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=order_manager,
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(db),
        process_exchange_order_event=processor,
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )
    loader = Mock(return_value=snapshot(orders=[remote_order()]))

    result = reconciler.reconcile(
        snapshot_loader=loader,
    )

    assert result["matched_count"] == 1
    assert result["unresolved_count"] == 0
    loader.assert_called_once()
    processor.assert_called_once()
    assert processor.call_args.kwargs == {"allow_remote_side_effects": False}
    assert db.add.call_count == 2
    assert [call.args[0].payload["phase"] for call in db.add.call_args_list] == [
        "planned",
        "completed",
    ]
    assert db.commit.call_count == 2


def test_reconciler_does_not_mutate_when_planned_audit_fails():
    order = local_order(status="SUBMITTED")
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = [order]
    processor = Mock(return_value={"action": "applied"})
    db = MagicMock()
    db.commit.side_effect = RuntimeError("audit unavailable")
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(db),
        process_exchange_order_event=processor,
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )

    with pytest.raises(RuntimeError, match="audit unavailable"):
        reconciler.reconcile(
            snapshot_loader=Mock(return_value=snapshot(orders=[remote_order()])),
        )

    processor.assert_not_called()
    db.rollback.assert_called_once()


def test_reconciler_leaves_planned_audit_when_completion_audit_fails():
    order = local_order(status="SUBMITTED")
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = [order]
    processor = Mock(return_value={"action": "applied"})
    db = MagicMock()
    db.commit.side_effect = [None, RuntimeError("audit unavailable")]
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(db),
        process_exchange_order_event=processor,
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )

    with pytest.raises(RuntimeError, match="audit unavailable"):
        reconciler.reconcile(
            snapshot_loader=Mock(return_value=snapshot(orders=[remote_order()])),
        )

    processor.assert_called_once()
    assert db.add.call_args_list[0].args[0].payload["phase"] == "planned"
    assert db.commit.call_count == 2
    db.rollback.assert_called_once()


@pytest.mark.parametrize(("stage", "code", "cause"), SAFE_SNAPSHOT_FAILURES)
def test_reconciler_snapshot_failure_blocks_every_owned_order_without_mutation(
    caplog,
    stage,
    code,
    cause,
):
    order = local_order(status="SUBMITTED")
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = [order]
    processor = Mock()
    db = MagicMock()
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(db),
        process_exchange_order_event=processor,
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )

    error = snapshot_failure(stage=stage, stable_error_code=code, safe_cause=cause)
    with caplog.at_level("ERROR", logger="OrderReconciler"):
        result = reconciler.reconcile(
            snapshot_loader=Mock(side_effect=error),
        )

    assert result["recoverable_count"] == 1
    assert result["matched_count"] == 0
    assert result["repaired_count"] == 0
    assert result["external_count"] == 0
    assert result["verification_blocked_count"] == 1
    assert result["unresolved_count"] == 1
    assert result["results"] == [
        {
            "order_id": "local-1",
            "classification": "unresolved",
            "reason": "remote_snapshot_failed",
            "verification_blocked": True,
            "unresolved": True,
        }
    ]
    assert result["external_orders"] == []
    assert result["auto_resume_safe"] is False
    assert_snapshot_diagnostics(
        result,
        db.add.call_args.args[0].payload,
        caplog.records,
        ("RuntimeError", stage, code, cause),
    )
    processor.assert_not_called()


def test_reconciler_snapshot_failure_logs_once_when_audit_commit_fails(caplog):
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = []
    db = MagicMock()
    db.commit.side_effect = RuntimeError("audit unavailable")
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(db),
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )

    error = snapshot_failure(
        stage="order_snapshot",
        stable_error_code="order_snapshot_failed",
        safe_cause="ORDER snapshot failed",
    )
    with caplog.at_level("ERROR", logger="OrderReconciler"):
        with pytest.raises(RuntimeError, match="audit unavailable"):
            reconciler.reconcile(snapshot_loader=Mock(side_effect=error))

    assert (
        sum(
            record.getMessage() == "Rithmic ledger snapshot acquisition failed"
            for record in caplog.records
        )
        == 1
    )
    db.rollback.assert_called_once()


@pytest.mark.parametrize(
    ("exception_type", "mutation", "field", "value"),
    [
        (RuntimeError, "missing", "stage", None),
        (RuntimeError, "missing", "stable_error_code", None),
        (RuntimeError, "missing", "safe_cause", None),
        (RuntimeError, "replace", "stage", 7),
        (RuntimeError, "replace", "stable_error_code", 7),
        (RuntimeError, "replace", "safe_cause", 7),
        (RuntimeError, "replace", "stage", "x" * 1000),
        (RuntimeError, "replace", "stable_error_code", "x" * 1000),
        (RuntimeError, "replace", "safe_cause", "x" * 1000),
        (RuntimeError, "replace", "stable_error_code", "pnl_snapshot_failed"),
        (ValueError, "unchanged", "stage", None),
    ],
)
def test_reconciler_snapshot_failure_falls_back_atomically(
    caplog,
    exception_type,
    mutation,
    field,
    value,
):
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = []
    db = MagicMock()
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(db),
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )
    attributes: dict[str, object] = {
        "stage": "order_snapshot",
        "stable_error_code": "order_snapshot_failed",
        "safe_cause": "ORDER snapshot failed",
    }
    if mutation == "missing":
        attributes.pop(field)
    elif mutation == "replace":
        attributes[field] = value
    error = snapshot_failure(exception_type, **attributes)

    with caplog.at_level("ERROR", logger="OrderReconciler"):
        result = reconciler.reconcile(snapshot_loader=Mock(side_effect=error))

    expected = (
        "RuntimeError" if exception_type is RuntimeError else "Exception",
        "unclassified_internal",
        "unclassified_ledger_snapshot_failure",
        "ledger snapshot failed before safe classification",
    )
    assert_snapshot_diagnostics(
        result,
        db.add.call_args.args[0].payload,
        caplog.records,
        expected,
    )
    assert result["recoverable_count"] == 0
    assert result["matched_count"] == 0
    assert result["repaired_count"] == 0
    assert result["external_count"] == 0
    assert result["unresolved_count"] == 1
    assert result["verification_blocked_count"] == 1
    assert result["auto_resume_safe"] is False
    assert result["results"] == []
    assert result["external_orders"] == []


@pytest.mark.parametrize(
    ("order_overrides", "reason"),
    [
        ({"account_profile": None}, "local_account_profile_missing"),
        ({"account_id": None}, "local_account_id_missing"),
        ({"account_profile": "lucid"}, "local_account_profile_mismatch"),
        ({"account_id": "OTHER"}, "local_account_id_mismatch"),
    ],
)
def test_reconciler_blocks_untrusted_local_account_identity(
    order_overrides,
    reason,
):
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = [local_order(**order_overrides)]
    loader = Mock(return_value=snapshot())
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(MagicMock()),
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )

    result = reconciler.reconcile(
        snapshot_loader=loader,
    )

    loader.assert_not_called()
    assert result["auto_resume_safe"] is False
    assert result["results"][0]["reason"] == reason


@pytest.mark.parametrize(
    ("profile", "account_id", "orders"),
    [
        (None, "ACCOUNT", []),
        ("test", None, [local_order()]),
    ],
)
def test_reconciler_blocks_missing_configured_account_identity(
    profile,
    account_id,
    orders,
):
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = orders
    loader = Mock(return_value=snapshot())
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(MagicMock()),
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
        profile=profile,
        account_id=account_id,
    )

    result = reconciler.reconcile(
        snapshot_loader=loader,
    )

    loader.assert_not_called()
    assert result["unresolved_count"] == max(1, len(orders))
    assert result["verification_blocked_count"] == max(1, len(orders))
    assert result["auto_resume_safe"] is False


def test_reconciler_blocks_entire_mixed_account_identity_batch():
    matching = local_order(id="matching")
    mismatched = local_order(id="mismatched", account_id="OTHER")
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = [matching, mismatched]
    loader = Mock(return_value=snapshot())
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(MagicMock()),
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )

    result = reconciler.reconcile(
        snapshot_loader=loader,
    )

    loader.assert_not_called()
    assert result["unresolved_count"] == 2
    assert result["verification_blocked_count"] == 2
    assert result["auto_resume_safe"] is False
    assert [item["reason"] for item in result["results"]] == [
        "account_identity_batch_blocked",
        "local_account_id_mismatch",
    ]


def test_reconciler_checks_remote_exposure_even_without_recoverable_orders():
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = []
    processor = Mock()
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(MagicMock()),
        process_exchange_order_event=processor,
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )
    loader = Mock(
        return_value=snapshot(
            positions=[SimpleNamespace(symbol="NQU6", net_quantity="1")]
        )
    )

    result = reconciler.reconcile(
        snapshot_loader=loader,
    )

    loader.assert_called_once_with("test", "ACCOUNT")
    processor.assert_not_called()
    assert result["recoverable_count"] == 0
    assert result["unresolved_count"] == 1
    assert result["ledger_verification"]["position_drifts"] == [
        {
            "product_id": "RITHMIC:NQU6",
            "local_quantity": "0",
            "remote_quantity": "1",
        }
    ]


def test_reconciler_blocks_account_mismatch_even_without_recoverable_orders():
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = []
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(MagicMock()),
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )
    remote_snapshot = snapshot()
    remote_snapshot.account_id = "OTHER"

    result = reconciler.reconcile(
        snapshot_loader=Mock(return_value=remote_snapshot),
    )

    assert result["unresolved_count"] == 1
    assert "remote_account_id_mismatch" in result["ledger_verification"]["errors"]


def test_reconciler_blocks_unowned_working_order_without_adopting_it():
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = []
    processor = Mock()
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(MagicMock()),
        process_exchange_order_event=processor,
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )

    result = reconciler.reconcile(
        snapshot_loader=Mock(return_value=snapshot(orders=[remote_order()])),
    )

    processor.assert_not_called()
    assert result["external_count"] == 1
    assert result["unresolved_count"] == 1
    assert result["verification_blocked_count"] == 1


def test_reconciler_clean_snapshot_explicitly_allows_auto_resume(caplog):
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = []
    db = MagicMock()
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(db),
        process_exchange_order_event=Mock(),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )

    with caplog.at_level("ERROR", logger="OrderReconciler"):
        result = reconciler.reconcile(
            snapshot_loader=Mock(return_value=snapshot()),
        )

    assert result["auto_resume_safe"] is True
    assert not any(key.startswith("snapshot_error_") for key in result)
    assert not any(
        key.startswith("snapshot_error_") for key in db.add.call_args.args[0].payload
    )
    assert not any(
        record.getMessage() == "Rithmic ledger snapshot acquisition failed"
        for record in caplog.records
    )


@pytest.mark.parametrize(
    ("remote_snapshot", "expected_error"),
    [
        (
            snapshot(
                orders=[remote_order()],
                positions=[SimpleNamespace(symbol="NQU6", net_quantity="1")],
            ),
            None,
        ),
        (snapshot(orders=[remote_order()], account_summary=False), "remote_account_summary_missing"),
    ],
)
def test_reconciler_keeps_startup_blocked_on_ledger_verification_failure(
    remote_snapshot,
    expected_error,
):
    order = local_order(status="SUBMITTED")
    repo = MagicMock()
    repo.list_client_orders_by_statuses.return_value = [order]
    reconciler = owned_reconciler(
        adapter=MagicMock(),
        order_manager=SimpleNamespace(repo=repo),
        clock=SimpleNamespace(now=lambda: 1_700_000_200),
        db_session_factory=lambda: nullcontext(MagicMock()),
        process_exchange_order_event=Mock(return_value={"action": "applied"}),
        place_pending_protection_for_filled_entries=Mock(),
        fail_pending_conditionals_for_terminal_entry=Mock(),
        protective_terminal_without_fill_failure=Mock(),
        cancel_protective_order_when_sibling_closed=Mock(),
        cancel_linked_conditional_for_protection_fill=Mock(),
        local_positions_loader=lambda: [],
    )

    result = reconciler.reconcile(
        snapshot_loader=Mock(return_value=remote_snapshot),
    )

    assert result["unresolved_count"] == 1
    assert result["verification_blocked_count"] == 1
    assert result["ledger_verification"]["verification_blocked"] is True
    if expected_error is None:
        assert result["ledger_verification"]["position_drifts"]
    else:
        assert expected_error in result["ledger_verification"]["errors"]
