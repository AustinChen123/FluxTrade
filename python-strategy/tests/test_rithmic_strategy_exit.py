from __future__ import annotations

from decimal import Decimal
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from src.core.adapters.rithmic_strategy_exit import RithmicStrategyExitService
from src.core.execution import ExitDecision
from src.core.models import OrderStatus, Position, PositionSide, Signal, SignalType


PRODUCT = "RITHMIC:NQ-202609"


def _signal(signal_type: SignalType = SignalType.EXIT_LONG) -> Signal:
    return Signal(
        strategy_id="strategy",
        product_id=PRODUCT,
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=signal_type,
        quantity=Decimal("1"),
    )


def _decision(
    *,
    quantity: Decimal = Decimal("1"),
    position_quantity: Decimal | None = Decimal("1"),
) -> ExitDecision:
    return ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=quantity,
        position_quantity=position_quantity,
    )


def _position(
    quantity: str = "1",
    side: PositionSide = PositionSide.LONG,
) -> Position:
    return Position(
        strategy_id="LIVE",
        product_id=PRODUCT,
        side=side,
        quantity=Decimal(quantity),
        entry_price=Decimal("20000"),
        unrealized_pnl=Decimal("0"),
    )


def _working_order() -> SimpleNamespace:
    return SimpleNamespace(
        notification_type="NEW",
        status="open",
        quantity="1",
        filled_quantity="0",
    )


def _snapshot(
    *,
    positions: list[Position] | None = None,
    orders: list[object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        positions=[] if positions is None else positions,
        orders=[] if orders is None else orders,
    )


def _service() -> tuple[RithmicStrategyExitService, SimpleNamespace]:
    adapter = MagicMock()
    adapter.account_id = "ACCOUNT"
    adapter.configured_product_ids = (PRODUCT,)
    adapter.positions_from_ledger_snapshot.side_effect = lambda snapshot: list(
        snapshot.positions
    )
    execution_engine = MagicMock()
    execution_engine.clock.now.return_value = 1_704_067_200
    execution_engine.list_recoverable_client_orders.return_value = []
    execution_engine.order_manager.repo.list_orders_by_statuses.return_value = []
    execution_engine.reconcile_rithmic_owned_orders.return_value = {
        "auto_resume_safe": True
    }
    account_service = MagicMock()
    dependencies = SimpleNamespace(
        adapter=adapter,
        execution_engine=execution_engine,
        account_service=account_service,
        stop_order_event_stream=MagicMock(return_value=True),
        assert_leadership=MagicMock(),
        restart_order_stream=MagicMock(),
        lockdown=MagicMock(),
    )
    service = RithmicStrategyExitService(
        adapter=adapter,
        execution_engine=execution_engine,
        account_service=account_service,
        profile="test",
        account_id="ACCOUNT",
        stop_order_event_stream=dependencies.stop_order_event_stream,
        assert_leadership=dependencies.assert_leadership,
        restart_order_stream=dependencies.restart_order_stream,
        lockdown=dependencies.lockdown,
        logger=logging.getLogger("test.rithmic_strategy_exit"),
    )
    return service, dependencies


@pytest.mark.parametrize(
    ("signal", "decision", "expected"),
    (
        (_signal(SignalType.LONG), _decision(), "requires_exit_signal"),
        (
            _signal(),
            _decision(quantity=Decimal("0.5")),
            "partial_strategy_exit_unsupported",
        ),
        (
            _signal(),
            _decision(position_quantity=None),
            "partial_strategy_exit_unsupported",
        ),
    ),
)
def test_invalid_request_fails_before_runtime_or_money_mutation(
    signal: Signal,
    decision: ExitDecision,
    expected: str,
) -> None:
    service, dependencies = _service()

    with pytest.raises((ValueError, RuntimeError), match=expected):
        service.execute(signal, decision)

    dependencies.stop_order_event_stream.assert_not_called()
    dependencies.adapter.start_order_event_stream.assert_not_called()
    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.restart_order_stream.assert_not_called()
    dependencies.lockdown.assert_not_called()


def test_stop_timeout_prohibits_money_path_but_runs_finalizer() -> None:
    service, dependencies = _service()
    dependencies.stop_order_event_stream.return_value = False

    with pytest.raises(
        RuntimeError,
        match="rithmic_strategy_exit_event_stream_stop_timeout",
    ):
        service.execute(_signal(), _decision())

    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.assert_not_called()
    dependencies.execution_engine.reconcile_rithmic_owned_orders.assert_not_called()
    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.account_service.replace_positions_for_products.assert_not_called()
    dependencies.assert_leadership.assert_called_once_with()
    dependencies.restart_order_stream.assert_called_once_with()
    dependencies.lockdown.assert_called_once_with(
        "rithmic_strategy_exit_requires_reconciliation:RuntimeError"
    )


@pytest.mark.parametrize("preflight_quantity", (None, "1"))
def test_success_preserves_already_flat_and_native_exit_paths(
    preflight_quantity: str | None,
) -> None:
    service, dependencies = _service()
    preflight = _snapshot(
        positions=(
            [] if preflight_quantity is None else [_position(preflight_quantity)]
        )
    )
    flat = _snapshot()

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=[preflight, flat],
    ):
        result = service.execute(_signal(), _decision())

    assert result == {
        "status": "verified_flat",
        "cancelled_orders": 0,
        "product_id": PRODUCT,
    }
    if preflight_quantity is None:
        dependencies.execution_engine.exit_authoritative_position.assert_not_called()
        assert dependencies.adapter.start_order_event_stream.call_count == 1
    else:
        dependencies.execution_engine.exit_authoritative_position.assert_called_once_with(
            PRODUCT,
            account_id="ACCOUNT",
        )
        assert dependencies.adapter.start_order_event_stream.call_count == 2
    assert dependencies.account_service.replace_positions_for_products.call_count == 2
    dependencies.restart_order_stream.assert_called_once_with()
    dependencies.lockdown.assert_not_called()


@pytest.mark.parametrize("with_position_and_order", (False, True))
def test_success_preserves_exact_money_path_order(
    with_position_and_order: bool,
) -> None:
    service, dependencies = _service()
    trace: list[str] = []
    order = SimpleNamespace(
        id="order-1",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id="client-1",
        type="stop_loss",
    )
    dependencies.stop_order_event_stream.side_effect = lambda **_kwargs: (
        trace.append("stop") or True
    )
    dependencies.assert_leadership.side_effect = lambda: trace.append("fence")
    dependencies.adapter.start_order_event_stream.side_effect = lambda: trace.append(
        "start"
    )
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.side_effect = (
        lambda _statuses: trace.append("list_orders")
        or ([order] if with_position_and_order else [])
    )
    dependencies.adapter.get_order_by_client_id.side_effect = (
        lambda *_args, **_kwargs: trace.append("lookup")
        or SimpleNamespace(status="open", exchange_order_id="basket-1")
    )
    dependencies.adapter.cancel_order.side_effect = (
        lambda *_args, **_kwargs: trace.append("cancel") or True
    )
    dependencies.adapter.close.side_effect = lambda: trace.append("close")
    dependencies.execution_engine.reconcile_rithmic_owned_orders.side_effect = (
        lambda *_args, **_kwargs: trace.append("reconcile")
        or {"auto_resume_safe": True}
    )
    dependencies.account_service.replace_positions_for_products.side_effect = (
        lambda *_args, **_kwargs: trace.append("publish")
    )
    dependencies.execution_engine.exit_authoritative_position.side_effect = (
        lambda *_args, **_kwargs: trace.append("native_exit")
    )
    dependencies.restart_order_stream.side_effect = lambda: trace.append("restart")
    snapshots = [
        _snapshot(positions=[_position()] if with_position_and_order else []),
        _snapshot(),
    ]

    def load_snapshot(*_args, **_kwargs):
        trace.append("snapshot")
        return snapshots.pop(0)

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=load_snapshot,
    ):
        assert service.execute(_signal(), _decision())["status"] == "verified_flat"

    prefix = ["stop", "fence", "start", "list_orders"]
    if with_position_and_order:
        prefix.extend(["fence", "lookup", "fence", "cancel"])
    assert trace == [
        *prefix,
        "close",
        "snapshot",
        "reconcile",
        "fence",
        "publish",
        *(["fence", "start", "native_exit"] if with_position_and_order else []),
        "fence",
        "close",
        "snapshot",
        "reconcile",
        "fence",
        "publish",
        "fence",
        "restart",
    ]


@pytest.mark.parametrize("failure_phase", ("cancel", "native_exit"))
def test_leadership_fence_failure_prevents_following_money_mutation(
    failure_phase: str,
) -> None:
    service, dependencies = _service()
    primary = RuntimeError(f"{failure_phase}-leadership-lost")
    if failure_phase == "cancel":
        order = SimpleNamespace(
            id="order-1",
            strategy_id="strategy",
            product_id=PRODUCT,
            status=OrderStatus.SUBMITTED.value,
            client_order_id="client-1",
            type="stop_loss",
        )
        dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
            order
        ]
        dependencies.adapter.get_order_by_client_id.return_value = SimpleNamespace(
            status="open",
            exchange_order_id="basket-1",
        )
        dependencies.assert_leadership.side_effect = [None, None, primary, None]
        snapshot = _snapshot()
    else:
        dependencies.assert_leadership.side_effect = [None, None, primary, None]
        snapshot = _snapshot(positions=[_position()])

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            return_value=snapshot,
        ) as loader,
        pytest.raises(RuntimeError) as caught,
    ):
        service.execute(_signal(), _decision())

    assert caught.value is primary
    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    if failure_phase == "cancel":
        dependencies.adapter.cancel_order.assert_not_called()
        loader.assert_not_called()
        dependencies.account_service.replace_positions_for_products.assert_not_called()
    else:
        dependencies.account_service.replace_positions_for_products.assert_called_once()
    dependencies.restart_order_stream.assert_called_once_with()


def test_cancels_only_matching_active_orders_and_skips_remote_terminal() -> None:
    service, dependencies = _service()
    matching = SimpleNamespace(
        id="matching",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id="matching-client",
        type="stop_loss",
    )
    local_new = SimpleNamespace(
        id="new",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.NEW.value,
        client_order_id=None,
        type="limit",
    )
    terminal = SimpleNamespace(
        id="terminal",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id="terminal-client",
        type="take_profit",
    )
    wrong_strategy = SimpleNamespace(**{**vars(matching), "strategy_id": "other"})
    wrong_product = SimpleNamespace(**{**vars(matching), "product_id": "RITHMIC:ES"})
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
        matching,
        local_new,
        terminal,
        wrong_strategy,
        wrong_product,
    ]
    dependencies.adapter.get_order_by_client_id.side_effect = [
        SimpleNamespace(status="open", exchange_order_id="matching-basket"),
        SimpleNamespace(status="filled", exchange_order_id="terminal-basket"),
    ]

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=[_snapshot(), _snapshot()],
    ):
        result = service.execute(_signal(), _decision())

    assert result["cancelled_orders"] == 2
    dependencies.execution_engine.order_manager.fail_order.assert_called_once_with(
        local_new,
        "strategy_exit",
    )
    dependencies.adapter.cancel_order.assert_called_once_with(
        "matching-basket",
        PRODUCT,
        order_type="stop_loss",
    )
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.assert_called_once_with(
        {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
    )


@pytest.mark.parametrize(
    ("mode", "expected"),
    (
        ("identity", "cancel_identity_missing"),
        ("lookup", "cancel_lookup_missing"),
        ("cancel", "cancel_failed"),
    ),
)
def test_cancel_failures_preserve_exact_primary_reason(
    mode: str,
    expected: str,
) -> None:
    service, dependencies = _service()
    order = SimpleNamespace(
        id="order-1",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id=None if mode == "identity" else "client-1",
        type="stop_loss",
    )
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
        order
    ]
    if mode == "lookup":
        dependencies.adapter.get_order_by_client_id.return_value = None
    elif mode == "cancel":
        dependencies.adapter.get_order_by_client_id.return_value = SimpleNamespace(
            status="open",
            exchange_order_id="basket-1",
        )
        dependencies.adapter.cancel_order.return_value = False

    with pytest.raises(RuntimeError, match=expected):
        service.execute(_signal(), _decision())

    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.restart_order_stream.assert_called_once_with()
    dependencies.lockdown.assert_called_once_with(
        "rithmic_strategy_exit_requires_reconciliation:RuntimeError"
    )


@pytest.mark.parametrize("failure", ("working_order", "unsafe_reconciliation"))
def test_preflight_failures_prevent_native_exit(failure: str) -> None:
    service, dependencies = _service()
    snapshot = _snapshot(
        positions=[_position()],
        orders=[_working_order()] if failure == "working_order" else [],
    )
    if failure == "unsafe_reconciliation":
        dependencies.execution_engine.reconcile_rithmic_owned_orders.return_value = {
            "auto_resume_safe": False
        }

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            return_value=snapshot,
        ),
        pytest.raises(RuntimeError),
    ):
        service.execute(_signal(), _decision())

    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.account_service.replace_positions_for_products.assert_not_called()


@pytest.mark.parametrize(
    "position",
    (
        _position(side=PositionSide.SHORT),
        _position(quantity="2"),
    ),
)
def test_position_drift_prevents_native_exit(position: Position) -> None:
    service, dependencies = _service()

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            return_value=_snapshot(positions=[position]),
        ),
        pytest.raises(RuntimeError, match="rithmic_strategy_exit_position_drift"),
    ):
        service.execute(_signal(), _decision())

    dependencies.execution_engine.exit_authoritative_position.assert_not_called()


@pytest.mark.parametrize("remaining", ("working_order", "position"))
def test_verification_runs_all_six_attempts_before_failing(remaining: str) -> None:
    service, dependencies = _service()
    preflight = _snapshot(positions=[_position()] if remaining == "position" else [])
    unresolved = _snapshot(
        positions=[_position()] if remaining == "position" else [],
        orders=[_working_order()] if remaining == "working_order" else [],
    )

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            side_effect=[preflight, *[unresolved for _ in range(6)]],
        ) as loader,
        pytest.raises(RuntimeError, match="rithmic_strategy_exit_flat_not_verified"),
    ):
        service.execute(_signal(), _decision())

    assert loader.call_count == 7
    if remaining == "working_order":
        dependencies.execution_engine.reconcile_rithmic_owned_orders.assert_called_once()
        dependencies.account_service.replace_positions_for_products.assert_called_once()
        dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    else:
        assert (
            dependencies.execution_engine.reconcile_rithmic_owned_orders.call_count == 7
        )
        assert (
            dependencies.account_service.replace_positions_for_products.call_count == 7
        )
        dependencies.execution_engine.exit_authoritative_position.assert_called_once_with(
            PRODUCT,
            account_id="ACCOUNT",
        )


def test_snapshot_filters_rithmic_orders_and_uses_integer_clock() -> None:
    service, dependencies = _service()
    rithmic_order = SimpleNamespace(exchange_id="RITHMIC")
    other_order = SimpleNamespace(exchange_id="BINANCE")
    dependencies.execution_engine.list_recoverable_client_orders.return_value = [
        other_order,
        rithmic_order,
    ]

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=[_snapshot(), _snapshot()],
    ) as loader:
        service.execute(_signal(), _decision())

    assert loader.call_args_list == [
        call("test", "ACCOUNT", [rithmic_order], 1_704_067_200),
        call("test", "ACCOUNT", [rithmic_order], 1_704_067_200),
    ]


@pytest.mark.parametrize("body_primary", (False, True))
@pytest.mark.parametrize("finalizer_failure", (None, "leadership", "start"))
def test_body_primary_and_finalizer_failure_precedence(
    body_primary: bool,
    finalizer_failure: str | None,
) -> None:
    service, dependencies = _service()
    primary = RuntimeError("body-primary")
    finalizer = RuntimeError("finalizer-failure")
    if body_primary:
        dependencies.execution_engine.reconcile_rithmic_owned_orders.side_effect = (
            primary
        )
        leadership_calls_before_finalizer = 1
    else:
        leadership_calls_before_finalizer = 4
    if finalizer_failure == "leadership":
        dependencies.assert_leadership.side_effect = [
            *[None for _ in range(leadership_calls_before_finalizer)],
            finalizer,
        ]
    elif finalizer_failure == "start":
        dependencies.restart_order_stream.side_effect = finalizer

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=[_snapshot(), _snapshot()],
    ):
        if body_primary:
            with pytest.raises(RuntimeError) as caught:
                service.execute(_signal(), _decision())
            assert caught.value is primary
        elif finalizer_failure is not None:
            with pytest.raises(
                RuntimeError,
                match="rithmic_strategy_exit_order_stream_restart_failed",
            ) as caught:
                service.execute(_signal(), _decision())
            assert caught.value is not finalizer
        else:
            assert service.execute(_signal(), _decision())["status"] == "verified_flat"

    if finalizer_failure is None:
        dependencies.restart_order_stream.assert_called_once_with()
        dependencies.adapter.close.assert_called()
    else:
        dependencies.adapter.close.assert_called()
        assert dependencies.lockdown.call_args_list[-1] == call(
            "rithmic_strategy_exit_order_stream_restart_failed"
        )
        if finalizer_failure == "leadership":
            dependencies.restart_order_stream.assert_not_called()
        else:
            dependencies.restart_order_stream.assert_called_once_with()
