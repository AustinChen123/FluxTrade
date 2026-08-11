from copy import deepcopy
from threading import Event, Lock, Thread
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.core.adapters.rithmic_emergency_flatten import (
    RithmicEmergencyFlattenService,
)
from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)


def _result(**overrides):
    result = {
        "cancelled_orders": 0,
        "cancel_failures": [],
        "flattened_positions": 1,
        "flatten_pending": [],
        "flatten_failures": [],
        "recovery_failures": [],
        "already_flat": False,
        "drain_timeout": False,
    }
    result.update(overrides)
    return result


def _snapshot(*, positions=(), working=False):
    orders = ()
    if working:
        orders = (
            SimpleNamespace(
                notification_type="NEW",
                quantity="1",
                filled_quantity="0",
                status="open",
            ),
        )
    return SimpleNamespace(orders=orders, positions=tuple(positions))


def _owner(*, stop=True, restart_error=None, run_when=None):
    adapter = MagicMock()
    adapter.account_id = "ACCOUNT"
    adapter.configured_product_ids = ("RITHMIC:NQ-202609",)
    adapter.positions_from_ledger_snapshot.side_effect = lambda snapshot: list(
        snapshot.positions
    )
    execution_engine = MagicMock()
    execution_engine.clock.now.return_value = 1_700_000_000.0
    execution_engine.list_recoverable_client_orders.return_value = []
    execution_engine.reconcile_owned_orders.return_value = {"auto_resume_safe": True}
    account_service = MagicMock()
    ops_safety = MagicMock()
    stop_current_worker = MagicMock(return_value=stop)
    clear_polling_stop = MagicMock()
    restart_generic_worker = MagicMock(side_effect=restart_error)
    run_when_submissions_drained = run_when or MagicMock(
        side_effect=lambda callback: callback()
    )
    logger = MagicMock()
    service = RithmicEmergencyFlattenService(
        adapter=adapter,
        execution_engine=execution_engine,
        account_service=account_service,
        ops_safety=ops_safety,
        operation_gate=RithmicOrderEventLifecycleGate(),
        profile="test",
        account_id="ACCOUNT",
        stop_current_worker=stop_current_worker,
        clear_polling_stop=clear_polling_stop,
        restart_generic_worker=restart_generic_worker,
        run_when_submissions_drained=run_when_submissions_drained,
        logger=logger,
    )
    return SimpleNamespace(
        service=service,
        adapter=adapter,
        execution_engine=execution_engine,
        account_service=account_service,
        ops_safety=ops_safety,
        stop=stop_current_worker,
        clear=clear_polling_stop,
        restart=restart_generic_worker,
        run_when=run_when_submissions_drained,
        logger=logger,
    )


def test_stop_timeout_audits_without_entering_money_path() -> None:
    owner = _owner(stop=False)

    with pytest.raises(
        RuntimeError,
        match="rithmic_emergency_flatten_event_stream_stop_timeout",
    ):
        owner.service.execute(
            actor="ops",
            reason="drill",
            operation_id="operation-1",
        )

    owner.stop.assert_called_once_with(timeout=30.0)
    owner.clear.assert_called_once_with()
    owner.ops_safety.kill_switch_with_authoritative_positions.assert_not_called()
    owner.execution_engine.reconcile_owned_orders.assert_not_called()
    owner.adapter.close.assert_not_called()
    owner.adapter.start_order_event_stream.assert_not_called()
    owner.restart.assert_not_called()
    audit = owner.ops_safety.record_kill_switch_result.call_args.kwargs
    assert audit["actor"] == "ops"
    assert audit["reason"] == "drill"
    assert audit["operation_id"] == "operation-1"
    assert audit["result"]["authoritative_flatten_verified"] is False
    assert audit["result"]["flatten_failures"] == [
        {
            "strategy_id": "LIVE",
            "product_id": "unknown",
            "reason": "rithmic_emergency_flatten_event_stream_stop_timeout",
        }
    ]


def test_verified_flatten_preserves_full_lifecycle_order() -> None:
    trace = []
    owner = _owner()
    owner.stop.side_effect = lambda **_kwargs: trace.append("stop") or True
    owner.adapter.close.side_effect = lambda: trace.append("close")
    owner.adapter.positions_from_ledger_snapshot.side_effect = (
        lambda snapshot: trace.append("positions") or list(snapshot.positions)
    )
    owner.adapter.start_order_event_stream.side_effect = lambda: trace.append(
        "native_start"
    )
    owner.account_service.replace_positions_for_products.side_effect = (
        lambda *_args, **_kwargs: trace.append("publish")
    )
    owner.execution_engine.reconcile_owned_orders.side_effect = (
        lambda *_args, **_kwargs: trace.append("reconcile")
        or {"auto_resume_safe": True}
    )
    owner.restart.side_effect = lambda: trace.append("generic_restart")
    owner.ops_safety.record_kill_switch_result.side_effect = (
        lambda **_kwargs: trace.append("audit")
    )

    def kill_switch(**kwargs):
        trace.append("kill_switch")
        assert kwargs["account_id"] == "ACCOUNT"
        kwargs["position_loader"]()
        return _result()

    owner.ops_safety.kill_switch_with_authoritative_positions.side_effect = kill_switch
    snapshots = [_snapshot(positions=("pre-exit",)), _snapshot()]

    def load_snapshot(*_args, **_kwargs):
        trace.append("snapshot")
        return snapshots.pop(0)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=load_snapshot,
    ):
        result = owner.service.execute(
            actor="ops",
            reason="drill",
            operation_id="operation-1",
        )

    assert result["authoritative_flatten_verified"] is True
    assert trace == [
        "stop",
        "kill_switch",
        "close",
        "snapshot",
        "positions",
        "native_start",
        "close",
        "snapshot",
        "reconcile",
        "positions",
        "publish",
        "reconcile",
        "audit",
        "generic_restart",
    ]


def test_already_flat_result_is_preserved_after_authoritative_verification() -> None:
    owner = _owner()
    owner.service._load_snapshot = MagicMock(return_value=_snapshot())
    owner.service._reconcile = MagicMock(return_value={"auto_resume_safe": True})
    owner.ops_safety.kill_switch_with_authoritative_positions.return_value = _result(
        flattened_positions=0,
        already_flat=True,
    )

    result = owner.service.execute(actor="ops", reason="drill")

    assert result["already_flat"] is True
    assert result["authoritative_flatten_verified"] is True


def test_drain_timeout_skips_all_verification_snapshots() -> None:
    owner = _owner()
    owner.service._load_snapshot = MagicMock()
    owner.service._reconcile = MagicMock()
    owner.ops_safety.kill_switch_with_authoritative_positions.return_value = _result(
        flattened_positions=0,
        drain_timeout=True,
    )

    result = owner.service.execute(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is False
    owner.service._load_snapshot.assert_not_called()
    owner.service._reconcile.assert_not_called()
    owner.account_service.replace_positions_for_products.assert_not_called()


def test_working_order_verification_uses_all_six_snapshots_without_resubmit() -> None:
    owner = _owner()

    def kill_switch(**kwargs):
        kwargs["position_loader"]()
        return _result()

    owner.ops_safety.kill_switch_with_authoritative_positions.side_effect = kill_switch
    snapshots = [_snapshot(), *[_snapshot(working=True) for _ in range(6)]]
    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=snapshots,
    ) as snapshot_loader:
        result = owner.service.execute(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is False
    assert snapshot_loader.call_count == 7
    assert owner.ops_safety.kill_switch_with_authoritative_positions.call_count == 1
    assert owner.execution_engine.reconcile_owned_orders.call_count == 6
    owner.account_service.replace_positions_for_products.assert_not_called()


def test_fresh_residual_positions_allow_at_most_three_submissions() -> None:
    owner = _owner()

    def kill_switch(**kwargs):
        kwargs["position_loader"]()
        return _result()

    owner.ops_safety.kill_switch_with_authoritative_positions.side_effect = kill_switch
    snapshots = [_snapshot(positions=("residual",)) for _ in range(6)]
    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=snapshots,
    ) as snapshot_loader:
        result = owner.service.execute(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is False
    assert snapshot_loader.call_count == 6
    assert owner.ops_safety.kill_switch_with_authoritative_positions.call_count == 3
    assert owner.execution_engine.reconcile_owned_orders.call_count == 6


@pytest.mark.parametrize(
    ("reconciliations", "expected_submissions"),
    [
        ((False, True), 2),
        ((True, False), 1),
    ],
)
def test_only_second_non_working_reconciliation_controls_resubmission(
    reconciliations,
    expected_submissions,
) -> None:
    owner = _owner()
    owner.service._load_snapshot = MagicMock(
        return_value=_snapshot(positions=("residual",))
    )
    owner.service._reconcile = MagicMock(
        side_effect=[{"auto_resume_safe": value} for value in reconciliations]
    )
    owner.ops_safety.kill_switch_with_authoritative_positions.side_effect = [
        _result(),
        _result(drain_timeout=True),
    ]

    result = owner.service.execute(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is False
    assert (
        owner.ops_safety.kill_switch_with_authoritative_positions.call_count
        == expected_submissions
    )
    assert owner.service._reconcile.call_count == 2
    owner.account_service.replace_positions_for_products.assert_called_once_with(
        ["residual"],
        ("RITHMIC:NQ-202609",),
        timestamp_ms=1_700_000_000_000,
    )


@pytest.mark.parametrize(
    ("remaining_positions", "verified"),
    [
        ((), True),
        (("residual",), False),
    ],
)
def test_ambiguous_native_flatten_never_resubmits(
    remaining_positions,
    verified,
) -> None:
    owner = _owner()

    def kill_switch(**kwargs):
        kwargs["position_loader"]()
        return _result(
            flatten_failures=[
                {
                    "strategy_id": "LIVE",
                    "product_id": "RITHMIC:NQ-202609",
                    "reason": "submission_ambiguous",
                }
            ]
        )

    owner.ops_safety.kill_switch_with_authoritative_positions.side_effect = kill_switch
    snapshots = [_snapshot(), _snapshot(positions=remaining_positions)]
    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=snapshots,
    ):
        result = owner.service.execute(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is verified
    assert owner.ops_safety.kill_switch_with_authoritative_positions.call_count == 1


@pytest.mark.parametrize("body", ["verified", "unverified", "primary"])
@pytest.mark.parametrize("restart_fails", [False, True])
def test_finalizer_preserves_primary_and_restart_precedence(
    body,
    restart_fails,
) -> None:
    primary = RuntimeError("primary flatten failure")
    restart = RuntimeError("restart failure")
    owner = _owner(restart_error=restart if restart_fails else None)
    audit_snapshots = []
    order = []
    owner.ops_safety.record_kill_switch_result.side_effect = (
        lambda **values: order.append("audit")
        or audit_snapshots.append(deepcopy(values))
    )

    def restart_worker():
        order.append("restart")
        if restart_fails:
            raise restart

    owner.restart.side_effect = restart_worker
    owner.service._load_snapshot = MagicMock(return_value=_snapshot())
    owner.service._reconcile = MagicMock(return_value={"auto_resume_safe": True})
    owner.adapter.positions_from_ledger_snapshot.return_value = []
    if body == "verified":
        owner.ops_safety.kill_switch_with_authoritative_positions.return_value = (
            _result()
        )
    elif body == "unverified":
        owner.ops_safety.kill_switch_with_authoritative_positions.return_value = (
            _result(drain_timeout=True)
        )
    else:
        owner.ops_safety.kill_switch_with_authoritative_positions.side_effect = primary

    if body == "primary":
        with pytest.raises(RuntimeError) as caught:
            owner.service.execute(actor="ops", reason="drill")
        assert caught.value is primary
    elif restart_fails:
        with pytest.raises(RuntimeError) as caught:
            owner.service.execute(actor="ops", reason="drill")
        assert caught.value is restart
    else:
        result = owner.service.execute(actor="ops", reason="drill")
        assert result["authoritative_flatten_verified"] is (body == "verified")

    expected_audits = 2 if restart_fails else 1
    assert owner.ops_safety.record_kill_switch_result.call_count == expected_audits
    assert order == (
        ["audit", "restart", "audit"] if restart_fails else ["audit", "restart"]
    )
    assert audit_snapshots[0]["actor"] == "ops"
    assert audit_snapshots[0]["reason"] == "drill"
    assert audit_snapshots[0]["result"]["authoritative_flatten_verified"] is (
        body == "verified"
    )
    assert audit_snapshots[0]["result"]["recovery_failures"] == []
    if restart_fails:
        final_result = audit_snapshots[1]["result"]
        assert final_result["authoritative_flatten_verified"] is (body == "verified")
        assert final_result["recovery_failures"][-1] == {
            "reason": "rithmic_order_stream_restart_failed:RuntimeError"
        }
    if body == "primary" and restart_fails:
        owner.logger.exception.assert_called_once_with(
            "Order stream restart also failed after emergency flatten failure"
        )


def test_compensation_executes_immediately_after_already_drained() -> None:
    owner = _owner()
    owner.service.execute = MagicMock()

    owner.service.schedule_portfolio_exit_compensation("verification_failed")

    owner.service.execute.assert_called_once_with(
        actor="engine",
        reason="portfolio_exit_compensation:verification_failed",
    )


def test_compensation_queues_once_until_submission_drain() -> None:
    callbacks = []
    owner = _owner(run_when=MagicMock(side_effect=callbacks.append))
    owner.service.execute = MagicMock()

    owner.service.schedule_portfolio_exit_compensation("verification_failed")

    owner.service.execute.assert_not_called()
    assert len(callbacks) == 1
    callbacks[0]()
    owner.service.execute.assert_called_once_with(
        actor="engine",
        reason="portfolio_exit_compensation:verification_failed",
    )


@pytest.mark.parametrize("failure_owner", ["registration", "synchronous_callback"])
def test_synchronous_compensation_failures_propagate(failure_owner) -> None:
    failure = RuntimeError(f"{failure_owner} failed")
    if failure_owner == "registration":
        owner = _owner(run_when=MagicMock(side_effect=failure))
    else:
        owner = _owner()
        owner.service.execute = MagicMock(side_effect=failure)

    with pytest.raises(RuntimeError) as caught:
        owner.service.schedule_portfolio_exit_compensation("verification_failed")

    assert caught.value is failure


def test_operator_and_queued_compensation_keep_aggregate_identity_local() -> None:
    callbacks = []
    owner = _owner(run_when=MagicMock(side_effect=callbacks.append))
    audits = []
    owner.ops_safety.record_kill_switch_result.side_effect = (
        lambda **values: audits.append(deepcopy(values))
    )
    owner.ops_safety.kill_switch_with_authoritative_positions.side_effect = (
        lambda **_kwargs: _result(drain_timeout=True)
    )

    owner.service.schedule_portfolio_exit_compensation("first")
    owner.service.execute(
        actor="ops",
        reason="operator",
        operation_id="operation-1",
    )
    callbacks[0]()

    assert [(audit["actor"], audit["reason"]) for audit in audits] == [
        ("ops", "operator"),
        ("engine", "portfolio_exit_compensation:first"),
    ]
    assert audits[0]["operation_id"] == "operation-1"
    assert "operation_id" not in audits[1]
    assert audits[0]["result"] is not audits[1]["result"]
    audits[0]["result"]["flatten_failures"].append({"reason": "operator-only"})
    assert audits[1]["result"]["flatten_failures"] == [
        {
            "strategy_id": "LIVE",
            "product_id": "unknown",
            "reason": "rithmic_authoritative_flatten_not_verified",
        }
    ]


def test_operator_and_compensation_cannot_overlap_owner_lifecycle() -> None:
    owner = _owner()
    second_gate_attempted = Event()

    class ObservableLock:
        def __init__(self) -> None:
            self._lock = Lock()
            self._attempt_lock = Lock()
            self._attempts = 0

        def __enter__(self) -> "ObservableLock":
            with self._attempt_lock:
                self._attempts += 1
                if self._attempts == 2:
                    second_gate_attempted.set()
            self._lock.acquire()
            return self

        def __exit__(self, *_args: object) -> None:
            self._lock.release()

    setattr(owner.service.operation_gate, "_lock", ObservableLock())
    first_stop_entered = Event()
    release_first_stop = Event()
    second_stop_entered = Event()
    second_started = Event()
    call_lock = Lock()
    stop_calls = 0

    def stop_current_worker(**_kwargs) -> bool:
        nonlocal stop_calls
        with call_lock:
            stop_calls += 1
            call_number = stop_calls
        if call_number == 1:
            first_stop_entered.set()
            assert release_first_stop.wait(timeout=2.0)
        else:
            second_stop_entered.set()
        return False

    owner.stop.side_effect = stop_current_worker
    errors = []

    def execute(*, actor: str, reason: str) -> None:
        try:
            owner.service.execute(actor=actor, reason=reason)
        except RuntimeError as error:
            errors.append(error)

    first = Thread(target=execute, kwargs={"actor": "ops", "reason": "operator"})

    def execute_compensation() -> None:
        second_started.set()
        execute(actor="engine", reason="portfolio_exit_compensation:failed")

    second = Thread(target=execute_compensation)
    first.start()
    assert first_stop_entered.wait(timeout=2.0)
    second.start()
    assert second_started.wait(timeout=2.0)

    second_attempted = second_gate_attempted.wait(timeout=2.0)
    overlapped = second_stop_entered.is_set()
    calls_before_release = stop_calls
    release_first_stop.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)
    assert not first.is_alive()
    assert not second.is_alive()
    assert second_attempted
    assert overlapped is False
    assert calls_before_release == 1
    assert second_stop_entered.is_set()
    assert stop_calls == 2
    assert len(errors) == 2


def test_queued_compensation_failure_is_owned_by_submission_completion(
    engine_factory,
) -> None:
    engine = engine_factory()
    owner = _owner(
        run_when=engine.execution_engine.run_when_submissions_drained,
    )
    owner.service.execute = MagicMock(
        side_effect=RuntimeError("queued compensation failed")
    )
    engine.execution_engine.logger = MagicMock()
    with engine.execution_engine._submission_gate:
        engine.execution_engine._submissions_in_flight = 1

    owner.service.schedule_portfolio_exit_compensation("verification_failed")
    owner.service.execute.assert_not_called()
    engine.execution_engine._finish_submission()

    owner.service.execute.assert_called_once_with(
        actor="engine",
        reason="portfolio_exit_compensation:verification_failed",
    )
    engine.execution_engine.logger.exception.assert_called_once_with(
        "Submission drain callback failed"
    )
