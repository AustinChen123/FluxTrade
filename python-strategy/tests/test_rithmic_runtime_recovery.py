from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.adapters.rithmic_runtime_recovery import (
    RithmicRuntimeRecoveryService,
)


def _service() -> tuple[RithmicRuntimeRecoveryService, SimpleNamespace]:
    dependencies = SimpleNamespace(
        adapter=SimpleNamespace(close=MagicMock()),
        halt_for_reconcile=MagicMock(return_value=True),
        stop_order_event_stream=MagicMock(return_value=True),
        reconcile_owned_orders=MagicMock(
            return_value={"recoverable_count": 2, "auto_resume_safe": True}
        ),
        publish_authoritative_summary=MagicMock(),
        assert_runtime_leadership=MagicMock(),
        start_order_event_stream=MagicMock(),
        resume_after_reconcile=MagicMock(),
        lockdown=MagicMock(),
    )
    service = RithmicRuntimeRecoveryService(
        adapter=dependencies.adapter,
        profile="profile",
        account_id="ACCOUNT",
        halt_for_reconcile=dependencies.halt_for_reconcile,
        stop_order_event_stream=dependencies.stop_order_event_stream,
        reconcile_owned_orders=dependencies.reconcile_owned_orders,
        publish_authoritative_summary=dependencies.publish_authoritative_summary,
        assert_runtime_leadership=dependencies.assert_runtime_leadership,
        start_order_event_stream=dependencies.start_order_event_stream,
        resume_after_reconcile=dependencies.resume_after_reconcile,
        lockdown=dependencies.lockdown,
        logger=logging.getLogger("test.rithmic_runtime_recovery"),
    )
    return service, dependencies


def test_success_preserves_exact_runtime_recovery_order() -> None:
    calls: list[str] = []
    service, dependencies = _service()
    dependencies.halt_for_reconcile.side_effect = (
        lambda **_: calls.append("halt") or True
    )
    dependencies.stop_order_event_stream.side_effect = (
        lambda **_: calls.append("stop") or True
    )
    dependencies.adapter.close.side_effect = lambda: calls.append("close")
    dependencies.reconcile_owned_orders.side_effect = lambda *_: calls.append(
        "reconcile"
    ) or {"recoverable_count": 2, "auto_resume_safe": True}
    dependencies.publish_authoritative_summary.side_effect = lambda *_: calls.append(
        "publish"
    )
    dependencies.assert_runtime_leadership.side_effect = lambda: calls.append(
        "leadership"
    )
    dependencies.start_order_event_stream.side_effect = lambda: calls.append("restart")
    dependencies.resume_after_reconcile.side_effect = lambda: calls.append("resume")

    assert service.run_once() is True

    assert calls == [
        "halt",
        "stop",
        "close",
        "reconcile",
        "publish",
        "leadership",
        "restart",
        "leadership",
        "leadership",
        "resume",
    ]
    dependencies.lockdown.assert_not_called()


@pytest.mark.parametrize("timeout_stage", ("drain", "stream_stop"))
def test_timeouts_prohibit_every_downstream_side_effect(timeout_stage: str) -> None:
    service, dependencies = _service()
    if timeout_stage == "drain":
        dependencies.halt_for_reconcile.return_value = False
        expected_reason = "rithmic_runtime_reconciliation_drain_timeout"
    else:
        dependencies.stop_order_event_stream.return_value = False
        expected_reason = "rithmic_runtime_reconciliation_stream_stop_timeout"

    assert service.run_once() is False

    dependencies.lockdown.assert_called_once_with(expected_reason)
    dependencies.adapter.close.assert_not_called()
    dependencies.reconcile_owned_orders.assert_not_called()
    dependencies.publish_authoritative_summary.assert_not_called()
    dependencies.assert_runtime_leadership.assert_not_called()
    dependencies.start_order_event_stream.assert_not_called()
    dependencies.resume_after_reconcile.assert_not_called()
    if timeout_stage == "drain":
        dependencies.stop_order_event_stream.assert_not_called()
    else:
        dependencies.stop_order_event_stream.assert_called_once_with(timeout=30.0)


@pytest.mark.parametrize(
    ("primary_failure", "primary_reason"),
    (
        ("reconcile", "rithmic_runtime_reconciliation_failed"),
        ("unresolved", "rithmic_runtime_reconciliation_unresolved"),
        ("projection", "rithmic_runtime_reconciliation_failed"),
    ),
)
@pytest.mark.parametrize("restart_fails", (False, True))
def test_primary_failure_still_restarts_and_restart_reason_has_precedence(
    primary_failure: str,
    primary_reason: str,
    restart_fails: bool,
) -> None:
    service, dependencies = _service()
    if primary_failure == "reconcile":
        dependencies.reconcile_owned_orders.side_effect = RuntimeError("ledger down")
    elif primary_failure == "unresolved":
        dependencies.reconcile_owned_orders.return_value = {
            "recoverable_count": 1,
            "auto_resume_safe": False,
        }
    else:
        dependencies.publish_authoritative_summary.side_effect = RuntimeError(
            "projection failed"
        )
    if restart_fails:
        dependencies.start_order_event_stream.side_effect = RuntimeError("offline")

    assert service.run_once() is False

    dependencies.start_order_event_stream.assert_called_once_with()
    expected_reason = (
        "rithmic_runtime_reconciliation_stream_restart_failed"
        if restart_fails
        else primary_reason
    )
    dependencies.lockdown.assert_called_once_with(expected_reason)
    dependencies.resume_after_reconcile.assert_not_called()
    if primary_failure == "projection":
        dependencies.publish_authoritative_summary.assert_called_once_with(
            {"recoverable_count": 2, "auto_resume_safe": True}
        )
    else:
        dependencies.publish_authoritative_summary.assert_not_called()
    assert dependencies.assert_runtime_leadership.call_count == 2
    assert dependencies.adapter.close.call_count == (2 if restart_fails else 1)


@pytest.mark.parametrize(
    ("fence", "restart_fails", "expected_close_count"),
    ((1, False, 2), (2, False, 2), (2, True, 3), (3, False, 2)),
)
def test_leadership_loss_has_precedence_and_preserves_exception_identity(
    fence: int,
    restart_fails: bool,
    expected_close_count: int,
) -> None:
    service, dependencies = _service()
    error = RuntimeError(f"leadership-{fence}")
    dependencies.assert_runtime_leadership.side_effect = [
        *(None for _ in range(fence - 1)),
        error,
    ]
    if restart_fails:
        dependencies.start_order_event_stream.side_effect = RuntimeError("offline")

    with pytest.raises(RuntimeError) as caught:
        service.run_once()

    assert caught.value is error
    assert dependencies.adapter.close.call_count == expected_close_count
    dependencies.lockdown.assert_not_called()
    dependencies.resume_after_reconcile.assert_not_called()
    if fence == 1:
        dependencies.start_order_event_stream.assert_not_called()
    else:
        dependencies.start_order_event_stream.assert_called_once_with()
