from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from src.core.adapters.rithmic_runtime_recovery import (
    RithmicRuntimeRecoveryService,
    rithmic_maintenance_active,
)


def _service(
    *,
    now: list[float] | None = None,
) -> tuple[RithmicRuntimeRecoveryService, SimpleNamespace]:
    clock = now if now is not None else [0.0]
    dependencies = SimpleNamespace(
        adapter=SimpleNamespace(close=MagicMock()),
        halt_for_reconcile=MagicMock(return_value=True),
        stop_order_event_stream=MagicMock(return_value=True),
        reconcile_owned_orders=MagicMock(
            return_value={"recoverable_count": 2, "auto_resume_safe": True}
        ),
        publish_authoritative_summary=MagicMock(),
        assert_runtime_leadership=MagicMock(),
        start_order_event_stream=MagicMock(return_value=True),
        resume_after_reconcile=MagicMock(),
        lockdown=MagicMock(),
        logger=MagicMock(),
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
        logger=dependencies.logger,
        monotonic_seconds=lambda: clock[0],
        maintenance_active=lambda: False,
    )
    return service, dependencies


@pytest.mark.parametrize(
    ("iso_timestamp", "expected"),
    (
        ("2026-09-14T17:14:59-04:00", False),
        ("2026-09-14T17:15:00-04:00", True),
        ("2026-09-14T17:49:59-04:00", True),
        ("2026-09-14T17:50:00-04:00", False),
        ("2026-09-18T17:14:59-04:00", False),
        ("2026-09-18T17:15:00-04:00", True),
        ("2026-09-19T12:00:00-04:00", True),
        ("2026-09-20T11:59:59-04:00", True),
        ("2026-09-20T12:14:59-04:00", True),
        ("2026-09-20T12:15:00-04:00", False),
        ("2026-09-21T09:00:00-04:00", False),
    ),
)
def test_maintenance_state_matrix(
    iso_timestamp: str,
    expected: bool,
) -> None:
    observed = datetime.fromisoformat(iso_timestamp).astimezone(
        ZoneInfo("America/New_York")
    )

    assert rithmic_maintenance_active(observed) is expected


def test_maintenance_window_skips_all_provider_io() -> None:
    service, dependencies = _service()
    service._maintenance_active = lambda: True

    assert service.run_once() is False

    dependencies.halt_for_reconcile.assert_not_called()
    dependencies.stop_order_event_stream.assert_not_called()
    dependencies.reconcile_owned_orders.assert_not_called()
    dependencies.start_order_event_stream.assert_not_called()


def test_maintenance_transition_after_drain_defers_provider_reconciliation() -> None:
    service, dependencies = _service()
    service._maintenance_active = MagicMock(side_effect=[False, True])

    assert service.run_once() is False

    dependencies.halt_for_reconcile.assert_called_once_with(timeout=30.0)
    dependencies.stop_order_event_stream.assert_called_once_with(timeout=30.0)
    dependencies.adapter.close.assert_called_once_with()
    dependencies.reconcile_owned_orders.assert_not_called()
    dependencies.start_order_event_stream.assert_not_called()
    dependencies.resume_after_reconcile.assert_not_called()


def test_maintenance_transition_after_ledger_defers_order_stream_restart() -> None:
    service, dependencies = _service()
    service._maintenance_active = MagicMock(side_effect=[False, False, True])

    assert service.run_once() is False

    dependencies.reconcile_owned_orders.assert_called_once_with("profile", "ACCOUNT")
    dependencies.publish_authoritative_summary.assert_called_once_with(
        {"recoverable_count": 2, "auto_resume_safe": True}
    )
    dependencies.start_order_event_stream.assert_not_called()
    dependencies.resume_after_reconcile.assert_not_called()
    assert dependencies.adapter.close.call_count == 2


def test_maintenance_transition_does_not_hide_authoritative_order_anomaly() -> None:
    service, dependencies = _service()
    service._maintenance_active = MagicMock(side_effect=[False, False, True])
    dependencies.reconcile_owned_orders.return_value = {
        "recoverable_count": 1,
        "auto_resume_safe": False,
        "snapshot_error_code": "invalid_ledger_snapshot_request",
    }

    assert service.run_once() is False

    dependencies.lockdown.assert_called_once_with(
        "rithmic_runtime_reconciliation_unresolved"
    )
    dependencies.start_order_event_stream.assert_not_called()
    dependencies.resume_after_reconcile.assert_not_called()


def test_maintenance_transition_keeps_typed_provider_suppression_transient() -> None:
    service, dependencies = _service()
    service._maintenance_active = MagicMock(side_effect=[False, False, True])
    dependencies.reconcile_owned_orders.return_value = {
        "recoverable_count": 0,
        "auto_resume_safe": False,
        "snapshot_error_code": "order_connect_failed",
        "snapshot_retryable": True,
    }

    assert service.run_once() is False

    dependencies.lockdown.assert_not_called()
    dependencies.start_order_event_stream.assert_not_called()
    dependencies.resume_after_reconcile.assert_not_called()


def test_post_maintenance_verification_is_limited_to_three_attempts() -> None:
    now = [0.0]
    maintenance = [True]
    service, dependencies = _service(now=now)
    service._maintenance_active = lambda: maintenance[0]
    dependencies.reconcile_owned_orders.side_effect = RuntimeError("provider down")

    assert service.run_once() is False
    maintenance[0] = False
    for timestamp in (0.0, 900.0, 2_700.0):
        now[0] = timestamp
        assert service.run_once() is False
    now[0] = 99_999.0
    assert service.run_once() is False

    assert dependencies.reconcile_owned_orders.call_count == 3
    assert dependencies.halt_for_reconcile.call_count == 3
    assert dependencies.stop_order_event_stream.call_count == 3
    dependencies.lockdown.assert_not_called()
    dependencies.logger.error.assert_called_once_with(
        "Post-maintenance Rithmic verification exhausted; "
        "provider I/O remains quiescent until a new maintenance "
        "recovery epoch or service restart"
    )


def test_successful_post_maintenance_verification_restores_normal_schedule() -> None:
    now = [0.0]
    maintenance = [True]
    service, dependencies = _service(now=now)
    service._maintenance_active = lambda: maintenance[0]
    dependencies.reconcile_owned_orders.side_effect = [
        RuntimeError("provider down"),
        {"recoverable_count": 0, "auto_resume_safe": True},
        RuntimeError("later transient"),
    ]

    assert service.run_once() is False
    maintenance[0] = False
    assert service.run_once() is False
    now[0] = 900.0
    assert service.run_once() is True
    now[0] = 4_500.0
    assert service.run_once() is False

    assert dependencies.reconcile_owned_orders.call_count == 3
    dependencies.lockdown.assert_not_called()


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
    dependencies.start_order_event_stream.side_effect = (
        lambda: calls.append("restart") or True
    )
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


def test_disconnected_runtime_handle_never_reopens_entry_gate() -> None:
    service, dependencies = _service()
    dependencies.start_order_event_stream.return_value = False

    assert service.run_once() is False

    dependencies.resume_after_reconcile.assert_not_called()
    dependencies.lockdown.assert_not_called()
    dependencies.logger.warning.assert_called_once_with(
        "Periodic Rithmic recovery remains entry-blocked: reason=%s",
        "rithmic_runtime_reconciliation_stream_disconnected",
    )


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
        if restart_fails and primary_failure == "reconcile"
        else primary_reason
    )
    if primary_failure == "reconcile":
        dependencies.lockdown.assert_not_called()
    else:
        dependencies.lockdown.assert_called_once_with(expected_reason)
    dependencies.logger.warning.assert_called_once_with(
        "Periodic Rithmic recovery remains entry-blocked: reason=%s",
        expected_reason,
    )
    dependencies.resume_after_reconcile.assert_not_called()
    if primary_failure == "projection":
        dependencies.publish_authoritative_summary.assert_called_once_with(
            {"recoverable_count": 2, "auto_resume_safe": True}
        )
    else:
        dependencies.publish_authoritative_summary.assert_not_called()
    assert dependencies.assert_runtime_leadership.call_count == 2
    assert dependencies.adapter.close.call_count == (2 if restart_fails else 1)


def test_explicitly_retryable_provider_summary_keeps_only_local_entry_gate() -> None:
    service, dependencies = _service()
    dependencies.reconcile_owned_orders.return_value = {
        "recoverable_count": 0,
        "auto_resume_safe": False,
        "snapshot_error_code": "order_connect_failed",
        "snapshot_retryable": True,
    }

    assert service.run_once() is False

    dependencies.lockdown.assert_not_called()
    dependencies.resume_after_reconcile.assert_not_called()


def test_stage_code_without_retryable_disposition_preserves_lockdown() -> None:
    service, dependencies = _service()
    dependencies.reconcile_owned_orders.return_value = {
        "recoverable_count": 0,
        "auto_resume_safe": False,
        "snapshot_error_code": "order_connect_failed",
        "snapshot_retryable": False,
    }

    assert service.run_once() is False

    dependencies.lockdown.assert_called_once_with(
        "rithmic_runtime_reconciliation_unresolved"
    )
    dependencies.resume_after_reconcile.assert_not_called()


@pytest.mark.parametrize(
    "snapshot_error_code",
    (
        None,
        "invalid_ledger_snapshot_request",
        "unclassified_ledger_snapshot_failure",
        "future_unknown_error",
    ),
)
def test_unsafe_or_unknown_summary_preserves_durable_lockdown(
    snapshot_error_code: str | None,
) -> None:
    service, dependencies = _service()
    summary = {
        "recoverable_count": 1,
        "auto_resume_safe": False,
    }
    if snapshot_error_code is not None:
        summary["snapshot_error_code"] = snapshot_error_code
    dependencies.reconcile_owned_orders.return_value = summary
    dependencies.start_order_event_stream.side_effect = RuntimeError("offline")

    assert service.run_once() is False

    dependencies.lockdown.assert_called_once_with(
        "rithmic_runtime_reconciliation_unresolved"
    )


def test_provider_failures_use_bounded_backoff_without_repeating_provider_io() -> None:
    now = [0.0]
    service, dependencies = _service(now=now)
    dependencies.reconcile_owned_orders.side_effect = RuntimeError("provider down")

    expected_attempts = (
        (0.0, 1),
        (899.0, 1),
        (900.0, 2),
        (2_699.0, 2),
        (2_700.0, 3),
        (6_299.0, 3),
        (6_300.0, 4),
        (20_699.0, 4),
        (20_700.0, 5),
    )
    for timestamp, attempt_count in expected_attempts:
        now[0] = timestamp
        assert service.run_once() is False
        assert dependencies.reconcile_owned_orders.call_count == attempt_count

    dependencies.lockdown.assert_not_called()
    assert dependencies.halt_for_reconcile.call_count == 5
    assert dependencies.stop_order_event_stream.call_count == 5


def test_success_resets_failure_backoff_and_throttles_healthy_queries() -> None:
    now = [0.0]
    service, dependencies = _service(now=now)
    dependencies.reconcile_owned_orders.side_effect = [
        RuntimeError("maintenance"),
        {"recoverable_count": 0, "auto_resume_safe": True},
        RuntimeError("maintenance again"),
    ]

    assert service.run_once() is False
    now[0] = 900.0
    assert service.run_once() is True
    now[0] = 4_499.0
    assert service.run_once() is True
    assert dependencies.reconcile_owned_orders.call_count == 2
    now[0] = 4_500.0
    assert service.run_once() is False
    now[0] = 5_399.0
    assert service.run_once() is False
    assert dependencies.reconcile_owned_orders.call_count == 3
    now[0] = 5_400.0
    dependencies.reconcile_owned_orders.side_effect = RuntimeError("still down")
    assert service.run_once() is False
    assert dependencies.reconcile_owned_orders.call_count == 4


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
