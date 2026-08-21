from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.adapters.rithmic_order_reconnect import (
    RithmicOrderReconnectService,
)


class _OrderRuntime:
    connection_generation: MagicMock
    close: MagicMock
    start_order_event_stream: MagicMock

    def __init__(self, generation: int) -> None:
        self.connection_generation = MagicMock(return_value=generation)
        self.close = MagicMock()
        self.start_order_event_stream = MagicMock()


def _service(
    *,
    generation: int = 1,
) -> tuple[RithmicOrderReconnectService, SimpleNamespace]:
    adapter = _OrderRuntime(generation)
    dependencies = SimpleNamespace(
        adapter=adapter,
        audit_external_orders=MagicMock(return_value=True),
        reconcile_owned_orders=MagicMock(
            return_value={"recoverable_count": 2, "auto_resume_safe": True}
        ),
        publish_authoritative_summary=MagicMock(),
        halt_for_reconcile=MagicMock(return_value=True),
        resume_after_reconcile=MagicMock(),
        assert_runtime_leadership=MagicMock(),
    )
    service = RithmicOrderReconnectService(
        adapter=adapter,
        profile="profile",
        account_id="ACCOUNT",
        audit_external_orders=dependencies.audit_external_orders,
        reconcile_owned_orders=dependencies.reconcile_owned_orders,
        publish_authoritative_summary=dependencies.publish_authoritative_summary,
        halt_for_reconcile=dependencies.halt_for_reconcile,
        resume_after_reconcile=dependencies.resume_after_reconcile,
        assert_runtime_leadership=dependencies.assert_runtime_leadership,
        logger=logging.getLogger("test.rithmic_order_reconnect"),
    )
    service.on_runtime_started()
    return service, dependencies


def test_unchanged_generation_has_no_reconciliation_side_effects() -> None:
    service, dependencies = _service(generation=1)

    assert service.reconcile_if_needed() is True

    dependencies.halt_for_reconcile.assert_not_called()
    dependencies.adapter.close.assert_not_called()
    dependencies.reconcile_owned_orders.assert_not_called()
    dependencies.adapter.start_order_event_stream.assert_not_called()
    assert service.last_generation == 1
    assert service.pending_generation is None


def test_generation_read_failure_reconciles_fail_closed() -> None:
    service, dependencies = _service()
    dependencies.adapter.connection_generation.side_effect = RuntimeError(
        "binding unavailable"
    )

    assert service.reconcile_if_needed() is True

    dependencies.halt_for_reconcile.assert_called_once_with(timeout=30.0)
    dependencies.reconcile_owned_orders.assert_called_once_with("profile", "ACCOUNT")
    dependencies.publish_authoritative_summary.assert_called_once_with(
        {"recoverable_count": 2, "auto_resume_safe": True}
    )
    dependencies.adapter.start_order_event_stream.assert_called_once_with()
    dependencies.resume_after_reconcile.assert_called_once_with()
    assert service.last_generation == 1
    assert service.pending_generation is None


def test_halt_timeout_preserves_pending_generation() -> None:
    service, dependencies = _service(generation=2)
    dependencies.halt_for_reconcile.return_value = False

    assert service.reconcile_if_needed() is False

    dependencies.adapter.close.assert_not_called()
    dependencies.reconcile_owned_orders.assert_not_called()
    dependencies.resume_after_reconcile.assert_not_called()
    assert service.last_generation == 1
    assert service.pending_generation == 2


@pytest.mark.parametrize(
    "failure",
    ("audit_disabled", "reconcile_exception", "unresolved", "projection_exception"),
)
def test_reconciliation_failures_keep_submissions_gated_and_retryable(
    failure: str,
) -> None:
    service, dependencies = _service(generation=2)
    if failure == "audit_disabled":
        dependencies.audit_external_orders.return_value = False
    elif failure == "reconcile_exception":
        dependencies.reconcile_owned_orders.side_effect = RuntimeError("ledger down")
    elif failure == "unresolved":
        dependencies.reconcile_owned_orders.return_value = {
            "recoverable_count": 1,
            "auto_resume_safe": False,
        }
    else:
        dependencies.publish_authoritative_summary.side_effect = RuntimeError(
            "projection failed"
        )

    assert service.reconcile_if_needed() is False

    dependencies.resume_after_reconcile.assert_not_called()
    dependencies.adapter.start_order_event_stream.assert_not_called()
    assert service.last_generation == 1
    assert service.pending_generation == 2


@pytest.mark.parametrize("leadership_call", (1, 2))
def test_leadership_loss_closes_runtime_and_preserves_pending_generation(
    leadership_call: int,
) -> None:
    service, dependencies = _service(generation=2)
    dependencies.assert_runtime_leadership.side_effect = (
        [RuntimeError("ownership lost")]
        if leadership_call == 1
        else [None, RuntimeError("ownership lost")]
    )

    with pytest.raises(RuntimeError, match="ownership lost"):
        service.reconcile_if_needed()

    assert dependencies.adapter.close.call_count == 2
    dependencies.resume_after_reconcile.assert_not_called()
    assert service.last_generation == 1
    assert service.pending_generation == 2


def test_restart_failure_preserves_pending_and_next_tick_can_recover() -> None:
    service, dependencies = _service(generation=2)
    dependencies.adapter.start_order_event_stream.side_effect = RuntimeError("offline")

    assert service.reconcile_if_needed() is False
    assert service.pending_generation == 2
    dependencies.resume_after_reconcile.assert_not_called()

    dependencies.adapter.start_order_event_stream.side_effect = None
    assert service.reconcile_if_needed() is True

    assert dependencies.reconcile_owned_orders.call_count == 2
    assert dependencies.adapter.start_order_event_stream.call_count == 2
    dependencies.resume_after_reconcile.assert_called_once_with()
    assert service.last_generation == 1
    assert service.pending_generation is None


def test_runtime_started_atomically_replaces_stale_generation_state() -> None:
    service, dependencies = _service(generation=4)
    dependencies.halt_for_reconcile.return_value = False
    assert service.reconcile_if_needed() is False
    assert service.pending_generation == 4

    service.on_runtime_started()

    assert service.last_generation == 1
    assert service.pending_generation is None


def test_success_preserves_exact_reconnect_order() -> None:
    calls: list[str] = []
    service, dependencies = _service(generation=2)
    dependencies.halt_for_reconcile.side_effect = (
        lambda **_: calls.append("halt") or True
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
    dependencies.adapter.start_order_event_stream.side_effect = lambda: calls.append(
        "restart"
    )
    dependencies.resume_after_reconcile.side_effect = lambda: calls.append("resume")

    assert service.reconcile_if_needed() is True

    assert calls == [
        "halt",
        "close",
        "reconcile",
        "publish",
        "leadership",
        "restart",
        "leadership",
        "resume",
    ]
