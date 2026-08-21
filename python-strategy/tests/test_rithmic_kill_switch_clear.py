from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.adapters.rithmic_kill_switch_clear import (
    RithmicKillSwitchClearPreparationService,
)
from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)


def _build_service(
    *,
    thread=None,
    halt_result: bool = True,
    summary: dict | None = None,
):
    calls: list[str] = []
    adapter = MagicMock()
    current_thread = MagicMock(return_value=thread)
    set_stop = MagicMock(side_effect=lambda: calls.append("set_stop"))
    clear_stop = MagicMock(side_effect=lambda: calls.append("clear_stop"))
    halt = MagicMock(
        return_value=halt_result,
        side_effect=lambda **_: calls.append("halt") or halt_result,
    )
    reconcile = MagicMock(
        return_value=summary or {"recoverable_count": 0, "auto_resume_safe": True},
        side_effect=lambda *_: calls.append("reconcile")
        or summary
        or {"recoverable_count": 0, "auto_resume_safe": True},
    )
    publish = MagicMock(side_effect=lambda _: calls.append("publish"))
    leadership = MagicMock(side_effect=lambda: calls.append("leadership"))
    start = MagicMock(side_effect=lambda: calls.append("start"))
    resume = MagicMock(side_effect=lambda: calls.append("resume"))
    drift_generation = MagicMock(
        return_value=7,
        side_effect=lambda: calls.append("generation") or 7,
    )
    adapter.close.side_effect = lambda: calls.append("close")
    service = RithmicKillSwitchClearPreparationService(
        adapter=adapter,
        profile="test",
        account_id="ACCOUNT",
        operation_gate=RithmicOrderEventLifecycleGate(),
        set_order_event_stop=set_stop,
        clear_order_event_stop=clear_stop,
        current_order_event_thread=current_thread,
        halt_for_reconcile=halt,
        reconcile_owned_orders=reconcile,
        publish_authoritative_summary=publish,
        current_drift_generation=drift_generation,
        assert_runtime_leadership=leadership,
        start_order_event_stream=start,
        resume_after_reconcile=resume,
        logger=logging.getLogger(__name__),
    )
    return service, SimpleNamespace(
        adapter=adapter,
        calls=calls,
        current_thread=current_thread,
        set_stop=set_stop,
        clear_stop=clear_stop,
        halt=halt,
        reconcile=reconcile,
        publish=publish,
        leadership=leadership,
        start=start,
        resume=resume,
        drift_generation=drift_generation,
    )


def test_success_uses_the_current_worker_and_preserves_call_order():
    stale_thread = MagicMock()
    current_thread = MagicMock()
    current_thread.is_alive.side_effect = [True, False]
    service, owner = _build_service(thread=stale_thread)
    owner.current_thread.return_value = current_thread

    assert service.prepare() == (True, 7)

    stale_thread.join.assert_not_called()
    current_thread.join.assert_called_once_with(timeout=30.0)
    owner.halt.assert_called_once_with(timeout=30.0)
    owner.reconcile.assert_called_once_with("test", "ACCOUNT")
    assert owner.calls == [
        "set_stop",
        "leadership",
        "halt",
        "close",
        "reconcile",
        "generation",
        "leadership",
        "start",
        "leadership",
        "publish",
        "leadership",
    ]
    owner.resume.assert_not_called()


def test_prepare_enters_the_shared_operation_gate():
    service, _ = _build_service()
    gate = MagicMock()
    sentinel = (False, 19)
    gate.run.return_value = sentinel
    service._operation_gate = gate

    assert service.prepare() is sentinel

    gate.run.assert_called_once_with(service._prepare_serialized)


def test_stream_stop_timeout_prohibits_money_path_and_clears_stop_event():
    thread = MagicMock()
    thread.is_alive.return_value = True
    service, owner = _build_service(thread=thread)

    assert service.prepare() == (False, None)

    thread.join.assert_called_once_with(timeout=30.0)
    owner.clear_stop.assert_called_once_with()
    owner.halt.assert_not_called()
    owner.adapter.close.assert_not_called()
    owner.reconcile.assert_not_called()
    owner.start.assert_not_called()
    owner.resume.assert_not_called()
    assert owner.calls == ["set_stop", "leadership", "clear_stop"]


def test_drain_timeout_restarts_then_resumes_but_remains_unverified():
    service, owner = _build_service(halt_result=False)

    assert service.prepare() == (False, None)

    owner.adapter.close.assert_not_called()
    owner.reconcile.assert_not_called()
    owner.publish.assert_not_called()
    assert owner.calls == [
        "set_stop",
        "halt",
        "leadership",
        "start",
        "leadership",
        "resume",
    ]


def test_leadership_failure_preserves_identity_and_prohibits_restart():
    service, owner = _build_service()
    failure = RuntimeError("leadership lost")
    owner.leadership.side_effect = failure

    with pytest.raises(RuntimeError) as raised:
        service.prepare()

    assert raised.value is failure
    owner.start.assert_not_called()
    owner.publish.assert_not_called()
    owner.resume.assert_not_called()


@pytest.mark.parametrize(
    ("failure", "expected_calls", "resumed"),
    [
        (
            "reconcile",
            [
                "set_stop",
                "halt",
                "close",
                "reconcile",
                "generation",
                "leadership",
                "start",
                "leadership",
                "resume",
            ],
            True,
        ),
        (
            "unresolved",
            [
                "set_stop",
                "halt",
                "close",
                "reconcile",
                "generation",
                "leadership",
                "start",
                "leadership",
                "resume",
            ],
            True,
        ),
        (
            "restart",
            [
                "set_stop",
                "halt",
                "close",
                "reconcile",
                "generation",
                "leadership",
                "start",
            ],
            False,
        ),
        (
            "projection",
            [
                "set_stop",
                "halt",
                "close",
                "reconcile",
                "generation",
                "leadership",
                "start",
                "leadership",
                "publish",
                "leadership",
                "resume",
            ],
            True,
        ),
    ],
)
def test_failure_matrix_preserves_restart_resume_and_result(
    failure,
    expected_calls,
    resumed,
):
    summary = (
        {"recoverable_count": 1, "auto_resume_safe": False}
        if failure == "unresolved"
        else None
    )
    service, owner = _build_service(summary=summary)
    if failure == "reconcile":
        owner.reconcile.side_effect = lambda *_: (
            owner.calls.append("reconcile"),
            (_ for _ in ()).throw(RuntimeError("ledger offline")),
        )[1]
    elif failure == "restart":
        owner.start.side_effect = lambda: (
            owner.calls.append("start"),
            (_ for _ in ()).throw(RuntimeError("stream offline")),
        )[1]
    elif failure == "projection":
        owner.publish.side_effect = lambda _: (
            owner.calls.append("publish"),
            (_ for _ in ()).throw(RuntimeError("projection failed")),
        )[1]

    assert service.prepare() == (False, None)

    assert owner.calls == expected_calls
    assert owner.resume.called is resumed
