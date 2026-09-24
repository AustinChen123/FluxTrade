from contextlib import contextmanager
from unittest.mock import MagicMock
from datetime import UTC, datetime
from typing import Any

import pytest

from src.core.strategy_deactivation_service import StrategyDeactivationService
from src.core.strategy_state_manager import InvalidStrategyStateTransition
from src.core import strategy_deactivation_service as owner
from src.core.strategy_state_manager import (
    StrategyStateManager,
    LockedStrategyState,
    StrategyStateTransitionResult,
)
from src.core.models import StrategyStatus
from src.core.strategy_activation_request_store import (
    ProfileActivationRequestRecord,
    ProfileActivationRequestStatus,
)
from test_profile_activation_request import request


@pytest.mark.parametrize(
    "case",
    "success implicit missing stale early transition_error base_error".split()
    + [None, "", True, "x" * 65],
)
def test_cancel_pending_transaction_order_and_failures(monkeypatch, case):
    manager = StrategyStateManager(MagicMock(), MagicMock())
    session, calls = MagicMock(), MagicMock()
    value, now = request(), datetime(2026, 1, 1, tzinfo=UTC)
    calls.state.return_value = LockedStrategyState(
        "strategy", StrategyStatus.READY, 1 if case == "stale" else 0
    )
    calls.pending.return_value = (
        None
        if case == "missing"
        else ProfileActivationRequestRecord(
            value, ProfileActivationRequestStatus.PENDING, now
        )
    )
    calls.transition.return_value = StrategyStateTransitionResult(
        "strategy", StrategyStatus.READY, StrategyStatus.STOPPED, 1, now
    )
    calls.terminal.return_value = ProfileActivationRequestRecord(
        value,
        ProfileActivationRequestStatus.CANCELLED,
        now,
        now,
        "ACTIVATION_CANCELLED",
    )
    monkeypatch.setattr(manager, "lock_state_in_transaction", calls.state)
    monkeypatch.setattr(manager, "transition_in_transaction", calls.transition)
    monkeypatch.setattr(owner, "lock_pending_profile_activation_request", calls.pending)
    monkeypatch.setattr(owner, "terminalize_profile_activation_request", calls.terminal)
    error = BaseException("SECRET") if case == "base_error" else RuntimeError("SECRET")
    if case in ("transition_error", "base_error"):
        calls.transition.side_effect = error
    kwargs: dict[str, Any] = dict(
        environment=value.intent.key.environment,
        strategy_id="strategy",
        actor="operator",
        terminal_at=now,
        expected_version=0,
    )
    if case in (None, "", True, "x" * 65):
        kwargs["actor"] = case
        with pytest.raises(owner.ProfileActivationRequestValidationError):
            owner.cancel_pending_profile_activation_request(session, manager, **kwargs)
        assert not calls.mock_calls
        return
    if case == "early":
        kwargs["terminal_at"] = now.replace(year=2025)
    if case == "implicit":
        kwargs["expected_version"] = None
    if case in ("success", "implicit", "missing"):
        result = owner.cancel_pending_profile_activation_request(
            session, manager, **kwargs
        )
        if case in ("success", "implicit"):
            assert result is not None and result.record is calls.terminal.return_value
            calls.transition.assert_called_once_with(
                session,
                "strategy",
                StrategyStatus.STOPPED,
                actor="operator",
                reason="ACTIVATION_CANCELLED",
                changed_at=now,
                expected_version=0,
            )
            calls.terminal.assert_called_once_with(
                session,
                value,
                status=ProfileActivationRequestStatus.CANCELLED,
                terminal_at=now,
                terminal_reason="ACTIVATION_CANCELLED",
            )
        else:
            assert result is None
    else:
        expected = (
            owner.StaleStrategyStateVersion
            if case == "stale"
            else owner.ProfileActivationRequestValidationError
            if case == "early"
            else type(error)
        )
        with pytest.raises(expected) as caught:
            owner.cancel_pending_profile_activation_request(session, manager, **kwargs)
        if case in ("transition_error", "base_error"):
            assert caught.value is error
    assert [call[0] for call in calls.mock_calls][:2] == ["state", "pending"]
    if case not in ("success", "implicit"):
        calls.terminal.assert_not_called()
    assert not session.mock_calls and not manager._redis_client.mock_calls


@pytest.mark.parametrize("phase", ["commit", "missing", "rollback", "ack_lost"])
def test_pending_cancel_runtime_postcommit_order(monkeypatch, phase):
    events, session = [], MagicMock()
    context = MagicMock()
    context.__enter__.return_value = session
    error = RuntimeError("transaction uncertain")
    session.begin.return_value.__exit__.side_effect = lambda *_: events.append("commit")
    if phase == "ack_lost":
        context.__exit__.side_effect = error
    service, manager, runtime, registration, market = _service(
        events=events,
        profile_request_store=MagicMock(),
        db_session_factory=lambda: context,
        environment_identity=lambda: "live",
    )
    transition = StrategyStateTransitionResult(
        "strategy",
        StrategyStatus.READY,
        StrategyStatus.STOPPED,
        1,
        datetime(2026, 1, 1, tzinfo=UTC),
    )

    def cancel(*args, **kwargs):
        assert registration.held and market.held
        assert args == (session, manager) and kwargs["expected_version"] is None
        events.append("cancel")
        if phase == "rollback":
            raise error
        return None if phase == "missing" else MagicMock(transition=transition)

    monkeypatch.setattr(owner, "cancel_pending_profile_activation_request", cancel)
    manager.after_committed_transition.side_effect = lambda result: events.append(
        "postcommit"
    )
    manager.transition_to_stopped.side_effect = lambda *a, **k: events.append("legacy")
    runtime.unregister_locked.side_effect = (
        lambda _: events.append("unregister") or True
    )
    if phase in ("rollback", "ack_lost"):
        with pytest.raises(RuntimeError) as caught:
            service.deactivate_locked("strategy", actor="operator", reason=None)
        assert caught.value is error
        manager.after_committed_transition.assert_not_called()
        runtime.unregister_locked.assert_not_called()
    else:
        assert service.deactivate_locked("strategy", actor="operator", reason=None)
        assert events[2:-2] == [
            "cancel",
            "commit",
            "legacy" if phase == "missing" else "postcommit",
            "unregister",
        ]
        if phase == "commit":
            manager.after_committed_transition.assert_called_once_with(transition)
            manager.transition_to_stopped.assert_not_called()


class _LockProbe:
    def __init__(self, name, events):
        self.name = name
        self.events = events
        self.held = False

    @contextmanager
    def acquire(self):
        self.events.append(f"enter:{self.name}")
        self.held = True
        try:
            yield
        finally:
            self.held = False
            self.events.append(f"exit:{self.name}")


def _service(*, events, portfolio_id=None, **capabilities):
    registration = _LockProbe("registration", events)
    market = _LockProbe("market", events)
    coordinator = MagicMock()
    coordinator.portfolio_id_for_sleeve.return_value = portfolio_id
    state_manager = MagicMock()
    runtime_artifacts = MagicMock()
    service = StrategyDeactivationService(
        state_manager=state_manager,
        portfolio_coordinator=coordinator,
        runtime_artifacts=runtime_artifacts,
        registration_lock=registration.acquire(),
        market_processing_lock=market.acquire(),
        event_logger=MagicMock(),
        **capabilities,
    )
    return service, state_manager, runtime_artifacts, registration, market


def test_transition_and_unregister_share_exact_lock_order() -> None:
    events = []
    service, state_manager, runtime_artifacts, registration, market = _service(
        events=events
    )

    def transition(*_args, **_kwargs):
        assert registration.held and market.held
        events.append("transition")

    def unregister(_strategy_id):
        assert registration.held and market.held
        events.append("unregister")
        return True

    state_manager.transition_to_stopped.side_effect = transition
    runtime_artifacts.unregister_locked.side_effect = unregister

    assert service.deactivate_locked("strategy", actor="operator", reason="pause")
    assert events == [
        "enter:registration",
        "enter:market",
        "transition",
        "unregister",
        "exit:market",
        "exit:registration",
    ]
    state_manager.transition_to_stopped.assert_called_once_with(
        "strategy",
        actor="operator",
        reason="pause",
    )


def test_expected_version_is_forwarded_only_when_present() -> None:
    events = []
    service, state_manager, runtime_artifacts, *_locks = _service(events=events)
    runtime_artifacts.unregister_locked.return_value = True

    assert service.deactivate_locked(
        "strategy",
        actor="operator",
        reason=None,
        expected_version=7,
    )

    state_manager.transition_to_stopped.assert_called_once_with(
        "strategy",
        actor="operator",
        reason=None,
        expected_version=7,
    )


def test_portfolio_sleeve_is_rejected_before_transition_or_unregister() -> None:
    events = []
    service, state_manager, runtime_artifacts, *_locks = _service(
        events=events,
        portfolio_id="portfolio",
    )

    with pytest.raises(
        ValueError,
        match="portfolio sleeves must be controlled through the portfolio ID",
    ):
        service.deactivate_locked("portfolio.sleeve", actor="operator", reason=None)

    state_manager.transition_to_stopped.assert_not_called()
    runtime_artifacts.unregister_locked.assert_not_called()


@pytest.mark.parametrize(
    "error", [KeyError("missing"), InvalidStrategyStateTransition()]
)
def test_known_transition_failure_returns_false_without_unregister(error) -> None:
    events = []
    service, state_manager, runtime_artifacts, *_locks = _service(events=events)
    state_manager.transition_to_stopped.side_effect = error

    assert not service.deactivate_locked(
        "strategy",
        actor="operator",
        reason=None,
    )

    runtime_artifacts.unregister_locked.assert_not_called()


def test_unexpected_transition_failure_preserves_identity() -> None:
    events = []
    service, state_manager, runtime_artifacts, *_locks = _service(events=events)
    failure = RuntimeError("transition unavailable")
    state_manager.transition_to_stopped.side_effect = failure

    with pytest.raises(RuntimeError) as exc_info:
        service.deactivate_locked("strategy", actor="operator", reason=None)

    assert exc_info.value is failure
    runtime_artifacts.unregister_locked.assert_not_called()


def test_already_absent_runtime_remains_successful() -> None:
    events = []
    service, _state_manager, runtime_artifacts, *_locks = _service(events=events)
    runtime_artifacts.unregister_locked.return_value = False

    assert service.deactivate_locked("strategy", actor="operator", reason=None)
    runtime_artifacts.unregister_locked.assert_called_once_with("strategy")
