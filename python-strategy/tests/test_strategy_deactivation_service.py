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
    "success missing stale early transition_error base_error".split()
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
    if case in ("success", "missing"):
        result = owner.cancel_pending_profile_activation_request(
            session, manager, **kwargs
        )
        if case == "success":
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
    if case != "success":
        calls.terminal.assert_not_called()
    assert not session.mock_calls and not manager._redis_client.mock_calls


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


def _service(*, events, portfolio_id=None):
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
