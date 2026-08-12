from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from src.core.strategy_deactivation_service import StrategyDeactivationService
from src.core.strategy_state_manager import InvalidStrategyStateTransition


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
