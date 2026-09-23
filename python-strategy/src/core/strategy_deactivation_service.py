import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from sqlalchemy.orm import Session
from src.core.models import StrategyStatus
from src.core.strategy_activation_request_store import (
    ProfileActivationRequestRecord,
    ProfileActivationRequestStatus,
    ProfileActivationRequestValidationError,
    lock_pending_profile_activation_request,
    terminalize_profile_activation_request,
    _validate_pending_identity,
)

from src.core.portfolio_runtime import PortfolioCoordinator
from src.core.runtime_artifact_registry import RuntimeArtifactRegistry
from src.core.strategy_state_manager import (
    InvalidStrategyStateTransition,
    StrategyStateManager,
    StrategyStateTransitionResult,
    StaleStrategyStateVersion,
)


@dataclass(frozen=True, slots=True)
class ProfileActivationCancellationResult:
    transition: StrategyStateTransitionResult
    record: ProfileActivationRequestRecord


def cancel_pending_profile_activation_request(
    session: Session,
    state_manager: StrategyStateManager,
    *,
    environment: str,
    strategy_id: str,
    actor: str,
    terminal_at: datetime,
    expected_version: int,
) -> ProfileActivationCancellationResult | None:
    """Cancel and STOP in caller transaction; no commit or postcommit ownership."""
    _validate_pending_identity(environment, strategy_id)
    if (
        type(state_manager) is not StrategyStateManager
        or type(actor) is not str
        or not 1 <= len(actor) <= 64
        or type(terminal_at) is not datetime
        or terminal_at.tzinfo is not UTC
        or terminal_at.microsecond % 1000
        or type(expected_version) is not int
        or not 0 <= expected_version <= 2**31 - 1
    ):
        raise ProfileActivationRequestValidationError()
    state = state_manager.lock_state_in_transaction(session, strategy_id)
    record = lock_pending_profile_activation_request(
        session, environment=environment, strategy_id=strategy_id
    )
    if record is None:
        return None
    if state.version != expected_version:
        raise StaleStrategyStateVersion("PROFILE_ACTIVATION_STATE_STALE")
    if terminal_at < record.requested_at:
        raise ProfileActivationRequestValidationError()
    reason = "ACTIVATION_CANCELLED"
    transition = state_manager.transition_in_transaction(
        session,
        strategy_id,
        StrategyStatus.STOPPED,
        actor=actor,
        reason=reason,
        changed_at=terminal_at,
        expected_version=expected_version,
    )
    record = terminalize_profile_activation_request(
        session,
        record.request,
        status=ProfileActivationRequestStatus.CANCELLED,
        terminal_at=terminal_at,
        terminal_reason=reason,
    )
    return ProfileActivationCancellationResult(transition, record)


class StrategyDeactivationService:
    """Atomically stop one durable strategy and remove its runtime artifact."""

    def __init__(
        self,
        *,
        state_manager: StrategyStateManager,
        portfolio_coordinator: PortfolioCoordinator,
        runtime_artifacts: RuntimeArtifactRegistry,
        registration_lock: AbstractContextManager[object],
        market_processing_lock: AbstractContextManager[object],
        event_logger: logging.Logger,
    ) -> None:
        self._state_manager = state_manager
        self._portfolio_coordinator = portfolio_coordinator
        self._runtime_artifacts = runtime_artifacts
        self._registration_lock = registration_lock
        self._market_processing_lock = market_processing_lock
        self._logger = event_logger

    def deactivate_locked(
        self,
        strategy_id: str,
        *,
        actor: str,
        reason: str | None,
        expected_version: int | None = None,
    ) -> bool:
        """Run under Engine's per-strategy lifecycle lock."""
        self._logger.info("🛑 Stopping Strategy: %s", strategy_id)
        with self._registration_lock, self._market_processing_lock:
            if (
                self._portfolio_coordinator.portfolio_id_for_sleeve(strategy_id)
                is not None
            ):
                raise ValueError(
                    "portfolio sleeves must be controlled through the portfolio ID"
                )
            try:
                transition_kwargs = {"actor": actor, "reason": reason}
                if expected_version is not None:
                    transition_kwargs["expected_version"] = expected_version
                self._state_manager.transition_to_stopped(
                    strategy_id,
                    **transition_kwargs,
                )
            except (KeyError, InvalidStrategyStateTransition):
                self._logger.warning("Strategy %s is not active.", strategy_id)
                return False

            if not self._runtime_artifacts.unregister_locked(strategy_id):
                self._logger.warning(
                    "Strategy %s runtime was already absent; durable state reconciled.",
                    strategy_id,
                )

        self._logger.info("✅ Strategy %s stopped.", strategy_id)
        return True
