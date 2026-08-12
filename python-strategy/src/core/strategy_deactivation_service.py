import logging
from contextlib import AbstractContextManager

from src.core.portfolio_runtime import PortfolioCoordinator
from src.core.runtime_artifact_registry import RuntimeArtifactRegistry
from src.core.strategy_state_manager import (
    InvalidStrategyStateTransition,
    StrategyStateManager,
)


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
