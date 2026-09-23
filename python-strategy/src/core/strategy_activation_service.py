from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import Enum

from sqlalchemy.orm import Session

from src.core.models import StrategyStatus
from src.core.strategy_activation_intent import (
    ProfileActivationIntentError as ProfileActivationIntentError,
    ProfileActivationAdmission as ProfileActivationAdmission,
    ProfileActivationIntent as ProfileActivationIntent,
    classify_profile_activation_intent as classify_profile_activation_intent,
    ProfileActivationRequest,
    ProfileActivationCommand,
)
from src.core.strategy_activation_request_store import (
    ProfileActivationRequestRecord,
    ProfileActivationRequestStatus,
    ProfileActivationRequestValidationError,
    ProfileActivationRequestConflict,
    lock_profile_activation_request,
    terminalize_profile_activation_request,
)
from src.core.orm_models import StrategyState
from src.core.portfolio_runtime import (
    PortfolioDefinition,
    PortfolioFactory,
    PortfolioSleeve,
)
from src.core.strategy_hydration_service import StrategyHydrationService
from src.core.strategy_state_manager import (
    StaleStrategyStateVersion,
    StrategyStateManager,
    available_strategy_commands,
)
from src.strategies.base import BaseStrategy


ArtifactClass = type[BaseStrategy] | type[PortfolioFactory]
DbSessionFactory = Callable[[], AbstractContextManager[Session]]
ProductIdResolver = Callable[[dict], str]
ReadinessValidator = Callable[[ArtifactClass], None]
PortfolioBuilder = Callable[..., PortfolioDefinition]
ContextCapabilityValidator = Callable[[tuple[BaseStrategy, ...]], None]


class ProfileActivationConsumption(Enum):
    CONSUMED_NOW = "CONSUMED_NOW"
    ALREADY_CONSUMED = "ALREADY_CONSUMED"


@dataclass(frozen=True, slots=True)
class ProfileActivationConsumptionResult:
    disposition: ProfileActivationConsumption
    record: ProfileActivationRequestRecord

    def __post_init__(self) -> None:
        if (
            type(self.disposition) is not ProfileActivationConsumption
            or type(self.record) is not ProfileActivationRequestRecord
            or self.record.status is not ProfileActivationRequestStatus.CONSUMED
        ):
            raise ProfileActivationRequestValidationError()


def consume_profile_activation_request(
    session: Session,
    state_manager: StrategyStateManager,
    expected: ProfileActivationRequest,
    *,
    terminal_at: datetime,
) -> ProfileActivationConsumptionResult:
    """Consume in the caller transaction; no durable/runtime activation claim."""
    if (
        type(expected) is not ProfileActivationRequest
        or type(state_manager) is not StrategyStateManager
        or type(terminal_at) is not datetime
        or terminal_at.tzinfo is not UTC
        or terminal_at.microsecond % 1000 != 0
        or not isinstance(session, Session)
        or not session.is_active
        or not session.in_transaction()
        or session.get_bind().dialect.name != "postgresql"
    ):
        raise ProfileActivationRequestValidationError()
    reason = "ACTIVATION_COMMITTED"
    state = state_manager.lock_state_in_transaction(
        session, expected.intent.key.strategy_id
    )
    record = lock_profile_activation_request(session, expected)
    if record.status is not ProfileActivationRequestStatus.PENDING:
        if (record.status, record.terminal_at, record.terminal_reason) != (
            ProfileActivationRequestStatus.CONSUMED,
            terminal_at,
            reason,
        ):
            raise ProfileActivationRequestConflict()
        return ProfileActivationConsumptionResult(
            ProfileActivationConsumption.ALREADY_CONSUMED, record
        )
    if state.version != expected.intent.expected_state_version:
        raise StaleStrategyStateVersion("PROFILE_ACTIVATION_STATE_STALE")
    if expected.command.value not in available_strategy_commands(state.status):
        raise ProfileActivationRequestConflict()
    if terminal_at < record.requested_at:
        raise ProfileActivationRequestValidationError()
    state_manager.transition_in_transaction(
        session,
        expected.intent.key.strategy_id,
        StrategyStatus.ACTIVE,
        actor=expected.actor,
        reason=reason,
        changed_at=terminal_at,
        force=expected.command is ProfileActivationCommand.FORCE_RECOVER,
        expected_version=expected.intent.expected_state_version,
    )
    record = terminalize_profile_activation_request(
        session,
        expected,
        status=ProfileActivationRequestStatus.CONSUMED,
        terminal_at=terminal_at,
        terminal_reason=reason,
    )
    return ProfileActivationConsumptionResult(
        ProfileActivationConsumption.CONSUMED_NOW, record
    )


class StrategyActivationService:
    """Hydrate, publish, and transition one loaded artifact to RUNNING."""

    def __init__(
        self,
        *,
        db_session_factory: DbSessionFactory,
        transition_to_running: Callable[..., None],
        transition_to_error: Callable[..., None],
        hydration: StrategyHydrationService,
        register_strategy: Callable[[BaseStrategy], None],
        register_portfolio: Callable[[PortfolioDefinition], None],
        unregister_runtime_artifact: Callable[[str], bool],
        environment_identity: Callable[[], str],
        assert_context_capabilities: ContextCapabilityValidator,
        event_logger: logging.Logger,
    ) -> None:
        self._db_session_factory = db_session_factory
        self._transition_to_running = transition_to_running
        self._transition_to_error = transition_to_error
        self._hydration = hydration
        self._register_strategy = register_strategy
        self._register_portfolio = register_portfolio
        self._unregister_runtime_artifact = unregister_runtime_artifact
        self._environment_identity = environment_identity
        self._assert_context_capabilities = assert_context_capabilities
        self._logger = event_logger

    def activate_locked(
        self,
        strategy_id: str,
        *,
        artifact_cls: ArtifactClass | None,
        actor: str,
        reason: str | None,
        force: bool,
        expected_version: int | None,
        resolve_product_id: ProductIdResolver,
        assert_live_readiness: ReadinessValidator,
        build_portfolio_definition: PortfolioBuilder,
    ) -> bool:
        """Run under Engine's per-strategy lifecycle lock."""
        self._logger.info("🚀 Starting Strategy: %s", strategy_id)
        if artifact_cls is None:
            self._logger.error("Strategy %s not loaded.", strategy_id)
            return False

        self._assert_expected_version(strategy_id, expected_version)
        with self._db_session_factory() as db:
            state = (
                db.query(StrategyState)
                .filter(StrategyState.strategy_id == strategy_id)
                .first()
            )
            startable = {
                StrategyStatus.READY,
                StrategyStatus.WARNING,
                StrategyStatus.STOPPED,
                StrategyStatus.DISCOVERED,
            }
            if state is None or (state.status not in startable and not force):
                self._logger.error(
                    "Strategy %s is not in startable state (Current: %s)",
                    strategy_id,
                    state.status if state else "None",
                )
                return False

            try:
                config = json.loads(state.config_json or "{}")
                product_id = resolve_product_id(config)
                assert_live_readiness(artifact_cls)
                if issubclass(artifact_cls, PortfolioFactory):
                    definition = build_portfolio_definition(
                        artifact_cls,
                        portfolio_id=strategy_id,
                        product_id=product_id,
                        config=config,
                    )
                    self._assert_context_capabilities(
                        tuple(sleeve.strategy for sleeve in definition.sleeves)
                    )
                    warmed_sleeves: list[PortfolioSleeve] = []
                    for sleeve in definition.sleeves:
                        instance = sleeve.strategy
                        self._hydration.warm_up(db, instance)
                        if self._environment_identity() == "live":
                            self._hydration.fresh_instance_for_replay(instance)
                        warmed_sleeves.append(replace(sleeve, strategy=instance))
                    self._register_portfolio(
                        replace(definition, sleeves=tuple(warmed_sleeves))
                    )
                else:
                    instance = artifact_cls(strategy_id, product_id)
                    self._assert_context_capabilities((instance,))
                    self._hydration.warm_up(db, instance)
                    if self._environment_identity() == "live":
                        self._hydration.fresh_instance_for_replay(instance)
                    self._register_strategy(instance)
                state.uptime_start = int(time.time() * 1000)
                db.commit()
                self._logger.info(
                    "🔥 Strategy %s is now ACTIVE for %s",
                    strategy_id,
                    product_id,
                )
            except Exception as error:
                self._unregister_runtime_artifact(strategy_id)
                state.performance_json = json.dumps({"error": str(error)})
                db.commit()
                self._transition_to_error(
                    strategy_id,
                    str(error),
                    actor="system",
                    expected_version=expected_version,
                )
                self._logger.error("❌ Failed to start %s: %s", strategy_id, error)
                return False

        try:
            transition_kwargs = {
                "actor": actor,
                "force": force,
                "reason": reason,
            }
            if expected_version is not None:
                transition_kwargs["expected_version"] = expected_version
            self._transition_to_running(
                strategy_id,
                **transition_kwargs,
            )
        except Exception as error:
            self._unregister_runtime_artifact(strategy_id)
            self._logger.error(
                "❌ Failed to transition %s to ACTIVE: %s",
                strategy_id,
                error,
            )
            return False
        return True

    def _assert_expected_version(
        self,
        strategy_id: str,
        expected_version: int | None,
    ) -> None:
        if expected_version is None:
            return
        with self._db_session_factory() as db:
            state = (
                db.query(StrategyState)
                .filter(StrategyState.strategy_id == strategy_id)
                .first()
            )
            if state is None:
                raise KeyError(f"strategy state not found: {strategy_id}")
            current_version = int(state.version or 0)
        if current_version != expected_version:
            raise StaleStrategyStateVersion(
                f"{strategy_id} expected version {expected_version}, "
                f"found {current_version}"
            )
