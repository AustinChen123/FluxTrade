from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from typing import Protocol

from sqlalchemy.orm import Session

from src.core.models import StrategyStatus, Candlestick
from src.core.bootstrap_hydration_reader import BootstrapHydrationReader
from src.core.market_data.profiles.bootstrap_seed import BootstrapSeed
from src.core.market_data.profiles.bootstrap_seed_store import BootstrapSeedRecord
from src.core.market_data.profiles.bootstrap_hydration import BoundBootstrapHydration
from src.core.command_router import StrategyStartDisposition
from src.core.product_registry import to_stream_key
from src.core.market_data.profiles.bootstrap_seed import BootstrapKey
from src.core.market_data.profiles.decision_owner import IdentityResolver
from src.core.market_data.profiles.decision_identity import (
    MarketDataDecisionCompositionIdentity,
)
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
    ProfileActivationRequestStore,
    ProfileActivationAdmissionStatus,
    ProfileActivationAdmissionResult,
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
    StrategyStateTransitionResult,
    available_strategy_commands,
)
from src.strategies.base import BaseStrategy


ArtifactClass = type[BaseStrategy] | type[PortfolioFactory]
DbSessionFactory = Callable[[], AbstractContextManager[Session]]
ProductIdResolver = Callable[[dict], str]
ReadinessValidator = Callable[[ArtifactClass], None]
PortfolioBuilder = Callable[..., PortfolioDefinition]


class ContextCapabilityValidator(Protocol):
    def __call__(
        self,
        strategies: tuple[BaseStrategy, ...],
        *,
        profile_warmup_ready: bool = False,
    ) -> None: ...


class ProfileActivationConsumption(Enum):
    CONSUMED_NOW = "CONSUMED_NOW"
    ALREADY_CONSUMED = "ALREADY_CONSUMED"


@dataclass(frozen=True, slots=True)
class ProfileActivationConsumptionResult:
    disposition: ProfileActivationConsumption
    record: ProfileActivationRequestRecord
    transition: StrategyStateTransitionResult | None = None

    def __post_init__(self) -> None:
        if (
            type(self.disposition) is not ProfileActivationConsumption
            or type(self.record) is not ProfileActivationRequestRecord
            or self.record.status is not ProfileActivationRequestStatus.CONSUMED
            or (
                type(self.transition) is not StrategyStateTransitionResult
                if self.disposition is ProfileActivationConsumption.CONSUMED_NOW
                else self.transition is not None
            )
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
    transition = state_manager.transition_in_transaction(
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
        ProfileActivationConsumption.CONSUMED_NOW, record, transition
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
        profile_request_store: ProfileActivationRequestStore | None = None,
        profile_identity_resolver: IdentityResolver | None = None,
        bootstrap_reader: BootstrapHydrationReader | None = None,
        state_manager: StrategyStateManager | None = None,
        artifact_resolver: Callable[[str], ArtifactClass | None] | None = None,
        profile_seed_factory: Callable[[BaseStrategy, BootstrapKey, int], BootstrapSeed]
        | None = None,
        cutover_unregister_locked: Callable[[str], bool] | None = None,
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
        self._profile_request_store = profile_request_store
        self._profile_identity_resolver = profile_identity_resolver
        self._cutover_unregister_locked = cutover_unregister_locked
        self._bootstrap_reader, self._state_manager = bootstrap_reader, state_manager
        self._artifact_resolver, self._profile_seed_factory = (
            artifact_resolver,
            profile_seed_factory,
        )

    def cutover_pending_candle(self, candle: Candlestick) -> int:
        if (
            self._environment_identity() != "live"
            or self._profile_request_store is None
        ):
            return 0
        count = 0
        for record in self._profile_request_store.list_pending("live"):
            key = record.request.intent.key
            if (key.product_id, key.timeframe) == (candle.product_id, candle.timeframe):
                self.cutover_pending_request(record, candle)
                count += 1
        return count

    def cutover_pending_request(
        self, record: ProfileActivationRequestRecord, candle: Candlestick
    ) -> None:
        """Single private-instance cutover; caller owns application/registration fences."""
        if (
            type(record) is not ProfileActivationRequestRecord
            or record.status is not ProfileActivationRequestStatus.PENDING
            or type(candle) is not Candlestick
            or self._environment_identity() != "live"
        ):
            raise ProfileActivationRequestValidationError()
        request, key = record.request, record.request.intent.key
        if (key.environment, key.product_id, key.timeframe) != (
            "live",
            candle.product_id,
            candle.timeframe,
        ):
            raise ProfileActivationRequestValidationError()
        if (
            self._bootstrap_reader is None
            or self._state_manager is None
            or self._artifact_resolver is None
            or self._profile_seed_factory is None
            or self._profile_identity_resolver is None
            or self._cutover_unregister_locked is None
        ):
            raise ProfileActivationRequestValidationError()
        artifact = self._artifact_resolver(key.strategy_id)
        if (
            not isinstance(artifact, type)
            or not issubclass(artifact, BaseStrategy)
            or issubclass(artifact, PortfolioFactory)
        ):
            raise ProfileActivationRequestValidationError()
        instance = artifact(key.strategy_id, key.product_id)
        identity = self._profile_identity_resolver(instance)
        if (
            type(identity) is not MarketDataDecisionCompositionIdentity
            or (instance.strategy_id, instance.product_id)
            != (key.strategy_id, key.product_id)
            or (
                identity.execution_scope_id,
                identity.strategy_version,
                identity.config_hash,
            )
            != (key.execution_scope_id, key.strategy_version, key.config_hash)
            or instance.requirements != request.intent.requirements
        ):
            raise ProfileActivationRequestConflict()
        self._assert_context_capabilities((instance,), profile_warmup_ready=True)
        factory = self._profile_seed_factory

        def checked_factory(seed_key: BootstrapKey) -> BootstrapSeed:
            if seed_key != key:
                raise ProfileActivationRequestConflict()
            return factory(instance, seed_key, candle.timestamp)

        def verify(seed: BootstrapSeed) -> None:
            requirements = request.intent.requirements
            if (seed.key, seed.requirements, seed.lookback) != (
                key,
                requirements.profile_requirements,
                requirements.lookback_window,
            ):
                raise ProfileActivationRequestConflict()

        seed_record = self._bootstrap_reader.prepare_initial_seed_under_admission(
            instance,
            candle.timestamp,
            factory=checked_factory,
        )
        if type(seed_record) is not BootstrapSeedRecord:
            raise ProfileActivationRequestValidationError()
        verify(seed_record.value)
        bound = self._bootstrap_reader.prepare(
            instance, candle.timestamp, "BEFORE_PENDING"
        )
        if type(bound) is not BoundBootstrapHydration or bound.strategy is not instance:
            raise ProfileActivationRequestValidationError()
        verify(bound.plan.seed)
        self._hydration.hydrate_candles(
            instance, bound.candles, decision_scope_loader=bound.decision_scope_loader
        )
        now = datetime.now(UTC)
        now = now.replace(microsecond=now.microsecond // 1000 * 1000)
        with self._db_session_factory() as session:
            with session.begin():
                result = consume_profile_activation_request(
                    session, self._state_manager, request, terminal_at=now
                )
        if result.disposition is not ProfileActivationConsumption.CONSUMED_NOW:
            raise ProfileActivationRequestConflict()
        try:
            self._register_strategy(instance)
            assert result.transition is not None
            self._state_manager.after_committed_transition(result.transition)
        except BaseException:
            try:
                self._cutover_unregister_locked(key.strategy_id)
            except BaseException:
                self._logger.error("profile_cutover_unregister_failed")
            try:
                self._transition_to_error(
                    key.strategy_id,
                    "PROFILE_CUTOVER_FAILED",
                    actor="system",
                    expected_version=request.intent.expected_state_version + 1,
                )
            except BaseException:
                self._logger.error("profile_cutover_cleanup_failed")
            raise

    def persistent_pending_channels(self) -> tuple[str, ...]:
        """Discover subscription intent from durable requests after restart."""
        if (
            self._profile_request_store is None
            or self._profile_identity_resolver is None
            or self._environment_identity() != "live"
        ):
            return ()
        return tuple(
            sorted(
                {
                    to_stream_key(
                        record.request.intent.key.product_id,
                        record.request.intent.key.timeframe,
                    )
                    for record in self._profile_request_store.list_pending("live")
                }
            )
        )

    def _restore_profile_active(self, instance: BaseStrategy, version: int) -> bool:
        """Rebuild runtime from this ACTIVE generation without lifecycle writes."""
        if (
            type(version) is not int
            or not 1 <= version <= 2**31 - 1
            or self._profile_request_store is None
            or self._profile_identity_resolver is None
            or self._bootstrap_reader is None
        ):
            raise ProfileActivationRequestValidationError()
        record = self._profile_request_store.get_consumed(
            environment="live",
            strategy_id=instance.strategy_id,
            expected_state_version=version - 1,
        )
        if (
            type(record) is not ProfileActivationRequestRecord
            or record.status is not ProfileActivationRequestStatus.CONSUMED
        ):
            raise ProfileActivationRequestValidationError()
        identity = self._profile_identity_resolver(instance)
        if type(identity) is not MarketDataDecisionCompositionIdentity:
            raise ProfileActivationRequestValidationError()
        key = BootstrapKey(
            "live",
            identity.execution_scope_id,
            instance.strategy_id,
            identity.strategy_version,
            identity.config_hash,
            instance.product_id,
            instance.requirements.timeframe,
        )
        if (
            record.request.intent.key != key
            or record.request.intent.requirements != instance.requirements
            or record.request.intent.expected_state_version != version - 1
        ):
            raise ProfileActivationRequestConflict()
        self._assert_context_capabilities((instance,), profile_warmup_ready=True)
        bound = self._bootstrap_reader.prepare_latest_applied(instance)
        if (
            type(bound) is not BoundBootstrapHydration
            or bound.strategy is not instance
            or bound.plan.seed.key != key
            or bound.plan.seed.requirements
            != instance.requirements.profile_requirements
            or bound.plan.seed.lookback != instance.requirements.lookback_window
        ):
            raise ProfileActivationRequestConflict()
        self._hydration.hydrate_candles(
            instance, bound.candles, decision_scope_loader=bound.decision_scope_loader
        )
        try:
            self._register_strategy(instance)
        except BaseException:
            self._unregister_runtime_artifact(instance.strategy_id)
            raise
        return True

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
        activation_command: str | None = None,
        idempotency_key: str | None = None,
    ) -> bool | StrategyStartDisposition:
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

            restoring_profile = False
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
                    if (
                        self._environment_identity() == "live"
                        and instance.requirements.profile_requirements
                    ):
                        restoring_profile = (
                            state.status == StrategyStatus.ACTIVE
                            and actor == "system"
                            and reason == "startup_restore"
                        )
                        if restoring_profile:
                            return self._restore_profile_active(instance, state.version)
                        return self._admit_profile(
                            instance,
                            state.version,
                            StrategyStatus(state.status),
                            actor,
                            activation_command,
                            idempotency_key,
                        )
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
                if restoring_profile:
                    raise
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

    def _admit_profile(
        self,
        instance: BaseStrategy,
        version: int,
        status: StrategyStatus,
        actor: str,
        command: str | None,
        key: str | None,
    ) -> bool | StrategyStartDisposition:
        """Admission only; no hydration, registration or lifecycle transition."""
        if (
            self._profile_request_store is None
            or self._profile_identity_resolver is None
            or type(key) is not str
            or type(command) is not str
            or command not in available_strategy_commands(status)
        ):
            return False
        phase = "identity"
        try:
            identity = self._profile_identity_resolver(instance)
            if type(identity) is not MarketDataDecisionCompositionIdentity:
                return False
            intent = ProfileActivationIntent(
                BootstrapKey(
                    self._environment_identity(),
                    identity.execution_scope_id,
                    instance.strategy_id,
                    identity.strategy_version,
                    identity.config_hash,
                    instance.product_id,
                    instance.requirements.timeframe,
                ),
                instance.requirements,
                version,
            )
            request = ProfileActivationRequest(
                actor, key, ProfileActivationCommand(command), intent
            )
            phase = "admission"
            result = self._profile_request_store.admit_confirmed(
                request, current=intent
            )
            if (
                type(result) is ProfileActivationAdmissionResult
                and result.status is ProfileActivationAdmissionStatus.CONFIRMED
                and result.record is not None
                and result.record.request.canonical_bytes == request.canonical_bytes
                and result.record.status
                in (
                    ProfileActivationRequestStatus.PENDING,
                    ProfileActivationRequestStatus.CONSUMED,
                )
            ):
                return StrategyStartDisposition.WAITING_FOR_CUTOVER
        except Exception:
            self._logger.error("profile_activation_admission_failed phase=%s", phase)
            return False
        return False

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
