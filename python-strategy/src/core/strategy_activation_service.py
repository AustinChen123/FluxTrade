from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import replace

from sqlalchemy.orm import Session

from src.core.models import StrategyStatus
from src.core.orm_models import StrategyState
from src.core.portfolio_runtime import (
    PortfolioDefinition,
    PortfolioFactory,
    PortfolioSleeve,
)
from src.core.strategy_hydration_service import StrategyHydrationService
from src.core.strategy_state_manager import (
    StaleStrategyStateVersion,
)
from src.strategies.base import BaseStrategy


ArtifactClass = type[BaseStrategy] | type[PortfolioFactory]
DbSessionFactory = Callable[[], AbstractContextManager[Session]]
ProductIdResolver = Callable[[dict], str]
ReadinessValidator = Callable[[ArtifactClass], None]
PortfolioBuilder = Callable[..., PortfolioDefinition]
ContextCapabilityValidator = Callable[[tuple[BaseStrategy, ...]], None]


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
