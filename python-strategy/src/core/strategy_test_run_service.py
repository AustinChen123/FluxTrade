import json
import logging
import traceback
from contextlib import AbstractContextManager
from typing import Callable, cast

from sqlalchemy.orm import Session

from src.core.models import StrategyStatus
from src.core.orm_models import StrategyState
from src.core.portfolio_runtime import PortfolioFactory
from src.core.strategy_state_manager import StrategyStateManager
from src.strategies.base import BaseStrategy


ArtifactClass = type[BaseStrategy] | type[PortfolioFactory]
DbSessionFactory = Callable[[], AbstractContextManager[Session]]
ProductIdResolver = Callable[[dict], str]
ArtifactBuilder = Callable[..., tuple[BaseStrategy, ...]]
DataAvailabilityChecker = Callable[[Session, str, str, int], tuple[bool, str]]


class StrategyTestRunService:
    """Evaluate historical data readiness and persist its lifecycle result."""

    def __init__(
        self,
        *,
        db_session_factory: DbSessionFactory,
        state_manager: StrategyStateManager,
        event_logger: logging.Logger,
    ) -> None:
        self._db_session_factory = db_session_factory
        self._state_manager = state_manager
        self._logger = event_logger

    def run(
        self,
        strategy_id: str,
        *,
        days: int = 1,
        artifact_cls: ArtifactClass | None,
        resolve_product_id: ProductIdResolver,
        build_artifact_instances: ArtifactBuilder,
        check_data_availability: DataAvailabilityChecker,
    ) -> None:
        self._logger.info("🧪 Test Run for %s (days=%s)", strategy_id, days)
        if artifact_cls is None:
            self._logger.error("Strategy %s not loaded.", strategy_id)
            return

        with self._db_session_factory() as db:
            state = (
                db.query(StrategyState)
                .filter(StrategyState.strategy_id == strategy_id)
                .first()
            )
            if not state:
                self._logger.error("Strategy %s not in DB.", strategy_id)
                return

            expected_version = int(state.version or 0)
            next_status = StrategyStatus.READY
            transition_reason = "test_run_completed"
            test_error: Exception | None = None
            try:
                config = cast(dict, json.loads(state.config_json or "{}"))
                product_id = resolve_product_id(config)
                instances = build_artifact_instances(
                    artifact_cls,
                    strategy_id=strategy_id,
                    product_id=product_id,
                    config=config,
                )
                for instance in instances:
                    requirements = instance.requirements
                    is_available, backfill_command = check_data_availability(
                        db,
                        requirements.product_id,
                        requirements.timeframe,
                        requirements.lookback_window,
                    )
                    if is_available:
                        continue
                    self._logger.warning(
                        "⚠️ Insufficient data for %s. Command: %s",
                        strategy_id,
                        backfill_command,
                    )
                    state.performance_json = json.dumps(
                        {"backfill_command": backfill_command}
                    )
                    next_status = StrategyStatus.WARNING
                    transition_reason = "insufficient_data"
                    break
                db.commit()
            except Exception as error:
                test_error = error
                error_trace = traceback.format_exc()
                state.performance_json = json.dumps({"error": error_trace})
                db.commit()
                next_status = StrategyStatus.ERROR
                transition_reason = error_trace

        if next_status == StrategyStatus.ERROR:
            self._state_manager.transition_to_error(
                strategy_id,
                transition_reason,
                actor="system",
                expected_version=expected_version,
            )
            self._logger.error("❌ Test Run failed for %s: %s", strategy_id, test_error)
            return
        self._state_manager.transition_to_status(
            strategy_id,
            next_status,
            actor="system",
            reason=transition_reason,
            expected_version=expected_version,
        )
        if next_status == StrategyStatus.READY:
            self._logger.info("✅ Strategy %s is READY.", strategy_id)
