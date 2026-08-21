import json
import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from src.core.models import StrategyStatus
from src.core.orm_models import Strategy, StrategyState
from src.core.portfolio_runtime import PortfolioFactory
from src.strategies.base import BaseStrategy

LoadedArtifact = type[BaseStrategy] | type[PortfolioFactory]
ArtifactLoadResult = LoadedArtifact | str


def synchronize_strategy_artifacts(
    *,
    artifact_loader: Callable[[], dict[str, ArtifactLoadResult]],
    publish_loaded_classes: Callable[[dict[str, LoadedArtifact]], None],
    db_session_factory: Callable[[], AbstractContextManager[Session]],
    transition_to_error: Callable[..., object],
    event_logger: logging.Logger,
) -> dict[str, LoadedArtifact]:
    """Discover configured artifacts and synchronize their durable state rows."""
    event_logger.info("🔍 Scanning configured strategy artifacts...")
    found = artifact_loader()
    loaded = {
        strategy_id: result
        for strategy_id, result in found.items()
        if not isinstance(result, str)
    }
    publish_loaded_classes(loaded)

    with db_session_factory() as db:
        for strategy_id, result in found.items():
            if db.get(Strategy, strategy_id) is None:
                db.add(
                    Strategy(
                        id=strategy_id,
                        name=strategy_id,
                        configuration_json="{}",
                    )
                )
            state = (
                db.query(StrategyState)
                .filter(StrategyState.strategy_id == strategy_id)
                .first()
            )
            is_new = state is None
            if state is None:
                state = StrategyState(
                    strategy_id=strategy_id,
                    status=(
                        StrategyStatus.ERROR
                        if isinstance(result, str)
                        else StrategyStatus.DISCOVERED
                    ),
                    config_json="{}",
                )
                db.add(state)

            if isinstance(result, str):
                if is_new:
                    setattr(state, "performance_json", json.dumps({"error": result}))
                    setattr(state, "last_error_message", result)
                    setattr(state, "entered_error_at", datetime.now(UTC))
                else:
                    raw_version = getattr(state, "version", 0)
                    expected_version = raw_version if type(raw_version) is int else 0
                    db.commit()
                    transition_to_error(
                        strategy_id,
                        result,
                        actor="system",
                        expected_version=expected_version,
                    )
                    continue

            db.commit()

    event_logger.info("✅ Scan Complete. Total loaded: %s", len(loaded))
    return loaded
