import json
import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy.orm import Session

from src.core.models import StrategyStatus
from src.core.orm_models import Strategy, StrategyState
from src.core.portfolio_runtime import PortfolioFactory
from src.strategies.base import BaseStrategy

LoadedArtifact = type[BaseStrategy] | type[PortfolioFactory]
ArtifactLoadResult = LoadedArtifact | str
_PROTECTION_DECLARATIONS = {"NONE", "SL", "TP", "SL+TP"}


def _strategy_configuration_for_artifact(
    existing_json: str | None,
    artifact: LoadedArtifact,
) -> str:
    """Merge catalog-owned declaration metadata into the existing config row."""
    try:
        configuration: Any = json.loads(existing_json or "{}")
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("strategy configuration_json is invalid") from exc
    if not isinstance(configuration, dict):
        raise ValueError("strategy configuration_json must contain an object")

    declaration = getattr(
        artifact,
        "__fluxtrade_system_placed_protection__",
        None,
    )
    if declaration in (None, ""):
        configuration.pop("system_placed_protection", None)
        configuration.pop("system_placed_protection_revision", None)
    else:
        if (
            not isinstance(declaration, str)
            or declaration not in _PROTECTION_DECLARATIONS
        ):
            raise ValueError("invalid system_placed_protection declaration")
        revision = getattr(artifact, "__fluxtrade_catalog_sha256__", None)
        if not isinstance(revision, str) or not revision:
            raise ValueError("system_placed_protection revision is required")
        configuration["system_placed_protection"] = declaration
        configuration["system_placed_protection_revision"] = revision
    return json.dumps(configuration, sort_keys=True, separators=(",", ":"))


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
            strategy = db.get(Strategy, strategy_id)
            if strategy is None:
                strategy = Strategy(
                    id=strategy_id,
                    name=strategy_id,
                    configuration_json="{}",
                )
                db.add(strategy)
            declaration = (
                getattr(result, "__fluxtrade_system_placed_protection__", None)
                if not isinstance(result, str)
                else None
            )
            if not isinstance(result, str) and declaration not in (None, ""):
                try:
                    configuration_json = _strategy_configuration_for_artifact(
                        cast(
                            str | None,
                            getattr(strategy, "configuration_json", None),
                        ),
                        result,
                    )
                except ValueError as exc:
                    event_logger.warning(
                        "Skipping strategy artifact %s during catalog sync: %s",
                        strategy_id,
                        exc,
                    )
                    db.rollback()
                    continue
                setattr(strategy, "configuration_json", configuration_json)
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
