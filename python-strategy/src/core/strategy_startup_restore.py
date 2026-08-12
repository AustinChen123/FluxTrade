import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import cast

from sqlalchemy.orm import Session

from src.core.models import StrategyStatus
from src.core.orm_models import StrategyState


def restore_active_strategies(
    *,
    db_session_factory: Callable[[], AbstractContextManager[Session]],
    is_strategy_loaded: Callable[[str], bool],
    activate_strategy: Callable[..., object],
    transition_to_error: Callable[..., object],
    event_logger: logging.Logger,
) -> None:
    """Restore durably ACTIVE strategies in query order at process startup."""
    with db_session_factory() as db:
        active_states = (
            db.query(StrategyState)
            .filter(StrategyState.status == StrategyStatus.ACTIVE.value)
            .all()
        )

    for state in active_states:
        strategy_id = cast(str, state.strategy_id)
        if not is_strategy_loaded(strategy_id):
            event_logger.error(
                "Startup restore: strategy class not loaded for %s — marking ERROR",
                strategy_id,
            )
            transition_to_error(
                strategy_id,
                "startup_restore_class_missing",
                actor="system",
            )
            continue
        try:
            activate_strategy(
                strategy_id,
                actor="system",
                reason="startup_restore",
                force=True,
            )
        except Exception as error:
            event_logger.exception(
                "Startup restore: failed to activate %s — marking ERROR",
                strategy_id,
            )
            transition_to_error(
                strategy_id,
                f"startup_restore_failed: {error}",
                actor="system",
            )
