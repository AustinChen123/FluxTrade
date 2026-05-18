"""Strategy lifecycle state manager."""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime
from typing import ContextManager, Callable, Optional

from sqlalchemy.orm import Session

from src.core.models import StrategyStatus
from src.core.orm_models import StrategyState, StrategyStateTransition

logger = logging.getLogger(__name__)

STATE_CHANGE_CHANNEL = "strategy_state_changes"


class InvalidStrategyStateTransition(RuntimeError):
    """Raised when a requested strategy state transition is not allowed."""


class StrategyStateManager:
    """Manage strategy lifecycle state with a local O(1) status cache."""

    def __init__(
        self,
        db_session_factory: Callable[[], ContextManager[Session]],
        redis_client,
    ) -> None:
        self._db_session_factory = db_session_factory
        self._redis_client = redis_client
        self._cache: dict[str, StrategyStatus] = {}
        self._lock = threading.Lock()

    def initialize_cache_from_db(self) -> None:
        """Load all strategy statuses from DB into the local cache."""
        with self._db_session_factory() as db:
            states = db.query(StrategyState).all()

        with self._lock:
            self._cache = {
                state.strategy_id: StrategyStatus(state.status)
                for state in states
            }

    def get_status(self, strategy_id: str) -> Optional[StrategyStatus]:
        with self._lock:
            return self._cache.get(strategy_id)

    def is_running(self, strategy_id: str) -> bool:
        return self.get_status(strategy_id) == StrategyStatus.ACTIVE

    def is_stopped(self, strategy_id: str) -> bool:
        return self.get_status(strategy_id) == StrategyStatus.STOPPED

    def is_error(self, strategy_id: str) -> bool:
        return self.get_status(strategy_id) == StrategyStatus.ERROR

    def transition_to_running(
        self,
        strategy_id: str,
        *,
        actor: str = "operator",
        force: bool = False,
        reason: Optional[str] = None,
    ) -> None:
        self._transition(
            strategy_id,
            StrategyStatus.ACTIVE,
            actor=actor,
            force=force,
            reason=reason,
        )

    def transition_to_stopped(
        self,
        strategy_id: str,
        *,
        actor: str = "operator",
        reason: Optional[str] = None,
    ) -> None:
        self._transition(
            strategy_id,
            StrategyStatus.STOPPED,
            actor=actor,
            reason=reason,
        )

    def transition_to_error(
        self,
        strategy_id: str,
        reason: str,
        *,
        actor: str = "system",
    ) -> None:
        self._transition(
            strategy_id,
            StrategyStatus.ERROR,
            actor=actor,
            reason=reason,
        )

    def on_state_change_message(self, message: dict) -> None:
        """Apply a pub/sub state-change message to the local cache."""
        strategy_id = message.get("strategy_id")
        status = message.get("status")
        if not strategy_id or not status:
            logger.warning("Ignoring malformed strategy state message: %s", message)
            return

        with self._lock:
            self._cache[str(strategy_id)] = StrategyStatus(status)

    def _transition(
        self,
        strategy_id: str,
        to_status: StrategyStatus,
        *,
        actor: str,
        reason: Optional[str],
        force: bool = False,
    ) -> None:
        now = datetime.now(UTC)
        with self._db_session_factory() as db:
            state = db.query(StrategyState).filter_by(strategy_id=strategy_id).first()
            if state is None:
                raise KeyError(f"strategy state not found: {strategy_id}")

            from_status = StrategyStatus(state.status)
            if from_status == StrategyStatus.ERROR and to_status == StrategyStatus.ACTIVE and not force:
                raise InvalidStrategyStateTransition(
                    f"{strategy_id} is in ERROR and requires force=True to resume"
                )

            state.status = to_status.value
            if to_status == StrategyStatus.ERROR:
                state.last_error_message = reason
                state.entered_error_at = now
            elif to_status == StrategyStatus.STOPPED:
                state.stopped_at = now
            elif to_status == StrategyStatus.ACTIVE:
                state.recovered_at = now if from_status == StrategyStatus.ERROR else state.recovered_at

            db.add(
                StrategyStateTransition(
                    strategy_id=strategy_id,
                    from_status=from_status.value,
                    to_status=to_status.value,
                    transitioned_at=now,
                    reason=reason,
                    actor=actor,
                )
            )
            db.commit()

        with self._lock:
            self._cache[strategy_id] = to_status
        self._publish_state_change(strategy_id, to_status, now)

    def _publish_state_change(
        self,
        strategy_id: str,
        status: StrategyStatus,
        changed_at: datetime,
    ) -> None:
        payload = {
            "strategy_id": strategy_id,
            "status": status.value,
            "timestamp": int(changed_at.timestamp() * 1000),
        }
        self._redis_client.publish(STATE_CHANGE_CHANNEL, json.dumps(payload))
