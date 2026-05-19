"""Strategy lifecycle state manager."""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import UTC, datetime
from typing import ContextManager, Callable, Optional

from sqlalchemy.orm import Session

from src.core.models import StrategyStatus
from src.core.orm_models import StrategyState, StrategyStateTransition

logger = logging.getLogger(__name__)

STATE_CHANGE_CHANNEL = "strategy_state_changes"


class InvalidStrategyStateTransition(RuntimeError):
    """Raised when a requested strategy state transition is not allowed."""


class StaleStrategyStateVersion(RuntimeError):
    """Raised when optimistic locking detects a concurrent state update."""


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
        self._subscriber_stop = threading.Event()
        self._subscriber_thread: threading.Thread | None = None

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

    def start_subscriber(self) -> None:
        """Start a daemon Redis subscriber for cross-process state changes."""
        if self._subscriber_thread and self._subscriber_thread.is_alive():
            return

        self._subscriber_stop.clear()
        self._subscriber_thread = threading.Thread(
            target=self._subscriber_loop,
            name="strategy-state-subscriber",
            daemon=True,
        )
        self._subscriber_thread.start()

    def shutdown(self) -> None:
        """Stop the subscriber thread if it is running."""
        self._subscriber_stop.set()
        if self._subscriber_thread is not None:
            self._subscriber_thread.join(timeout=5)

    def _subscriber_loop(self) -> None:
        pubsub = self._redis_client.pubsub()
        try:
            pubsub.subscribe(STATE_CHANGE_CHANNEL)
            while not self._subscriber_stop.is_set():
                try:
                    message = pubsub.get_message(timeout=1.0)
                    if not message or message.get("type") != "message":
                        continue
                    payload = self._decode_state_change_message(message.get("data"))
                    self.on_state_change_message(payload)
                except json.JSONDecodeError as e:
                    logger.warning("Ignoring malformed strategy state message: %s", e)
                except Exception:
                    logger.exception("Strategy state subscriber failed")
                    time.sleep(1)
        finally:
            close = getattr(pubsub, "close", None)
            if close is not None:
                close()

    @staticmethod
    def _decode_state_change_message(data) -> dict:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        if isinstance(data, str):
            return json.loads(data)
        if isinstance(data, dict):
            return data
        raise json.JSONDecodeError("unsupported message payload", str(data), 0)

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

            expected_version = int(state.version or 0)
            update_values = {
                "status": to_status.value,
                "version": expected_version + 1,
            }
            if to_status == StrategyStatus.ERROR:
                update_values["last_error_message"] = reason
                update_values["entered_error_at"] = now
            elif to_status == StrategyStatus.STOPPED:
                update_values["stopped_at"] = now
            elif to_status == StrategyStatus.ACTIVE:
                if from_status == StrategyStatus.ERROR:
                    update_values["recovered_at"] = now

            updated = (
                db.query(StrategyState)
                .filter_by(strategy_id=strategy_id)
                .filter(StrategyState.version == expected_version)
                .update(update_values, synchronize_session=False)
            )
            if updated != 1:
                db.rollback()
                raise StaleStrategyStateVersion(
                    f"{strategy_id} expected version {expected_version}"
                )

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
