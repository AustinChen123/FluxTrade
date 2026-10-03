from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.orm_models import SystemEvent
from src.core.runtime_environment import RuntimeEnvironment


class OpsStatusQuery:
    """Read the independent durable and Redis kill-switch authorities."""

    def __init__(
        self,
        redis_client: Any,
        db_session_factory: Callable[[], Session],
        environment: RuntimeEnvironment,
    ) -> None:
        self._redis = redis_client
        self._sessions = db_session_factory
        self._environment = environment

    def read(self) -> dict[str, str | bool | None]:
        redis_value = self._redis.get(self._environment.key("system:state"))
        redis_state = _state(redis_value)
        with self._sessions() as session:
            event = session.execute(
                select(SystemEvent)
                .where(SystemEvent.event_type == "ops", SystemEvent.event_subtype == "kill_switch_state")
                .order_by(SystemEvent.id.desc())
                .limit(1)
            ).scalar_one_or_none()
        durable_state = None
        if event is not None and isinstance(event.payload, dict):
            durable_state = _state(event.payload.get("state"))
        listener_rows = self._redis.pubsub_numsub("cmd:strategy:control")
        listener_available = bool(listener_rows and int(listener_rows[0][1]) > 0)
        states = (redis_state, durable_state)
        state = "LOCKDOWN" if "LOCKDOWN" in states else (
            "OK" if states == ("OK", "OK") else "UNKNOWN"
        )
        return {"state": state, "redis_state": redis_state,
                "durable_state": durable_state, "listener_available": listener_available}


def _state(value: Any) -> str | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            return None
    return value if isinstance(value, str) and value in {"OK", "LOCKDOWN"} else None
