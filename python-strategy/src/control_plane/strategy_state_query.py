from __future__ import annotations

import json
import time
from typing import Any

from sqlalchemy import func, or_

from src.control_plane.backtest_jobs import SessionFactory
from src.core.orm_models import StrategyState, StrategyStateTransition


class StrategyStateQueryService:
    """Read-only access to durable strategy lifecycle state."""

    def __init__(self, db_session_factory: SessionFactory) -> None:
        self._db_session_factory = db_session_factory

    def list_states(
        self,
        *,
        status: str | None = None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        with self._db_session_factory() as session:
            query = session.query(StrategyState)
            if status is not None:
                query = query.filter(StrategyState.status == status)
            total = query.count()
            states = (
                query.order_by(StrategyState.strategy_id)
                .offset(offset)
                .limit(limit)
                .all()
            )
            return [_state_payload(state) for state in states], total

    def get_state(self, strategy_id: str) -> dict[str, Any]:
        with self._db_session_factory() as session:
            state = session.get(StrategyState, strategy_id)
            if state is None:
                raise KeyError(strategy_id)
            return _state_payload(state)

    def summarize_states(
        self,
        *,
        stale_after_ms: int = 120_000,
    ) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        stale_before = now_ms - stale_after_ms
        with self._db_session_factory() as session:
            status_rows = (
                session.query(StrategyState.status, func.count(StrategyState.strategy_id))
                .group_by(StrategyState.status)
                .all()
            )
            stale_heartbeat_count = (
                session.query(StrategyState)
                .filter(StrategyState.status != "STOPPED")
                .filter(
                    or_(
                        StrategyState.last_heartbeat.is_(None),
                        StrategyState.last_heartbeat < stale_before,
                    )
                )
                .count()
            )
            by_status = {status: count for status, count in status_rows}
            return {
                "total": sum(by_status.values()),
                "by_status": by_status,
                "stale_heartbeat_count": stale_heartbeat_count,
                "stale_after_ms": stale_after_ms,
                "observed_at_ms": now_ms,
            }

    def list_transitions(
        self,
        strategy_id: str,
        *,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        with self._db_session_factory() as session:
            query = session.query(StrategyStateTransition).filter(
                StrategyStateTransition.strategy_id == strategy_id
            )
            total = query.count()
            transitions = (
                query.order_by(
                    StrategyStateTransition.transitioned_at.desc(),
                    StrategyStateTransition.id.desc(),
                )
                .offset(offset)
                .limit(limit)
                .all()
            )
            return [_transition_payload(row) for row in transitions], total


def _state_payload(state: StrategyState) -> dict[str, Any]:
    return {
        "strategy_id": state.strategy_id,
        "status": state.status,
        "config": _loads_json_object(state.config_json),
        "performance": _loads_json_object(state.performance_json),
        "last_heartbeat": state.last_heartbeat,
        "uptime_start": state.uptime_start,
        "last_error_message": state.last_error_message,
        "entered_error_at": _iso_or_none(state.entered_error_at),
        "recovered_at": _iso_or_none(state.recovered_at),
        "stopped_at": _iso_or_none(state.stopped_at),
        "version": state.version,
    }


def _transition_payload(row: StrategyStateTransition) -> dict[str, Any]:
    return {
        "id": row.id,
        "strategy_id": row.strategy_id,
        "from_status": row.from_status,
        "to_status": row.to_status,
        "transitioned_at": _iso_or_none(row.transitioned_at),
        "reason": row.reason,
        "actor": row.actor,
    }


def _loads_json_object(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    parsed = json.loads(value)
    if isinstance(parsed, dict):
        return parsed
    return {"value": parsed}


def _iso_or_none(value: Any) -> str | None:
    return None if value is None else value.isoformat()
