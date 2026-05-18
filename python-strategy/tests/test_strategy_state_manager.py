"""Tests for strategy lifecycle state manager."""

from __future__ import annotations

import json
from contextlib import nullcontext
from unittest.mock import MagicMock

import pytest

from src.core.models import StrategyStatus
from src.core.orm_models import StrategyState, StrategyStateTransition
from src.core.strategy_state_manager import (
    InvalidStrategyStateTransition,
    STATE_CHANGE_CHANNEL,
    StrategyStateManager,
)


class _FakeQuery:
    def __init__(self, model, db):
        self._model = model
        self._db = db
        self._strategy_id = None

    def all(self):
        if self._model is StrategyState:
            return list(self._db.states.values())
        return []

    def filter_by(self, **kwargs):
        self._strategy_id = kwargs.get("strategy_id")
        return self

    def first(self):
        if self._model is StrategyState:
            return self._db.states.get(self._strategy_id)
        return None


class _FakeSession:
    def __init__(self, states: list[StrategyState]):
        self.states = {state.strategy_id: state for state in states}
        self.transitions: list[StrategyStateTransition] = []
        self.commit_count = 0

    def query(self, model):
        return _FakeQuery(model, self)

    def add(self, row):
        if isinstance(row, StrategyStateTransition):
            self.transitions.append(row)

    def commit(self):
        self.commit_count += 1


def _state(strategy_id: str, status: StrategyStatus) -> StrategyState:
    return StrategyState(strategy_id=strategy_id, status=status.value)


def _manager(db: _FakeSession, redis_client=None) -> StrategyStateManager:
    return StrategyStateManager(
        db_session_factory=lambda: nullcontext(db),
        redis_client=redis_client or MagicMock(),
    )


def test_initialize_cache_from_db_loads_statuses() -> None:
    db = _FakeSession(
        [
            _state("s1", StrategyStatus.ACTIVE),
            _state("s2", StrategyStatus.STOPPED),
        ]
    )
    manager = _manager(db)

    manager.initialize_cache_from_db()

    assert manager.is_running("s1") is True
    assert manager.is_stopped("s2") is True
    assert manager.get_status("missing") is None


def test_transition_to_stopped_updates_db_cache_history_and_pubsub() -> None:
    redis_client = MagicMock()
    db = _FakeSession([_state("s1", StrategyStatus.ACTIVE)])
    manager = _manager(db, redis_client)
    manager.initialize_cache_from_db()

    manager.transition_to_stopped("s1", actor="operator", reason="maintenance")

    assert db.states["s1"].status == StrategyStatus.STOPPED.value
    assert db.states["s1"].stopped_at is not None
    assert manager.is_stopped("s1") is True
    assert db.commit_count == 1
    assert len(db.transitions) == 1
    transition = db.transitions[0]
    assert transition.from_status == StrategyStatus.ACTIVE.value
    assert transition.to_status == StrategyStatus.STOPPED.value
    assert transition.reason == "maintenance"
    assert transition.actor == "operator"
    channel, message = redis_client.publish.call_args.args
    assert channel == STATE_CHANGE_CHANNEL
    assert json.loads(message)["status"] == StrategyStatus.STOPPED.value


def test_transition_to_error_records_error_metadata() -> None:
    db = _FakeSession([_state("s1", StrategyStatus.ACTIVE)])
    manager = _manager(db)

    manager.transition_to_error("s1", "daily loss exceeded")

    assert db.states["s1"].status == StrategyStatus.ERROR.value
    assert db.states["s1"].last_error_message == "daily loss exceeded"
    assert db.states["s1"].entered_error_at is not None
    assert manager.is_error("s1") is True


def test_error_state_requires_force_to_resume() -> None:
    db = _FakeSession([_state("s1", StrategyStatus.ERROR)])
    manager = _manager(db)

    with pytest.raises(InvalidStrategyStateTransition):
        manager.transition_to_running("s1")

    assert db.states["s1"].status == StrategyStatus.ERROR.value
    assert db.transitions == []


def test_force_resume_from_error_records_recovered_at() -> None:
    db = _FakeSession([_state("s1", StrategyStatus.ERROR)])
    manager = _manager(db)

    manager.transition_to_running("s1", force=True, reason="operator confirmed")

    assert db.states["s1"].status == StrategyStatus.ACTIVE.value
    assert db.states["s1"].recovered_at is not None
    assert manager.is_running("s1") is True
    assert db.transitions[0].to_status == StrategyStatus.ACTIVE.value
    assert db.transitions[0].reason == "operator confirmed"


def test_missing_strategy_raises_key_error() -> None:
    db = _FakeSession([])
    manager = _manager(db)

    with pytest.raises(KeyError):
        manager.transition_to_stopped("missing")


def test_on_state_change_message_updates_cache() -> None:
    db = _FakeSession([])
    manager = _manager(db)

    manager.on_state_change_message(
        {"strategy_id": "s1", "status": StrategyStatus.STOPPED.value}
    )

    assert manager.is_stopped("s1") is True


def test_on_state_change_message_ignores_malformed_message() -> None:
    db = _FakeSession([])
    manager = _manager(db)

    manager.on_state_change_message({"strategy_id": "s1"})

    assert manager.get_status("s1") is None
