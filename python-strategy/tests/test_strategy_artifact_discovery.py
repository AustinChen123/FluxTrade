from contextlib import nullcontext
from typing import cast
from unittest.mock import MagicMock, call

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from src.core.models import StrategyStatus
from src.core.orm_models import Base, Strategy, StrategyState
from src.core.strategy_artifact_discovery import (
    ArtifactLoadResult,
    synchronize_strategy_artifacts,
)
from src.core.strategy_state_manager import StrategyStateManager
from src.strategies.base import BaseStrategy


def _dependencies(found: dict[str, object], states: list[object | None]):
    loader = MagicMock(return_value=found)
    db = MagicMock()
    db.query.return_value.filter.return_value.first.side_effect = states
    transition_to_error = MagicMock()
    event_logger = MagicMock()
    return loader, db, transition_to_error, event_logger


def test_success_returns_only_loaded_classes_and_discovers_new_states():
    class FirstArtifact:
        pass

    class SecondArtifact:
        pass

    loader, db, transition, event_logger = _dependencies(
        {
            "first": FirstArtifact,
            "bad": "load failure",
            "second": SecondArtifact,
        },
        [None, None, None],
    )
    publish_loaded = MagicMock()
    query = db.query.return_value

    def query_after_publication(_model):
        assert publish_loaded.called
        return query

    db.query.side_effect = query_after_publication

    loaded = synchronize_strategy_artifacts(
        artifact_loader=loader,
        publish_loaded_classes=publish_loaded,
        db_session_factory=lambda: nullcontext(db),
        transition_to_error=transition,
        event_logger=event_logger,
    )

    assert loaded == {"first": FirstArtifact, "second": SecondArtifact}
    publish_loaded.assert_called_once_with(loaded)
    loader.assert_called_once_with()
    assert db.add.call_count == 3
    assert db.commit.call_count == 3
    transition.assert_not_called()
    added = [entry.args[0] for entry in db.add.call_args_list]
    assert [entry.strategy_id for entry in added] == ["first", "bad", "second"]
    assert [entry.status for entry in added] == [
        StrategyStatus.DISCOVERED,
        StrategyStatus.ERROR,
        StrategyStatus.DISCOVERED,
    ]
    assert added[1].performance_json == '{"error": "load failure"}'
    assert added[1].last_error_message == "load failure"
    assert added[1].entered_error_at is not None


def test_existing_load_error_commits_before_transitioning_to_error():
    existing = MagicMock(version=7)
    loader, db, transition, event_logger = _dependencies(
        {"bad": "load failure"},
        [existing],
    )
    events = MagicMock()
    events.attach_mock(db.commit, "commit")
    events.attach_mock(transition, "transition")

    loaded = synchronize_strategy_artifacts(
        artifact_loader=loader,
        publish_loaded_classes=MagicMock(),
        db_session_factory=lambda: nullcontext(db),
        transition_to_error=transition,
        event_logger=event_logger,
    )

    assert loaded == {}
    assert events.mock_calls == [
        call.commit(),
        call.transition(
            "bad",
            "load failure",
            actor="system",
            expected_version=7,
        ),
    ]
    db.add.assert_not_called()


def test_each_discovered_artifact_commits_independently():
    class Artifact:
        pass

    existing = MagicMock()
    loader, db, transition, event_logger = _dependencies(
        {"existing": Artifact, "new": Artifact},
        [existing, None],
    )

    synchronize_strategy_artifacts(
        artifact_loader=loader,
        publish_loaded_classes=MagicMock(),
        db_session_factory=lambda: nullcontext(db),
        transition_to_error=transition,
        event_logger=event_logger,
    )

    assert db.commit.call_count == 2
    db.add.assert_called_once()


def test_loaded_classes_publish_before_database_failure():
    class Artifact:
        pass

    loader, db, transition, event_logger = _dependencies(
        {"current": Artifact},
        [],
    )
    db.query.side_effect = RuntimeError("database unavailable")
    publish_loaded = MagicMock()

    with pytest.raises(RuntimeError, match="database unavailable"):
        synchronize_strategy_artifacts(
            artifact_loader=loader,
            publish_loaded_classes=publish_loaded,
            db_session_factory=lambda: nullcontext(db),
            transition_to_error=transition,
            event_logger=event_logger,
        )

    publish_loaded.assert_called_once_with({"current": Artifact})
    db.commit.assert_not_called()


def test_new_artifact_creates_strategy_parent_before_lifecycle_transition(
    tmp_path,
    request: pytest.FixtureRequest,
):
    class Artifact:
        pass

    found: dict[str, ArtifactLoadResult] = {
        "fresh-artifact": cast(type[BaseStrategy], Artifact)
    }

    engine = create_engine(f"sqlite:///{tmp_path / 'artifact-state.db'}")
    request.addfinalizer(engine.dispose)

    @event.listens_for(engine, "connect")
    def enable_foreign_keys(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)

    synchronize_strategy_artifacts(
        artifact_loader=lambda: found,
        publish_loaded_classes=lambda _loaded: None,
        db_session_factory=lambda: session_factory(),
        transition_to_error=MagicMock(),
        event_logger=MagicMock(),
    )

    manager = StrategyStateManager(lambda: session_factory(), MagicMock())
    manager.transition_to_error("fresh-artifact", "warmup unavailable")

    with session_factory() as db:
        assert db.get(Strategy, "fresh-artifact") is not None
        state = db.get(StrategyState, "fresh-artifact")
        assert state is not None
        assert state.status == StrategyStatus.ERROR
