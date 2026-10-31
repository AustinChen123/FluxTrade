from contextlib import nullcontext
import json
from typing import cast
from unittest.mock import MagicMock, call

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from src.core.models import StrategyStatus
from src.core.orm_models import Base, Strategy, StrategyState, StrategyStateTransition
from src.core.strategy_artifact_discovery import (
    ArtifactLoadResult,
    synchronize_strategy_artifacts,
)
from src.core.strategy_state_manager import StrategyStateManager
from src.strategies.base import BaseStrategy


def _dependencies(found: dict[str, object], states: list[object | None]):
    loader = MagicMock(return_value=found)
    db = MagicMock()
    db.get.return_value = Strategy(
        id="existing",
        name="Existing",
        configuration_json="{}",
    )
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


@pytest.mark.parametrize("declaration", ["NONE", "SL", "TP", "SL+TP"])
def test_catalog_declaration_updates_existing_strategy_configuration(declaration):
    class Artifact:
        __fluxtrade_system_placed_protection__ = declaration
        __fluxtrade_catalog_sha256__ = "catalog-revision"

    existing = Strategy(
        id="existing",
        name="Existing",
        configuration_json='{"risk_limit":"kept"}',
    )
    loader, db, transition, event_logger = _dependencies({"existing": Artifact}, [None])
    db.get.return_value = existing

    synchronize_strategy_artifacts(
        artifact_loader=loader,
        publish_loaded_classes=MagicMock(),
        db_session_factory=lambda: nullcontext(db),
        transition_to_error=transition,
        event_logger=event_logger,
    )

    assert json.loads(cast(str, existing.configuration_json)) == {
        "risk_limit": "kept",
        "system_placed_protection": declaration,
        "system_placed_protection_revision": "catalog-revision",
    }
    db.commit.assert_called_once()


def test_undeclared_artifact_preserves_unparseable_existing_configuration():
    class Artifact:
        pass

    existing = Strategy(
        id="existing",
        name="Existing",
        configuration_json="not-json",
    )
    loader, db, transition, event_logger = _dependencies(
        {"existing": Artifact}, [None]
    )
    db.get.return_value = existing

    synchronize_strategy_artifacts(
        artifact_loader=loader,
        publish_loaded_classes=MagicMock(),
        db_session_factory=lambda: nullcontext(db),
        transition_to_error=transition,
        event_logger=event_logger,
    )

    assert cast(str, existing.configuration_json) == "not-json"
    db.commit.assert_called_once()
    db.rollback.assert_not_called()


def test_invalid_existing_configuration_skips_only_declared_artifact():
    class Artifact:
        __fluxtrade_system_placed_protection__ = "SL"
        __fluxtrade_catalog_sha256__ = "catalog-revision"

    class OtherArtifact:
        __fluxtrade_system_placed_protection__ = "TP"
        __fluxtrade_catalog_sha256__ = "other-revision"

    existing = Strategy(
        id="existing",
        name="Existing",
        configuration_json="not-json",
    )
    other = Strategy(
        id="other",
        name="Other",
        configuration_json="{}",
    )
    loader, db, transition, event_logger = _dependencies(
        {"existing": Artifact, "other": OtherArtifact}, [None, None]
    )
    db.get.return_value = existing
    db.get.side_effect = [existing, other]

    synchronize_strategy_artifacts(
        artifact_loader=loader,
        publish_loaded_classes=MagicMock(),
        db_session_factory=lambda: nullcontext(db),
        transition_to_error=transition,
        event_logger=event_logger,
    )

    assert cast(str, existing.configuration_json) == "not-json"
    assert json.loads(cast(str, other.configuration_json)) == {
        "system_placed_protection": "TP",
        "system_placed_protection_revision": "other-revision",
    }
    event_logger.warning.assert_called_once()
    assert "Skipping strategy artifact %s during catalog sync: %s" in str(
        event_logger.warning.call_args.args[0]
    )
    assert db.rollback.call_count == 1
    assert db.commit.call_count == 1


def test_catalog_commit_failure_rolls_back_declaration_and_preserves_existing_keys(
    tmp_path,
):
    class Artifact:
        __fluxtrade_system_placed_protection__ = "SL+TP"
        __fluxtrade_catalog_sha256__ = "new-revision"

    engine = create_engine(f"sqlite:///{tmp_path / 'catalog-commit-failure.db'}")
    Base.metadata.create_all(engine, tables=[Strategy.__table__, StrategyState.__table__])
    session_factory = sessionmaker(bind=engine)
    with session_factory() as db:
        db.add(
            Strategy(
                id="existing",
                name="Existing",
                configuration_json='{"risk_limit":7,"preserve":"yes"}',
            )
        )
        db.commit()

    def fail_catalog_commit(_session):
        raise RuntimeError("catalog transaction commit failed")

    event.listen(session_factory, "before_commit", fail_catalog_commit)
    try:
        def load_artifact() -> dict[str, ArtifactLoadResult]:
            return {"existing": cast(type[BaseStrategy], Artifact)}

        with pytest.raises(RuntimeError, match="catalog transaction commit failed"):
            synchronize_strategy_artifacts(
                artifact_loader=load_artifact,
                publish_loaded_classes=MagicMock(),
                db_session_factory=session_factory,
                transition_to_error=MagicMock(),
                event_logger=MagicMock(),
            )
    finally:
        event.remove(session_factory, "before_commit", fail_catalog_commit)

    with session_factory() as db:
        persisted = db.get(Strategy, "existing")
        assert persisted is not None
        assert json.loads(cast(str, persisted.configuration_json)) == {
            "risk_limit": 7,
            "preserve": "yes",
        }
    engine.dispose()


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
        __fluxtrade_system_placed_protection__ = "TP"
        __fluxtrade_catalog_sha256__ = "catalog-revision"

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

    Base.metadata.create_all(
        engine, tables=[Strategy.__table__, StrategyState.__table__, StrategyStateTransition.__table__]
    )
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
        strategy = db.get(Strategy, "fresh-artifact")
        assert strategy is not None
        assert json.loads(cast(str, strategy.configuration_json)) == {
            "system_placed_protection": "TP",
            "system_placed_protection_revision": "catalog-revision",
        }
        state = db.get(StrategyState, "fresh-artifact")
        assert state is not None
        assert state.status == StrategyStatus.ERROR
