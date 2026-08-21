import json
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.models import StrategyStatus
from src.core.strategy_test_run_service import StrategyTestRunService
from src.strategies.base import BaseStrategy


def _service(*, state, events):
    database = MagicMock()
    database.query.return_value.filter.return_value.first.return_value = state
    database.commit.side_effect = lambda: events.append("commit")
    state_manager = MagicMock()
    state_manager.transition_to_status.side_effect = (
        lambda *_args, **_kwargs: events.append("transition_status")
    )
    state_manager.transition_to_error.side_effect = (
        lambda *_args, **_kwargs: events.append("transition_error")
    )
    return (
        StrategyTestRunService(
            db_session_factory=lambda: nullcontext(database),
            state_manager=state_manager,
            event_logger=MagicMock(),
        ),
        database,
        state_manager,
    )


def _artifact(product_id, timeframe, lookback_window):
    return SimpleNamespace(
        requirements=SimpleNamespace(
            product_id=product_id,
            timeframe=timeframe,
            lookback_window=lookback_window,
        )
    )


def test_missing_artifact_and_state_are_noops() -> None:
    events = []
    state = MagicMock(config_json="{}", version=3)
    service, database, state_manager = _service(state=state, events=events)

    service.run(
        "missing-artifact",
        artifact_cls=None,
        resolve_product_id=MagicMock(),
        build_artifact_instances=MagicMock(),
        check_data_availability=MagicMock(),
    )
    database.query.return_value.filter.return_value.first.return_value = None
    service.run(
        "missing-state",
        artifact_cls=BaseStrategy,
        resolve_product_id=MagicMock(),
        build_artifact_instances=MagicMock(),
        check_data_availability=MagicMock(),
    )

    assert events == []
    database.commit.assert_not_called()
    state_manager.transition_to_status.assert_not_called()
    state_manager.transition_to_error.assert_not_called()


def test_ready_checks_all_artifacts_in_order_and_commits_before_transition() -> None:
    events = []
    state = MagicMock(
        config_json='{"product_id":"TEST:PRODUCT"}',
        version=7,
    )
    service, _database, state_manager = _service(state=state, events=events)
    first = _artifact("TEST:PRODUCT", "1m", 30)
    second = _artifact("TEST:PRODUCT", "5m", 60)
    availability = MagicMock(
        side_effect=lambda _db, product, timeframe, lookback: (
            events.append((product, timeframe, lookback)) or (True, "")
        )
    )

    service.run(
        "portfolio",
        artifact_cls=BaseStrategy,
        resolve_product_id=MagicMock(return_value="TEST:PRODUCT"),
        build_artifact_instances=MagicMock(return_value=(first, second)),
        check_data_availability=availability,
    )

    assert events == [
        ("TEST:PRODUCT", "1m", 30),
        ("TEST:PRODUCT", "5m", 60),
        "commit",
        "transition_status",
    ]
    state_manager.transition_to_status.assert_called_once_with(
        "portfolio",
        StrategyStatus.READY,
        actor="system",
        reason="test_run_completed",
        expected_version=7,
    )


def test_first_insufficient_artifact_stops_and_persists_backfill_command() -> None:
    events = []
    state = MagicMock(config_json='{"product_id":"product"}', version=4)
    service, _database, state_manager = _service(state=state, events=events)
    artifacts = (
        _artifact("product", "1m", 10),
        _artifact("product", "5m", 20),
        _artifact("product", "15m", 30),
    )
    availability = MagicMock(side_effect=[(True, ""), (False, "backfill-now")])

    service.run(
        "portfolio",
        artifact_cls=BaseStrategy,
        resolve_product_id=MagicMock(return_value="product"),
        build_artifact_instances=MagicMock(return_value=artifacts),
        check_data_availability=availability,
    )

    assert availability.call_count == 2
    assert json.loads(state.performance_json) == {"backfill_command": "backfill-now"}
    assert events == ["commit", "transition_status"]
    state_manager.transition_to_status.assert_called_once_with(
        "portfolio",
        StrategyStatus.WARNING,
        actor="system",
        reason="insufficient_data",
        expected_version=4,
    )


def test_evaluation_error_persists_trace_before_error_transition() -> None:
    events = []
    state = MagicMock(config_json='{"product_id":"product"}', version=9)
    service, _database, state_manager = _service(state=state, events=events)
    failure = RuntimeError("availability failed")

    service.run(
        "strategy",
        artifact_cls=BaseStrategy,
        resolve_product_id=MagicMock(return_value="product"),
        build_artifact_instances=MagicMock(
            return_value=(_artifact("product", "1m", 10),)
        ),
        check_data_availability=MagicMock(side_effect=failure),
    )

    persisted_trace = json.loads(state.performance_json)["error"]
    assert persisted_trace.splitlines()[-1] == "RuntimeError: availability failed"
    assert events == ["commit", "transition_error"]
    error_call = state_manager.transition_to_error.call_args
    assert error_call.args == ("strategy", persisted_trace)
    assert error_call.kwargs == {"actor": "system", "expected_version": 9}


@pytest.mark.parametrize("invalid_config", ["[]", "null"])
def test_non_mapping_config_uses_existing_error_disposition(invalid_config) -> None:
    events = []
    state = MagicMock(config_json=invalid_config, version=2)
    service, _database, state_manager = _service(state=state, events=events)

    service.run(
        "strategy",
        artifact_cls=BaseStrategy,
        resolve_product_id=lambda config: config["product_id"],
        build_artifact_instances=MagicMock(),
        check_data_availability=MagicMock(),
    )

    assert events == ["commit", "transition_error"]
    state_manager.transition_to_error.assert_called_once()
