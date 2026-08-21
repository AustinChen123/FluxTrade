from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from unittest.mock import MagicMock, call

import pytest
from sqlalchemy.orm import Session

from src.core.strategy_startup_restore import restore_active_strategies


def _state(strategy_id: str) -> MagicMock:
    return MagicMock(strategy_id=strategy_id)


def _db_factory(
    states: list[MagicMock],
) -> tuple[MagicMock, Callable[[], AbstractContextManager[Session]]]:
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = states

    def factory() -> AbstractContextManager[Session]:
        return nullcontext(db)

    return db, factory


def test_loaded_active_strategy_activates_with_exact_startup_contract():
    db, db_factory = _db_factory([_state("alpha")])
    activate = MagicMock()
    transition = MagicMock()

    restore_active_strategies(
        db_session_factory=db_factory,
        is_strategy_loaded=lambda strategy_id: strategy_id == "alpha",
        activate_strategy=activate,
        transition_to_error=transition,
        event_logger=MagicMock(),
    )

    activate.assert_called_once_with(
        "alpha",
        actor="system",
        reason="startup_restore",
        force=True,
    )
    transition.assert_not_called()
    db.query.assert_called_once()


def test_mixed_batch_preserves_order_and_isolates_missing_and_failed_entries():
    db, db_factory = _db_factory(
        [_state("good-a"), _state("missing"), _state("failed"), _state("good-b")]
    )
    events: list[tuple[str, str]] = []
    transition = MagicMock(
        side_effect=lambda strategy_id, _message, **_kwargs: events.append(
            ("error", strategy_id)
        )
    )

    def activate(strategy_id: str, **_kwargs: object) -> None:
        events.append(("activate", strategy_id))
        if strategy_id == "failed":
            raise RuntimeError("activation unavailable")

    restore_active_strategies(
        db_session_factory=db_factory,
        is_strategy_loaded=lambda strategy_id: strategy_id != "missing",
        activate_strategy=activate,
        transition_to_error=transition,
        event_logger=MagicMock(),
    )

    assert events == [
        ("activate", "good-a"),
        ("error", "missing"),
        ("activate", "failed"),
        ("error", "failed"),
        ("activate", "good-b"),
    ]
    assert transition.call_args_list == [
        call("missing", "startup_restore_class_missing", actor="system"),
        call(
            "failed",
            "startup_restore_failed: activation unavailable",
            actor="system",
        ),
    ]
    db.query.assert_called_once()


def test_missing_and_activation_failure_keep_existing_log_envelopes():
    _db, db_factory = _db_factory([_state("missing"), _state("failed")])
    error = RuntimeError("activation unavailable")
    event_logger = MagicMock()

    def activate(_strategy_id: str, **_kwargs: object) -> None:
        raise error

    restore_active_strategies(
        db_session_factory=db_factory,
        is_strategy_loaded=lambda strategy_id: strategy_id == "failed",
        activate_strategy=activate,
        transition_to_error=MagicMock(),
        event_logger=event_logger,
    )

    assert event_logger.error.call_args_list == [
        call(
            "Startup restore: strategy class not loaded for %s — marking ERROR",
            "missing",
        )
    ]
    assert event_logger.exception.call_args_list == [
        call(
            "Startup restore: failed to activate %s — marking ERROR",
            "failed",
        )
    ]


def test_query_failure_propagates_same_exception_before_any_restore():
    error = RuntimeError("database unavailable")
    db = MagicMock()
    db.query.side_effect = error
    activate = MagicMock()
    transition = MagicMock()

    with pytest.raises(RuntimeError) as caught:
        restore_active_strategies(
            db_session_factory=lambda: nullcontext(db),
            is_strategy_loaded=lambda _strategy_id: True,
            activate_strategy=activate,
            transition_to_error=transition,
            event_logger=MagicMock(),
        )

    assert caught.value is error
    activate.assert_not_called()
    transition.assert_not_called()
