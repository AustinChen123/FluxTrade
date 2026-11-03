from __future__ import annotations

import time
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, cast
from unittest.mock import MagicMock

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from src.core.ops_diagnostic_dispatcher import OpsDiagnosticDispatcher
from src.core.orm_models import Base, SystemEvent
from src.core.ops_safety import OpsSafetyService
from src.core.safe_diagnostics import SafeDiagnostic


def _diagnostic() -> SafeDiagnostic:
    value = SafeDiagnostic.create(
        {
            "source_stage": "order_event",
            "stable_error_code": "rithmic_order_event_failed",
            "retryable": False,
            "template_id": 352,
            "raw_bits": 4607182418800017408,
        }
    )
    assert value is not None
    return value


def test_blocked_diagnostic_commit_does_not_gate_safety_or_clear(
    tmp_path: Path,
) -> None:
    db_engine = create_engine(f"sqlite:///{tmp_path / 'f4-commit-gate.db'}")
    Base.metadata.create_all(db_engine, tables=[SystemEvent.__table__])
    regular_sessions = sessionmaker(bind=db_engine)
    commit_entered = threading.Event()
    release_commit = threading.Event()

    @contextmanager
    def blocked_commit_sessions() -> Iterator[Session]:
        with regular_sessions() as session:
            commit = session.commit

            def blocked_commit() -> None:
                commit_entered.set()
                assert release_commit.wait(3)
                commit()

            session.commit = blocked_commit  # type: ignore[method-assign]
            yield session

    dispatcher = OpsDiagnosticDispatcher(
        session_factory=blocked_commit_sessions, queue_capacity=2
    )
    assert dispatcher.submit(_diagnostic())
    assert commit_entered.wait(1)

    execution = MagicMock()
    execution.order_manager.repo.list_orders_by_statuses.return_value = []
    execution.adapter.get_all_positions.return_value = []
    account = MagicMock()
    account.get_all_positions.return_value = []
    safety = OpsSafetyService(execution, account, regular_sessions)
    safety.persist_kill_switch_state(
        "LOCKDOWN",
        actor="rithmic_order_stream",
        reason="rithmic_external_order_detected: account_id=ACCOUNT_SECRET",
    )
    assert safety.latest_kill_switch_state() == "LOCKDOWN"
    clear = safety.clear_kill_switch(
        persist_clear=lambda: safety.persist_kill_switch_state(
            "OK", actor="operator", reason="verified_flat"
        )
    )
    assert clear["cleared"] is True
    assert safety.latest_kill_switch_state() == "OK"

    stopped_at = time.monotonic()
    dispatcher.stop()
    assert time.monotonic() - stopped_at < 0.1
    release_commit.set()
    deadline = time.monotonic() + 2
    while dispatcher.counters["committed"] < 1 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert dispatcher.counters["committed"] == 1
    assert safety.latest_kill_switch_state() == "OK"
    with regular_sessions() as session:
        rows = list(session.execute(select(SystemEvent)).scalars())
    assert any(getattr(row, "event_subtype") == "rithmic_diagnostic" for row in rows)
    assert [
        cast(dict[str, object], row.payload)["state"]
        for row in rows
        if getattr(row, "event_subtype") == "kill_switch_state"
    ] == ["LOCKDOWN", "OK"]
    assert all(
        "ACCOUNT_SECRET" not in repr(cast(dict[str, object], row.payload))
        for row in rows
        if getattr(row, "event_subtype") == "kill_switch_state"
    )
    dispatcher.stop()
    db_engine.dispose()
