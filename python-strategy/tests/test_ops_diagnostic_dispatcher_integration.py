from __future__ import annotations

import multiprocessing
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, cast
from unittest.mock import MagicMock

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from src.core.ops_diagnostic_dispatcher import (
    OpsDiagnosticDispatcher,
    get_process_ops_diagnostic_dispatcher,
)
from src.core.orm_models import Base, SystemEvent
from src.core.safe_diagnostics import SafeDiagnostic
from src.core.adapters.rithmic_external_order_drift import (
    RithmicExternalOrderDriftService,
)
from src.core.ops_safety import OpsSafetyService
from src.core.adapters.rithmic_order_event_stream import (
    RithmicOrderEventStreamService,
)


def _write_from_fresh_dispatcher_process(db_path: str, label: str, result: Any) -> None:
    db_engine = create_engine(f"sqlite:///{db_path}")
    sessions = sessionmaker(bind=db_engine)
    owner = get_process_ops_diagnostic_dispatcher(session_factory=sessions)
    assert get_process_ops_diagnostic_dispatcher(session_factory=sessions) is owner
    value = SafeDiagnostic.create(
        {
            "source_stage": "order_event",
            "stable_error_code": "rithmic_order_event_failed",
            "safe_cause": "unclassified",
            "retryable": False,
            "template_id": 352,
        }
    )
    assert value is not None
    assert owner.submit(value)
    deadline = time.monotonic() + 30
    while owner.counters["committed"] < 1 and time.monotonic() < deadline:
        time.sleep(0.005)
    owner_state = owner.counters
    owner.stop()
    db_engine.dispose()
    if owner_state["committed"] != 1 or owner_state["dropped"] != 0:
        raise AssertionError(f"fresh process dispatcher did not settle: {owner_state}")
    result.put(label)


def _block_dispatcher_process(db_path: str, result: Any) -> None:
    db_engine = create_engine(f"sqlite:///{db_path}")
    sessions = sessionmaker(bind=db_engine)
    entered = threading.Event()

    @contextmanager
    def blocked_sessions() -> Iterator[Session]:
        entered.set()
        assert threading.Event().wait(30)
        with sessions() as session:
            yield session

    owner = get_process_ops_diagnostic_dispatcher(session_factory=blocked_sessions)
    first = SafeDiagnostic.create(
        {
            "source_stage": "order_event",
            "stable_error_code": "rithmic_order_event_failed",
            "safe_cause": "unclassified",
            "retryable": False,
            "template_id": 352,
        }
    )
    assert first is not None
    assert owner.submit(first)
    assert entered.wait(30)
    assert owner.submit(first)
    result.put(owner.counters)
    threading.Event().wait(30)


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


def test_python_dispatcher_owner_restarts_in_a_fresh_os_process(tmp_path: Path) -> None:
    db_path = str(tmp_path / "python-owner-restart.db")
    db_engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(db_engine, tables=[SystemEvent.__table__])
    db_engine.dispose()
    context = multiprocessing.get_context("spawn")
    result = context.Queue()
    for label in ("first", "restarted"):
        child = context.Process(
            target=_write_from_fresh_dispatcher_process,
            args=(db_path, label, result),
        )
        child.start()
        child.join(30)
        assert not child.is_alive(), f"{label} dispatcher process did not exit"
        assert child.exitcode == 0
        assert result.get(timeout=30) == label

    # A process crash can lose an in-flight and a queued optional diagnostic.
    # It does not replay committed rows; a fresh process owns a new dispatcher.
    crashing = context.Process(
        target=_block_dispatcher_process,
        args=(db_path, result),
    )
    crashing.start()
    crash_state = result.get(timeout=30)
    assert crash_state["accepted"] == 2
    assert crash_state["queued"] == 1
    crashing.terminate()
    crashing.join(30)
    assert not crashing.is_alive()

    restarted = context.Process(
        target=_write_from_fresh_dispatcher_process,
        args=(db_path, "after-crash", result),
    )
    restarted.start()
    restarted.join(30)
    assert not restarted.is_alive()
    assert restarted.exitcode == 0
    assert result.get(timeout=30) == "after-crash"

    db_engine = create_engine(f"sqlite:///{db_path}")
    with db_engine.connect() as connection:
        payloads = list(
            connection.execute(select(SystemEvent.payload).order_by(SystemEvent.id))
        )
    db_engine.dispose()
    assert len(payloads) == 3
    assert all(row.payload["safe_diagnostic"]["template_id"] == 352 for row in payloads)


def test_real_stream_restart_and_drift_safety_survive_blocked_dispatcher(
    tmp_path: Path,
) -> None:
    db_engine = create_engine(f"sqlite:///{tmp_path / 'f4-diagnostics.db'}")
    Base.metadata.create_all(db_engine, tables=[SystemEvent.__table__])
    sessions = sessionmaker(bind=db_engine)
    open_entered = threading.Event()
    release_open = threading.Event()
    release_open_timed_out = threading.Event()

    @contextmanager
    def blocked_then_real_sessions() -> Iterator[Session]:
        if not open_entered.is_set():
            open_entered.set()
            if not release_open.wait(120):
                release_open_timed_out.set()
                raise TimeoutError("blocked diagnostic session release timed out")
        with sessions() as session:
            yield session

    dispatcher = OpsDiagnosticDispatcher(
        session_factory=blocked_then_real_sessions, queue_capacity=2
    )
    execution = MagicMock()
    execution.order_manager.repo.list_orders_by_statuses.return_value = []
    execution.adapter.get_all_positions.return_value = []
    execution.resume_submissions = MagicMock()
    account = MagicMock()
    account.get_all_positions.return_value = []
    ops_safety = OpsSafetyService(execution, account, sessions)
    local_halt = {"active": False}
    redis_state = {"state": "OK"}
    logger = MagicMock()

    def persist_lockdown(reason: str) -> None:
        ops_safety.persist_kill_switch_state(
            "LOCKDOWN", actor="rithmic_order_stream", reason=reason
        )

    drift = RithmicExternalOrderDriftService(
        halt_submissions=lambda: local_halt.__setitem__("active", True),
        clear_local_halt=lambda: local_halt.__setitem__("active", False),
        persist_lockdown_state=persist_lockdown,
        persist_redis_lockdown=lambda: redis_state.__setitem__("state", "LOCKDOWN"),
        assert_runtime_leadership=lambda: None,
        resume_after_reconcile=lambda: None,
        logger=logger,
        diagnostic_sink=dispatcher.submit,
    )

    class FailingAdapter:
        def start_order_event_stream(self) -> None:
            return None

        def poll_order_event(self) -> None:
            error = RuntimeError("terminal stream failure with ACCOUNT_SECRET")
            setattr(
                error,
                "safe_diagnostic",
                {
                    "schema_version": 1,
                    "source_stage": "order_event",
                    "stable_error_code": "rithmic_order_event_failed",
                    "retryable": False,
                },
            )
            raise error

    stream_workers: list[threading.Thread] = []

    def start_terminal_stream() -> RithmicOrderEventStreamService:
        service = RithmicOrderEventStreamService(
            adapter=FailingAdapter(),
            stop_event=threading.Event(),
            is_running=lambda: True,
            publish_worker=stream_workers.append,
            reconcile_if_needed=lambda: True,
            process_event=lambda _event: {"action": "applied"},
            lockdown=drift.detect,
            assert_runtime_leadership=lambda: None,
            halt_submissions=lambda: local_halt.__setitem__("active", True),
            on_runtime_started=lambda: True,
            logger=logger,
            diagnostic_dispatcher=dispatcher,
        )
        assert service.start()
        stream_workers[-1].join(30)
        assert not stream_workers[-1].is_alive()
        return service

    start_terminal_stream()
    assert open_entered.wait(30)
    # ORDER stream restart reuses the same process dispatcher while its DB
    # session factory is still blocked on the first diagnostic.
    start_terminal_stream()
    drift.detect(
        "rithmic_external_order_detected: account_id=ACCOUNT_SECRET "
        "client_order_id=ORDER_SECRET exchange_order_id=EXCHANGE_SECRET"
    )

    assert local_halt["active"] is True
    assert redis_state["state"] == "LOCKDOWN"
    assert ops_safety.latest_kill_switch_state() == "LOCKDOWN"
    assert dispatcher.counters["accepted"] == 3
    assert len(stream_workers) == 2
    assert "ACCOUNT_SECRET" not in repr(logger.error.call_args_list)
    assert "diagnostic_details_redacted" in repr(logger.error.call_args_list)

    clear_result = ops_safety.clear_kill_switch(
        persist_clear=lambda: ops_safety.persist_kill_switch_state(
            "OK", actor="operator", reason="verified_flat"
        )
    )
    assert clear_result == {"cleared": True, "reason": None}
    assert ops_safety.latest_kill_switch_state() == "OK"
    release_open.set()
    deadline = time.monotonic() + 30
    while dispatcher.counters["committed"] < 3 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert not release_open_timed_out.is_set(), "dispatcher session gate timed out"
    assert dispatcher.counters["committed"] == 3
    assert ops_safety.latest_kill_switch_state() == "OK"

    with sessions() as session:
        events = list(
            session.execute(
                select(SystemEvent).order_by(SystemEvent.id)
            ).scalars()
        )
    lockdown = next(
        row for row in events
        if getattr(row, "event_subtype") == "kill_switch_state"
        and cast(dict[str, object], row.payload).get("state") == "LOCKDOWN"
    )
    lockdown_payload = cast(dict[str, object], lockdown.payload)
    assert lockdown_payload["reason"] == (
        "rithmic_external_order_detected: diagnostic_details_redacted"
    )
    assert "ACCOUNT_SECRET" not in repr(lockdown_payload)
    assert "ORDER_SECRET" not in repr(lockdown_payload)
    assert "EXCHANGE_SECRET" not in repr(lockdown_payload)
    assert sum(
        getattr(row, "event_subtype") == "rithmic_diagnostic" for row in events
    ) == 3
    assert all(
        getattr(row, "event_subtype") != "kill_switch_state"
        for row in events
        if getattr(row, "event_subtype") == "rithmic_diagnostic"
    )
    dispatcher.stop()
    db_engine.dispose()
