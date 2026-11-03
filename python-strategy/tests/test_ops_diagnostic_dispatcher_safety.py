from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import ContextManager, cast
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session

from src.core.ops_diagnostic_dispatcher import (
    OpsDiagnosticDispatcher,
)
from src.core.orm_models import Base, SystemEvent
from src.core.safe_diagnostics import SafeDiagnostic, safe_lockdown_reason
from src.core.adapters.rithmic_external_order_drift import (
    RithmicExternalOrderDriftService,
)
from src.core.ops_safety import OpsSafetyService


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

def test_dispatcher_startup_failure_cannot_suppress_real_lockdown(
    tmp_path: Path,
) -> None:
    db_engine = create_engine(f"sqlite:///{tmp_path / 'f4-startup-failure.db'}")
    Base.metadata.create_all(db_engine, tables=[SystemEvent.__table__])
    sessions = sessionmaker(bind=db_engine)

    def failed_thread(**_kwargs: object) -> MagicMock:
        worker = MagicMock()
        worker.start.side_effect = RuntimeError("startup failure")
        return worker

    dispatcher = OpsDiagnosticDispatcher(
        session_factory=sessions, thread_factory=failed_thread
    )
    execution = MagicMock()
    execution.order_manager.repo.list_orders_by_statuses.return_value = []
    execution.adapter.get_all_positions.return_value = []
    account = MagicMock()
    account.get_all_positions.return_value = []
    safety = OpsSafetyService(execution, account, sessions)
    halted = threading.Event()
    logger = MagicMock()
    drift = RithmicExternalOrderDriftService(
        halt_submissions=halted.set,
        clear_local_halt=MagicMock(),
        persist_lockdown_state=lambda reason: safety.persist_kill_switch_state(
            "LOCKDOWN", actor="rithmic_order_stream", reason=reason
        ),
        persist_redis_lockdown=MagicMock(),
        assert_runtime_leadership=MagicMock(),
        resume_after_reconcile=MagicMock(),
        logger=logger,
        diagnostic_sink=dispatcher.submit,
    )

    drift.detect(
        "rithmic_external_order_detected: account_id=ACCOUNT_SECRET "
        "client_order_id=ORDER_SECRET"
    )

    assert halted.is_set()
    assert safety.latest_kill_switch_state() == "LOCKDOWN"
    assert dispatcher.counters["available"] is False
    assert "ACCOUNT_SECRET" not in repr(logger.error.call_args)
    assert "diagnostic_details_redacted" in repr(logger.error.call_args)
    with sessions() as session:
        persisted = session.execute(select(SystemEvent)).scalar_one()
    persisted_payload = cast(dict[str, object], persisted.payload)
    assert persisted_payload["reason"] == (
        "rithmic_external_order_detected: diagnostic_details_redacted"
    )
    assert "ORDER_SECRET" not in repr(persisted_payload)
    db_engine.dispose()


def test_driver_rollback_and_session_close_failures_are_counted_without_retry_owner() -> None:
    class BrokenSession:
        def get_bind(self) -> SimpleNamespace:
            return SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

        def add(self, _event: SystemEvent) -> None:
            pass

        def commit(self) -> None:
            raise RuntimeError("driver commit failure")

        def rollback(self) -> None:
            raise RuntimeError("driver rollback failure")

    @contextmanager
    def broken_factory() -> Iterator[BrokenSession]:
        try:
            yield BrokenSession()
        finally:
            raise RuntimeError("session close failure")

    dispatcher = OpsDiagnosticDispatcher(
        session_factory=cast(Callable[[], ContextManager[Session]], broken_factory),
        health_interval_seconds=60,
    )
    assert dispatcher.submit(_diagnostic())
    deadline = time.monotonic() + 2
    while dispatcher.counters["dropped"] != 1 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert dispatcher.counters["accepted"] == 1
    assert dispatcher.counters["committed"] == 0
    assert dispatcher.counters["dropped"] == 1
    assert dispatcher.counters["available"] is True
    dispatcher.stop()


def test_drift_halts_before_safe_diagnostic_and_redacts_reason() -> None:
    calls: list[str] = []
    logger = MagicMock()
    sink = MagicMock(side_effect=lambda _value: calls.append("diagnostic"))
    persisted = MagicMock(side_effect=lambda _reason: calls.append("persist"))
    service = RithmicExternalOrderDriftService(
        halt_submissions=lambda: calls.append("halt"),
        clear_local_halt=MagicMock(),
        persist_lockdown_state=persisted,
        persist_redis_lockdown=MagicMock(),
        assert_runtime_leadership=MagicMock(),
        resume_after_reconcile=MagicMock(),
        logger=logger,
        diagnostic_sink=sink,
    )
    reason = (
        "rithmic_external_order_detected: account_id=ACCOUNT_SECRET "
        "client_order_id=ORDER_SECRET exchange_order_id=EXCHANGE_SECRET"
    )

    service.detect(reason)

    assert calls == ["halt", "persist", "diagnostic"]
    assert "ACCOUNT_SECRET" not in repr(logger.error.call_args)
    assert "ORDER_SECRET" not in repr(logger.error.call_args)
    assert "EXCHANGE_SECRET" not in repr(persisted.call_args)
    assert persisted.call_args.args[0] == safe_lockdown_reason(reason)
    sink.assert_called_once()


def test_contended_diagnostic_admission_cannot_delay_both_lockdown_writes() -> None:
    calls: list[str] = []
    dispatcher = OpsDiagnosticDispatcher(session_factory=MagicMock())
    dispatcher._lock.acquire()
    try:
        service = RithmicExternalOrderDriftService(
            halt_submissions=lambda: calls.append("halt"),
            clear_local_halt=MagicMock(),
            persist_lockdown_state=lambda _reason: calls.append("durable"),
            persist_redis_lockdown=lambda: calls.append("redis"),
            assert_runtime_leadership=MagicMock(),
            resume_after_reconcile=MagicMock(),
            logger=MagicMock(),
            diagnostic_sink=dispatcher.submit,
        )
        service.detect("rithmic_external_order_detected: ACCOUNT_SECRET")
        assert calls == ["halt", "durable", "redis"]
    finally:
        dispatcher._lock.release()
        dispatcher.stop()


def test_ops_safety_persists_redacted_rithmic_kill_switch_reason() -> None:
    session = MagicMock()

    @contextmanager
    def sessions() -> Iterator[MagicMock]:
        yield session

    service = OpsSafetyService(MagicMock(), MagicMock(), sessions)
    with patch("src.core.ops_safety.write_system_event") as write:
        service.persist_kill_switch_state(
            "LOCKDOWN",
            actor="rithmic_order_stream",
            reason=(
                "rithmic_external_order_detected: "
                "account_id=ACCOUNT_SECRET client_order_id=ORDER_SECRET"
            ),
        )

    payload = write.call_args.kwargs["payload"]
    assert payload["reason"] == (
        "rithmic_external_order_detected: diagnostic_details_redacted"
    )
    assert "ACCOUNT_SECRET" not in repr(payload)
    assert "ORDER_SECRET" not in repr(payload)


def test_dead_worker_is_not_replaced_and_producers_fail_fast() -> None:
    entered = threading.Event()
    release = threading.Event()

    @contextmanager
    def dying_sessions() -> Iterator[MagicMock]:
        entered.set()
        assert release.wait(1)
        raise SystemExit("worker exits unexpectedly")
        yield MagicMock()

    worker_refs: list[threading.Thread] = []

    def capture_worker(**kwargs: object) -> threading.Thread:
        worker = threading.Thread(**kwargs)  # type: ignore[arg-type]
        worker_refs.append(worker)
        return worker

    original_excepthook = threading.excepthook
    threading.excepthook = lambda _args: None
    try:
        dispatcher = OpsDiagnosticDispatcher(
            session_factory=dying_sessions,
            queue_capacity=1,
            thread_factory=capture_worker,
        )
        assert dispatcher.submit(_diagnostic())
        assert entered.wait(1)
        assert dispatcher.submit(_diagnostic())
        release.set()
        worker_refs[0].join(1)
        assert not worker_refs[0].is_alive()
        counters = dispatcher.counters
        assert counters["available"] is False
        assert counters["reporter_available"] is True
        assert counters["accepted"] == 2
        assert counters["dropped"] == 2
        assert counters["committed"] == 0
        assert counters["queued"] == 0
        assert len(worker_refs) == 1
        dispatcher.stop()
        assert dispatcher.counters["dropped"] == 2
    finally:
        threading.excepthook = original_excepthook


