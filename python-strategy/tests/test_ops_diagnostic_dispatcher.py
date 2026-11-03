from __future__ import annotations

import threading
import time
import logging
from contextlib import contextmanager
from typing import Iterator
from unittest.mock import MagicMock

import pytest

from src.core.ops_diagnostic_dispatcher import (
    OpsDiagnosticDispatcher,
)
from src.core import ops_diagnostic_dispatcher as dispatcher_module
from src.core.safe_diagnostics import SafeDiagnostic, safe_lockdown_reason


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


def test_safe_diagnostic_rejects_untyped_text_and_unknown_fields() -> None:
    assert SafeDiagnostic.create({"exception": "account-secret-123"}) is None
    assert SafeDiagnostic.create({"stable_error_code": "account-secret-123"}) is None
    assert SafeDiagnostic.create({"retryable": 1}) is None
    assert SafeDiagnostic.create({"template_id": float("nan")}) is None
    assert SafeDiagnostic.create(
        {"source_stage": "not_a_supported_stage", "retryable": False}
    ) is None
    assert SafeDiagnostic.create(
        {"source_stage": "order_event", "field": "total_unfilled_size"}
    ) is not None
    assert safe_lockdown_reason("rithmic_runtime_reconciliation_unresolved") == (
        "rithmic_runtime_reconciliation_unresolved: diagnostic_details_redacted"
    )
    for reason in (
        "rithmic_runtime_reconciliation_drain_timeout",
        "rithmic_runtime_reconciliation_failed",
        "rithmic_runtime_reconciliation_stream_disconnected",
        "rithmic_runtime_reconciliation_stream_restart_failed",
        "rithmic_runtime_reconciliation_stream_stop_timeout",
        "rithmic_emergency_flatten_event_stream_stop_timeout",
        "rithmic_portfolio_exit_order_stream_restart_failed",
        "rithmic_strategy_exit_preflight_reconciliation_blocked",
    ):
        assert safe_lockdown_reason(reason) == (
            f"{reason}: diagnostic_details_redacted"
        )


def test_dispatcher_commits_only_worker_owned_session() -> None:
    committed = threading.Event()
    session = MagicMock()
    session.commit.side_effect = committed.set

    @contextmanager
    def sessions() -> Iterator[MagicMock]:
        yield session

    dispatcher = OpsDiagnosticDispatcher(session_factory=sessions)
    assert dispatcher.submit(_diagnostic())
    assert committed.wait(2)
    deadline = time.monotonic() + 2
    while dispatcher.counters["committed"] != 1 and time.monotonic() < deadline:
        time.sleep(0.005)
    session.commit.assert_called_once()
    assert dispatcher.counters["committed"] == 1
    dispatcher.stop()


def test_blocked_db_does_not_block_producer_and_queue_stays_bounded(caplog) -> None:
    caplog.set_level(logging.INFO)
    entered = threading.Event()
    release = threading.Event()

    @contextmanager
    def blocked_sessions() -> Iterator[MagicMock]:
        entered.set()
        release.wait(5)
        yield MagicMock()

    dispatcher = OpsDiagnosticDispatcher(
        session_factory=blocked_sessions,
        queue_capacity=1,
        health_interval_seconds=0.01,
    )
    start = time.monotonic()
    assert dispatcher.submit(_diagnostic())
    assert time.monotonic() - start < 0.1
    assert entered.wait(1)
    deadline = time.monotonic() + 1
    while "Ops diagnostic dispatcher health" not in caplog.text and time.monotonic() < deadline:
        time.sleep(0.005)
    assert "Ops diagnostic dispatcher health" in caplog.text
    assert dispatcher.submit(_diagnostic())
    assert not dispatcher.submit(_diagnostic())
    assert dispatcher.counters["queued"] == 1
    pending = list(dispatcher._queue.queue)
    assert len(pending) == 1
    assert isinstance(pending[0].diagnostic, SafeDiagnostic)
    dispatcher.stop()
    assert "Ops diagnostic dispatcher shutdown" in caplog.text
    assert dispatcher.counters["dropped"] == 2
    assert dispatcher.submit(_diagnostic()) is False
    assert dispatcher.counters["dropped"] == 3
    release.set()
    deadline = time.monotonic() + 2
    while dispatcher.counters["committed"] == 0 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert dispatcher.counters["accepted"] == 2
    assert dispatcher.counters["committed"] == 1
    assert dispatcher.counters["dropped"] == 3
    assert dispatcher.counters["queued"] == 0


def test_indefinitely_blocked_commit_isolated_and_reported_stalled(caplog) -> None:
    caplog.set_level(logging.INFO)
    entered = threading.Event()
    release = threading.Event()
    session = MagicMock()

    def blocked_commit() -> None:
        entered.set()
        release.wait()

    session.commit.side_effect = blocked_commit

    @contextmanager
    def sessions() -> Iterator[MagicMock]:
        yield session

    dispatcher = OpsDiagnosticDispatcher(
        session_factory=sessions,
        queue_capacity=1,
        health_interval_seconds=0.01,
        stall_after_seconds=0.02,
    )
    assert dispatcher.submit(_diagnostic())
    assert entered.wait(1)
    deadline = time.monotonic() + 1
    while not dispatcher.counters["stalled"] and time.monotonic() < deadline:
        time.sleep(0.002)
    assert dispatcher.counters["stalled"] is True
    assert dispatcher.submit(_diagnostic())
    assert not dispatcher.submit(_diagnostic())
    assert dispatcher.counters["dropped"] == 1
    assert dispatcher.counters["queued"] == 1
    assert dispatcher.counters["available"] is True
    deadline = time.monotonic() + 1
    while "'stalled': True" not in caplog.text and time.monotonic() < deadline:
        time.sleep(0.002)
    assert "'stalled': True" in caplog.text
    dispatcher.stop()
    release.set()
    deadline = time.monotonic() + 2
    while dispatcher.counters["committed"] < 1 and time.monotonic() < deadline:
        time.sleep(0.002)
    assert dispatcher.counters["committed"] == 1
    assert dispatcher.counters["dropped"] == 2


def test_thread_start_failure_is_unavailable_without_sync_fallback() -> None:
    def failed_thread(**_kwargs: object) -> MagicMock:
        worker = MagicMock()
        worker.start.side_effect = RuntimeError("do not retain or render this")
        return worker

    sessions = MagicMock()
    dispatcher = OpsDiagnosticDispatcher(
        session_factory=sessions, thread_factory=failed_thread
    )
    assert not dispatcher.submit(_diagnostic())
    assert dispatcher.counters["available"] is False
    sessions.assert_not_called()


def test_health_thread_start_failure_keeps_single_terminal_writer(monkeypatch) -> None:
    writer = MagicMock()
    writer.is_alive.return_value = True

    def factory(**kwargs: object) -> MagicMock:
        return writer

    class BrokenReporter:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            raise RuntimeError("reporter start failure")

    monkeypatch.setattr(threading, "Thread", lambda **kwargs: BrokenReporter())
    # The writer factory remains separately injected; only the optional
    # reporter uses threading.Thread.
    dispatcher = OpsDiagnosticDispatcher(
        session_factory=MagicMock(), thread_factory=factory
    )
    assert dispatcher.counters["available"] is True
    assert dispatcher._worker is writer
    assert dispatcher._health_worker is None
    dispatcher.stop()


@pytest.mark.parametrize("failure", ("construct", "start"))
def test_process_accessor_contains_health_reporter_failure_and_reuses_owner(
    monkeypatch, failure: str
) -> None:
    monkeypatch.setattr(dispatcher_module, "_PROCESS_OWNER", None)

    class BrokenReporter:
        def __init__(self, **_kwargs: object) -> None:
            if failure == "construct":
                raise RuntimeError("reporter construction failure")

        def start(self) -> None:
            raise RuntimeError("reporter start failure")

    monkeypatch.setattr(threading, "Thread", BrokenReporter)
    first = dispatcher_module.get_process_ops_diagnostic_dispatcher(
        session_factory=MagicMock()
    )
    second = dispatcher_module.get_process_ops_diagnostic_dispatcher(
        session_factory=MagicMock()
    )
    assert first is second
    assert first._health_worker is None
    assert first.counters["available"] is True
    first.stop()


@pytest.mark.parametrize(
    "rejected",
    (None, SafeDiagnostic(2, ()), SafeDiagnostic(1, (("exception", "secret"),)), _diagnostic()),
)
def test_rejected_or_contended_admission_does_not_wait_for_lock(rejected) -> None:
    dispatcher = OpsDiagnosticDispatcher(session_factory=MagicMock())
    entered = threading.Event()
    returned = threading.Event()

    def submit_none() -> None:
        entered.set()
        dispatcher.submit(rejected)
        returned.set()

    dispatcher._lock.acquire()
    try:
        thread = threading.Thread(target=submit_none)
        thread.start()
        assert entered.wait(1)
        assert returned.wait(0.1)
    finally:
        dispatcher._lock.release()
    thread.join(timeout=1)
    assert dispatcher.counters["dropped"] == 1
    dispatcher.stop()


