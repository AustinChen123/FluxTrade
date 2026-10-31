import logging
import os
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from queue import Queue

import pytest

from src.core.notifications import (
    NotificationDispatcher,
)
from src.core.protection_alerts import ProtectionAlert


def _alert(
    *, evidence_id: str = "event-1", occurred_at: datetime | None = None
) -> ProtectionAlert:
    return ProtectionAlert.create(
        event_type="protection_loss",
        severity="critical",
        reason="declared protection ended without fill",
        entry_id="entry-1",
        evidence_id=evidence_id,
        occurred_at=occurred_at or datetime(2026, 10, 31, tzinfo=UTC),
    )


def test_dispatcher_writes_jsonl_and_repeated_lifecycle_is_idempotent(tmp_path):
    path = tmp_path / "nested" / "alerts.jsonl"
    dispatcher = NotificationDispatcher(path=path)
    assert dispatcher.start() is True
    worker = dispatcher._worker
    assert dispatcher.start() is True
    assert dispatcher._worker is worker
    assert dispatcher.emit(_alert()) is True
    deadline = time.monotonic() + 2
    while not path.exists() or path.stat().st_size == 0:
        assert time.monotonic() < deadline
        time.sleep(0.005)
    assert path.read_bytes().endswith(b"\n")
    assert path.stat().st_mode & 0o777 == 0o600
    assert dispatcher.stop(timeout=0.01) is True
    assert dispatcher.start() is False
    assert dispatcher.stop() is True


def test_alert_path_can_be_configured_by_environment(monkeypatch, tmp_path):
    target = tmp_path / "configured.jsonl"
    monkeypatch.setenv("FLUXTRADE_PROTECTION_ALERTS_PATH", str(target))
    assert NotificationDispatcher().path == target


def test_raw_sink_completes_short_writes(tmp_path):
    calls = []

    def short_write(fd, data):
        calls.append(len(data))
        return os.write(fd, data[:1])

    path = tmp_path / "short.jsonl"
    dispatcher = NotificationDispatcher(path=path, write=short_write)
    assert dispatcher.start()
    assert dispatcher.emit(_alert())
    deadline = time.monotonic() + 2
    while not path.exists() or not path.read_bytes().endswith(b"\n"):
        assert time.monotonic() < deadline
        time.sleep(0.005)
    assert len(calls) > 1
    dispatcher.stop()


def test_blocked_sink_keeps_producer_nonblocking_and_queue_full_is_counted(tmp_path, caplog):
    entered = threading.Event()
    release = threading.Event()

    def blocked_write(fd, data):
        entered.set()
        release.wait(3)
        return os.write(fd, data)

    dispatcher = NotificationDispatcher(
        path=tmp_path / "alerts.jsonl", queue_capacity=1, write=blocked_write
    )
    assert dispatcher.start()
    assert dispatcher.emit(_alert())
    assert entered.wait(1)
    assert dispatcher.emit(_alert(evidence_id="event-2"))
    start = time.monotonic()
    with caplog.at_level(logging.INFO):
        logging.getLogger("notification-test").info("caller remains usable")
        assert dispatcher.emit(_alert(evidence_id="event-3")) is False
    assert time.monotonic() - start < 0.2
    assert dispatcher.counters["queue_full"] == 1
    assert [record.getMessage() for record in caplog.records] == [
        "caller remains usable"
    ]
    stop_started = time.monotonic()
    dispatcher.stop(timeout=0.01)
    assert time.monotonic() - stop_started < 0.2
    release.set()


def test_sink_throw_is_isolated_and_counted(tmp_path):
    attempted = threading.Event()

    def broken_write(_fd, _data):
        attempted.set()
        raise OSError("sink failed")

    dispatcher = NotificationDispatcher(path=tmp_path / "alerts.jsonl", write=broken_write)
    assert dispatcher.start()
    assert dispatcher.emit(_alert())
    assert attempted.wait(1)
    deadline = time.monotonic() + 1
    while dispatcher.counters["sink_errors"] == 0:
        assert time.monotonic() < deadline
        time.sleep(0.005)
    dispatcher.stop()


def test_creation_and_thread_start_failures_disable_delivery(tmp_path):
    bad_path = tmp_path / "file"
    bad_path.write_text("not a directory")
    assert NotificationDispatcher(path=bad_path / "alerts.jsonl").start() is False

    class BrokenThread:
        def start(self):
            raise RuntimeError("thread start failed")

        def is_alive(self):
            return False

    dispatcher = NotificationDispatcher(
        path=tmp_path / "thread-failure.jsonl",
        thread_factory=lambda **_kwargs: BrokenThread(),
    )
    assert dispatcher.start() is False
    assert dispatcher.start() is False
    assert dispatcher.counters["start_failures"] == 1

    class StartThenRaise(threading.Thread):
        def start(self):
            super().start()
            raise RuntimeError("start raised after launch")

    stranded = NotificationDispatcher(
        path=tmp_path / "stranded.jsonl",
        thread_factory=lambda **kwargs: StartThenRaise(**kwargs),
    )
    assert stranded.start() is False
    worker = stranded._worker
    assert isinstance(worker, threading.Thread)
    worker.join(timeout=1)
    assert not worker.is_alive()


def test_stalled_preparation_does_not_block_emit_or_bounded_stop(tmp_path, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    opened_fds: list[int] = []
    real_open = os.open

    def stalled_open(*args, **kwargs):
        entered.set()
        assert release.wait(2)
        fd = real_open(*args, **kwargs)
        opened_fds.append(fd)
        return fd

    monkeypatch.setattr("src.core.notifications.os.open", stalled_open)
    dispatcher = NotificationDispatcher(path=tmp_path / "stalled.jsonl")
    start_results: list[bool] = []
    starter = threading.Thread(target=lambda: start_results.append(dispatcher.start()))
    starter.start()
    assert entered.wait(1)

    before = time.monotonic()
    assert dispatcher.emit(_alert()) is False
    assert dispatcher.stop(timeout=0.01) is True
    assert time.monotonic() - before < 0.1

    release.set()
    starter.join(timeout=1)
    assert not starter.is_alive()
    assert start_results == [False]
    assert dispatcher._worker is None
    assert len(opened_fds) == 1
    with pytest.raises(OSError):
        os.fstat(opened_fds[0])


def test_stop_before_dequeue_claim_discards_dequeued_record(tmp_path):
    dequeued = threading.Event()
    release_claim = threading.Event()
    writes: list[bytes] = []
    backing_queue: Queue[ProtectionAlert] = Queue(maxsize=1)

    class PausingQueue:
        def put_nowait(self, alert: ProtectionAlert) -> None:
            backing_queue.put_nowait(alert)

        def get_nowait(self) -> ProtectionAlert:
            return backing_queue.get_nowait()

        def get(self, timeout: float) -> ProtectionAlert:
            alert = backing_queue.get(timeout=timeout)
            dequeued.set()
            assert release_claim.wait(2)
            return alert

    def capture_write(_fd: int, data: bytes) -> int:
        writes.append(data)
        return len(data)

    dispatcher = NotificationDispatcher(
        path=tmp_path / "stop-before-claim.jsonl", write=capture_write
    )
    dispatcher._queue = PausingQueue()  # type: ignore[assignment]
    assert dispatcher.start()
    assert dispatcher.emit(_alert())
    assert dequeued.wait(1)
    assert dispatcher.stop()
    release_claim.set()

    worker = dispatcher._worker
    assert isinstance(worker, threading.Thread)
    worker.join(timeout=1)
    assert not worker.is_alive()
    assert writes == []


def test_stuck_worker_does_not_block_real_logging_shutdown_or_process_exit(tmp_path):
    script = """
import logging, threading
from src.core.notifications import NotificationDispatcher, ProtectionAlert
started = threading.Event()
blocked = threading.Event()
def writer(fd, data):
    started.set()
    blocked.wait()
    return len(data)
dispatcher = NotificationDispatcher(path=r'%s', write=writer)
assert dispatcher.start()
alert = ProtectionAlert.create(event_type='x', severity='warning', reason='y', entry_id='e', evidence_id='v')
assert dispatcher.emit(alert)
assert started.wait(1)
logging.shutdown()
""" % (str(tmp_path / "subprocess.jsonl"),)
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.getcwd()
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=os.getcwd(),
        env=environment,
        capture_output=True,
        timeout=3,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")

