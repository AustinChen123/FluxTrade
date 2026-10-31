"""Bounded, provider-neutral protection alert delivery."""

from __future__ import annotations

import os
import queue
import threading
from pathlib import Path
from typing import Callable, Protocol

from src.core.protection_alerts import ProtectionAlert

DEFAULT_ALERTS_PATH = "/var/log/fluxtrade/protection-alerts.jsonl"
ALERTS_PATH_ENV = "FLUXTRADE_PROTECTION_ALERTS_PATH"

class NotificationSink(Protocol):
    def emit(self, alert: ProtectionAlert) -> bool: ...


class _Thread(Protocol):
    def start(self) -> None: ...

    def is_alive(self) -> bool: ...


class _Counters:
    def __init__(self) -> None:
        self.queue_full = 0
        self.sink_errors = 0
        self.start_failures = 0
        self.rejected = 0


class NotificationDispatcher:
    """Own one bounded queue, daemon writer, and dedicated raw file descriptor."""

    def __init__(
        self,
        *,
        path: str | os.PathLike[str] | None = None,
        queue_capacity: int = 128,
        thread_factory: Callable[..., _Thread] = threading.Thread,
        write: Callable[[int, bytes], int] = os.write,
    ) -> None:
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be positive")
        configured_path = path or os.environ.get(ALERTS_PATH_ENV, DEFAULT_ALERTS_PATH)
        self.path = Path(configured_path)
        self._queue: queue.Queue[ProtectionAlert] = queue.Queue(queue_capacity)
        self._thread_factory = thread_factory
        self._write = write
        self._lock = threading.Lock()
        self._state = "new"
        self._fd: int | None = None
        self._worker: _Thread | None = None
        self._worker_claimed_fd = False
        self._counters = _Counters()

    @property
    def counters(self) -> dict[str, int]:
        with self._lock:
            return {
                "queue_full": self._counters.queue_full,
                "sink_errors": self._counters.sink_errors,
                "start_failures": self._counters.start_failures,
                "rejected": self._counters.rejected,
            }

    def start(self) -> bool:
        with self._lock:
            if self._state == "running":
                return True
            if self._state != "new":
                return False
            self._state = "preparing"

        fd: int | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(
                self.path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                0o600,
            )
            worker = self._thread_factory(
                target=self._run, name="protection-notification-writer", daemon=True
            )
        except Exception:
            with self._lock:
                self._counters.start_failures += 1
                if self._state == "preparing":
                    self._state = "unavailable"
            if fd is not None:
                self._close_unowned_fd(fd)
            return False

        with self._lock:
            if self._state != "preparing":
                self._counters.start_failures += 1
                publish = False
            else:
                self._fd = fd
                self._worker = worker
                self._worker_claimed_fd = False
                self._state = "launching"
                publish = True
        if not publish:
            self._close_unowned_fd(fd)
            return False

        try:
            worker.start()
        except Exception:
            alive = False
            try:
                alive = worker.is_alive()
            except Exception:
                # If liveness cannot be established, the worker may own the fd.
                alive = True
            with self._lock:
                self._counters.start_failures += 1
                if self._state == "launching":
                    self._state = "unavailable"
                worker_owns_fd = self._worker_claimed_fd or alive
                if not worker_owns_fd and self._fd == fd:
                    self._fd = None
            if not worker_owns_fd:
                self._close_unowned_fd(fd)
            return False

        with self._lock:
            if self._state == "launching":
                self._state = "running"
            return self._state == "running"

    def emit(self, alert: ProtectionAlert) -> bool:
        """Attempt one nonblocking enqueue; producer never logs or raises."""
        try:
            with self._lock:
                if self._state != "running":
                    self._counters.rejected += 1
                    return False
                try:
                    self._queue.put_nowait(alert)
                    return True
                except queue.Full:
                    self._counters.queue_full += 1
                    return False
        except Exception:
            return False

    def stop(self, timeout: float = 0.25) -> bool:
        """Reject new alerts and abandon pending records without joining or closing."""
        del timeout  # No caller wait, flush, join, or concurrent descriptor close.
        with self._lock:
            if self._state in {"stopped", "unavailable"}:
                return True
            if self._state in {"new", "preparing"}:
                self._state = "stopped"
            else:
                self._state = "stopping"
            return True

    def _run(self) -> None:
        fd: int | None = None
        try:
            with self._lock:
                fd = self._fd
                self._worker_claimed_fd = fd is not None
                if self._state not in {"launching", "running"}:
                    return
            while True:
                with self._lock:
                    if self._state in {"stopping", "stopped", "unavailable"}:
                        return
                    fd = self._fd
                if fd is None:
                    return
                try:
                    alert = self._queue.get(timeout=0.05)
                except queue.Empty:
                    continue
                with self._lock:
                    if self._state not in {"launching", "running"} or self._fd is None:
                        continue
                    fd = self._fd
                try:
                    self._write_all(fd, alert.to_json_line())
                except Exception:
                    with self._lock:
                        self._counters.sink_errors += 1
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            with self._lock:
                self._fd = None
                self._state = "stopped"

    @staticmethod
    def _close_unowned_fd(fd: int) -> None:
        try:
            os.close(fd)
        except OSError:
            pass

    def _write_all(self, fd: int, record: bytes) -> None:
        view = memoryview(record)
        while view:
            written = self._write(fd, bytes(view))
            if written <= 0:
                raise OSError("short protection alert write")
            view = view[written:]

