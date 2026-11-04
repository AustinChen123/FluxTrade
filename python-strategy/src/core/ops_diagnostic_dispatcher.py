"""Bounded process-owned delivery for non-authoritative ops diagnostics."""

from __future__ import annotations

import queue
import threading
import time
from itertools import count
from logging import getLogger
from collections.abc import Callable
from typing import ContextManager, Protocol

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.orm_models import SystemEvent
from src.core.safe_diagnostics import SafeDiagnostic

_MAX_QUEUE = 128
_logger = getLogger(__name__)


class _Worker(Protocol):
    def start(self) -> None: ...

    def is_alive(self) -> bool: ...


class _Item:
    __slots__ = ("diagnostic",)

    def __init__(self, diagnostic: SafeDiagnostic) -> None:
        self.diagnostic = diagnostic


class OpsDiagnosticDispatcher:
    """Own one fixed queue/worker and worker-owned DB sessions for process life."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], ContextManager[Session]],
        queue_capacity: int = _MAX_QUEUE,
        thread_factory: Callable[..., _Worker] = threading.Thread,
        health_interval_seconds: float = 60.0,
        stall_after_seconds: float = 5.0,
    ) -> None:
        if not 1 <= queue_capacity <= _MAX_QUEUE:
            raise ValueError("diagnostic queue capacity is out of bounds")
        if health_interval_seconds <= 0:
            raise ValueError("diagnostic health interval must be positive")
        if stall_after_seconds <= 0:
            raise ValueError("diagnostic stall age must be positive")
        self._session_factory = session_factory
        self._queue: queue.Queue[_Item] = queue.Queue(queue_capacity)
        self._lock = threading.Lock()
        self._stopped = False
        self._available = False
        self._worker: _Worker | None = None
        self._accepted = 0
        self._dropped = 0
        # next(count) is a bounded, nonblocking increment on CPython. Producers
        # use this when the admission lock is contended.
        self._drop_sequence = count(1)
        self._contended_drops = 0
        self._committed = 0
        self._health_reporter_available = False
        self._health_stop = threading.Event()
        self._health_interval_seconds = health_interval_seconds
        self._stall_after_seconds = stall_after_seconds
        self._inflight_since: float | None = None
        try:
            worker = thread_factory(
                target=self._run, name="ops-diagnostic-writer", daemon=True
            )
            worker.start()
            self._worker = worker
            self._available = True
        except Exception:
            self._dropped += 1
        self._health_worker: _Worker | None = None
        try:
            health_worker = threading.Thread(
                target=self._report_health,
                name="ops-diagnostic-health",
                daemon=True,
            )
            health_worker.start()
            self._health_worker = health_worker
        except Exception:
            # Reporting is optional. The writer remains the terminal process
            # owner and is never replaced because its reporter failed.
            self._health_stop.set()
        else:
            self._health_reporter_available = True

    @property
    def counters(self) -> dict[str, int | bool]:
        worker_alive = self._worker is not None and self._worker.is_alive()
        with self._lock:
            stalled = (
                self._inflight_since is not None
                and time.monotonic() - self._inflight_since >= self._stall_after_seconds
            )
            return {
                "available": self._available and not self._stopped and worker_alive,
                "reporter_available": self._health_reporter_available,
                "accepted": self._accepted,
                "dropped": self._dropped + self._contended_drops,
                "committed": self._committed,
                "queued": self._queue.qsize(),
                "stalled": stalled,
            }

    def submit(self, diagnostic: SafeDiagnostic | None) -> bool:
        if diagnostic is None:
            self._record_contended_drop()
            return False
        try:
            validated = SafeDiagnostic.create(dict(diagnostic.fields))
        except Exception:
            validated = None
        if diagnostic.schema_version != 1 or validated is None:
            self._record_contended_drop()
            return False
        if not self._lock.acquire(blocking=False):
            self._record_contended_drop()
            return False
        try:
            if (
                not self._available
                or self._stopped
                or self._worker is None
                or not self._worker.is_alive()
            ):
                self._available = False
                self._dropped += 1
                return False
            try:
                self._queue.put_nowait(_Item(validated))
            except queue.Full:
                self._dropped += 1
                return False
            self._accepted += 1
            return True
        finally:
            self._lock.release()

    def stop(self) -> None:
        """Signal shutdown without draining or joining the possibly blocked worker."""
        with self._lock:
            if self._stopped:
                self._health_stop.set()
                return
            self._stopped = True
            self._discard_queued_locked()
            self._health_stop.set()
            summary = {
                "available": self._available
                and self._worker is not None
                and self._worker.is_alive(),
                "reporter_available": self._health_reporter_available,
                "accepted": self._accepted,
                "dropped": self._dropped + self._contended_drops,
                "committed": self._committed,
                "queued": self._queue.qsize(),
            }
        _logger.info("Ops diagnostic dispatcher shutdown: %s", summary)

    def _record_contended_drop(self) -> None:
        self._contended_drops = next(self._drop_sequence)

    def _run(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=0.1)
            except queue.Empty:
                with self._lock:
                    if self._stopped:
                        return
                continue
            with self._lock:
                if self._stopped:
                    self._dropped += 1
                    self._queue.task_done()
                    continue
                self._inflight_since = time.monotonic()
            try:
                self._persist(item.diagnostic)
            except BaseException as error:
                with self._lock:
                    self._dropped += 1
                    if not isinstance(error, Exception):
                        # A fatal exit terminates this fixed owner. The
                        # in-flight and queued accepted items have no owner
                        # that can complete them after this thread exits.
                        self._available = False
                        self._stopped = True
                        self._discard_queued_locked()
                if not isinstance(error, Exception):
                    raise
            else:
                with self._lock:
                    self._committed += 1
            finally:
                with self._lock:
                    self._inflight_since = None
                self._queue.task_done()

    def _discard_queued_locked(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                return
            else:
                self._dropped += 1
                self._queue.task_done()

    def _report_health(self) -> None:
        while not self._health_stop.wait(self._health_interval_seconds):
            _logger.info("Ops diagnostic dispatcher health: %s", self.counters)

    def close_session(self, session: Session) -> None:
        """Close a worker-owned session without hiding close failures."""
        session.close()

    def _persist(self, diagnostic: SafeDiagnostic) -> None:
        with self._session_factory() as session:
            try:
                # PostgreSQL bounds the item statement after a worker-owned
                # connection has been acquired. Pool/connect deadlines remain
                # those configured by the injected application session factory.
                if session.get_bind().dialect.name == "postgresql":
                    session.execute(text("SET LOCAL statement_timeout = '2000ms'"))
                session.add(
                    SystemEvent(
                        event_type="ops",
                        event_subtype="rithmic_diagnostic",
                        payload={"safe_diagnostic": diagnostic.as_dict()},
                    )
                )
                session.commit()
            except Exception:
                try:
                    session.rollback()
                except Exception:
                    pass
                raise


_PROCESS_OWNER_LOCK = threading.Lock()
_PROCESS_OWNER: OpsDiagnosticDispatcher | None = None


def get_process_ops_diagnostic_dispatcher(
    *, session_factory: Callable[[], ContextManager[Session]]
) -> OpsDiagnosticDispatcher:
    """Return the one process owner; a failed/stopped owner is never replaced."""
    global _PROCESS_OWNER
    with _PROCESS_OWNER_LOCK:
        if _PROCESS_OWNER is None:
            _PROCESS_OWNER = OpsDiagnosticDispatcher(session_factory=session_factory)
        return _PROCESS_OWNER
