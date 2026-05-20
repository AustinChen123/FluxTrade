from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Protocol
from uuid import uuid4

from pydantic import BaseModel

from src.control_plane.models import JobRecord, JobStatus


class JobStore(Protocol):
    """Storage boundary for control-plane jobs."""

    def create(self, *, kind: str, request: BaseModel) -> JobRecord: ...

    def get(self, job_id: str) -> JobRecord | None: ...

    def list(self) -> list[JobRecord]: ...

    def mark_running(self, job_id: str) -> JobRecord: ...

    def mark_succeeded(self, job_id: str, result: dict[str, Any]) -> JobRecord: ...

    def mark_failed(self, job_id: str, error: str) -> JobRecord: ...

    def mark_cancelled(self, job_id: str, reason: str | None = None) -> JobRecord: ...


class InMemoryJobStore:
    """Thread-safe in-memory job store for local control-plane operation."""

    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = Lock()

    def create(self, *, kind: str, request: BaseModel) -> JobRecord:
        job = JobRecord.new(job_id=uuid4().hex, kind=kind, request=request)
        with self._lock:
            self._jobs[job.id] = job
        return job

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self) -> list[JobRecord]:
        with self._lock:
            return sorted(
                self._jobs.values(),
                key=lambda job: job.created_at,
                reverse=True,
            )

    def mark_running(self, job_id: str) -> JobRecord:
        return self._update(
            job_id,
            status=JobStatus.RUNNING,
            started_at=datetime.now(UTC),
            error=None,
        )

    def mark_succeeded(self, job_id: str, result: dict[str, Any]) -> JobRecord:
        now = datetime.now(UTC)
        return self._update(
            job_id,
            status=JobStatus.SUCCEEDED,
            updated_at=now,
            finished_at=now,
            result=result,
            error=None,
        )

    def mark_failed(self, job_id: str, error: str) -> JobRecord:
        now = datetime.now(UTC)
        return self._update(
            job_id,
            status=JobStatus.FAILED,
            updated_at=now,
            finished_at=now,
            error=error,
        )

    def mark_cancelled(self, job_id: str, reason: str | None = None) -> JobRecord:
        now = datetime.now(UTC)
        return self._update(
            job_id,
            status=JobStatus.CANCELLED,
            updated_at=now,
            finished_at=now,
            error=reason,
        )

    def _update(self, job_id: str, **changes: Any) -> JobRecord:
        with self._lock:
            current = self._jobs[job_id]
            data = current.model_dump()
            data.update(changes)
            data.setdefault("updated_at", datetime.now(UTC))
            if "updated_at" not in changes:
                data["updated_at"] = datetime.now(UTC)
            updated = JobRecord.model_validate(data)
            self._jobs[job_id] = updated
            return updated


class SqliteJobStore:
    """SQLite-backed job store for durable local control-plane operation."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._lock = Lock()
        self._initialize()

    def create(self, *, kind: str, request: BaseModel) -> JobRecord:
        job = JobRecord.new(job_id=uuid4().hex, kind=kind, request=request)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO control_plane_jobs (
                    id, kind, status, created_at, updated_at, started_at,
                    finished_at, request_json, result_json, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._record_to_row(job),
            )
            conn.commit()
        return job

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, started_at,
                       finished_at, request_json, result_json, error
                FROM control_plane_jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
        return None if row is None else self._row_to_record(row)

    def list(self) -> list[JobRecord]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, started_at,
                       finished_at, request_json, result_json, error
                FROM control_plane_jobs
                ORDER BY created_at DESC
                """
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def mark_running(self, job_id: str) -> JobRecord:
        return self._update(
            job_id,
            status=JobStatus.RUNNING,
            started_at=datetime.now(UTC),
            error=None,
        )

    def mark_succeeded(self, job_id: str, result: dict[str, Any]) -> JobRecord:
        now = datetime.now(UTC)
        return self._update(
            job_id,
            status=JobStatus.SUCCEEDED,
            updated_at=now,
            finished_at=now,
            result=result,
            error=None,
        )

    def mark_failed(self, job_id: str, error: str) -> JobRecord:
        now = datetime.now(UTC)
        return self._update(
            job_id,
            status=JobStatus.FAILED,
            updated_at=now,
            finished_at=now,
            error=error,
        )

    def mark_cancelled(self, job_id: str, reason: str | None = None) -> JobRecord:
        now = datetime.now(UTC)
        return self._update(
            job_id,
            status=JobStatus.CANCELLED,
            updated_at=now,
            finished_at=now,
            error=reason,
        )

    def _initialize(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS control_plane_jobs (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    request_json TEXT NOT NULL,
                    result_json TEXT,
                    error TEXT
                )
                """
            )
            conn.commit()

    def _update(self, job_id: str, **changes: Any) -> JobRecord:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, started_at,
                       finished_at, request_json, result_json, error
                FROM control_plane_jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(job_id)

            data = self._row_to_record(row).model_dump()
            data.update(changes)
            if "updated_at" not in changes:
                data["updated_at"] = datetime.now(UTC)
            updated = JobRecord.model_validate(data)
            conn.execute(
                """
                UPDATE control_plane_jobs
                SET status = ?, updated_at = ?, started_at = ?, finished_at = ?,
                    result_json = ?, error = ?
                WHERE id = ?
                """,
                (
                    updated.status.value,
                    _format_datetime(updated.updated_at),
                    _format_datetime(updated.started_at),
                    _format_datetime(updated.finished_at),
                    _dumps(updated.result) if updated.result is not None else None,
                    updated.error,
                    job_id,
                ),
            )
            conn.commit()
            return updated

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _record_to_row(job: JobRecord) -> tuple[Any, ...]:
        return (
            job.id,
            job.kind,
            job.status.value,
            _format_datetime(job.created_at),
            _format_datetime(job.updated_at),
            _format_datetime(job.started_at),
            _format_datetime(job.finished_at),
            _dumps(job.request),
            _dumps(job.result) if job.result is not None else None,
            job.error,
        )

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> JobRecord:
        return JobRecord.model_validate(
            {
                "id": row["id"],
                "kind": row["kind"],
                "status": row["status"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "request": json.loads(row["request_json"]),
                "result": (
                    json.loads(row["result_json"])
                    if row["result_json"] is not None
                    else None
                ),
                "error": row["error"],
            }
        )


def _format_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
