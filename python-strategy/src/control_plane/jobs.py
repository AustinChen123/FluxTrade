from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Mapping, Protocol
from uuid import uuid4

from pydantic import BaseModel

from src.control_plane.ga_lifecycle import (
    GaCheckpointReference,
    GaJobRecord,
    GaJobStatus,
    GaJobStoreError,
    new_ga_job_record,
    transition_ga_job,
)
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

    def mark_interrupted_active_jobs(self, error: str) -> list[JobRecord]: ...

    def create_ga_job(
        self,
        *,
        request: Mapping[str, Any],
        epoch_id: str,
        retry_of_job_id: str | None = None,
    ) -> GaJobRecord: ...

    def get_ga_job(self, job_id: str) -> GaJobRecord | None: ...

    def list_ga_jobs(self) -> list[GaJobRecord]: ...

    def transition_ga_job(
        self,
        job_id: str,
        action: str,
        expected_version: object,
        payload: Mapping[str, object] | None = None,
    ) -> GaJobRecord: ...

    def interrupt_ga_jobs(
        self, error: str = "control_plane_interrupted"
    ) -> list[GaJobRecord]: ...


class InMemoryJobStore:
    """Thread-safe in-memory job store for local control-plane operation."""

    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._ga_jobs: dict[str, GaJobRecord] = {}
        self._lock = Lock()

    def create(self, *, kind: str, request: BaseModel) -> JobRecord:
        if kind == "ga":
            raise ValueError("GA jobs require the GA store API")
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

    def mark_interrupted_active_jobs(self, error: str) -> list[JobRecord]:
        interrupted = []
        with self._lock:
            job_ids = [
                job.id
                for job in self._jobs.values()
                if job.status in {JobStatus.QUEUED, JobStatus.RUNNING}
            ]
        for job_id in job_ids:
            interrupted.append(self.mark_failed(job_id, error))
        return interrupted

    def create_ga_job(
        self,
        *,
        request: Mapping[str, Any],
        epoch_id: str,
        retry_of_job_id: str | None = None,
    ) -> GaJobRecord:
        record = new_ga_job_record(uuid4().hex, request, epoch_id, retry_of_job_id)
        with self._lock:
            self._ga_jobs[record.id] = record
        return record.detached_copy()

    def get_ga_job(self, job_id: str) -> GaJobRecord | None:
        with self._lock:
            record = self._ga_jobs.get(job_id)
            return None if record is None else record.detached_copy()

    def list_ga_jobs(self) -> list[GaJobRecord]:
        with self._lock:
            records = sorted(
                self._ga_jobs.values(),
                key=lambda job: job.id,
            )
            return [record.detached_copy() for record in records]

    def transition_ga_job(
        self,
        job_id: str,
        action: str,
        expected_version: object,
        payload: Mapping[str, object] | None = None,
    ) -> GaJobRecord:
        with self._lock:
            current = self._ga_jobs.get(job_id)
            if current is None:
                raise GaJobStoreError("ga_job_not_found")
            updated = transition_ga_job(current, action, expected_version, payload)
            self._ga_jobs[job_id] = updated
            return updated.detached_copy()

    def interrupt_ga_jobs(
        self,
        error: str = "control_plane_interrupted",
    ) -> list[GaJobRecord]:
        if not isinstance(error, str) or not error.strip():
            raise GaJobStoreError("validation_error")
        with self._lock:
            interrupted = []
            for job_id, current in tuple(self._ga_jobs.items()):
                if current.status in {
                    GaJobStatus.RUNNING,
                    GaJobStatus.PAUSING,
                    GaJobStatus.CANCELLING,
                }:
                    updated = transition_ga_job(
                        current,
                        "interrupt",
                        current.version,
                        {"error": error},
                    )
                    self._ga_jobs[job_id] = updated
                    interrupted.append(updated.detached_copy())
            return interrupted

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
        if kind == "ga":
            raise ValueError("GA jobs require the GA store API")
        job = JobRecord.new(job_id=uuid4().hex, kind=kind, request=request)
        with self._lock, closing(self._connect()) as conn:
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
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, started_at,
                       finished_at, request_json, result_json, error
                FROM control_plane_jobs
                WHERE id = ? AND kind <> 'ga'
                """,
                (job_id,),
            ).fetchone()
        return None if row is None else self._row_to_record(row)

    def list(self) -> list[JobRecord]:
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, started_at,
                       finished_at, request_json, result_json, error
                FROM control_plane_jobs
                WHERE kind <> 'ga'
                ORDER BY created_at DESC
                """
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def create_ga_job(
        self,
        *,
        request: Mapping[str, Any],
        epoch_id: str,
        retry_of_job_id: str | None = None,
    ) -> GaJobRecord:
        record = new_ga_job_record(uuid4().hex, request, epoch_id, retry_of_job_id)
        now = _format_datetime(datetime.now(UTC))
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO control_plane_jobs (
                    id, kind, status, created_at, updated_at, request_json,
                    error, ga_version, ga_epoch_id, ga_completed_generation,
                    ga_checkpoint_generation, ga_retry_of_job_id
                ) VALUES (?, 'ga', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.status.value,
                    now,
                    now,
                    _dumps(record.request),
                    record.error,
                    record.version,
                    record.epoch_id,
                    record.completed_generation,
                    None,
                    record.retry_of_job_id,
                ),
            )
            conn.commit()
        return record.detached_copy()

    def get_ga_job(self, job_id: str) -> GaJobRecord | None:
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                f"SELECT {_GA_COLUMNS} FROM control_plane_jobs WHERE id = ? AND kind = 'ga'",
                (job_id,),
            ).fetchone()
        return None if row is None else _ga_row_to_record(row)

    def list_ga_jobs(self) -> list[GaJobRecord]:
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT {_GA_COLUMNS} FROM control_plane_jobs WHERE kind = 'ga' ORDER BY created_at DESC"
            ).fetchall()
        return [_ga_row_to_record(row) for row in rows]

    def transition_ga_job(
        self,
        job_id: str,
        action: str,
        expected_version: object,
        payload: Mapping[str, object] | None = None,
    ) -> GaJobRecord:
        with self._lock, closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    f"SELECT {_GA_COLUMNS} FROM control_plane_jobs WHERE id = ? AND kind = 'ga'",
                    (job_id,),
                ).fetchone()
                if row is None:
                    raise GaJobStoreError("ga_job_not_found")
                current = _ga_row_to_record(row)
                updated = transition_ga_job(current, action, expected_version, payload)
                _write_ga_record(conn, updated, current.version)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return updated.detached_copy()

    def interrupt_ga_jobs(
        self,
        error: str = "control_plane_interrupted",
    ) -> list[GaJobRecord]:
        if not isinstance(error, str) or not error.strip():
            raise GaJobStoreError("validation_error")
        interrupted: list[GaJobRecord] = []
        with self._lock, closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    f"SELECT {_GA_COLUMNS} FROM control_plane_jobs "
                    "WHERE kind = 'ga' AND status IN (?, ?, ?)",
                    (
                        GaJobStatus.RUNNING.value,
                        GaJobStatus.PAUSING.value,
                        GaJobStatus.CANCELLING.value,
                    ),
                ).fetchall()
                for row in rows:
                    current = _ga_row_to_record(row)
                    updated = transition_ga_job(
                        current,
                        "interrupt",
                        current.version,
                        {"error": error},
                    )
                    _write_ga_record(conn, updated, current.version)
                    interrupted.append(updated.detached_copy())
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return interrupted

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

    def mark_interrupted_active_jobs(self, error: str) -> list[JobRecord]:
        now = datetime.now(UTC)
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, started_at,
                       finished_at, request_json, result_json, error
                FROM control_plane_jobs
                WHERE kind <> 'ga' AND status IN (?, ?)
                """,
                (JobStatus.QUEUED.value, JobStatus.RUNNING.value),
            ).fetchall()
            if not rows:
                return []
            conn.execute(
                """
                UPDATE control_plane_jobs
                SET status = ?, updated_at = ?, finished_at = ?, error = ?
                WHERE kind <> 'ga' AND status IN (?, ?)
                """,
                (
                    JobStatus.FAILED.value,
                    _format_datetime(now),
                    _format_datetime(now),
                    error,
                    JobStatus.QUEUED.value,
                    JobStatus.RUNNING.value,
                ),
            )
            conn.commit()

        return [
            JobRecord.model_validate(
                {
                    **self._row_to_record(row).model_dump(),
                    "status": JobStatus.FAILED,
                    "updated_at": now,
                    "finished_at": now,
                    "error": error,
                }
            )
            for row in rows
        ]

    def _initialize(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, closing(self._connect()) as conn:
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
                    error TEXT,
                    ga_version INTEGER,
                    ga_epoch_id TEXT,
                    ga_completed_generation INTEGER,
                    ga_checkpoint_generation INTEGER,
                    ga_retry_of_job_id TEXT
                )
                """
            )
            columns = {
                row["name"]
                for row in conn.execute(
                    "PRAGMA table_info(control_plane_jobs)"
                ).fetchall()
            }
            for name, column_type in (
                ("ga_version", "INTEGER"),
                ("ga_epoch_id", "TEXT"),
                ("ga_completed_generation", "INTEGER"),
                ("ga_checkpoint_generation", "INTEGER"),
                ("ga_retry_of_job_id", "TEXT"),
            ):
                if name not in columns:
                    conn.execute(
                        f"ALTER TABLE control_plane_jobs ADD COLUMN {name} {column_type}"
                    )
            conn.commit()

    def _update(self, job_id: str, **changes: Any) -> JobRecord:
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, started_at,
                       finished_at, request_json, result_json, error
                FROM control_plane_jobs
                WHERE id = ? AND kind <> 'ga'
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
                WHERE id = ? AND kind <> 'ga'
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


_GA_COLUMNS = (
    "id, status, request_json, error, ga_version, ga_epoch_id, "
    "ga_completed_generation, ga_checkpoint_generation, ga_retry_of_job_id"
)


def _ga_row_to_record(row: sqlite3.Row) -> GaJobRecord:
    return GaJobRecord(
        id=row["id"],
        status=GaJobStatus(row["status"]),
        version=row["ga_version"],
        request=json.loads(row["request_json"]),
        epoch_id=row["ga_epoch_id"],
        completed_generation=row["ga_completed_generation"],
        checkpoint=(
            None
            if row["ga_checkpoint_generation"] is None
            else GaCheckpointReference(
                row["ga_epoch_id"],
                row["ga_checkpoint_generation"],
            )
        ),
        retry_of_job_id=row["ga_retry_of_job_id"],
        error=row["error"],
    )


def _write_ga_record(
    conn: sqlite3.Connection,
    updated: GaJobRecord,
    expected_version: int,
) -> None:
    now = _format_datetime(datetime.now(UTC))
    finished_at = (
        now
        if updated.status
        in {
            GaJobStatus.CANCELLED,
            GaJobStatus.SUCCEEDED,
            GaJobStatus.FAILED,
        }
        else None
    )
    cursor = conn.execute(
        """
        UPDATE control_plane_jobs
        SET status = ?, updated_at = ?, finished_at = ?, error = ?,
            ga_version = ?, ga_completed_generation = ?,
            ga_checkpoint_generation = ?
        WHERE id = ? AND kind = 'ga' AND ga_version = ?
        """,
        (
            updated.status.value,
            now,
            finished_at,
            updated.error,
            updated.version,
            updated.completed_generation,
            None
            if updated.checkpoint is None
            else updated.checkpoint.completed_generation,
            updated.id,
            expected_version,
        ),
    )
    if cursor.rowcount != 1:
        raise GaJobStoreError("job_version_conflict")


def _dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
