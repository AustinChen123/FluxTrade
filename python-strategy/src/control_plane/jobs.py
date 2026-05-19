from __future__ import annotations

from datetime import UTC, datetime
from threading import Lock
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from src.control_plane.models import JobRecord, JobStatus


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
