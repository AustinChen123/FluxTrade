"""Job-local PostgreSQL leases. Callers must omit secrets from error details/JSON."""

import re
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import cast

from sqlalchemy import Table, and_, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from src.core.product_registry import validate_product_id

from .orm import VolumeProfileIngestJob
from .publication import CanonicalJsonObject
from .types import BIGINT_MAX, DAY_MS

_JOB = cast(Table, VolumeProfileIngestJob.__table__)


class JobIntegrityError(ValueError):
    """Malformed persisted job state; never a global trading lock."""


class JobConflict(ValueError):
    """The immutable specification differs for an existing job ID."""


class LeaseLost(ValueError):
    """The claim is stale, expired, or no longer running."""


def _safe(value: str, limit: int = 64) -> None:
    if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1," + str(limit) + "}", value):
        raise ValueError("invalid job identifier")


def _integer(value: int, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value <= BIGINT_MAX:
        raise ValueError("invalid job integer")


def _utc(value: object) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise ValueError("invalid job timestamp")
    try:
        if value.utcoffset() is None:
            raise ValueError
        return value.astimezone(timezone.utc)
    except Exception:
        raise ValueError("invalid job timestamp") from None


def _duration(value: timedelta, zero: bool = False) -> None:
    # All lease/retry intervals are bounded to one day; no float conversion.
    if type(value) is not timedelta or not timedelta(0) <= value <= timedelta(days=1) or (not zero and not value):
        raise ValueError("invalid job duration")


def _error(code: str, detail: str) -> None:
    _safe(code)
    if type(detail) is not str or len(detail) > 512 or any(ord(c) < 32 or ord(c) == 127 for c in detail):
        raise ValueError("invalid job error detail")
    try:
        detail.encode("utf-8")
    except UnicodeError:
        raise ValueError("invalid job error detail") from None


@dataclass(frozen=True, slots=True)
class JobSpec:
    id: str
    product_id: str
    window_start_ms: int
    window_end_ms: int
    grid_id: str
    algorithm_version: str
    config_sha256: str

    def __post_init__(self) -> None:
        _safe(self.id)
        _safe(self.grid_id)
        _safe(self.algorithm_version, 32)
        if type(self.product_id) is not str or len(self.product_id) > 64:
            raise ValueError("invalid job product")
        validate_product_id(self.product_id)
        _integer(self.window_start_ms)
        _integer(self.window_end_ms)
        if self.window_start_ms % DAY_MS or self.window_end_ms - self.window_start_ms != DAY_MS:
            raise ValueError("invalid job daily window")
        if type(self.config_sha256) is not str or not re.fullmatch(r"[0-9a-f]{64}", self.config_sha256):
            raise ValueError("invalid job config digest")


@dataclass(frozen=True, slots=True)
class JobClaim:
    spec: JobSpec
    worker_id: str
    attempt: int
    lease_expires_at: datetime
    source_cursor: CanonicalJsonObject
    progress_manifest: CanonicalJsonObject

    def __post_init__(self) -> None:
        if type(self.spec) is not JobSpec or any(
            type(v) is not CanonicalJsonObject for v in (self.source_cursor, self.progress_manifest)
        ):
            raise ValueError("invalid job claim values")
        _safe(self.worker_id, 128)
        _integer(self.attempt, 1)
        object.__setattr__(self, "lease_expires_at", _utc(self.lease_expires_at))


@dataclass(frozen=True, slots=True)
class JobState:
    spec: JobSpec
    status: str
    attempt: int
    source_cursor: CanonicalJsonObject
    progress_manifest: CanonicalJsonObject
    lease_owner: str | None
    lease_expires_at: datetime | None
    retry_after_at: datetime | None
    last_error_code: str | None
    last_error_detail: str | None
    completed_snapshot_id: str | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        _integer(self.attempt)
        if type(self.spec) is not JobSpec or any(
            type(v) is not CanonicalJsonObject for v in (self.source_cursor, self.progress_manifest)
        ):
            raise ValueError("invalid job state values")
        if type(self.status) is not str or self.status not in (
            "RUNNING",
            "RETRYABLE",
            "FAILED",
            "DONE",
        ):
            raise ValueError("invalid job status")
        for key in ("created_at", "updated_at", "lease_expires_at", "retry_after_at"):
            value = getattr(self, key)
            if value is not None or key in ("created_at", "updated_at"):
                object.__setattr__(self, key, _utc(value))
        if self.status == "RUNNING":
            self.claim()
        elif self.lease_owner is not None or self.lease_expires_at is not None:
            raise ValueError("invalid inactive lease")
        if (self.last_error_code is None) != (self.last_error_detail is None):
            raise ValueError("invalid job error pair")
        if self.last_error_code is not None:
            _error(self.last_error_code, cast(str, self.last_error_detail))
        if self.status in ("RUNNING", "DONE") and self.last_error_code is not None:
            raise ValueError("invalid active/completed job error")
        if self.status == "FAILED" and self.last_error_code is None:
            raise ValueError("missing failed job error")
        if self.status != "RETRYABLE":
            _integer(self.attempt, 1)
        if self.retry_after_at is not None and self.status != "RETRYABLE":
            raise ValueError("invalid retry state")
        if self.status == "DONE":
            if (
                type(self.completed_snapshot_id) is not str
                or not re.fullmatch(r"[0-9a-f]{64}", self.completed_snapshot_id)
            ):
                raise ValueError("invalid completed state")
        elif self.completed_snapshot_id is not None:
            raise ValueError("unexpected completed snapshot")

    def claim(self) -> JobClaim:
        if self.status != "RUNNING":
            raise ValueError("job is not running")
        return JobClaim(
            self.spec,
            cast(str, self.lease_owner),
            self.attempt,
            cast(datetime, self.lease_expires_at),
            self.source_cursor,
            self.progress_manifest,
        )


def _state(row: RowMapping) -> JobState:
    try:
        spec = JobSpec(**{key: row[key] for key in JobSpec.__dataclass_fields__})
        values = {key: row[key] for key in JobState.__dataclass_fields__ if key != "spec"}
        for key in ("source_cursor", "progress_manifest"):
            values[key] = CanonicalJsonObject(values[key])
        return JobState(spec, **values)
    except (ValueError, TypeError, KeyError, OverflowError):
        raise JobIntegrityError("job integrity check failed") from None


class ProfileIngestJobStore:
    def __init__(self, sessions: Callable[[], AbstractContextManager[Session]]) -> None:
        self._sessions = sessions

    @contextmanager
    def _transaction(self, write: bool = True) -> Iterator[Session]:
        with self._sessions() as session:
            if session.get_bind().dialect.name != "postgresql" or session.in_transaction():
                raise ValueError("job store requires fresh PostgreSQL transaction")
            with session.begin():
                if write:
                    session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                yield session

    def register(self, spec: JobSpec) -> JobState:
        if type(spec) is not JobSpec:
            raise ValueError("expected exact job spec")
        with self._transaction() as session:
            session.execute(
                insert(_JOB)
                .values(
                    **asdict(spec),
                    status="RETRYABLE",
                    attempt=0,
                    source_cursor={},
                    progress_manifest={},
                )
                .on_conflict_do_nothing(index_elements=[_JOB.c.id])
            )
            row = session.execute(select(_JOB).where(_JOB.c.id == spec.id)).mappings().one()
            state = _state(row)
            if state.spec != spec:
                raise JobConflict("job specification conflict")
            return state

    def get(self, job_id: str) -> JobState | None:
        _safe(job_id)
        with self._transaction(False) as session:
            row = session.execute(select(_JOB).where(_JOB.c.id == job_id)).mappings().one_or_none()
            return None if row is None else _state(row)

    def claim_next(self, worker_id: str, lease_duration: timedelta) -> JobClaim | None:
        _safe(worker_id, 128)
        _duration(lease_duration)
        with self._transaction() as session:
            due = or_(
                and_(
                    _JOB.c.status == "RETRYABLE",
                    or_(
                        _JOB.c.retry_after_at.is_(None),
                        _JOB.c.retry_after_at <= func.clock_timestamp(),
                    ),
                ),
                and_(
                    _JOB.c.status == "RUNNING",
                    _JOB.c.lease_expires_at <= func.clock_timestamp(),
                ),
            )
            row = (
                session.execute(
                    select(_JOB)
                    .where(due)
                    .order_by(_JOB.c.window_start_ms, _JOB.c.id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            state = _state(row)
            if state.attempt == BIGINT_MAX:
                raise JobIntegrityError("job attempt exhausted")
            now = _utc(session.scalar(select(func.clock_timestamp())))
            row = (
                session.execute(
                    update(_JOB)
                    .where(_JOB.c.id == state.spec.id)
                    .values(
                        status="RUNNING",
                        attempt=state.attempt + 1,
                        lease_owner=worker_id,
                        lease_expires_at=now + lease_duration,
                        retry_after_at=None,
                        last_error_code=None,
                        last_error_detail=None,
                        updated_at=now,
                    )
                    .returning(*_JOB.c)
                )
                .mappings()
                .one()
            )
            return _state(row).claim()

    @contextmanager
    def _fenced(self, claim: JobClaim) -> Iterator[tuple[Session, datetime]]:
        if type(claim) is not JobClaim:
            raise ValueError("expected exact job claim")
        with self._transaction() as session:
            row = (
                session.execute(select(_JOB).where(_JOB.c.id == claim.spec.id).with_for_update())
                .mappings()
                .one_or_none()
            )
            now = _utc(session.scalar(select(func.clock_timestamp())))
            state = None if row is None else _state(row)
            if (
                state is None
                or state.status != "RUNNING"
                or state.spec != claim.spec
                or state.lease_owner != claim.worker_id
                or state.attempt != claim.attempt
                or cast(datetime, state.lease_expires_at) <= now
            ):
                raise LeaseLost("job lease lost")
            yield session, now

    def _save(self, session: Session, claim: JobClaim, now: datetime, **values: object) -> JobState:
        fence = and_(
            _JOB.c.id == claim.spec.id,
            _JOB.c.status == "RUNNING",
            _JOB.c.lease_owner == claim.worker_id,
            _JOB.c.attempt == claim.attempt,
            _JOB.c.lease_expires_at > func.clock_timestamp(),
        )
        row = (
            session.execute(update(_JOB).where(fence).values(updated_at=now, **values).returning(*_JOB.c))
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise LeaseLost("job lease lost")
        return _state(row)

    def checkpoint(
        self,
        claim: JobClaim,
        cursor: CanonicalJsonObject,
        manifest: CanonicalJsonObject,
        extend_duration: timedelta,
    ) -> JobClaim:
        if type(cursor) is not CanonicalJsonObject or type(manifest) is not CanonicalJsonObject:
            raise ValueError("expected canonical checkpoint objects")
        _duration(extend_duration)
        with self._fenced(claim) as (session, now):
            return self._save(
                session,
                claim,
                now,
                source_cursor=cursor.thaw(),
                progress_manifest=manifest.thaw(),
                lease_expires_at=now + extend_duration,
            ).claim()

    def retry(self, claim: JobClaim, delay: timedelta, error_code: str, detail: str) -> JobState:
        _duration(delay, zero=True)
        _error(error_code, detail)
        with self._fenced(claim) as (session, now):
            return self._save(
                session,
                claim,
                now,
                status="RETRYABLE",
                lease_owner=None,
                lease_expires_at=None,
                retry_after_at=now + delay,
                last_error_code=error_code,
                last_error_detail=detail,
            )

    def fail(self, claim: JobClaim, error_code: str, detail: str) -> JobState:
        _error(error_code, detail)
        with self._fenced(claim) as (session, now):
            return self._save(
                session,
                claim,
                now,
                status="FAILED",
                lease_owner=None,
                lease_expires_at=None,
                last_error_code=error_code,
                last_error_detail=detail,
            )
