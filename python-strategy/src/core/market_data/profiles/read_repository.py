"""Bounded header-only profile reads; no completeness, freshness or selection policy.

Transaction waits are bounded; connection/pool and end-to-end deadlines are not.
"""
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import cast

from sqlalchemy import Table, and_, case, func, literal_column, select, text
from sqlalchemy.orm import Session

from .orm import MarketDataInvalidation, VolumeProfileSnapshot
from .read_results import CANDIDATE_ACCOUNTING_BYTES, MAX_CANDIDATES, MAX_MANIFEST_DAYS, MAX_OPERATION_BYTES, ProfileCandidate
from .read_types import DailyProfileRef, _integer, _scope, _window
from .repository import ProfileIntegrityError, TransactionWaitPolicy

_SNAPSHOT = cast(Table, VolumeProfileSnapshot.__table__)
_INVALIDATION = cast(Table, MarketDataInvalidation.__table__)


class ProfileReadTooLarge(ValueError):
    """A reader bound was exceeded; no partial result is returned."""


def _timestamp_projection(name: str, *, nullable: bool = False):
    column = _SNAPSHOT.c[name]
    finite = and_(column.is_not(None), func.isfinite(column),
                  column >= literal_column("TIMESTAMPTZ '0001-01-01 00:00:00+00'"),
                  column < literal_column("TIMESTAMPTZ '10000-01-01 00:00:00+00'"))
    valid = (column.is_(None) | finite) if nullable else finite
    return valid.label(name + "_valid"), case((finite, column), else_=None).label(name)


class ProfileReadRepository:
    def __init__(self, sessions: Callable[[], AbstractContextManager[Session]],
                 policy: TransactionWaitPolicy = TransactionWaitPolicy(250, 1500)) -> None:
        if type(policy) is not TransactionWaitPolicy:
            raise ValueError("expected exact transaction wait policy")
        self._sessions, self._policy = sessions, policy

    @contextmanager
    def _transaction(self) -> Iterator[Session]:
        with self._sessions() as session:
            if session.get_bind().dialect.name != "postgresql" or session.in_transaction():
                raise ValueError("profile reader requires fresh PostgreSQL transaction")
            with session.begin():
                session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
                session.execute(text(
                    "SELECT set_config('lock_timeout', :lock_timeout, true), "
                    "set_config('statement_timeout', :statement_timeout, true), set_config('TimeZone', 'UTC', true)"
                ).bindparams(lock_timeout=f"{self._policy.lock_timeout_ms}ms",
                             statement_timeout=f"{self._policy.statement_timeout_ms}ms"))
                yield session

    def list_candidates(self, *, product_id: str, base_grid_id: str, algorithm_version: str,
                        start_ms: int, end_ms: int, revision: int | None = None) -> tuple[ProfileCandidate, ...]:
        _scope(product_id, base_grid_id, algorithm_version)
        _window(start_ms, end_ms, MAX_MANIFEST_DAYS)
        if revision is not None:
            _integer(revision, 1)
            _window(start_ms, end_ms, 1)
        revoked = select(1).where(_INVALIDATION.c.snapshot_id == _SNAPSHOT.c.id).exists().label("revoked")
        fields = ("id", "revision", "content_sha256", "window_start_ms", "window_end_ms",
                  "product_id", "grid_id", "algorithm_version", "availability_basis")
        query = select(*(_SNAPSHOT.c[name] for name in fields),
                       *_timestamp_projection("computed_at"), *_timestamp_projection("published_at"),
                       *_timestamp_projection("source_available_at", nullable=True), revoked).where(
            _SNAPSHOT.c.product_id == product_id, _SNAPSHOT.c.grid_id == base_grid_id,
            _SNAPSHOT.c.algorithm_version == algorithm_version, _SNAPSHOT.c.window_start_ms >= start_ms,
            _SNAPSHOT.c.window_end_ms <= end_ms, _SNAPSHOT.c.quality == "VERIFIED")
        if revision is not None:
            query = query.where(_SNAPSHOT.c.revision == revision)
        query = query.order_by(_SNAPSHOT.c.window_start_ms, _SNAPSHOT.c.revision, _SNAPSHOT.c.id).limit(MAX_CANDIDATES + 1)
        with self._transaction() as session:
            rows = session.execute(query).mappings().all()
            if len(rows) > MAX_CANDIDATES or len(rows) * CANDIDATE_ACCOUNTING_BYTES > MAX_OPERATION_BYTES:
                raise ProfileReadTooLarge("profile candidate read exceeds limit")
            candidates = []
            try:
                for row in rows:
                    if any(row[name + "_valid"] is not True for name in ("computed_at", "published_at", "source_available_at")):
                        raise ValueError
                    _scope(row["product_id"], row["grid_id"], row["algorithm_version"])
                    ref = DailyProfileRef(row["id"], row["revision"], row["content_sha256"], row["window_start_ms"], row["window_end_ms"])
                    if ((row["product_id"], row["grid_id"], row["algorithm_version"]) != (product_id, base_grid_id, algorithm_version)
                            or not start_ms <= ref.window_start_ms < ref.window_end_ms <= end_ms
                            or (revision is not None and ref.revision != revision)):
                        raise ValueError
                    candidates.append(ProfileCandidate(ref, row["computed_at"], row["published_at"], row["source_available_at"],
                                                       row["availability_basis"], row["revoked"]))
            except (ValueError, TypeError, KeyError, OverflowError):
                raise ProfileIntegrityError("profile integrity check failed") from None
            return tuple(candidates)
