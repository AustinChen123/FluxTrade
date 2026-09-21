"""Bounded candidate and verified-snapshot reads; no freshness or selection policy.

Transaction waits are bounded; connection/pool and end-to-end deadlines are not.
"""
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
import json
from typing import Any, cast

from sqlalchemy import Table, Text, and_, case, cast as sql_cast, func, literal_column, select, text
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement

from .invalidation import _event
from .orm import MarketDataInvalidation, VolumeProfileBin, VolumeProfileSnapshot
from .read_results import CANDIDATE_ACCOUNTING_BYTES, MAX_CANDIDATES, MAX_MANIFEST_DAYS, MAX_OPERATION_BYTES, ProfileCandidate
from .read_results import (
    BIN_ACCOUNTING_BYTES, EVENT_ACCOUNTING_BYTES, HEADER_ACCOUNTING_BYTES, MAX_BINS,
    MAX_INVALIDATIONS_PER_OPERATION, MAX_INVALIDATIONS_PER_SNAPSHOT, MAX_METADATA_TEXT_BYTES,
    MAX_NUMERIC_TEXT_BYTES, VerifiedDailyRead,
)
from .read_types import DailyProfileRef, _hex, _integer, _scope, _window
from .repository import ProfileIntegrityError, TransactionWaitPolicy, _verify_rows

_SNAPSHOT = cast(Table, VolumeProfileSnapshot.__table__)
_INVALIDATION = cast(Table, MarketDataInvalidation.__table__)
_BIN = cast(Table, VolumeProfileBin.__table__)


class ProfileReadTooLarge(ValueError):
    """A reader bound was exceeded; no partial result is returned."""


def _timestamp_projection(name: str, *, nullable: bool = False, table: Table = _SNAPSHOT):
    column = table.c[name]
    finite = and_(column.is_not(None), func.isfinite(column),
                  column >= literal_column("TIMESTAMPTZ '0001-01-01 00:00:00+00'"),
                  column < literal_column("TIMESTAMPTZ '10000-01-01 00:00:00+00'"))
    valid = (column.is_(None) | finite) if nullable else finite
    return valid.label(name + "_valid"), case((finite, column), else_=None).label(name)


def _bounded_count(value: object, maximum: int) -> int:
    if type(value) is not int or value < 0:
        raise ProfileIntegrityError("profile integrity check failed")
    if value > maximum:
        raise ProfileReadTooLarge("profile read exceeds limit")
    return value


def _size_flag(value: object) -> None:
    if type(value) is not bool:
        raise ProfileIntegrityError("profile integrity check failed")
    if value:
        raise ProfileReadTooLarge("profile read exceeds limit")


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

    def get_verified(self, snapshot_id: str) -> VerifiedDailyRead | None:
        """Read one complete immutable snapshot, including revocation evidence, not policy."""
        _hex(snapshot_id)
        fields = ("id", "revision", "content_sha256", "product_id", "window_start_ms", "window_end_ms", "grid_id",
                  "algorithm_version", "period", "timezone", "quality", "aggregate_count", "occupied_bins",
                  "availability_basis", "raw_retention_state")
        metadata = ("source_manifest", "reconciliation")
        numeric = ("bin_origin", "bin_step", "base_volume", "quote_volume")
        projection: list[ColumnElement[Any]] = [_SNAPSHOT.c[name] for name in fields]
        for name in ("computed_at", "published_at", "source_available_at"):
            projection.extend(_timestamp_projection(name, nullable=name == "source_available_at"))
        for name in metadata:
            length = func.octet_length(sql_cast(_SNAPSHOT.c[name], Text))
            projection.extend(((length > MAX_METADATA_TEXT_BYTES).label(name + "_oversized"),
                               case((length <= MAX_METADATA_TEXT_BYTES, length), else_=MAX_METADATA_TEXT_BYTES + 1).label(name + "_bytes")))
        for name in numeric:
            length = func.octet_length(sql_cast(_SNAPSHOT.c[name], Text))
            projection.extend(((length > MAX_NUMERIC_TEXT_BYTES).label(name + "_oversized"),
                               case((length <= MAX_NUMERIC_TEXT_BYTES, _SNAPSHOT.c[name]), else_=None).label(name)))
        with self._transaction() as session:
            header = session.execute(select(*projection).where(_SNAPSHOT.c.id == snapshot_id)).mappings().one_or_none()
            if header is None or header["quality"] != "VERIFIED":
                return None
            # Queries are outside reconstruction catches: DBAPI errors retain their identity.
            try:
                if header["id"] != snapshot_id or any(header[name + "_valid"] is not True
                        for name in ("computed_at", "published_at", "source_available_at")):
                    raise ValueError
                sizes = []
                for name in metadata:
                    _size_flag(header[name + "_oversized"])
                    sizes.append(_bounded_count(header[name + "_bytes"], MAX_METADATA_TEXT_BYTES))
                for name in numeric:
                    _size_flag(header[name + "_oversized"])
            except ProfileReadTooLarge:
                raise
            except (ValueError, TypeError, KeyError):
                raise ProfileIntegrityError("profile integrity check failed") from None
            bins_probe = select(*((func.octet_length(sql_cast(_BIN.c[name], Text)) > MAX_NUMERIC_TEXT_BYTES).label(name + "_oversized")
                                  for name in ("base_volume", "quote_volume"))).where(_BIN.c.snapshot_id == snapshot_id).limit(MAX_BINS + 1).subquery()
            bin_counts = session.execute(select(func.count().label("count"),
                func.coalesce(func.bool_or(bins_probe.c.base_volume_oversized | bins_probe.c.quote_volume_oversized), False).label("oversized"))
                .select_from(bins_probe)).mappings().one()
            bin_count = _bounded_count(bin_counts.get("count"), MAX_BINS)
            _size_flag(bin_counts.get("oversized"))
            event_limit = min(MAX_INVALIDATIONS_PER_SNAPSHOT, MAX_INVALIDATIONS_PER_OPERATION)
            event_probe = select(_INVALIDATION.c.event_id).where(_INVALIDATION.c.snapshot_id == snapshot_id).limit(event_limit + 1).subquery()
            event_count = _bounded_count(session.scalar(select(func.count()).select_from(event_probe)), event_limit)
            if HEADER_ACCOUNTING_BYTES + sum(sizes) + bin_count * BIN_ACCOUNTING_BYTES + event_count * EVENT_ACCOUNTING_BYTES > MAX_OPERATION_BYTES:
                raise ProfileReadTooLarge("profile read exceeds limit")
            payload = session.execute(select(_SNAPSHOT.c.id, *(sql_cast(_SNAPSHOT.c[name], Text).label(name) for name in metadata))
                                      .where(_SNAPSHOT.c.id == snapshot_id)).mappings().one_or_none()
            bins = session.execute(select(_BIN.c.snapshot_id, _BIN.c.bin_index, _BIN.c.base_volume, _BIN.c.quote_volume, _BIN.c.aggregate_count)
                                   .where(_BIN.c.snapshot_id == snapshot_id).order_by(_BIN.c.snapshot_id, _BIN.c.bin_index).limit(MAX_BINS + 1)).mappings().all()
            events = session.execute(select(*(_INVALIDATION.c[name] for name in
                ("event_id", "snapshot_id", "reason_code", "replacement_snapshot_id", "source")),
                *_timestamp_projection("recorded_at", table=_INVALIDATION)).where(_INVALIDATION.c.snapshot_id == snapshot_id)
                .order_by(_INVALIDATION.c.snapshot_id, _INVALIDATION.c.recorded_at, _INVALIDATION.c.event_id)
                .limit(MAX_INVALIDATIONS_PER_OPERATION + 1)).mappings().all()
            _bounded_count(len(bins), MAX_BINS)
            _bounded_count(len(events), event_limit)
            try:
                if (payload is None or payload["id"] != snapshot_id or len(bins) != bin_count or len(events) != event_count
                        or any(b["snapshot_id"] != snapshot_id for b in bins)
                        or any(e["snapshot_id"] != snapshot_id or e["recorded_at_valid"] is not True for e in events)):
                    raise ValueError
                decoded = dict(header)
                for name, size in zip(metadata, sizes):
                    raw = payload[name]
                    if type(raw) is not str or len(raw.encode("utf-8")) != size:
                        raise ValueError
                    decoded[name] = json.loads(raw)
                publication = _verify_rows(cast(RowMapping, decoded), bins)
                invalidations = tuple(_event(e) for e in events)
                ref = DailyProfileRef(header["id"], header["revision"], header["content_sha256"], header["window_start_ms"], header["window_end_ms"])
                return VerifiedDailyRead(ref, header["computed_at"], header["published_at"], publication, invalidations)
            except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
                raise ProfileIntegrityError("profile integrity check failed") from None
