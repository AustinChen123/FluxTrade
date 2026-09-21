"""Short PostgreSQL publication transactions; no jobs, selectors or cleanup."""

import hashlib
import json
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import cast

from sqlalchemy import Table, and_, func, insert, select, text, update
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from .orm import VolumeProfileBin, VolumeProfileSnapshot
from .publication import CanonicalJsonObject, VerifiedProfilePublication
from .types import BIGINT_MAX, ProfileBin, VolumeProfileContent

_SNAPSHOT = cast(Table, VolumeProfileSnapshot.__table__)
_BIN = cast(Table, VolumeProfileBin.__table__)


@dataclass(frozen=True, slots=True)
class TransactionWaitPolicy:
    """Bounded PostgreSQL waits, local to one profile persistence transaction."""

    lock_timeout_ms: int = 2000
    statement_timeout_ms: int = 10000

    def __post_init__(self) -> None:
        if (type(self.lock_timeout_ms) is not int or type(self.statement_timeout_ms) is not int
                or not 1 <= self.lock_timeout_ms <= 60000
                or not self.lock_timeout_ms <= self.statement_timeout_ms <= 300000):
            raise ValueError("invalid profile transaction wait policy")


def _configure_write_transaction(session: Session, policy: TransactionWaitPolicy) -> None:
    session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
    session.execute(text(
        "SELECT set_config('lock_timeout', :lock_timeout, true), "
        "set_config('statement_timeout', :statement_timeout, true)"
    ).bindparams(lock_timeout=f"{policy.lock_timeout_ms}ms",
                 statement_timeout=f"{policy.statement_timeout_ms}ms"))


class ProfileIntegrityError(ValueError):
    """Stored data does not satisfy the exact publication contract."""


def _require(condition: bool) -> None:
    if not condition:
        raise ProfileIntegrityError("profile integrity check failed")


def _utc(value: object) -> datetime:
    _require(type(value) is datetime)
    stamp = cast(datetime, value)
    _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
    return stamp.astimezone(timezone.utc)


def _logical(content: VolumeProfileContent) -> dict[str, str | int]:
    return {
        key: getattr(content, key)
        for key in (
            "product_id",
            "window_start_ms",
            "window_end_ms",
            "grid_id",
            "algorithm_version",
        )
    }


def _lock_parts(logical: dict[str, str | int]) -> tuple[int, int]:
    raw = json.dumps(logical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).digest()
    first = int.from_bytes(digest[:4], signed=True)
    return first, int.from_bytes(digest[4:8], signed=True)


@dataclass(frozen=True, slots=True)
class PublishedProfile:
    snapshot_id: str
    revision: int
    content_sha256: str
    already_present: bool
    computed_at: datetime
    published_at: datetime
    publication: VerifiedProfilePublication


def _verify(
    session: Session,
    row: RowMapping,
    quality: str = "VERIFIED",
    expected: VerifiedProfilePublication | None = None,
) -> VerifiedProfilePublication:
    """Package-internal exact readback shared only by profile persistence owners."""
    bins = (
        session.execute(
            select(_BIN)
            .where(_BIN.c.snapshot_id == row["id"])
            .order_by(_BIN.c.bin_index)
        )
        .mappings()
        .all()
    )
    try:
        content = VolumeProfileContent(
            row["product_id"],
            row["window_start_ms"],
            row["window_end_ms"],
            row["grid_id"],
            row["bin_origin"],
            row["bin_step"],
            row["algorithm_version"],
            tuple(
                ProfileBin(
                    b["bin_index"],
                    b["base_volume"],
                    b["quote_volume"],
                    b["aggregate_count"],
                )
                for b in bins
            ),
        )
        available = row["source_available_at"]
        publication = VerifiedProfilePublication(
            content,
            CanonicalJsonObject(row["source_manifest"]),
            CanonicalJsonObject(row["reconciliation"]),
            None if available is None else _utc(available),
            row["availability_basis"],
            row["raw_retention_state"],
        )
        _require(row["id"] == row["content_sha256"] == content.content_sha256)
        _require(row["period"] == "1d" and row["timezone"] == "UTC")
        _require(type(row["revision"]) is int and 0 < row["revision"] <= BIGINT_MAX)
        _require(
            type(row["base_volume"]) is Decimal and type(row["quote_volume"]) is Decimal
        )
        for field in (
            "base_volume",
            "quote_volume",
            "aggregate_count",
            "occupied_bins",
        ):
            _require(row[field] == getattr(content, field))
        _require(row["quality"] == quality)
        _utc(row["computed_at"])
        if quality == "VERIFIED":
            _utc(row["published_at"])
        else:
            _require(row["published_at"] is None)
        _require(expected is None or publication == expected)
        return publication
    except (ValueError, TypeError, KeyError, OverflowError):
        raise ProfileIntegrityError("profile integrity check failed") from None


def _result(
    row: RowMapping, publication: VerifiedProfilePublication, present: bool
) -> PublishedProfile:
    return PublishedProfile(
        row["id"],
        row["revision"],
        row["content_sha256"],
        present,
        _utc(row["computed_at"]),
        _utc(row["published_at"]),
        publication,
    )


def _publish_in_transaction(session: Session, publication: VerifiedProfilePublication) -> PublishedProfile:
    """Package-internal: caller owns the configured, active PostgreSQL transaction."""
    if type(publication) is not VerifiedProfilePublication:
        raise ValueError("expected exact verified publication")
    if session.get_bind().dialect.name != "postgresql" or not session.in_transaction():
        raise ValueError("publication requires an active PostgreSQL transaction")
    content = publication.content
    logical = _logical(content)
    scope = and_(*(_SNAPSHOT.c[key] == value for key, value in logical.items()))
    first, second = _lock_parts(logical)
    session.execute(
        text("SELECT pg_advisory_xact_lock(:first, :second)"),
        {"first": first, "second": second},
    )
    existing = (
        session.execute(
            select(_SNAPSHOT).where(
                scope, _SNAPSHOT.c.content_sha256 == content.content_sha256
            )
        )
        .mappings()
        .one_or_none()
    )
    if existing is not None:
        return _result(existing, _verify(session, existing), True)
    prior = session.scalar(
        select(func.max(_SNAPSHOT.c.revision)).where(scope)
    )
    _require(
        prior is None or (type(prior) is int and 0 < prior < BIGINT_MAX)
    )
    revision = 1 if prior is None else prior + 1
    now = _utc(session.scalar(select(func.clock_timestamp())))
    snapshot_id = content.content_sha256
    session.execute(
        insert(_SNAPSHOT),
        {
            **logical,
            "id": snapshot_id,
            "period": "1d",
            "timezone": "UTC",
            "bin_origin": content.bin_origin,
            "bin_step": content.bin_step,
            "revision": revision,
            "content_sha256": snapshot_id,
            "source_manifest": publication.source_manifest.thaw(),
            "reconciliation": publication.reconciliation.thaw(),
            "base_volume": content.base_volume,
            "quote_volume": content.quote_volume,
            "aggregate_count": content.aggregate_count,
            "occupied_bins": content.occupied_bins,
            "quality": "PARTIAL",
            "computed_at": now,
            "published_at": None,
            "source_available_at": publication.source_available_at,
            "availability_basis": publication.availability_basis,
            "raw_retention_state": publication.raw_retention_state,
        },
    )
    if content.bins:
        session.execute(
            insert(_BIN),
            [
                {"snapshot_id": snapshot_id, **asdict(b)}
                for b in content.bins
            ],
        )
    session.flush()
    row = (
        session.execute(
            select(_SNAPSHOT).where(_SNAPSHOT.c.id == snapshot_id)
        )
        .mappings()
        .one()
    )
    _verify(session, row, "PARTIAL", publication)
    row = (
        session.execute(
            update(_SNAPSHOT)
            .where(_SNAPSHOT.c.id == snapshot_id)
            .values(quality="VERIFIED", published_at=func.clock_timestamp())
            .returning(*_SNAPSHOT.c)
        )
        .mappings()
        .one()
    )
    return _result(row, _verify(session, row, expected=publication), False)


class ProfileRepository:
    def __init__(self, sessions: Callable[[], AbstractContextManager[Session]],
                 policy: TransactionWaitPolicy = TransactionWaitPolicy()) -> None:
        if type(policy) is not TransactionWaitPolicy:
            raise ValueError("expected exact profile transaction wait policy")
        self._sessions = sessions
        self._policy = policy

    @staticmethod
    def _check_session(session: Session) -> None:
        if session.get_bind().dialect.name != "postgresql":
            raise ValueError("profile repository requires PostgreSQL")
        if session.in_transaction():
            raise ValueError("profile repository requires a fresh transaction")

    def publish(self, publication: VerifiedProfilePublication) -> PublishedProfile:
        if type(publication) is not VerifiedProfilePublication:
            raise ValueError("expected exact verified publication")
        with self._sessions() as session:
            self._check_session(session)
            with session.begin():
                _configure_write_transaction(session, self._policy)
                return _publish_in_transaction(session, publication)

    def get_verified(self, snapshot_id: str) -> PublishedProfile | None:
        with self._sessions() as session:
            self._check_session(session)
            with session.begin():
                row = (
                    session.execute(
                        select(_SNAPSHOT).where(_SNAPSHOT.c.id == snapshot_id)
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None or row["quality"] != "VERIFIED":
                    return None
                return _result(row, _verify(session, row), True)
