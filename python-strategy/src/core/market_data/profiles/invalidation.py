"""Confirmed append-only invalidations; bounded DB statements, not pool/connect deadlines."""
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import asdict, dataclass
from typing import cast

from sqlalchemy import Table, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from .orm import MarketDataInvalidation, VolumeProfileSnapshot
from .read_types import ProfileInvalidation, _hex, _safe
from .repository import TransactionWaitPolicy

_EVENT = cast(Table, MarketDataInvalidation.__table__)
_SNAPSHOT = cast(Table, VolumeProfileSnapshot.__table__)
_FIELDS = ("event_id", "snapshot_id", "reason_code", "replacement_snapshot_id", "source")
_LOGICAL = ("product_id", "window_start_ms", "window_end_ms", "grid_id", "algorithm_version")
MAX_INVALIDATION_MEMBERSHIP_IDS = 32 * 90  # Decision-input profiles × manifest days.


class InvalidationConflict(ValueError):
    """An event ID already binds different caller fields."""


class InvalidationReferenceError(ValueError):
    """Target/replacement headers do not satisfy the confirmed invalidation contract."""


class InvalidationIntegrityError(ValueError):
    """Malformed persisted event; not an absent event."""


class InvalidationReadTooLarge(ValueError):
    """A bounded read cannot return the complete snapshot event set."""


@dataclass(frozen=True, slots=True)
class ConfirmedInvalidationRequest:
    event_id: str
    snapshot_id: str
    reason_code: str
    replacement_snapshot_id: str | None
    source: str

    def __post_init__(self) -> None:
        _safe(self.event_id, 128)
        _hex(self.snapshot_id)
        _safe(self.reason_code)
        _safe(self.source)
        if self.replacement_snapshot_id is not None:
            _hex(self.replacement_snapshot_id)
            if self.replacement_snapshot_id == self.snapshot_id:
                raise ValueError("invalid profile replacement")


@dataclass(frozen=True, slots=True)
class InvalidationAppendResult:
    event: ProfileInvalidation
    already_present: bool

    def __post_init__(self) -> None:
        if type(self.event) is not ProfileInvalidation or type(self.already_present) is not bool:
            raise ValueError("invalid invalidation append result")


def _event(row: RowMapping) -> ProfileInvalidation:
    try:
        return ProfileInvalidation(**{key: row[key] for key in (*_FIELDS, "recorded_at")})
    except (ValueError, TypeError, KeyError, OverflowError):
        raise InvalidationIntegrityError("invalid stored invalidation") from None


def _existing(row: RowMapping, request: ConfirmedInvalidationRequest) -> InvalidationAppendResult:
    event = _event(row)
    if any(getattr(event, key) != getattr(request, key) for key in _FIELDS):
        raise InvalidationConflict("invalidation event conflict")
    return InvalidationAppendResult(event, True)


class ProfileInvalidationStore:
    def __init__(self, sessions: Callable[[], AbstractContextManager[Session]],
                 policy: TransactionWaitPolicy = TransactionWaitPolicy()) -> None:
        if type(policy) is not TransactionWaitPolicy:
            raise ValueError("expected exact transaction wait policy")
        self._sessions, self._policy = sessions, policy

    @contextmanager
    def _transaction(self, *, read_only: bool = False) -> Iterator[Session]:
        with self._sessions() as session:
            if session.get_bind().dialect.name != "postgresql" or session.in_transaction():
                raise ValueError("invalidation store requires fresh PostgreSQL transaction")
            with session.begin():
                session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED" + (", READ ONLY" if read_only else "")))
                session.execute(text(
                    "SELECT set_config('lock_timeout', :lock_timeout, true), "
                    "set_config('statement_timeout', :statement_timeout, true), set_config('TimeZone', 'UTC', true)"
                ).bindparams(lock_timeout=f"{self._policy.lock_timeout_ms}ms",
                             statement_timeout=f"{self._policy.statement_timeout_ms}ms"))
                yield session

    def append_confirmed(self, request: ConfirmedInvalidationRequest) -> InvalidationAppendResult:
        """Caller has already confirmed the data error; never infer it or retry automatically."""
        if type(request) is not ConfirmedInvalidationRequest:
            raise ValueError("expected exact confirmed invalidation request")
        with self._transaction() as session:
            query = select(_EVENT).where(_EVENT.c.event_id == request.event_id)
            existing = session.execute(query).mappings().one_or_none()
            if existing is not None:
                return _existing(existing, request)
            headers = []
            for identifier in (request.snapshot_id, request.replacement_snapshot_id):
                if identifier is not None:
                    header = session.execute(select(_SNAPSHOT.c.quality, *(_SNAPSHOT.c[key] for key in _LOGICAL))
                                             .where(_SNAPSHOT.c.id == identifier)).mappings().one_or_none()
                    if header is None or header["quality"] != "VERIFIED":
                        raise InvalidationReferenceError("invalid invalidation reference")
                    headers.append(header)
            if len(headers) == 2 and any(headers[0][key] != headers[1][key] for key in _LOGICAL):
                raise InvalidationReferenceError("invalid invalidation reference")
            row = session.execute(insert(_EVENT).values(**asdict(request)).on_conflict_do_nothing(
                index_elements=[_EVENT.c.event_id]).returning(_EVENT)).mappings().one_or_none()
            if row is not None:
                return InvalidationAppendResult(_existing(row, request).event, False)
            winner = session.execute(query).mappings().one_or_none()
            if winner is None:
                raise InvalidationIntegrityError("invalid stored invalidation")
            return _existing(winner, request)

    def any_revoked(self, snapshot_ids: tuple[str, ...]) -> bool:
        """One bounded membership read; no event materialization or automatic retry."""
        if type(snapshot_ids) is not tuple or len(snapshot_ids) > MAX_INVALIDATION_MEMBERSHIP_IDS:
            raise ValueError("invalid invalidation membership input")
        for identifier in snapshot_ids:
            _hex(identifier)
        identifiers = tuple(sorted(set(snapshot_ids)))
        if not identifiers:
            return False
        with self._transaction(read_only=True) as session:
            result = session.execute(select(select(_EVENT.c.snapshot_id).where(
                _EVENT.c.snapshot_id.in_(identifiers)).exists())).scalar_one()
            if type(result) is not bool:
                raise InvalidationIntegrityError("invalid invalidation membership result")
            return result

    def get(self, event_id: str) -> ProfileInvalidation | None:
        _safe(event_id, 128)
        with self._transaction(read_only=True) as session:
            row = session.execute(select(_EVENT).where(_EVENT.c.event_id == event_id)).mappings().one_or_none()
            return None if row is None else _event(row)

    def list_for_snapshot(self, snapshot_id: str, *, limit: int = 100) -> tuple[ProfileInvalidation, ...]:
        _hex(snapshot_id)
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("invalid invalidation read limit")
        with self._transaction(read_only=True) as session:
            rows = session.execute(select(_EVENT).where(_EVENT.c.snapshot_id == snapshot_id)
                                   .order_by(_EVENT.c.recorded_at, _EVENT.c.event_id).limit(limit + 1)).mappings().all()
            if len(rows) > limit:
                raise InvalidationReadTooLarge("invalidation read exceeds limit")
            return tuple(_event(row) for row in rows)
