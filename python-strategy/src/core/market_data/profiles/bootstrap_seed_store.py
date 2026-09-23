"""Immutable bootstrap pins; statement bounds are not end-to-end deadlines.

No callback or external input is awaited in a transaction. ACK-unknown callers
may request one fresh confirmation via pin_confirmed; no automatic insert retry.
Requirements digest covers the versioned canonical sorted requirements array;
policy digest is the seed's already-bound availability policy digest. C is content,
not part of the durable key. This owner makes no callback/replay completion claim.
"""

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import json
from typing import Any, cast

from sqlalchemy import Table, and_, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, RowMapping
from sqlalchemy.orm import Session

from .bootstrap_seed import (
    BootstrapHistoryEvidence,
    BootstrapKey,
    BootstrapSeed,
    _bytes,
)
from .orm import BootstrapSeed as InputRow
from .repository import TransactionWaitPolicy
from .read_types import _integer
from src.core.data_provider import timeframe_to_ms

_TABLE = cast(Table, InputRow.__table__)


class BootstrapSeedConflict(ValueError):
    def __init__(self) -> None:
        super().__init__("BOOTSTRAP_SEED_CONFLICT")


class BootstrapSeedIntegrityError(ValueError):
    def __init__(self) -> None:
        super().__init__("BOOTSTRAP_SEED_INTEGRITY")


class BootstrapSeedAdmissionError(ValueError):
    def __init__(self) -> None:
        super().__init__("BOOTSTRAP_SEED_ADMISSION")


_ADMISSION_GRANT = object()


@dataclass(slots=True)
class _AdmissionLease:
    key: BootstrapKey
    connection: Connection
    active: bool = True


@dataclass(frozen=True, slots=True, init=False)
class BootstrapSeedAdmission:
    """Granted shared lease; authority covers cooperating startup callers only.

    SQL history authority is not configured; this handle is not ABSENT proof.
    This does not enable activation or protect against raw writers.
    """

    key: BootstrapKey
    _lease: _AdmissionLease = field(repr=False)

    def __init__(
        self,
        key: BootstrapKey | None = None,
        *,
        _grant: object = None,
        _lease: _AdmissionLease | None = None,
    ) -> None:
        if (
            _grant is not _ADMISSION_GRANT
            or type(key) is not BootstrapKey
            or type(_lease) is not _AdmissionLease
            or _lease.key is not key
        ):
            raise BootstrapSeedAdmissionError()
        object.__setattr__(self, "_lease", _lease)
        object.__setattr__(self, "key", key)

    @property
    def _active(self) -> bool:
        return self._lease.active

    def __deepcopy__(self, memo: dict[int, Any]) -> "BootstrapSeedAdmission":
        return self

    def read_history(
        self, key: BootstrapKey, boundary_bar_start_ms: int
    ) -> BootstrapHistoryEvidence:
        try:
            _integer(boundary_bar_start_ms)
            if (
                not self._active
                or type(key) is not BootstrapKey
                or key != self.key
                or boundary_bar_start_ms % timeframe_to_ms(key.timeframe)
            ):
                raise ValueError
        except ValueError:
            raise BootstrapSeedAdmissionError() from None
        # SQL history authority is not configured in this lifecycle slice.
        raise BootstrapSeedAdmissionError()


def _admission_lock_key(key: BootstrapKey) -> int:
    if type(key) is not BootstrapKey:
        raise BootstrapSeedAdmissionError()
    scope = {
        name: getattr(key, name)
        for name in (
            "environment",
            "execution_scope_id",
            "strategy_id",
            "product_id",
            "timeframe",
        )
    }
    scope["namespace"] = "bootstrap_initial_admission_v1"
    return int.from_bytes(
        hashlib.sha256(_bytes(scope)).digest()[:8], "big", signed=True
    )


class BootstrapSeedPinStatus(StrEnum):
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"
    UNCONFIRMED = "UNCONFIRMED"


@dataclass(frozen=True, slots=True)
class BootstrapSeedRecord:
    value: BootstrapSeed
    recorded_at: datetime
    already_present: bool

    def __post_init__(self) -> None:
        stamp = self.recorded_at
        if (
            type(self.value) is not BootstrapSeed
            or type(self.already_present) is not bool
            or type(stamp) is not datetime
            or stamp.tzinfo is not timezone.utc
            or stamp.microsecond % 1000
        ):
            raise BootstrapSeedIntegrityError()


@dataclass(frozen=True, slots=True)
class BootstrapSeedPinResult:
    status: BootstrapSeedPinStatus
    record: BootstrapSeedRecord | None = None

    def __post_init__(self) -> None:
        if (
            type(self.status) is not BootstrapSeedPinStatus
            or (
                self.status is BootstrapSeedPinStatus.CONFIRMED
                and type(self.record) is not BootstrapSeedRecord
            )
            or (
                self.status is not BootstrapSeedPinStatus.CONFIRMED
                and self.record is not None
            )
        ):
            raise BootstrapSeedIntegrityError()


def _values(value: BootstrapSeed) -> dict[str, Any]:
    return dict(
        **asdict(value.key),
        seed_id=value.key.seed_id,
        requirements_digest=hashlib.sha256(
            _bytes(
                {
                    "schema_version": 1,
                    "requirements": [
                        json.loads(r.canonical_bytes) for r in value.requirements
                    ],
                }
            )
        ).hexdigest(),
        policy_digest=value.availability_policy_digest,
        dataset_digest=value.dataset_digest,
        seed_digest=value.digest,
        cutover_ms=value.cutover_ms,
        lookback=value.lookback,
        canonical_payload=value.canonical_bytes,
    )


def _hydrate(
    row: RowMapping, key: BootstrapKey, already_present: bool
) -> BootstrapSeedRecord:
    try:
        raw = row["canonical_payload"]
        # psycopg2 BYTEA is memoryview; normalize only the driver's binary forms.
        if type(raw) is memoryview:
            raw = raw.tobytes()
        value = BootstrapSeed.from_canonical_bytes(
            raw, max_seed_candles=9223372036854775807
        )
        for name, expected in _values(value).items():
            actual = raw if name == "canonical_payload" else row[name]
            if type(actual) is not type(expected) or actual != expected:
                raise ValueError
        if value.key != key:
            raise ValueError
        return BootstrapSeedRecord(value, row["recorded_at"], already_present)
    except (ValueError, TypeError, KeyError, OverflowError):
        raise BootstrapSeedIntegrityError() from None


def _exact_value(value: BootstrapSeed) -> None:
    if type(value) is not BootstrapSeed:
        raise BootstrapSeedIntegrityError()


class BootstrapSeedStore:
    def __init__(
        self,
        sessions: Callable[[], AbstractContextManager[Session]],
        policy: TransactionWaitPolicy = TransactionWaitPolicy(),
    ) -> None:
        if type(policy) is not TransactionWaitPolicy:
            raise BootstrapSeedIntegrityError()
        self._sessions, self._policy = sessions, policy

    @contextmanager
    def initial_admission(self, key: BootstrapKey) -> Iterator[BootstrapSeedAdmission]:
        """Serialize cooperating startup callers only, not arbitrary raw writers.

        Hold one physical connection until unlock; this is not history authority
        and grants no capability outside the context. Pin uses a second connection.
        """
        lock_key = _admission_lock_key(key)
        with self._sessions() as session:
            if (
                session.get_bind().dialect.name != "postgresql"
                or session.in_transaction()
            ):
                raise BootstrapSeedAdmissionError()
            with session.begin():
                try:
                    connection = session.connection()
                except Exception as error:
                    raise BootstrapSeedAdmissionError() from error
                if connection.closed or connection.invalidated:
                    raise BootstrapSeedAdmissionError()
                session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                session.execute(
                    text(
                        "SELECT set_config('lock_timeout', :lock_timeout, true), "
                        "set_config('statement_timeout', :statement_timeout, true)"
                    ),
                    {
                        "lock_timeout": f"{self._policy.lock_timeout_ms}ms",
                        "statement_timeout": f"{self._policy.statement_timeout_ms}ms",
                    },
                )
                try:
                    acquired = connection.execute(
                        text("SELECT pg_try_advisory_lock(:lock_key)"),
                        {"lock_key": lock_key},
                    ).scalar_one()
                except BaseException as error:
                    connection.invalidate()
                    if not isinstance(error, Exception):
                        raise
                    raise BootstrapSeedAdmissionError() from error
                if acquired is not True:
                    if acquired is not False:
                        connection.invalidate()
                    raise BootstrapSeedAdmissionError()
                lease = _AdmissionLease(key, connection)
                admission = BootstrapSeedAdmission(
                    key, _grant=_ADMISSION_GRANT, _lease=lease
                )
                try:
                    yield admission
                finally:
                    lease.active = False
                    try:
                        released = connection.execute(
                            text("SELECT pg_advisory_unlock(:lock_key)"),
                            {"lock_key": lock_key},
                        ).scalar_one()
                        if released is not True:
                            raise BootstrapSeedAdmissionError()
                    except BaseException as error:
                        connection.invalidate()
                        raise BootstrapSeedAdmissionError() from error

    @contextmanager
    def _transaction(self, *, read_only: bool = False) -> Iterator[Session]:
        with self._sessions() as session:
            if (
                session.get_bind().dialect.name != "postgresql"
                or session.in_transaction()
            ):
                raise BootstrapSeedIntegrityError()
            with session.begin():
                session.execute(
                    text(
                        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
                        + (", READ ONLY" if read_only else "")
                    )
                )
                session.execute(
                    text(
                        "SELECT set_config('lock_timeout', :lock_timeout, true), "
                        "set_config('statement_timeout', :statement_timeout, true), "
                        "set_config('TimeZone', 'UTC', true)"
                    ).bindparams(
                        lock_timeout=f"{self._policy.lock_timeout_ms}ms",
                        statement_timeout=f"{self._policy.statement_timeout_ms}ms",
                    )
                )
                yield session

    def _lookup(
        self, session: Session, key: BootstrapKey
    ) -> BootstrapSeedRecord | None:
        query = (
            select(_TABLE)
            .where(
                or_(
                    _TABLE.c.seed_id == key.seed_id,
                    and_(
                        *(
                            _TABLE.c[name] == value
                            for name, value in asdict(key).items()
                        )
                    ),
                )
            )
            .limit(2)
        )
        rows = session.execute(query).mappings().all()
        if len(rows) > 1:
            raise BootstrapSeedIntegrityError()
        return _hydrate(rows[0], key, True) if rows else None

    @staticmethod
    def _match(
        record: BootstrapSeedRecord, expected: BootstrapSeed
    ) -> BootstrapSeedRecord:
        if record.value.canonical_bytes != expected.canonical_bytes:
            raise BootstrapSeedConflict()
        return record

    def get(self, key: BootstrapKey) -> BootstrapSeedRecord | None:
        if type(key) is not BootstrapKey:
            raise BootstrapSeedIntegrityError()
        with self._transaction(read_only=True) as session:
            return self._lookup(session, key)

    def confirm(self, expected: BootstrapSeed) -> BootstrapSeedRecord | None:
        """Fresh read-only exact confirmation, including after a lost commit ACK."""
        _exact_value(expected)
        record = self.get(expected.key)
        return self._match(record, expected) if record is not None else None

    def pin(self, value: BootstrapSeed) -> BootstrapSeedRecord:
        _exact_value(value)
        with self._transaction() as session:
            existing = self._lookup(session, value.key)
            if existing is not None:
                return self._match(existing, value)
            row = (
                session.execute(
                    insert(_TABLE)
                    .values(**_values(value))
                    .on_conflict_do_nothing()
                    .returning(_TABLE)
                )
                .mappings()
                .one_or_none()
            )
            if row is not None:
                return self._match(_hydrate(row, value.key, False), value)
            winner = self._lookup(session, value.key)
            if winner is None:
                raise BootstrapSeedIntegrityError()
            return self._match(winner, value)

    def pin_confirmed(self, value: BootstrapSeed) -> BootstrapSeedPinResult:
        """Pin once and use at most one fresh read to classify an unknown ACK."""
        try:
            return BootstrapSeedPinResult(
                BootstrapSeedPinStatus.CONFIRMED, self.pin(value)
            )
        except (BootstrapSeedConflict, BootstrapSeedIntegrityError):
            return BootstrapSeedPinResult(BootstrapSeedPinStatus.FAILED)
        except Exception:
            try:
                record = self.confirm(value)
            except (BootstrapSeedConflict, BootstrapSeedIntegrityError):
                return BootstrapSeedPinResult(BootstrapSeedPinStatus.FAILED)
            except Exception:
                return BootstrapSeedPinResult(BootstrapSeedPinStatus.UNCONFIRMED)
            return BootstrapSeedPinResult(
                BootstrapSeedPinStatus.CONFIRMED
                if record is not None
                else BootstrapSeedPinStatus.FAILED,
                record,
            )
