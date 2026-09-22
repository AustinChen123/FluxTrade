"""Immutable input pins. Statement bounds are not pool/connect/end-to-end deadlines.

No callback or external input is awaited in a transaction. ACK-unknown callers
must explicitly confirm in a new session; this owner never retries automatically.
"""

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, cast

from sqlalchemy import Table, and_, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from .decision_application import MarketDataDecisionKey
from .decision_input import MarketDataDecisionInput
from .orm import MarketDataDecisionInput as InputRow
from .repository import TransactionWaitPolicy

_TABLE = cast(Table, InputRow.__table__)


class DecisionInputConflict(ValueError):
    def __init__(self) -> None:
        super().__init__("DECISION_INPUT_CONFLICT")


class DecisionInputIntegrityError(ValueError):
    def __init__(self) -> None:
        super().__init__("DECISION_INPUT_INTEGRITY")


class DecisionInputPinStatus(StrEnum):
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"
    UNCONFIRMED = "UNCONFIRMED"


@dataclass(frozen=True, slots=True)
class DecisionInputRecord:
    value: MarketDataDecisionInput
    recorded_at: datetime
    already_present: bool

    def __post_init__(self) -> None:
        stamp = self.recorded_at
        if (
            type(self.value) is not MarketDataDecisionInput
            or type(self.already_present) is not bool
            or type(stamp) is not datetime
            or stamp.tzinfo is not timezone.utc
            or stamp.microsecond % 1000
        ):
            raise DecisionInputIntegrityError()


@dataclass(frozen=True, slots=True)
class DecisionInputPinResult:
    status: DecisionInputPinStatus
    record: DecisionInputRecord | None = None

    def __post_init__(self) -> None:
        if (
            type(self.status) is not DecisionInputPinStatus
            or (
                self.status is DecisionInputPinStatus.CONFIRMED
                and type(self.record) is not DecisionInputRecord
            )
            or (
                self.status is not DecisionInputPinStatus.CONFIRMED
                and self.record is not None
            )
        ):
            raise DecisionInputIntegrityError()


def _values(value: MarketDataDecisionInput) -> dict[str, Any]:
    return dict(
        **asdict(value.key),
        input_id=value.input_id,
        requirements_digest=value.requirements_digest,
        policy_digest=value.policy_digest,
        input_digest=value.input_digest,
        decision_time_ms=value.decision_time_ms,
        contract_version=1,
        canonical_payload=value.canonical_bytes,
    )


def _hydrate(
    row: RowMapping, key: MarketDataDecisionKey, already_present: bool
) -> DecisionInputRecord:
    try:
        raw = row["canonical_payload"]
        # psycopg2 BYTEA is memoryview; normalize only the driver's binary forms.
        if type(raw) is memoryview:
            raw = raw.tobytes()
        value = MarketDataDecisionInput.from_canonical_bytes(raw)
        for name, expected in _values(value).items():
            actual = raw if name == "canonical_payload" else row[name]
            if type(actual) is not type(expected) or actual != expected:
                raise ValueError
        if value.key != key:
            raise ValueError
        return DecisionInputRecord(value, row["recorded_at"], already_present)
    except (ValueError, TypeError, KeyError, OverflowError):
        raise DecisionInputIntegrityError() from None


def _exact_value(value: MarketDataDecisionInput) -> None:
    if type(value) is not MarketDataDecisionInput:
        raise DecisionInputIntegrityError()


class DecisionInputStore:
    def __init__(
        self,
        sessions: Callable[[], AbstractContextManager[Session]],
        policy: TransactionWaitPolicy = TransactionWaitPolicy(),
    ) -> None:
        if type(policy) is not TransactionWaitPolicy:
            raise DecisionInputIntegrityError()
        self._sessions, self._policy = sessions, policy

    @contextmanager
    def _transaction(self, *, read_only: bool = False) -> Iterator[Session]:
        with self._sessions() as session:
            if (
                session.get_bind().dialect.name != "postgresql"
                or session.in_transaction()
            ):
                raise DecisionInputIntegrityError()
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
        self, session: Session, key: MarketDataDecisionKey
    ) -> DecisionInputRecord | None:
        query = (
            select(_TABLE)
            .where(
                or_(
                    _TABLE.c.input_id == key.input_id,
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
            raise DecisionInputIntegrityError()
        return _hydrate(rows[0], key, True) if rows else None

    @staticmethod
    def _match(
        record: DecisionInputRecord, expected: MarketDataDecisionInput
    ) -> DecisionInputRecord:
        if record.value.canonical_bytes != expected.canonical_bytes:
            raise DecisionInputConflict()
        return record

    def get(self, key: MarketDataDecisionKey) -> DecisionInputRecord | None:
        if type(key) is not MarketDataDecisionKey:
            raise DecisionInputIntegrityError()
        with self._transaction(read_only=True) as session:
            return self._lookup(session, key)

    def confirm(self, expected: MarketDataDecisionInput) -> DecisionInputRecord | None:
        """Fresh read-only exact confirmation, including after a lost commit ACK."""
        _exact_value(expected)
        record = self.get(expected.key)
        return self._match(record, expected) if record is not None else None

    def pin(self, value: MarketDataDecisionInput) -> DecisionInputRecord:
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
                raise DecisionInputIntegrityError()
            return self._match(winner, value)

    def pin_confirmed(self, value: MarketDataDecisionInput) -> DecisionInputPinResult:
        """Pin once and use at most one fresh read to classify an unknown ACK."""
        try:
            return DecisionInputPinResult(
                DecisionInputPinStatus.CONFIRMED, self.pin(value)
            )
        except (DecisionInputConflict, DecisionInputIntegrityError):
            return DecisionInputPinResult(DecisionInputPinStatus.FAILED)
        except Exception:
            try:
                record = self.confirm(value)
            except (DecisionInputConflict, DecisionInputIntegrityError):
                return DecisionInputPinResult(DecisionInputPinStatus.FAILED)
            except Exception:
                return DecisionInputPinResult(DecisionInputPinStatus.UNCONFIRMED)
            return DecisionInputPinResult(
                DecisionInputPinStatus.CONFIRMED
                if record is not None
                else DecisionInputPinStatus.FAILED,
                record,
            )
