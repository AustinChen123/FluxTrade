"""Durable request admission/readback; no terminal mutations, retries or runtime work."""

from dataclasses import dataclass
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime, timezone
import re
from typing import Any, cast
from enum import Enum
from sqlalchemy.engine import RowMapping
from sqlalchemy import Table, select, text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from src.core.orm_models import StrategyState

from src.core.strategy_activation_intent import (
    ProfileActivationRequest,
    MAX_ACTIVATION_REQUEST_BYTES,
    ProfileActivationIntent,
    ProfileActivationAdmission,
    classify_profile_activation_intent,
)
from src.core.market_data.profiles.orm import ProfileActivationRequest as RequestRow
from src.core.market_data.profiles.repository import TransactionWaitPolicy

_TABLE = cast(Table, RequestRow.__table__)


class ProfileActivationRequestValidationError(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_ACTIVATION_REQUEST_INVALID")


class ProfileActivationRequestConflict(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_ACTIVATION_REQUEST_CONFLICT")


class ProfileActivationRequestStale(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_ACTIVATION_REQUEST_STALE")


class ProfileActivationRequestIntegrityError(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_ACTIVATION_REQUEST_INTEGRITY")


class ProfileActivationRequestStatus(Enum):
    PENDING = "PENDING"
    CONSUMED = "CONSUMED"
    CANCELLED = "CANCELLED"
    STALE = "STALE"


def _validate_request_id(value: str) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value, re.ASCII) is None:
        raise ProfileActivationRequestValidationError()


def _stamp(value: object) -> bool:
    return (
        type(value) is datetime
        and value.tzinfo is timezone.utc
        and value.microsecond % 1000 == 0
    )


@dataclass(frozen=True, slots=True)
class ProfileActivationRequestRecord:
    request: ProfileActivationRequest
    status: ProfileActivationRequestStatus
    requested_at: datetime
    terminal_at: datetime | None = None
    terminal_reason: str | None = None

    def __post_init__(self) -> None:
        if (
            type(self.request) is not ProfileActivationRequest
            or type(self.status) is not ProfileActivationRequestStatus
            or not _stamp(self.requested_at)
        ):
            raise ProfileActivationRequestIntegrityError()
        if self.status is ProfileActivationRequestStatus.PENDING:
            valid = self.terminal_at is None and self.terminal_reason is None
        else:
            valid = (
                _stamp(self.terminal_at)
                and self.terminal_at is not None
                and self.terminal_at >= self.requested_at
                and type(self.terminal_reason) is str
                and re.fullmatch(
                    r"[A-Z][A-Z0-9_]{0,127}", self.terminal_reason, re.ASCII
                )
                is not None
            )
        if not valid:
            raise ProfileActivationRequestIntegrityError()


def _hydrate(
    row: dict[str, Any] | RowMapping, request_id: str
) -> ProfileActivationRequestRecord:
    try:
        _validate_request_id(request_id)
        if type(row) not in (dict, RowMapping) or type(row["status"]) is not str:
            raise ValueError
        raw = row["canonical_payload"]
        if type(raw) is memoryview:
            if not 1 <= raw.nbytes <= MAX_ACTIVATION_REQUEST_BYTES:
                raise ValueError
            raw = raw.tobytes()
        value = ProfileActivationRequest.from_canonical_bytes(raw)
        key = value.intent.key
        expected = dict(
            request_id=value.request_id,
            payload_digest=value.payload_digest,
            environment=key.environment,
            execution_scope_id=key.execution_scope_id,
            strategy_id=key.strategy_id,
            expected_state_version=value.intent.expected_state_version,
            contract_version=1,
        )
        if value.request_id != request_id or any(
            type(row[name]) is not type(item) or row[name] != item
            for name, item in expected.items()
        ):
            raise ValueError
        return ProfileActivationRequestRecord(
            value,
            ProfileActivationRequestStatus(row["status"]),
            row["requested_at"],
            row["terminal_at"],
            row["terminal_reason"],
        )
    except (ValueError, TypeError, KeyError, OverflowError):
        raise ProfileActivationRequestIntegrityError() from None


class ProfileActivationRequestStore:
    def __init__(
        self,
        sessions: Callable[[], AbstractContextManager[Session]],
        policy: TransactionWaitPolicy = TransactionWaitPolicy(),
    ) -> None:
        if type(policy) is not TransactionWaitPolicy:
            raise ProfileActivationRequestValidationError()
        self._sessions, self._policy = sessions, policy

    @contextmanager
    def _transaction(self, *, read_only: bool) -> Iterator[Session]:
        with self._sessions() as session:
            if (
                session.get_bind().dialect.name != "postgresql"
                or session.in_transaction()
            ):
                raise ProfileActivationRequestIntegrityError()
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

    @staticmethod
    def _lookup(
        session: Session, predicate: Any
    ) -> ProfileActivationRequestRecord | None:
        rows = (
            session.execute(select(_TABLE).where(predicate).limit(2)).mappings().all()
        )
        if len(rows) > 1:
            raise ProfileActivationRequestIntegrityError()
        return _hydrate(rows[0], rows[0]["request_id"]) if rows else None

    def get(self, request_id: str) -> ProfileActivationRequestRecord | None:
        _validate_request_id(request_id)
        with self._transaction(read_only=True) as session:
            record = self._lookup(session, _TABLE.c.request_id == request_id)
            if record is not None and record.request.request_id != request_id:
                raise ProfileActivationRequestIntegrityError()
        return record

    def admit(
        self, request: ProfileActivationRequest, *, current: ProfileActivationIntent
    ) -> ProfileActivationRequestRecord:
        """Current is a trusted artifact/config/requirements snapshot supplied by caller.
        This store neither reconstructs nor authorizes that snapshot.
        """
        if (
            type(request) is not ProfileActivationRequest
            or type(current) is not ProfileActivationIntent
        ):
            raise ProfileActivationRequestValidationError()
        key = request.intent.key
        with self._transaction(read_only=False) as session:
            state = session.execute(
                select(StrategyState.version)
                .where(StrategyState.strategy_id == key.strategy_id)
                .with_for_update()
            ).one_or_none()
            if state is None:
                raise ProfileActivationRequestIntegrityError()
            version = state[0]
            if type(version) is not int or not 0 <= version <= 2147483647:
                raise ProfileActivationRequestIntegrityError()
            existing = self._lookup(session, _TABLE.c.request_id == request.request_id)
            if existing is None:
                if (
                    classify_profile_activation_intent(
                        request.intent, current=current, pending=None
                    )
                    is ProfileActivationAdmission.STALE
                    or version != request.intent.expected_state_version
                ):
                    raise ProfileActivationRequestStale()
                slot = (
                    (_TABLE.c.environment == key.environment)
                    & (_TABLE.c.execution_scope_id == key.execution_scope_id)
                    & (_TABLE.c.strategy_id == key.strategy_id)
                    & (_TABLE.c.status == "PENDING")
                )
                existing = self._lookup(session, slot)
                if existing is None:
                    values = dict(
                        request_id=request.request_id,
                        environment=key.environment,
                        execution_scope_id=key.execution_scope_id,
                        strategy_id=key.strategy_id,
                        expected_state_version=request.intent.expected_state_version,
                        canonical_payload=request.canonical_bytes,
                        payload_digest=request.payload_digest,
                        contract_version=1,
                        status="PENDING",
                    )
                    winner = (
                        session.execute(
                            insert(_TABLE)
                            .values(**values)
                            .on_conflict_do_nothing()
                            .returning(_TABLE)
                        )
                        .mappings()
                        .one_or_none()
                    )
                    existing = (
                        _hydrate(winner, request.request_id)
                        if winner is not None
                        else self._lookup(
                            session, _TABLE.c.request_id == request.request_id
                        )
                    )
                    if existing is None:
                        existing = self._lookup(session, slot)
                        if existing is None:
                            raise ProfileActivationRequestIntegrityError()
            if existing.request.canonical_bytes != request.canonical_bytes:
                raise ProfileActivationRequestConflict()
        return existing

    def confirm(
        self, expected: ProfileActivationRequest
    ) -> ProfileActivationRequestRecord | None:
        if type(expected) is not ProfileActivationRequest:
            raise ProfileActivationRequestValidationError()
        record = self.get(expected.request_id)
        if (
            record is not None
            and record.request.canonical_bytes != expected.canonical_bytes
        ):
            raise ProfileActivationRequestConflict()
        return record
