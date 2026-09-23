"""Strict durable readback only; no admission, writes, retries or runtime work."""

from dataclasses import dataclass
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timezone
import re
from typing import Any, cast
from enum import Enum
from sqlalchemy.engine import RowMapping
from sqlalchemy import Table, select, text
from sqlalchemy.orm import Session

from src.core.strategy_activation_intent import (
    ProfileActivationRequest,
    MAX_ACTIVATION_REQUEST_BYTES,
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

    def get(self, request_id: str) -> ProfileActivationRequestRecord | None:
        _validate_request_id(request_id)
        with self._sessions() as session:
            if (
                session.get_bind().dialect.name != "postgresql"
                or session.in_transaction()
            ):
                raise ProfileActivationRequestIntegrityError()
            with session.begin():
                session.execute(
                    text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
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
                rows = (
                    session.execute(
                        select(_TABLE).where(_TABLE.c.request_id == request_id).limit(2)
                    )
                    .mappings()
                    .all()
                )
                if len(rows) > 1:
                    raise ProfileActivationRequestIntegrityError()
                record = _hydrate(rows[0], request_id) if rows else None
        return record

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
