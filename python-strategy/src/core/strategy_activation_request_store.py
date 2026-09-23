"""Strict durable readback only; no admission, writes, retries or runtime work."""

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any
from enum import Enum
from sqlalchemy.engine import RowMapping

from src.core.strategy_activation_intent import (
    ProfileActivationRequest,
    MAX_ACTIVATION_REQUEST_BYTES,
)


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
        raise ProfileActivationRequestIntegrityError()


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
