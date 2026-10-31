"""Caller-transaction persistence and independent readback for Spider operations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, cast

from sqlalchemy import Table, select, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from src.core.spider_management.configuration_request import SpiderConfigurationRequestKind
from src.core.spider_management.configuration_request_store import (
    _configure_transaction,
    _hydrate as hydrate_configuration,
)
from src.core.spider_management.operation_request import (
    SpiderOperationRequest,
    SpiderOperationRequestConflict,
    SpiderOperationRequestError,
    SpiderOperationRequestIntegrityError,
    SpiderOperationRequestStale,
)
from src.core.spider_management.orm import (
    SpiderConfigurationRequest as ConfigurationRequestRow,
    SpiderOperationRequestRow,
    SpiderRuntimeOperation,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes

_OPERATION_TABLE = cast(Table, SpiderRuntimeOperation.__table__)
_REQUEST_TABLE = cast(Table, SpiderOperationRequestRow.__table__)
_CONFIG_TABLE = cast(Table, ConfigurationRequestRow.__table__)


class SpiderOperationReadbackStatus(Enum):
    CONFIRMED = "CONFIRMED"
    NOT_FOUND = "NOT_FOUND"
    UNKNOWN = "UNKNOWN"
    INVALID_EVIDENCE = "INVALID_EVIDENCE"


@dataclass(frozen=True, slots=True)
class SpiderOperationRecord:
    request: SpiderOperationRequest
    authorization_confirmed: bool
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class SpiderOperationReadback:
    status: SpiderOperationReadbackStatus
    record: SpiderOperationRecord | None = None


def _utc_millisecond(value: object) -> bool:
    return (
        type(value) is datetime
        and value.tzinfo is not None
        and value.utcoffset() == timezone.utc.utcoffset(value)
        and value.microsecond % 1000 == 0
    )


def _mapping(row: Mapping[Any, object]) -> SpiderOperationRecord:
    try:
        payload = row["request_payload"]
        if type(payload) is memoryview:
            payload = payload.tobytes()
        request = SpiderOperationRequest.from_canonical_bytes(cast(bytes, payload))
        recorded_at = row["recorded_at"]
        if (
            request.request_id != row["request_id"]
            or request.operation_id != row["operation_id"]
            or request.operation_core_sha256 != row["operation_core_sha256"]
            or request.request_kind != row["request_kind"]
            or request.authorization["revision"] != row["authorization_revision"]
            or request.proposal_request_id != row["proposal_request_id"]
            or (request.request_kind == "CONFIRMATION") is not row["authorization_confirmed"]
            or not _utc_millisecond(recorded_at)
            or row["environment"] != request.environment
            or row["execution_scope_id"] != request.execution_scope_id
            or row["strategy_id"] != request.strategy_id
            or row["actor"] != request.actor
            or row["idempotency_key"] != request.idempotency_key
            or row["operation_key"] != request.operation_key
        ):
            raise ValueError
        return SpiderOperationRecord(request, cast(bool, row["authorization_confirmed"]), cast(datetime, recorded_at))
    except (KeyError, TypeError, ValueError, OverflowError):
        raise SpiderOperationRequestIntegrityError() from None


class SpiderOperationRequestStore:
    def __init__(
        self,
        sessions: Callable[[], AbstractContextManager[Session]],
        *,
        lock_timeout_ms: int = 2000,
        statement_timeout_ms: int = 10000,
    ) -> None:
        if (
            type(lock_timeout_ms) is not int
            or type(statement_timeout_ms) is not int
            or not 1 <= lock_timeout_ms <= 60000
            or not lock_timeout_ms <= statement_timeout_ms <= 300000
        ):
            raise ValueError("invalid Spider operation readback timeouts")
        self._sessions = sessions
        self._lock_timeout_ms = lock_timeout_ms
        self._statement_timeout_ms = statement_timeout_ms

    @staticmethod
    def _require_transaction(session: Session) -> None:
        if not isinstance(session, Session) or not session.in_transaction() or not session.is_active:
            raise SpiderOperationRequestIntegrityError()

    @staticmethod
    def _record_existing(session: Session, request: SpiderOperationRequest) -> SpiderOperationRecord | None:
        row = session.execute(
            select(_REQUEST_TABLE).where(_REQUEST_TABLE.c.request_id == request.request_id)
        ).mappings().first()
        if row is None:
            return None
        record = _mapping(row)
        if record.request.canonical_request_bytes != request.canonical_request_bytes:
            raise SpiderOperationRequestConflict()
        return record

    @staticmethod
    def _validate_configuration(session: Session, request: SpiderOperationRequest) -> None:
        selection = cast(dict[str, object], request.operation_core["runtime_selection"])
        confirmation_id = cast(str, selection["confirmation_request_id"])
        row = session.execute(
            select(_CONFIG_TABLE).where(_CONFIG_TABLE.c.request_id == confirmation_id)
        ).mappings().first()
        if row is None:
            raise SpiderOperationRequestError("SPIDER_OPERATION_CONFIGURATION_UNAVAILABLE")
        configuration = hydrate_configuration(row)
        if (
            configuration.request.request_kind is not SpiderConfigurationRequestKind.CONFIRMATION
            or configuration.request.environment != request.environment
            or configuration.request.execution_scope_id != request.execution_scope_id
            or configuration.request.strategy_id != request.strategy_id
            or configuration.request.configuration.sha256 != request.operation_core["configuration_sha256"]
        ):
            raise SpiderOperationRequestConflict()

    @staticmethod
    def _head(session: Session, request: SpiderOperationRequest) -> SpiderRuntimeOperation | None:
        return session.execute(
            select(SpiderRuntimeOperation)
            .where(SpiderRuntimeOperation.operation_id == request.operation_id)
            .with_for_update()
        ).scalar_one_or_none()

    @staticmethod
    def _proposal_compatible(proposal: SpiderOperationRequest, confirmation: SpiderOperationRequest) -> bool:
        return (
            proposal.request_kind == "PROPOSAL"
            and proposal.request_id == confirmation.proposal_request_id
            and proposal.operation_id == confirmation.operation_id
            and proposal.operation_core_sha256 == confirmation.operation_core_sha256
            and proposal.operation_core == confirmation.operation_core
            and proposal.authorization == confirmation.authorization
            and proposal.environment == confirmation.environment
            and proposal.execution_scope_id == confirmation.execution_scope_id
            and proposal.strategy_id == confirmation.strategy_id
            and proposal.actor == confirmation.actor
            and proposal.operation_key == confirmation.operation_key
        )

    def record_in_transaction(self, session: Session, request: SpiderOperationRequest) -> SpiderOperationRecord:
        self._require_transaction(session)
        if session.get_bind().dialect.name != "postgresql":
            raise SpiderOperationRequestIntegrityError()
        _configure_transaction(
            session, self._lock_timeout_ms, self._statement_timeout_ms
        )
        existing = self._record_existing(session, request)
        if existing is not None:
            return existing
        self._validate_configuration(session, request)
        core_payload = request.operation_core
        core_bytes = request.canonical_request_bytes
        core_hash = request.operation_core_sha256

        head = self._head(session, request)
        if head is not None:
            # A concurrent exact retry may have committed while this caller
            # waited for the operation-head row lock.
            existing = self._record_existing(session, request)
            if existing is not None:
                return existing
        if head is None:
            if request.request_kind == "CONFIRMATION":
                raise SpiderOperationRequestStale()
            selection = cast(dict[str, object], request.operation_core["runtime_selection"])
            head = SpiderRuntimeOperation(
                operation_id=request.operation_id,
                environment=request.environment,
                execution_scope_id=request.execution_scope_id,
                strategy_id=request.strategy_id,
                actor=request.actor,
                operation_key=request.operation_key,
                operation_core_sha256=core_hash,
                operation_core_payload=canonical_bytes(core_payload),
                configuration_request_id=cast(str, selection["confirmation_request_id"]),
                phase="PREVIEW",
            )
            session.add(head)
            session.flush()
        elif (
            head.environment != request.environment
            or head.execution_scope_id != request.execution_scope_id
            or head.strategy_id != request.strategy_id
            or head.actor != request.actor
            or head.operation_key != request.operation_key
            or head.operation_core_sha256 != core_hash
        ):
            raise SpiderOperationRequestConflict()

        if request.request_kind == "CONFIRMATION":
            proposal_id = cast(str, request.proposal_request_id)
            proposal_row = session.execute(
                select(_REQUEST_TABLE).where(_REQUEST_TABLE.c.request_id == proposal_id)
            ).mappings().first()
            if proposal_row is None:
                raise SpiderOperationRequestStale()
            proposal = _mapping(proposal_row).request
            if not self._proposal_compatible(proposal, request):
                raise SpiderOperationRequestConflict()
            if head.authorization_revision is not None or head.confirmation_request_id is not None:
                raise SpiderOperationRequestStale()

        record = SpiderOperationRequestRow(
            request_id=request.request_id,
            operation_id=request.operation_id,
            environment=request.environment,
            execution_scope_id=request.execution_scope_id,
            strategy_id=request.strategy_id,
            request_kind=request.request_kind,
            actor=request.actor,
            idempotency_key=request.idempotency_key,
            operation_key=request.operation_key,
            operation_core_sha256=core_hash,
            authorization_revision=cast(int, request.authorization["revision"]),
            proposal_request_id=request.proposal_request_id,
            authorization_confirmed=request.request_kind == "CONFIRMATION",
            request_payload=core_bytes,
        )
        session.add(record)
        if request.request_kind == "CONFIRMATION":
            head.authorization_revision = cast(int, request.authorization["revision"])
            head.confirmation_request_id = request.request_id
        session.flush()
        mapping = session.execute(
            select(_REQUEST_TABLE).where(_REQUEST_TABLE.c.request_id == request.request_id)
        ).mappings().one()
        return _mapping(mapping)

    def readback(self, request: SpiderOperationRequest) -> SpiderOperationReadback:
        if type(request) is not SpiderOperationRequest:
            return SpiderOperationReadback(SpiderOperationReadbackStatus.INVALID_EVIDENCE)
        try:
            with self._sessions() as session:
                if not isinstance(session, Session):
                    return SpiderOperationReadback(SpiderOperationReadbackStatus.UNKNOWN)
                bind = session.get_bind()
                if session.in_transaction() or (
                    isinstance(bind, Connection) and bind.in_transaction()
                ):
                    return SpiderOperationReadback(SpiderOperationReadbackStatus.UNKNOWN)
                with session.begin():
                    if bind.dialect.name != "postgresql":
                        return SpiderOperationReadback(SpiderOperationReadbackStatus.UNKNOWN)
                    session.execute(
                        text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
                    )
                    _configure_transaction(
                        session, self._lock_timeout_ms, self._statement_timeout_ms
                    )
                    row = session.execute(
                        select(_REQUEST_TABLE).where(
                            _REQUEST_TABLE.c.request_id == request.request_id
                        )
                    ).mappings().first()
                    if row is None:
                        return SpiderOperationReadback(
                            SpiderOperationReadbackStatus.NOT_FOUND
                        )
                    record = _mapping(row)
                    if record.request.canonical_request_bytes != request.canonical_request_bytes:
                        return SpiderOperationReadback(
                            SpiderOperationReadbackStatus.INVALID_EVIDENCE
                        )
                    self._validate_configuration(session, request)
                    if request.request_kind == "CONFIRMATION":
                        proposal_row = session.execute(
                            select(_REQUEST_TABLE).where(
                                _REQUEST_TABLE.c.request_id == request.proposal_request_id
                            )
                        ).mappings().first()
                        if proposal_row is None or not self._proposal_compatible(
                            _mapping(proposal_row).request, request
                        ):
                            return SpiderOperationReadback(
                                SpiderOperationReadbackStatus.INVALID_EVIDENCE
                            )
                    return SpiderOperationReadback(
                        SpiderOperationReadbackStatus.CONFIRMED, record
                    )
        except SpiderOperationRequestIntegrityError:
            return SpiderOperationReadback(SpiderOperationReadbackStatus.INVALID_EVIDENCE)
        except SpiderOperationRequestError:
            return SpiderOperationReadback(SpiderOperationReadbackStatus.INVALID_EVIDENCE)
        except Exception:
            return SpiderOperationReadback(SpiderOperationReadbackStatus.UNKNOWN)


__all__ = [
    "SpiderOperationReadback", "SpiderOperationReadbackStatus", "SpiderOperationRecord",
    "SpiderOperationRequestStore",
]
