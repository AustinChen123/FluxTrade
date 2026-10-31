"""Caller-transaction writes and independent committed readback for Spider requests."""

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

from src.core.orm_models import StrategyState
from src.core.spider_management.configuration_request import (
    SpiderConfigurationReadbackStatus,
    SpiderConfigurationRequest,
    SpiderConfigurationRequestConflict,
    SpiderConfigurationRequestError,
    SpiderConfigurationRequestIntegrityError,
    SpiderConfigurationRequestKind,
    SpiderConfigurationRequestRecord,
    SpiderConfigurationRequestStale,
    SpiderManualConfiguration,
)
from src.core.spider_management.orm import SpiderConfigurationRequest as RequestRow

_TABLE = cast(Table, RequestRow.__table__)
_MAX_VERSION = 2**31 - 1


@dataclass(frozen=True, slots=True)
class SpiderConfigurationReadback:
    status: SpiderConfigurationReadbackStatus
    record: SpiderConfigurationRequestRecord | None = None

    def __post_init__(self) -> None:
        if type(self.status) is not SpiderConfigurationReadbackStatus or (
            self.status is SpiderConfigurationReadbackStatus.CONFIRMED
            and type(self.record) is not SpiderConfigurationRequestRecord
        ) or (
            self.status is not SpiderConfigurationReadbackStatus.CONFIRMED
            and self.record is not None
        ):
            raise SpiderConfigurationRequestIntegrityError()


class SpiderConfigurationWriteDisposition(Enum):
    RECORDED_IN_TRANSACTION = "RECORDED_IN_TRANSACTION"


@dataclass(frozen=True, slots=True)
class SpiderConfigurationWriteResult:
    disposition: SpiderConfigurationWriteDisposition
    record: SpiderConfigurationRequestRecord

    def __post_init__(self) -> None:
        if (
            self.disposition is not SpiderConfigurationWriteDisposition.RECORDED_IN_TRANSACTION
            or type(self.record) is not SpiderConfigurationRequestRecord
            or self.record.result != "NOT_APPLIED"
        ):
            raise SpiderConfigurationRequestIntegrityError()


def _utc_millisecond(value: object) -> bool:
    return (
        type(value) is datetime
        and value.tzinfo is not None
        and value.utcoffset() == timezone.utc.utcoffset(value)
        and value.microsecond % 1000 == 0
    )


def _hydrate(row: Mapping[Any, object]) -> SpiderConfigurationRequestRecord:
    try:
        raw = row["configuration_payload"]
        if type(raw) is memoryview:
            raw = raw.tobytes()
        configuration = SpiderManualConfiguration.from_canonical_bytes(cast(bytes, raw))
        if row["configuration_sha256"] != configuration.sha256:
            raise ValueError
        request = SpiderConfigurationRequest.from_fields(
            environment=cast(str, row["environment"]),
            execution_scope_id=cast(str, row["execution_scope_id"]),
            strategy_id=cast(str, row["strategy_id"]),
            request_kind=SpiderConfigurationRequestKind(cast(str, row["request_kind"])),
            actor=cast(str, row["actor"]),
            idempotency_key=cast(str, row["idempotency_key"]),
            expected_state_version=cast(int, row["expected_state_version"]),
            configuration=configuration.value,
            proposal_request_id=cast(str | None, row["proposal_request_id"]),
        )
        if request.request_id != row["request_id"]:
            raise ValueError
        recorded_at = row["recorded_at"]
        if not _utc_millisecond(recorded_at):
            raise ValueError
        return SpiderConfigurationRequestRecord(request, cast(datetime, recorded_at))
    except (KeyError, TypeError, ValueError, OverflowError):
        raise SpiderConfigurationRequestIntegrityError() from None


def _configure_transaction(
    session: Session, lock_timeout_ms: int, statement_timeout_ms: int
) -> None:
    session.execute(
        text(
            "SELECT set_config('TimeZone', 'UTC', true), "
            "set_config('lock_timeout', :lock_timeout, true), "
            "set_config('statement_timeout', :statement_timeout, true)"
        ),
        {
            "lock_timeout": f"{lock_timeout_ms}ms",
            "statement_timeout": f"{statement_timeout_ms}ms",
        },
    )


class SpiderConfigurationRequestStore:
    """Persist request evidence; caller owns commit and runtime lifecycle."""

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
            raise SpiderConfigurationRequestError()
        self._sessions = sessions
        self._lock_timeout_ms = lock_timeout_ms
        self._statement_timeout_ms = statement_timeout_ms

    def record_in_transaction(
        self, session: Session, request: SpiderConfigurationRequest
    ) -> SpiderConfigurationWriteResult:
        """Record within a caller-owned PostgreSQL transaction; does not commit."""
        if (
            type(request) is not SpiderConfigurationRequest
            or not isinstance(session, Session)
            or not session.is_active
            or not session.in_transaction()
            or session.get_bind().dialect.name != "postgresql"
        ):
            raise SpiderConfigurationRequestError()
        _configure_transaction(
            session, self._lock_timeout_ms, self._statement_timeout_ms
        )
        state = (
            session.execute(
                select(StrategyState.version)
                .where(StrategyState.strategy_id == request.strategy_id)
                .with_for_update()
            )
            .scalars()
            .one_or_none()
        )
        if type(state) is not int or not 0 <= state <= _MAX_VERSION:
            raise SpiderConfigurationRequestIntegrityError()

        existing_rows = (
            session.execute(
                select(_TABLE)
                .where(_TABLE.c.request_id == request.request_id)
                .with_for_update()
                .limit(2)
            )
            .mappings()
            .all()
        )
        if len(existing_rows) > 1:
            raise SpiderConfigurationRequestIntegrityError()
        if existing_rows:
            existing = _hydrate(existing_rows[0])
            _verify_confirmation(session, existing.request)
            if existing.request.canonical_bytes != request.canonical_bytes:
                raise SpiderConfigurationRequestConflict()
            return SpiderConfigurationWriteResult(
                SpiderConfigurationWriteDisposition.RECORDED_IN_TRANSACTION, existing
            )

        if state != request.expected_state_version:
            raise SpiderConfigurationRequestStale()
        if request.request_kind is SpiderConfigurationRequestKind.CONFIRMATION:
            proposal_rows = (
                session.execute(
                    select(_TABLE)
                    .where(_TABLE.c.request_id == request.proposal_request_id)
                    .with_for_update()
                    .limit(2)
                )
                .mappings()
                .all()
            )
            if len(proposal_rows) != 1:
                raise SpiderConfigurationRequestError()
            proposal = _hydrate(proposal_rows[0]).request
            if (
                proposal.request_kind is not SpiderConfigurationRequestKind.PROPOSAL
                or (proposal.environment, proposal.execution_scope_id, proposal.strategy_id)
                != (request.environment, request.execution_scope_id, request.strategy_id)
                or proposal.configuration.canonical_bytes
                != request.configuration.canonical_bytes
            ):
                raise SpiderConfigurationRequestError()

        inserted = (
            session.execute(
                _TABLE.insert()
                .values(
                    request_id=request.request_id,
                    environment=request.environment,
                    execution_scope_id=request.execution_scope_id,
                    strategy_id=request.strategy_id,
                    request_kind=request.request_kind.value,
                    actor=request.actor,
                    idempotency_key=request.idempotency_key,
                    expected_state_version=request.expected_state_version,
                    configuration_payload=request.configuration.canonical_bytes,
                    configuration_sha256=request.configuration.sha256,
                    proposal_request_id=request.proposal_request_id,
                )
                .returning(_TABLE)
            )
            .mappings()
            .one()
        )
        result = _hydrate(inserted)
        if result.request.canonical_bytes != request.canonical_bytes:
            raise SpiderConfigurationRequestIntegrityError()
        return SpiderConfigurationWriteResult(
            SpiderConfigurationWriteDisposition.RECORDED_IN_TRANSACTION, result
        )

    def readback(
        self, request: SpiderConfigurationRequest
    ) -> SpiderConfigurationReadback:
        """Read committed evidence in a fresh READ COMMITTED / READ ONLY session."""
        if type(request) is not SpiderConfigurationRequest:
            return SpiderConfigurationReadback(
                SpiderConfigurationReadbackStatus.INVALID_EVIDENCE
            )
        confirmed_record: SpiderConfigurationRequestRecord | None = None
        try:
            with self._sessions() as session:
                if not isinstance(session, Session):
                    return SpiderConfigurationReadback(
                        SpiderConfigurationReadbackStatus.UNKNOWN
                    )
                bind = session.get_bind()
                if session.in_transaction() or (
                    isinstance(bind, Connection) and bind.in_transaction()
                ):
                    return SpiderConfigurationReadback(
                        SpiderConfigurationReadbackStatus.UNKNOWN
                    )
                with session.begin():
                    if bind.dialect.name != "postgresql":
                        return SpiderConfigurationReadback(
                            SpiderConfigurationReadbackStatus.UNKNOWN
                        )
                    session.execute(
                        text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
                    )
                    _configure_transaction(
                        session, self._lock_timeout_ms, self._statement_timeout_ms
                    )
                    row = (
                        session.execute(
                            select(_TABLE).where(_TABLE.c.request_id == request.request_id)
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if row is None:
                        result = SpiderConfigurationReadbackStatus.NOT_FOUND
                    else:
                        record = _hydrate(row)
                        _verify_confirmation(session, record.request)
                        if (
                            (record.request.environment,
                             record.request.execution_scope_id,
                             record.request.strategy_id)
                            != (request.environment,
                                request.execution_scope_id,
                                request.strategy_id)
                            or record.request.canonical_bytes != request.canonical_bytes
                        ):
                            result = SpiderConfigurationReadbackStatus.INVALID_EVIDENCE
                        else:
                            result = SpiderConfigurationReadbackStatus.CONFIRMED
                            confirmed_record = record
                if result is SpiderConfigurationReadbackStatus.CONFIRMED:
                    assert confirmed_record is not None
                    return SpiderConfigurationReadback(result, confirmed_record)
                return SpiderConfigurationReadback(result)
        except SpiderConfigurationRequestIntegrityError:
            return SpiderConfigurationReadback(
                SpiderConfigurationReadbackStatus.INVALID_EVIDENCE
            )
        except Exception:
            return SpiderConfigurationReadback(SpiderConfigurationReadbackStatus.UNKNOWN)


def _verify_confirmation(
    session: Session, request: SpiderConfigurationRequest
) -> None:
    if request.request_kind is SpiderConfigurationRequestKind.PROPOSAL:
        return
    proposal_rows = (
        session.execute(
            select(_TABLE)
            .where(_TABLE.c.request_id == request.proposal_request_id)
            .limit(2)
        )
        .mappings()
        .all()
    )
    if len(proposal_rows) != 1:
        raise SpiderConfigurationRequestIntegrityError()
    proposal = _hydrate(proposal_rows[0]).request
    if (
        proposal.request_kind is not SpiderConfigurationRequestKind.PROPOSAL
        or (proposal.environment, proposal.execution_scope_id, proposal.strategy_id)
        != (request.environment, request.execution_scope_id, request.strategy_id)
        or proposal.configuration.canonical_bytes
        != request.configuration.canonical_bytes
    ):
        raise SpiderConfigurationRequestIntegrityError()
