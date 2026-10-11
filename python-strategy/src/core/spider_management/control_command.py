"""Closed canonical management-command and receipt evidence for Spider STOP."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
import re
from typing import Any, Mapping, cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.models import StrategyStatus
from src.core.orm_models import StrategyState
from src.core.spider_management.financial_delivery import validate_tape_index
from src.core.spider_management.orm import SpiderControlReceipt
from src.core.spider_management.orm import (
    SpiderConfigurationRequest,
    SpiderOperationRequestRow,
    SpiderRuntimeScope,
)
from src.core.strategy_state_manager import (
    StrategyStateManager,
    StrategyStateTransitionResult,
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}", re.ASCII)
_ACTOR = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}", re.ASCII)
_IDEMPOTENCY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}", re.ASCII)
_FIELDS = frozenset(
    {
        "schema_version",
        "command",
        "environment",
        "execution_scope_id",
        "strategy_id",
        "actor",
        "idempotency_key",
        "expected_version",
    }
)


class SpiderControlCommandError(ValueError):
    def __init__(self, reason_code: str = "SPIDER_CONTROL_REQUEST_INVALID") -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class SpiderControlCommandConflict(SpiderControlCommandError):
    def __init__(self) -> None:
        super().__init__("SPIDER_CONTROL_KEY_CONFLICT")


@dataclass(frozen=True, slots=True)
class SpiderControlWriteResult:
    disposition: str
    reason_code: str
    operation_id: str | None
    transition: StrategyStateTransitionResult | None = None


@dataclass(frozen=True, slots=True)
class SpiderManagementCommand:
    environment: str
    execution_scope_id: str
    strategy_id: str
    actor: str
    idempotency_key: str
    expected_version: int
    canonical_request_bytes: bytes
    request_sha256: str
    receipt_id: str

    @classmethod
    def from_message(cls, message: object, *, command: str) -> SpiderManagementCommand:
        if type(message) is not dict or command != "STOP":
            raise SpiderControlCommandError()
        params = cast(dict[str, object], cast(dict[object, object], message).get("params"))
        if type(params) is not dict:
            raise SpiderControlCommandError()
        data = cast(dict[str, object], params)
        strategy_id = data.get("strategy_id")
        short_id = data.get("id")
        if short_id is not None and strategy_id is not None and short_id != strategy_id:
            raise SpiderControlCommandError()
        if strategy_id is None:
            strategy_id = short_id
        environment = data.get("environment")
        execution_scope_id = data.get("execution_scope_id")
        actor = data.get("actor")
        idempotency_key = data.get("idempotency_key")
        expected_version = data.get("expected_version")
        for value in (environment, execution_scope_id, strategy_id):
            if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
                raise SpiderControlCommandError()
        if type(actor) is not str or _ACTOR.fullmatch(actor) is None:
            raise SpiderControlCommandError()
        if type(idempotency_key) is not str or _IDEMPOTENCY.fullmatch(idempotency_key) is None:
            raise SpiderControlCommandError()
        if type(expected_version) is not int or not 0 <= expected_version <= 2**31 - 1:
            raise SpiderControlCommandError()
        request = {
            "schema_version": "SPIDER_MANAGEMENT_COMMAND_V1",
            "command": "STOP",
            "environment": environment,
            "execution_scope_id": execution_scope_id,
            "strategy_id": strategy_id,
            "actor": actor,
            "idempotency_key": idempotency_key,
            "expected_version": expected_version,
        }
        payload = canonical_bytes(request)
        if not 1 <= len(payload) <= 1_048_576:
            raise SpiderControlCommandError()
        payload_hash = sha256(payload).hexdigest()
        receipt_id = sha256(b"SPIDER_CONTROL_RECEIPT_V1\0" + payload).hexdigest()
        return cls(
            cast(str, environment),
            cast(str, execution_scope_id),
            cast(str, strategy_id),
            cast(str, actor),
            cast(str, idempotency_key),
            cast(int, expected_version),
            payload,
            payload_hash,
            receipt_id,
        )

    @classmethod
    def from_canonical_bytes(cls, payload: object) -> SpiderManagementCommand:
        if type(payload) is not bytes or not 1 <= len(payload) <= 1_048_576:
            raise SpiderControlCommandError("SPIDER_CONTROL_RECEIPT_INVALID")
        try:
            value = decode_canonical(payload)
            if type(value) is not dict or set(cast(dict[object, object], value)) != _FIELDS:
                raise ValueError
            row = cast(dict[str, object], value)
            if row["schema_version"] != "SPIDER_MANAGEMENT_COMMAND_V1" or row["command"] != "STOP":
                raise ValueError
            message = {"params": {key: row[key] for key in _FIELDS - {"schema_version", "command"}}}
            parsed = cls.from_message(message, command="STOP")
            if parsed.canonical_request_bytes != payload:
                raise ValueError
            return parsed
        except (KeyError, TypeError, ValueError):
            raise SpiderControlCommandError("SPIDER_CONTROL_RECEIPT_INVALID") from None


def hydrate_control_receipt(row: Mapping[Any, object]) -> tuple[SpiderManagementCommand, dict[str, object]]:
    """Validate exact row-to-request identity before treating a receipt as proof."""
    try:
        payload = row["request_payload"]
        if type(payload) is memoryview:
            payload = payload.tobytes()
        request = SpiderManagementCommand.from_canonical_bytes(payload)
        request_sha256 = row["request_sha256"]
        expected_scope = (
            row["environment"], row["execution_scope_id"], row["strategy_id"]
        )
        if (
            row["receipt_id"] != request.receipt_id
            or request_sha256 != request.request_sha256
            or expected_scope != (request.environment, request.execution_scope_id, request.strategy_id)
            or row["actor"] != request.actor
            or row["idempotency_key"] != request.idempotency_key
            or type(row["generation"]) is not int
            or row["generation"] <= 0
            or type(row["before_state_version"]) is not int
            or row["before_state_version"] != request.expected_version
            or type(row["after_state_version"]) is not int
            or row["after_state_version"] != request.expected_version + 1
            or type(row["barrier_sequence"]) is not int
            or row["barrier_sequence"] < 0
            or row["outcome"] != "COMPLETED"
        ):
            raise ValueError
        return request, {
            "receipt_id": cast(str, row["receipt_id"]),
            "operation_id": cast(str | None, row["operation_id"]),
            "generation": cast(int, row["generation"]),
            "before_state_version": cast(int, row["before_state_version"]),
            "after_state_version": cast(int, row["after_state_version"]),
            "barrier_sequence": cast(int, row["barrier_sequence"]),
        }
    except (KeyError, TypeError, ValueError, OverflowError):
        raise SpiderControlCommandError("SPIDER_CONTROL_RECEIPT_INVALID") from None


def receipt_row(request: SpiderManagementCommand, *, operation_id: str | None,
                generation: int, barrier_sequence: int) -> SpiderControlReceipt:
    return SpiderControlReceipt(
        receipt_id=request.receipt_id,
        environment=request.environment,
        execution_scope_id=request.execution_scope_id,
        strategy_id=request.strategy_id,
        actor=request.actor,
        idempotency_key=request.idempotency_key,
        request_payload=request.canonical_request_bytes,
        request_sha256=request.request_sha256,
        operation_id=operation_id,
        generation=generation,
        before_state_version=request.expected_version,
        after_state_version=request.expected_version + 1,
        barrier_sequence=barrier_sequence,
        outcome="COMPLETED",
    )


def write_stop_in_transaction(
    session: Session,
    request: SpiderManagementCommand,
    state_manager: StrategyStateManager,
) -> SpiderControlWriteResult:
    """Apply the approved STOP row/state writes inside the caller's PG TX."""
    table = SpiderControlReceipt.__table__
    existing = session.execute(
        select(table)
        .where(
            table.c.environment == request.environment,
            table.c.execution_scope_id == request.execution_scope_id,
            table.c.strategy_id == request.strategy_id,
            table.c.actor == request.actor,
            table.c.idempotency_key == request.idempotency_key,
        )
        .with_for_update()
        .limit(2)
    ).mappings().all()
    if len(existing) > 1:
        return SpiderControlWriteResult(
            "UNKNOWN", "SPIDER_CONTROL_RECEIPT_INVALID", None
        )
    if existing:
        try:
            saved, proof = hydrate_control_receipt(existing[0])
        except SpiderControlCommandError:
            return SpiderControlWriteResult(
                "UNKNOWN", "SPIDER_CONTROL_RECEIPT_INVALID", None
            )
        if saved.canonical_request_bytes != request.canonical_request_bytes:
            return SpiderControlWriteResult(
                "REJECTED", "SPIDER_CONTROL_KEY_CONFLICT",
                cast(str | None, proof["operation_id"]),
            )
        return SpiderControlWriteResult(
            "COMPLETED", "SPIDER_STOPPED", cast(str | None, proof["operation_id"])
        )

    operation_confirmation = session.execute(
        select(SpiderOperationRequestRow.request_id)
        .where(
            SpiderOperationRequestRow.environment == request.environment,
            SpiderOperationRequestRow.execution_scope_id == request.execution_scope_id,
            SpiderOperationRequestRow.strategy_id == request.strategy_id,
            SpiderOperationRequestRow.actor == request.actor,
            SpiderOperationRequestRow.idempotency_key == request.idempotency_key,
            SpiderOperationRequestRow.request_kind == "CONFIRMATION",
        )
        .limit(1)
    ).first()
    configuration_confirmation = session.execute(
        select(SpiderConfigurationRequest.request_id)
        .where(
            SpiderConfigurationRequest.environment == request.environment,
            SpiderConfigurationRequest.execution_scope_id == request.execution_scope_id,
            SpiderConfigurationRequest.strategy_id == request.strategy_id,
            SpiderConfigurationRequest.actor == request.actor,
            SpiderConfigurationRequest.idempotency_key == request.idempotency_key,
            SpiderConfigurationRequest.request_kind == "CONFIRMATION",
        )
        .limit(1)
    ).first()
    if operation_confirmation is not None or configuration_confirmation is not None:
        return SpiderControlWriteResult(
            "REJECTED", "SPIDER_CONTROL_KEY_CONFLICT", None
        )

    runtime = session.execute(
        select(SpiderRuntimeScope)
        .where(
            SpiderRuntimeScope.environment == request.environment,
            SpiderRuntimeScope.execution_scope_id == request.execution_scope_id,
            SpiderRuntimeScope.strategy_id == request.strategy_id,
        )
        .with_for_update()
    ).scalar_one_or_none()
    if runtime is None:
        return SpiderControlWriteResult(
            "REJECTED", "SPIDER_RUNTIME_NOT_FOUND", None
        )
    if not validate_tape_index(session, runtime):
        return SpiderControlWriteResult(
            "UNKNOWN", "SPIDER_TAPE_UNAVAILABLE", runtime.current_operation_id
        )
    operation_id = runtime.current_operation_id
    locked = state_manager.lock_state_in_transaction(session, request.strategy_id)
    if (
        runtime.state_version != locked.version
        or request.expected_version != locked.version
    ):
        return SpiderControlWriteResult(
            "REJECTED", "SPIDER_CONTROL_STALE", operation_id
        )
    if runtime.control_mode not in {"RUN", "STOP"}:
        return SpiderControlWriteResult(
            "REJECTED", "SPIDER_CONTROL_UNAVAILABLE", operation_id
        )

    transition = state_manager.transition_in_transaction(
        session,
        request.strategy_id,
        StrategyStatus.STOPPED,
        actor=request.actor,
        reason=None,
        changed_at=datetime.now(UTC),
        expected_version=request.expected_version,
    )
    barrier = runtime.last_tape_sequence
    runtime.control_mode = "STOP"
    runtime.state_version = transition.version
    runtime.control_barrier_sequence = barrier
    session.add(receipt_row(
        request,
        operation_id=runtime.current_operation_id,
        generation=runtime.generation,
        barrier_sequence=barrier,
    ))
    session.flush()
    return SpiderControlWriteResult(
        "COMPLETED", "SPIDER_STOPPED", operation_id, transition
    )


def verify_stop_in_transaction(
    session: Session, request: SpiderManagementCommand
) -> dict[str, object] | None:
    """Validate an exact saved STOP against the current durable projections."""
    table = SpiderControlReceipt.__table__
    rows = session.execute(
        select(table).where(
            table.c.environment == request.environment,
            table.c.execution_scope_id == request.execution_scope_id,
            table.c.strategy_id == request.strategy_id,
            table.c.actor == request.actor,
            table.c.idempotency_key == request.idempotency_key,
        ).limit(2)
    ).mappings().all()
    if len(rows) != 1:
        return None
    try:
        saved, proof = hydrate_control_receipt(rows[0])
    except SpiderControlCommandError:
        return None
    if saved.canonical_request_bytes != request.canonical_request_bytes:
        return None
    runtime = session.execute(
        select(SpiderRuntimeScope).where(
            SpiderRuntimeScope.environment == request.environment,
            SpiderRuntimeScope.execution_scope_id == request.execution_scope_id,
            SpiderRuntimeScope.strategy_id == request.strategy_id,
        )
    ).scalar_one_or_none()
    state = session.execute(
        select(StrategyState.status, StrategyState.version).where(
            StrategyState.strategy_id == request.strategy_id
        )
    ).first()
    if (
        runtime is None
        or state is None
        or runtime.control_mode != "STOP"
        or runtime.generation != proof["generation"]
        or runtime.current_operation_id != proof["operation_id"]
        or runtime.state_version != state.version
        or state.version < proof["after_state_version"]
        or state.status != StrategyStatus.STOPPED.value
    ):
        return None
    if state.version == proof["after_state_version"]:
        if runtime.control_barrier_sequence != proof["barrier_sequence"]:
            return None
    else:
        current_rows = session.execute(
            select(table)
            .where(
                table.c.environment == request.environment,
                table.c.execution_scope_id == request.execution_scope_id,
                table.c.strategy_id == request.strategy_id,
                table.c.operation_id == runtime.current_operation_id,
                table.c.generation == runtime.generation,
                table.c.after_state_version == runtime.state_version,
                table.c.outcome == "COMPLETED",
            )
            .limit(2)
        ).mappings().all()
        if len(current_rows) != 1:
            return None
        try:
            _current_request, current_proof = hydrate_control_receipt(current_rows[0])
        except SpiderControlCommandError:
            return None
        if (
            current_proof["operation_id"] != runtime.current_operation_id
            or current_proof["generation"] != runtime.generation
            or current_proof["after_state_version"] != runtime.state_version
            or current_proof["barrier_sequence"] != runtime.control_barrier_sequence
        ):
            return None
    return proof
