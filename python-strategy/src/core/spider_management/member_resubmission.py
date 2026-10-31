"""Closed canonical intent for one explicit BUILD member resubmission."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import re
from typing import cast

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

_FIELDS = frozenset({
    "schema_version", "environment", "execution_scope_id", "strategy_id",
    "actor", "idempotency_key", "expected_version", "generation",
    "configuration_sha256", "initial_plan_id", "target_action_id",
})
_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}", re.ASCII)
_ACTOR = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}", re.ASCII)
_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}", re.ASCII)
_HASH = re.compile(r"[0-9a-f]{64}", re.ASCII)


class MemberResubmissionError(ValueError):
    def __init__(self, reason_code: str = "SPIDER_MEMBER_RESUBMISSION_INVALID") -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class MemberResubmissionRequest:
    environment: str
    execution_scope_id: str
    strategy_id: str
    actor: str
    idempotency_key: str
    expected_version: int
    generation: int
    configuration_sha256: str
    initial_plan_id: str
    target_action_id: str
    request_id: str
    canonical_request_bytes: bytes
    request_sha256: str

    @classmethod
    def from_object(cls, value: object) -> MemberResubmissionRequest:
        if type(value) is not dict or set(cast(dict[object, object], value)) != _FIELDS:
            raise MemberResubmissionError()
        row = cast(dict[str, object], value)
        if row.get("schema_version") != "spider_member_resubmission_request_v1":
            raise MemberResubmissionError()
        for name in ("environment", "execution_scope_id", "strategy_id"):
            if type(row.get(name)) is not str or _IDENTIFIER.fullmatch(cast(str, row[name])) is None:
                raise MemberResubmissionError()
        if type(row.get("actor")) is not str or _ACTOR.fullmatch(cast(str, row["actor"])) is None:
            raise MemberResubmissionError()
        if type(row.get("idempotency_key")) is not str or _KEY.fullmatch(cast(str, row["idempotency_key"])) is None:
            raise MemberResubmissionError()
        if type(row.get("expected_version")) is not int or not 0 <= cast(int, row["expected_version"]) < 2**31:
            raise MemberResubmissionError()
        if type(row.get("generation")) is not int or not 1 <= cast(int, row["generation"]) < 2**63:
            raise MemberResubmissionError()
        for name in ("configuration_sha256", "initial_plan_id", "target_action_id"):
            if type(row.get(name)) is not str or _HASH.fullmatch(cast(str, row[name])) is None:
                raise MemberResubmissionError()
        payload = canonical_bytes(row)
        if not 1 <= len(payload) <= 1_048_576:
            raise MemberResubmissionError()
        identity = {
            "domain": "spider_member_resubmission_request_v1",
            "environment": row["environment"],
            "execution_scope_id": row["execution_scope_id"],
            "strategy_id": row["strategy_id"],
            "actor": row["actor"],
            "idempotency_key": row["idempotency_key"],
        }
        return cls(
            cast(str, row["environment"]), cast(str, row["execution_scope_id"]),
            cast(str, row["strategy_id"]), cast(str, row["actor"]),
            cast(str, row["idempotency_key"]), cast(int, row["expected_version"]),
            cast(int, row["generation"]), cast(str, row["configuration_sha256"]),
            cast(str, row["initial_plan_id"]), cast(str, row["target_action_id"]),
            sha256(canonical_bytes(identity)).hexdigest(), payload, sha256(payload).hexdigest(),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: object) -> MemberResubmissionRequest:
        if type(payload) is not bytes or not 1 <= len(payload) <= 1_048_576:
            raise MemberResubmissionError("SPIDER_MEMBER_RESUBMISSION_SAVED_REQUEST_INVALID")
        try:
            row = decode_canonical(payload)
            request = cls.from_object(row)
            if request.canonical_request_bytes != payload:
                raise ValueError
            return request
        except (KeyError, TypeError, ValueError, UnicodeDecodeError):
            raise MemberResubmissionError("SPIDER_MEMBER_RESUBMISSION_SAVED_REQUEST_INVALID") from None


def control_receipt_id(request: MemberResubmissionRequest) -> str:
    return sha256(b"SPIDER_MEMBER_RESUBMISSION_CONTROL_V1\0" + request.canonical_request_bytes).hexdigest()
