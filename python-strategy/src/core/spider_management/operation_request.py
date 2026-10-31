"""Closed Spider INSTALL operation request DTOs and canonical identities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import re
from typing import cast

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.decimal_math import canonical_decimal_text

REQUEST_SCHEMA = "spider_operation_request_v1"
MAX_OPERATION_REQUEST_BYTES = 1_048_576
_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}", re.ASCII)
_ACTOR = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}", re.ASCII)
_IDEMPOTENCY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}", re.ASCII)
_HASH = re.compile(r"[0-9a-f]{64}", re.ASCII)
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z", re.ASCII)
_CANONICAL_DECIMAL = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?", re.ASCII)
_SELECTION_KEYS = frozenset(
    {
        "schema_version", "environment", "execution_scope_id", "strategy_id",
        "confirmation_request_id", "configuration_sha256", "host_contract_id",
        "host_contract_sha256", "account_profile", "account_id",
        "provider_profile_sha256", "build_policy_id", "time_policy_id", "time_zone",
    }
)
_CORE_KEYS = frozenset(
    {
        "operation_kind", "account_profile", "account_id", "source_generation",
        "expected_state_version", "confirmation_request_id", "configuration_sha256",
        "runtime_selection", "position_policy",
    }
)
_AUTH_KEYS = frozenset({"revision", "predecessor_confirmation_request_id", "basis", "expires_at", "effects"})
_BASIS_KEYS = frozenset({"financial_cursor", "tape_cursor", "specification_sha256", "valuation_sha256"})
_EFFECT_KEYS = frozenset(
    {
        "product_id", "side", "role", "quantity_unit", "quantity_cap", "valuation",
        "limit_price_min", "limit_price_max", "notional_cap",
    }
)
_VALUATION_KEYS = frozenset({"source_id", "as_of", "visible_sequence", "mark"})


class SpiderOperationRequestError(ValueError):
    def __init__(self, code: str = "SPIDER_OPERATION_REQUEST_INVALID") -> None:
        super().__init__(code)


class SpiderOperationRequestConflict(SpiderOperationRequestError):
    def __init__(self) -> None:
        super().__init__("SPIDER_OPERATION_REQUEST_CONFLICT")


class SpiderOperationRequestStale(SpiderOperationRequestError):
    def __init__(self) -> None:
        super().__init__("SPIDER_OPERATION_REQUEST_STALE")


class SpiderOperationRequestIntegrityError(SpiderOperationRequestError):
    def __init__(self) -> None:
        super().__init__("SPIDER_OPERATION_REQUEST_INTEGRITY")


def _shape(value: object, fields: frozenset[str]) -> dict[str, object]:
    if type(value) is not dict or set(cast(dict[object, object], value)) != fields:
        raise SpiderOperationRequestError()
    return cast(dict[str, object], value)


def _identifier(value: object) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise SpiderOperationRequestError()
    return value


def _hash(value: object) -> str:
    if type(value) is not str or _HASH.fullmatch(value) is None:
        raise SpiderOperationRequestError()
    return value


def _decimal(value: object, *, positive: bool = False) -> str:
    if type(value) is not str or _CANONICAL_DECIMAL.fullmatch(value) is None:
        raise SpiderOperationRequestError()
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise SpiderOperationRequestError() from None
    if not number.is_finite() or canonical_decimal_text(number) != value or (positive and number <= 0):
        raise SpiderOperationRequestError()
    return value


def _timestamp(value: object) -> str:
    if type(value) is not str or _TIME.fullmatch(value) is None:
        raise SpiderOperationRequestError()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        raise SpiderOperationRequestError() from None
    if f"{parsed:%Y-%m-%dT%H:%M:%S}.{parsed.microsecond // 1000:03d}Z" != value:
        raise SpiderOperationRequestError()
    return value


def _selection(value: object, environment: str, scope: str, strategy: str, account_profile: str, account_id: str, confirmation: str, config_hash: str) -> dict[str, object]:
    row = _shape(value, _SELECTION_KEYS)
    if (
        row["schema_version"] != "SPIDER_RUNTIME_SELECTION_V1"
        or row["environment"] != environment
        or row["execution_scope_id"] != scope
        or row["strategy_id"] != strategy
        or row["account_profile"] != account_profile
        or row["account_id"] != account_id
        or row["confirmation_request_id"] != confirmation
        or row["configuration_sha256"] != config_hash
        or row["build_policy_id"] != "SPIDER_BUILD_EXPLICIT_MEMBER_RESUBMIT_V1"
        or row["time_policy_id"] != "SPIDER_COUNT_DOWNTIME_NO_CATCHUP_V1"
        or row["time_zone"] != "UTC"
    ):
        raise SpiderOperationRequestError("SPIDER_OPERATION_REQUEST_UNSUPPORTED")
    for key in ("environment", "execution_scope_id", "strategy_id", "host_contract_id", "account_profile", "account_id", "build_policy_id", "time_policy_id", "time_zone"):
        _identifier(row[key])
    for key in ("confirmation_request_id", "configuration_sha256", "host_contract_sha256", "provider_profile_sha256"):
        _hash(row[key])
    return row


def _authorization(value: object) -> dict[str, object]:
    row = _shape(value, _AUTH_KEYS)
    revision = row["revision"]
    if type(revision) is not int or revision != 1:
        raise SpiderOperationRequestError()
    if row["predecessor_confirmation_request_id"] is not None:
        raise SpiderOperationRequestError("SPIDER_OPERATION_REQUEST_UNSUPPORTED")
    basis = _shape(row["basis"], _BASIS_KEYS)
    for cursor_key in ("financial_cursor", "tape_cursor"):
        cursor = basis[cursor_key]
        if type(cursor) is not str or not cursor:
            raise SpiderOperationRequestError()
    for key in ("specification_sha256", "valuation_sha256"):
        _hash(basis[key])
    _timestamp(row["expires_at"])
    effects = row["effects"]
    if type(effects) is not list:
        raise SpiderOperationRequestError()
    seen: set[tuple[object, ...]] = set()
    for item in effects:
        effect = _shape(item, _EFFECT_KEYS)
        product = _identifier(effect["product_id"])
        if effect["side"] not in ("LONG", "SHORT") or type(effect["side"]) is not str:
            raise SpiderOperationRequestError()
        role, unit = _identifier(effect["role"]), _identifier(effect["quantity_unit"])
        key = (product, effect["side"], role, unit)
        if key in seen:
            raise SpiderOperationRequestError()
        seen.add(key)
        _decimal(effect["quantity_cap"], positive=True)
        for bound in ("limit_price_min", "limit_price_max", "notional_cap"):
            if effect[bound] is not None:
                _decimal(effect[bound], positive=True)
        valuation = _shape(effect["valuation"], _VALUATION_KEYS)
        _identifier(valuation["source_id"])
        _timestamp(valuation["as_of"])
        if type(valuation["visible_sequence"]) is not int or valuation["visible_sequence"] < 0:
            raise SpiderOperationRequestError()
        _decimal(valuation["mark"], positive=True)
        low, high = effect["limit_price_min"], effect["limit_price_max"]
        if low is not None and high is not None and Decimal(cast(str, low)) > Decimal(cast(str, high)):
            raise SpiderOperationRequestError()
    return row


@dataclass(frozen=True, slots=True)
class SpiderOperationRequest:
    request_kind: str
    environment: str
    execution_scope_id: str
    strategy_id: str
    actor: str
    idempotency_key: str
    operation_key: str
    operation_core: dict[str, object]
    authorization: dict[str, object]
    proposal_request_id: str | None
    canonical_request_bytes: bytes

    def __post_init__(self) -> None:
        if (
            self.request_kind not in {"PROPOSAL", "CONFIRMATION"}
            or type(self.environment) is not str
            or type(self.execution_scope_id) is not str
            or type(self.strategy_id) is not str
            or type(self.actor) is not str
            or type(self.idempotency_key) is not str
            or type(self.operation_key) is not str
            or type(self.canonical_request_bytes) is not bytes
        ):
            raise SpiderOperationRequestError()
        if self.request_kind == "PROPOSAL" and self.proposal_request_id is not None:
            raise SpiderOperationRequestError()
        if self.request_kind == "CONFIRMATION" and _HASH.fullmatch(self.proposal_request_id or "") is None:
            raise SpiderOperationRequestError()

    @property
    def request_id(self) -> str:
        return sha256(canonical_bytes({
            "domain": REQUEST_SCHEMA,
            "environment": self.environment,
            "execution_scope_id": self.execution_scope_id,
            "strategy_id": self.strategy_id,
            "actor": self.actor,
            "idempotency_key": self.idempotency_key,
        })).hexdigest()

    @property
    def operation_id(self) -> str:
        return sha256(canonical_bytes({
            "domain": "spider_runtime_operation_v1",
            "environment": self.environment,
            "execution_scope_id": self.execution_scope_id,
            "strategy_id": self.strategy_id,
            "actor": self.actor,
            "operation_key": self.operation_key,
        })).hexdigest()

    @property
    def operation_core_sha256(self) -> str:
        return sha256(canonical_bytes(self.operation_core)).hexdigest()

    @classmethod
    def from_canonical_bytes(cls, raw: bytes) -> SpiderOperationRequest:
        try:
            if type(raw) is not bytes or not 1 <= len(raw) <= MAX_OPERATION_REQUEST_BYTES:
                raise SpiderOperationRequestError()
            value = decode_canonical(raw)
            body = _shape(value, frozenset({
                "schema_version", "request_kind", "environment", "execution_scope_id", "strategy_id",
                "actor", "idempotency_key", "operation_key", "operation_core", "authorization", "proposal_request_id",
            }))
            if body["schema_version"] != REQUEST_SCHEMA or body["request_kind"] not in ("PROPOSAL", "CONFIRMATION"):
                raise SpiderOperationRequestError()
            environment, scope, strategy = (_identifier(body[key]) for key in ("environment", "execution_scope_id", "strategy_id"))
            actor, idem, operation_key = body["actor"], body["idempotency_key"], body["operation_key"]
            if type(actor) is not str or _ACTOR.fullmatch(actor) is None or type(idem) is not str or _IDEMPOTENCY.fullmatch(idem) is None:
                raise SpiderOperationRequestError()
            operation_key = _identifier(operation_key)
            core = _shape(body["operation_core"], _CORE_KEYS)
            if core["operation_kind"] != "INSTALL" or core["position_policy"] != "RETAIN_ENABLED_V1":
                raise SpiderOperationRequestError("SPIDER_OPERATION_REQUEST_UNSUPPORTED")
            account_profile = _identifier(core["account_profile"])
            account_id = _identifier(core["account_id"])
            generation, version = core["source_generation"], core["expected_state_version"]
            if type(generation) is not int or generation != 0 or type(version) is not int or not 0 <= version <= 2**31 - 1:
                raise SpiderOperationRequestError("SPIDER_OPERATION_REQUEST_UNSUPPORTED")
            confirmation = _hash(core["confirmation_request_id"])
            config_hash = _hash(core["configuration_sha256"])
            runtime_selection = _selection(
                core["runtime_selection"], environment, scope, strategy,
                account_profile, account_id, confirmation, config_hash,
            )
            core["runtime_selection"] = runtime_selection
            authorization = _authorization(body["authorization"])
            proposal_id = body["proposal_request_id"]
            if body["request_kind"] == "PROPOSAL":
                if proposal_id is not None:
                    raise SpiderOperationRequestError()
            elif type(proposal_id) is not str or _HASH.fullmatch(proposal_id) is None:
                raise SpiderOperationRequestError()
            result = cls(
                cast(str, body["request_kind"]), environment, scope, strategy, actor,
                cast(str, idem), operation_key, core, authorization,
                cast(str | None, proposal_id), raw,
            )
            if result.canonical_request_bytes != canonical_bytes(body):
                raise SpiderOperationRequestError()
            return result
        except SpiderOperationRequestError:
            raise
        except (RecursionError, TypeError, ValueError, OverflowError):
            raise SpiderOperationRequestError() from None


__all__ = [
    "MAX_OPERATION_REQUEST_BYTES", "REQUEST_SCHEMA", "SpiderOperationRequest",
    "SpiderOperationRequestConflict", "SpiderOperationRequestError",
    "SpiderOperationRequestIntegrityError", "SpiderOperationRequestStale",
]
