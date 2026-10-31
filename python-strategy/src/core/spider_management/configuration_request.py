"""Canonical Spider configuration request evidence, not effective configuration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from hashlib import sha256
import re
from typing import cast

from src.core.backtest.spider_policy import DEFAULTS
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.decimal_math import canonical_decimal_text

MAX_REQUEST_BYTES = 65536
CONFIGURATION_SCHEMA = "spider_manual_configuration_v1"
REQUEST_SCHEMA = "spider_configuration_request_v1"
POLICY_SOURCE_SHA256 = "4dbf2bdbee87d4100004fe5b14a93f597e64a9bca6525ed2902afd3476ee40ec"
POLICY_IDS = frozenset(
    {"SPIDER_HOST_ORIGINAL_PAID_V1", "SPIDER_ORIGINAL_GROSS_PROTECTED_PAID_V1"}
)
_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}", re.ASCII)
_ACTOR = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}", re.ASCII)
_IDEMPOTENCY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}", re.ASCII)
_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
_PRODUCT_FIELDS = frozenset(
    {"product_id", "active", "leverage", "歩差", "單數", "hold上限", "hold下限"}
)
_RISK_FIELDS = frozenset(
    {"admission_policy_id", "response_policy_id", "response_parameters"}
)
_RESPONSE_FIELDS = frozenset(
    {"trigger_cap", "target_cap", "allocation", "quantity_rounding"}
)


class SpiderConfigurationRequestError(ValueError):
    def __init__(self, code: str = "SPIDER_CONFIGURATION_REQUEST_INVALID") -> None:
        super().__init__(code)


class SpiderConfigurationRequestConflict(SpiderConfigurationRequestError):
    def __init__(self) -> None:
        super().__init__("SPIDER_CONFIGURATION_REQUEST_CONFLICT")


class SpiderConfigurationRequestStale(SpiderConfigurationRequestError):
    def __init__(self) -> None:
        super().__init__("SPIDER_CONFIGURATION_REQUEST_STALE")


class SpiderConfigurationRequestIntegrityError(SpiderConfigurationRequestError):
    def __init__(self) -> None:
        super().__init__("SPIDER_CONFIGURATION_REQUEST_INTEGRITY")


def _shape(value: object, fields: frozenset[str]) -> dict[str, object]:
    if type(value) is not dict or set(cast(dict[object, object], value)) != fields:
        raise SpiderConfigurationRequestError()
    return cast(dict[str, object], value)


def _identifier(value: object) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise SpiderConfigurationRequestError()
    return value


def _decimal_text(value: object) -> str:
    if type(value) is not str:
        raise SpiderConfigurationRequestError()
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise SpiderConfigurationRequestError() from None
    if not parsed.is_finite() or canonical_decimal_text(parsed) != value:
        raise SpiderConfigurationRequestError()
    return value


def _manual_configuration(value: object) -> dict[str, object]:
    row = _shape(
        value,
        frozenset(
            {
                "schema_version",
                "policy_id",
                "policy_version",
                "policy_source_sha256",
                "parameters",
                "ordered_products",
                "product_settings",
                "risk_selection",
            }
        ),
    )
    if (
        row["schema_version"] != CONFIGURATION_SCHEMA
        or type(row["policy_id"]) is not str
        or row["policy_id"] not in POLICY_IDS
        or row["policy_version"] != "1"
        or row["policy_source_sha256"] != POLICY_SOURCE_SHA256
    ):
        raise SpiderConfigurationRequestError()

    parameters = _shape(row["parameters"], frozenset(DEFAULTS))
    for value_text in parameters.values():
        _decimal_text(value_text)

    products = row["ordered_products"]
    settings = row["product_settings"]
    if (
        type(products) is not list
        or not products
        or any(type(product) is not str or not product for product in products)
        or len(set(products)) != len(products)
        or type(settings) is not list
        or len(settings) != len(products)
    ):
        raise SpiderConfigurationRequestError()
    for product, item in zip(products, settings, strict=True):
        setting = _shape(item, _PRODUCT_FIELDS)
        if setting["product_id"] != product or setting["active"] not in ("true", "false"):
            raise SpiderConfigurationRequestError()
        for key in _PRODUCT_FIELDS - {"product_id", "active"}:
            _decimal_text(setting[key])

    risk = row["risk_selection"]
    if row["policy_id"] == "SPIDER_HOST_ORIGINAL_PAID_V1":
        if risk is not None:
            raise SpiderConfigurationRequestError()
    else:
        selected = _shape(risk, _RISK_FIELDS)
        if (
            selected["admission_policy_id"] != "CURRENT_MARK_GROSS_CAP_TWO_V1"
            or selected["response_policy_id"] != "PROPORTIONAL_PAID_GROSS_TARGET_V1"
        ):
            raise SpiderConfigurationRequestError()
        response = _shape(selected["response_parameters"], _RESPONSE_FIELDS)
        if response != {
            "trigger_cap": "2",
            "target_cap": "1.5",
            "allocation": "GROSS_PROPORTIONAL_V1",
            "quantity_rounding": "CEIL_STEP_NO_MINIMUM_UPLIFT_V1",
        }:
            raise SpiderConfigurationRequestError()
    try:
        encoded = canonical_bytes(row)
    except (TypeError, ValueError):
        raise SpiderConfigurationRequestError() from None
    if len(encoded) > MAX_REQUEST_BYTES:
        raise SpiderConfigurationRequestError()
    return row


class SpiderConfigurationRequestKind(Enum):
    PROPOSAL = "PROPOSAL"
    CONFIRMATION = "CONFIRMATION"


@dataclass(frozen=True, slots=True)
class SpiderManualConfiguration:
    canonical_bytes: bytes
    sha256: str

    def __post_init__(self) -> None:
        try:
            if (
                type(self.canonical_bytes) is not bytes
                or not 1 <= len(self.canonical_bytes) <= MAX_REQUEST_BYTES
                or type(self.sha256) is not str
                or self.sha256 != sha256(self.canonical_bytes).hexdigest()
            ):
                raise ValueError
            decoded = decode_canonical(self.canonical_bytes)
            normalized = _manual_configuration(decoded)
            if canonical_bytes(normalized) != self.canonical_bytes:
                raise ValueError
        except (RecursionError, TypeError, ValueError):
            raise SpiderConfigurationRequestError() from None

    @property
    def value(self) -> dict[str, object]:
        return cast(dict[str, object], decode_canonical(self.canonical_bytes))

    @classmethod
    def create(cls, value: object) -> SpiderManualConfiguration:
        normalized = _manual_configuration(value)
        encoded = canonical_bytes(normalized)
        return cls(encoded, sha256(encoded).hexdigest())

    @classmethod
    def from_canonical_bytes(cls, raw: bytes) -> SpiderManualConfiguration:
        try:
            if type(raw) is not bytes or not 1 <= len(raw) <= MAX_REQUEST_BYTES:
                raise ValueError
            value = decode_canonical(raw)
            result = cls.create(value)
            if result.canonical_bytes != raw:
                raise ValueError
            return result
        except (RecursionError, TypeError, ValueError):
            raise SpiderConfigurationRequestIntegrityError() from None


@dataclass(frozen=True, slots=True)
class SpiderConfigurationRequest:
    environment: str
    execution_scope_id: str
    strategy_id: str
    request_kind: SpiderConfigurationRequestKind
    actor: str
    idempotency_key: str
    expected_state_version: int
    configuration: SpiderManualConfiguration
    proposal_request_id: str | None = None

    def __post_init__(self) -> None:
        if (
            any(
                type(item) is not str or _IDENTIFIER.fullmatch(item) is None
                for item in (self.environment, self.execution_scope_id, self.strategy_id)
            )
            or type(self.request_kind) is not SpiderConfigurationRequestKind
            or type(self.actor) is not str
            or _ACTOR.fullmatch(self.actor) is None
            or type(self.idempotency_key) is not str
            or _IDEMPOTENCY.fullmatch(self.idempotency_key) is None
            or type(self.expected_state_version) is not int
            or not 0 <= self.expected_state_version <= 2**31 - 1
            or type(self.configuration) is not SpiderManualConfiguration
            or (
                self.request_kind is SpiderConfigurationRequestKind.PROPOSAL
                and self.proposal_request_id is not None
            )
            or (
                self.request_kind is SpiderConfigurationRequestKind.CONFIRMATION
                and (
                    type(self.proposal_request_id) is not str
                    or _SHA256.fullmatch(self.proposal_request_id) is None
                )
            )
        ):
            raise SpiderConfigurationRequestError()
        if len(self.canonical_bytes) > MAX_REQUEST_BYTES:
            raise SpiderConfigurationRequestError()

    @property
    def request_id(self) -> str:
        return sha256(
            canonical_bytes(
                {
                    "domain": REQUEST_SCHEMA,
                    "environment": self.environment,
                    "execution_scope_id": self.execution_scope_id,
                    "strategy_id": self.strategy_id,
                    "actor": self.actor,
                    "idempotency_key": self.idempotency_key,
                }
            )
        ).hexdigest()

    @property
    def canonical_bytes(self) -> bytes:
        value = {
            "schema_version": REQUEST_SCHEMA,
            "environment": self.environment,
            "execution_scope_id": self.execution_scope_id,
            "strategy_id": self.strategy_id,
            "request_kind": self.request_kind.value,
            "actor": self.actor,
            "idempotency_key": self.idempotency_key,
            "expected_state_version": self.expected_state_version,
            "configuration": self.configuration.value,
            "proposal_request_id": self.proposal_request_id,
        }
        try:
            encoded = canonical_bytes(value)
        except (TypeError, ValueError):
            raise SpiderConfigurationRequestError() from None
        if len(encoded) > MAX_REQUEST_BYTES:
            raise SpiderConfigurationRequestError()
        return encoded

    @classmethod
    def from_fields(
        cls,
        *,
        environment: str,
        execution_scope_id: str,
        strategy_id: str,
        request_kind: SpiderConfigurationRequestKind,
        actor: str,
        idempotency_key: str,
        expected_state_version: int,
        configuration: object,
        proposal_request_id: str | None = None,
    ) -> SpiderConfigurationRequest:
        return cls(
            environment,
            execution_scope_id,
            strategy_id,
            request_kind,
            actor,
            idempotency_key,
            expected_state_version,
            SpiderManualConfiguration.create(configuration),
            proposal_request_id,
        )

    @classmethod
    def from_canonical_bytes(cls, raw: bytes) -> SpiderConfigurationRequest:
        try:
            if type(raw) is not bytes or not 1 <= len(raw) <= MAX_REQUEST_BYTES:
                raise ValueError
            decoded = _shape(
                decode_canonical(raw),
                frozenset(
                    {
                        "schema_version",
                        "environment",
                        "execution_scope_id",
                        "strategy_id",
                        "request_kind",
                        "actor",
                        "idempotency_key",
                        "expected_state_version",
                        "configuration",
                        "proposal_request_id",
                    }
                ),
            )
            if decoded["schema_version"] != REQUEST_SCHEMA:
                raise ValueError
            configuration = SpiderManualConfiguration.create(decoded["configuration"])
            result = cls(
                cast(str, decoded["environment"]),
                cast(str, decoded["execution_scope_id"]),
                cast(str, decoded["strategy_id"]),
                SpiderConfigurationRequestKind(cast(str, decoded["request_kind"])),
                cast(str, decoded["actor"]),
                cast(str, decoded["idempotency_key"]),
                cast(int, decoded["expected_state_version"]),
                configuration,
                cast(str | None, decoded["proposal_request_id"]),
            )
            if result.canonical_bytes != raw:
                raise ValueError
            return result
        except (RecursionError, TypeError, ValueError):
            raise SpiderConfigurationRequestIntegrityError() from None


class SpiderConfigurationReadbackStatus(Enum):
    CONFIRMED = "CONFIRMED"
    NOT_FOUND = "NOT_FOUND"
    UNKNOWN = "UNKNOWN"
    INVALID_EVIDENCE = "INVALID_EVIDENCE"


@dataclass(frozen=True, slots=True)
class SpiderConfigurationRequestRecord:
    request: SpiderConfigurationRequest
    recorded_at: datetime
    result: str = "NOT_APPLIED"

    def __post_init__(self) -> None:
        if (
            type(self.request) is not SpiderConfigurationRequest
            or type(self.recorded_at) is not datetime
            or self.recorded_at.tzinfo is None
            or self.recorded_at.utcoffset() != timezone.utc.utcoffset(self.recorded_at)
            or self.recorded_at.microsecond % 1000
            or self.result != "NOT_APPLIED"
        ):
            raise SpiderConfigurationRequestIntegrityError()
