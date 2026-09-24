"""Pure caller-fixed profile activation intent; no runtime orchestration."""

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
from typing import Self

from src.core.market_data.profiles.bootstrap_seed import BootstrapKey
from src.core.market_data.profiles.requirements import ProfileRequirement
from src.strategies.base import StrategyRequirements, StrategyContextCapability

MAX_ACTIVATION_REQUEST_BYTES = 65536


class ProfileActivationCommand(Enum):
    START = "START"
    RESUME = "RESUME"
    FORCE_RECOVER = "FORCE_RECOVER"


class ProfileActivationRequestError(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_ACTIVATION_REQUEST_INVALID")


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _pairs(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _reject_number(value: str) -> None:
    raise ValueError


def _shape(value: object, fields: str) -> dict:
    if type(value) is not dict or set(value) != set(fields.split()):
        raise ValueError
    return value


def _version(value: object) -> None:
    if type(value) is not int or value != 1:
        raise ValueError


@dataclass(frozen=True, slots=True)
class ProfileActivationRequest:
    actor: str
    idempotency_key: str = field(repr=False)
    command: ProfileActivationCommand
    intent: "ProfileActivationIntent"

    def __post_init__(self) -> None:
        try:
            for value, limit in ((self.actor, 64), (self.idempotency_key, 128)):
                if (
                    type(value) is not str
                    or not 1 <= len(value) <= limit
                    or not value.isascii()
                    or not value[0].isalnum()
                    or any(not (c.isalnum() or c in "._:@-") for c in value)
                ):
                    raise ValueError
            if (
                type(self.command) is not ProfileActivationCommand
                or type(self.intent) is not ProfileActivationIntent
                or not 1 <= len(self.intent.requirements.profile_requirements) <= 32
                or len(self.canonical_bytes) > MAX_ACTIVATION_REQUEST_BYTES
            ):
                raise ValueError
        except (ValueError, TypeError, OverflowError):
            raise ProfileActivationRequestError() from None

    @property
    def command_ref(self) -> str:
        return hashlib.sha256(
            _canonical(
                {
                    "schema_version": 1,
                    "domain": "profile_activation_command",
                    "environment": self.intent.key.environment,
                    "execution_scope_id": self.intent.key.execution_scope_id,
                    "actor": self.actor,
                    "idempotency_key": self.idempotency_key,
                }
            )
        ).hexdigest()

    @property
    def request_id(self) -> str:
        return self.command_ref

    @property
    def canonical_bytes(self) -> bytes:
        requirements = self.intent.requirements
        return _canonical(
            {
                "schema_version": 1,
                "actor": self.actor,
                "idempotency_key": self.idempotency_key,
                "command": self.command.value,
                "command_ref": self.command_ref,
                "intent": {
                    "key": json.loads(self.intent.key.canonical_bytes),
                    "requirements": {
                        "product_id": requirements.product_id,
                        "timeframe": requirements.timeframe,
                        "lookback_window": requirements.lookback_window,
                        "required_context_capabilities": sorted(
                            c.value for c in requirements.required_context_capabilities
                        ),
                        "profile_requirements": [
                            json.loads(p.canonical_bytes)
                            for p in requirements.profile_requirements
                        ],
                    },
                    "expected_state_version": self.intent.expected_state_version,
                },
            }
        )

    @property
    def payload_digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_canonical_bytes(cls, raw: bytes) -> Self:
        try:
            if (
                type(raw) is not bytes
                or not 1 <= len(raw) <= MAX_ACTIVATION_REQUEST_BYTES
            ):
                raise ValueError
            data = _shape(
                json.loads(
                    raw.decode("utf-8"),
                    object_pairs_hook=_pairs,
                    parse_float=_reject_number,
                    parse_constant=_reject_number,
                ),
                "schema_version actor idempotency_key command command_ref intent",
            )
            _version(data["schema_version"])
            intent = _shape(data["intent"], "key requirements expected_state_version")
            key = _shape(
                intent["key"],
                "schema_version contract_version environment execution_scope_id strategy_id strategy_version config_hash product_id timeframe",
            )
            _version(key["schema_version"])
            _version(key["contract_version"])
            requirements = _shape(
                intent["requirements"],
                "product_id timeframe lookback_window required_context_capabilities profile_requirements",
            )
            profiles = requirements["profile_requirements"]
            capabilities = requirements["required_context_capabilities"]
            if (
                type(profiles) is not list
                or not 1 <= len(profiles) <= 32
                or type(capabilities) is not list
                or any(type(c) is not str for c in capabilities)
                or len(set(capabilities)) != len(capabilities)
            ):
                raise ValueError
            decoded_capabilities = frozenset(
                StrategyContextCapability(c) for c in capabilities
            )
            decoded_profiles = []
            for profile in profiles:
                profile = _shape(
                    profile,
                    "schema_version product_id base_grid_id output_grid_id algorithm_version window_days freshness_policy_id",
                )
                _version(profile["schema_version"])
                decoded_profiles.append(
                    ProfileRequirement(
                        **{k: v for k, v in profile.items() if k != "schema_version"}
                    )
                )
            if any(type(data[name]) is not str for name in ("command", "command_ref")):
                raise ValueError
            decoded_key = BootstrapKey(
                **{k: v for k, v in key.items() if k != "schema_version"}
            )
            decoded_requirements = StrategyRequirements(
                requirements["product_id"],
                requirements["timeframe"],
                requirements["lookback_window"],
                decoded_capabilities,
                tuple(decoded_profiles),
            )
            result = cls(
                data["actor"],
                data["idempotency_key"],
                ProfileActivationCommand(data["command"]),
                ProfileActivationIntent(
                    decoded_key, decoded_requirements, intent["expected_state_version"]
                ),
            )
            if (
                data["command_ref"] != result.command_ref
                or result.canonical_bytes != raw
            ):
                raise ValueError
            return result
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise ProfileActivationRequestError() from None


class ProfileActivationIntentError(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_ACTIVATION_INTENT_INVALID")


class ProfileActivationAdmission(Enum):
    ADMIT = "ADMIT"
    IDEMPOTENT = "IDEMPOTENT"
    CONFLICT = "CONFLICT"
    STALE = "STALE"


@dataclass(frozen=True, slots=True)
class ProfileActivationIntent:
    """Caller-fixed single-strategy intent, not activation or history authority."""

    key: BootstrapKey
    requirements: StrategyRequirements
    expected_state_version: int

    def __post_init__(self) -> None:
        if (
            type(self.key) is not BootstrapKey
            or type(self.requirements) is not StrategyRequirements
            or type(self.expected_state_version) is not int
            or not 0 <= self.expected_state_version <= (1 << 31) - 1
            or type(self.requirements.lookback_window) is not int
            or not 0 <= self.requirements.lookback_window <= (1 << 63) - 1
            or not self.requirements.profile_requirements
            or self.key.product_id != self.requirements.product_id
            or self.key.timeframe != self.requirements.timeframe
        ):
            raise ProfileActivationIntentError()


def classify_profile_activation_intent(
    requested: ProfileActivationIntent,
    *,
    current: ProfileActivationIntent,
    pending: ProfileActivationIntent | None,
) -> ProfileActivationAdmission:
    """Compare intent snapshots only; never admit a stale request over pending."""
    values = (requested, current) if pending is None else (requested, current, pending)
    if any(type(value) is not ProfileActivationIntent for value in values):
        raise ProfileActivationIntentError()
    targets = {
        (value.key.environment, value.key.execution_scope_id, value.key.strategy_id)
        for value in values
    }
    if len(targets) != 1:
        raise ProfileActivationIntentError()
    if requested != current:
        return ProfileActivationAdmission.STALE
    if pending is None:
        return ProfileActivationAdmission.ADMIT
    if pending == requested:
        return ProfileActivationAdmission.IDEMPOTENT
    return ProfileActivationAdmission.CONFLICT
