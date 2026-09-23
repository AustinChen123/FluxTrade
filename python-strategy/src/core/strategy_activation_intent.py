"""Pure caller-fixed profile activation intent; no runtime orchestration."""

from dataclasses import dataclass
from enum import Enum

from src.core.market_data.profiles.bootstrap_seed import BootstrapKey
from src.strategies.base import StrategyRequirements


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
