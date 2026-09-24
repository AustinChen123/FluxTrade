"""Stable composition identity for one strategy's market-data decisions."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum

from src.core.backtest.run_evidence import configuration_sha256
from src.core.portfolio_runtime import PortfolioDefinition
from src.strategies.base import BaseStrategy


_EXECUTION_SCOPE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}")
_STRATEGY_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}")
_SHA256 = re.compile(r"[0-9a-f]{64}")


class DecisionCompositionError(ValueError):
    """Reject an ambiguous or non-canonical decision composition identity."""

    def __init__(self) -> None:
        super().__init__("MARKET_DATA_DECISION_COMPOSITION_INVALID")


def _exact_match(value: object, pattern: re.Pattern[str]) -> None:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise ValueError


def validate_execution_scope_id(value: object) -> str:
    """Validate an operator-owned ID that remains stable across restarts."""
    _exact_match(value, _EXECUTION_SCOPE)
    return value  # type: ignore[return-value]


def validate_strategy_version(value: object) -> str:
    """Validate an exact catalog artifact version, including SemVer punctuation."""
    _exact_match(value, _STRATEGY_VERSION)
    return value  # type: ignore[return-value]


def validate_config_hash(value: object) -> str:
    """Validate the lowercase canonical configuration SHA-256."""
    _exact_match(value, _SHA256)
    return value  # type: ignore[return-value]


def _reject_float(value: object) -> None:
    if type(value) is float:
        raise TypeError("decision configuration cannot contain float")
    if isinstance(value, Enum):
        _reject_float(value.value)
        return
    if is_dataclass(value) and not isinstance(value, type):
        for field in fields(value):
            _reject_float(getattr(value, field.name))
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            _reject_float(key)
            _reject_float(item)
        return
    if isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _reject_float(item)


def decision_config_hash(configuration: object) -> str:
    """Hash exact effective configuration without monetary float coercion."""
    try:
        _reject_float(configuration)
        return configuration_sha256(configuration)
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise DecisionCompositionError() from None


@dataclass(frozen=True, slots=True)
class MarketDataDecisionCompositionIdentity:
    """The three composition-owned identities needed to build decision keys."""

    execution_scope_id: str
    strategy_version: str
    config_hash: str

    def __post_init__(self) -> None:
        try:
            validate_execution_scope_id(self.execution_scope_id)
            validate_strategy_version(self.strategy_version)
            validate_config_hash(self.config_hash)
        except ValueError:
            raise DecisionCompositionError() from None

    @classmethod
    def from_configuration(
        cls,
        *,
        execution_scope_id: str,
        strategy_version: str,
        configuration: object,
    ) -> "MarketDataDecisionCompositionIdentity":
        return cls(
            execution_scope_id=execution_scope_id,
            strategy_version=strategy_version,
            config_hash=decision_config_hash(configuration),
        )


def _strategy_configuration(strategy: BaseStrategy) -> dict[str, object]:
    try:
        replay_configuration = strategy.replay_configuration()
    except Exception:
        raise DecisionCompositionError() from None
    return {
        "strategy_id": strategy.strategy_id,
        "product_id": strategy.product_id,
        "requirements": strategy.requirements,
        "replay_configuration": replay_configuration,
    }


def strategy_decision_composition(
    execution_scope_id: str,
    strategy: BaseStrategy,
) -> MarketDataDecisionCompositionIdentity:
    """Bind a catalog-loaded strategy to its effective decision configuration."""
    if not isinstance(strategy, BaseStrategy):
        raise DecisionCompositionError()
    strategy_version = getattr(type(strategy), "__fluxtrade_artifact_version__", None)
    if type(strategy_version) is not str:
        raise DecisionCompositionError()
    return MarketDataDecisionCompositionIdentity.from_configuration(
        execution_scope_id=execution_scope_id,
        strategy_version=strategy_version,
        configuration={
            "schema_version": 1,
            "artifact_kind": "strategy",
            "strategy": _strategy_configuration(strategy),
        },
    )


def portfolio_decision_composition(
    execution_scope_id: str,
    definition: PortfolioDefinition,
) -> MarketDataDecisionCompositionIdentity:
    """Bind all portfolio sleeves to one exact factory-owned configuration."""
    if type(definition) is not PortfolioDefinition:
        raise DecisionCompositionError()
    strategy_version = definition.artifact_version
    if type(strategy_version) is not str:
        raise DecisionCompositionError()
    configuration = {
        "schema_version": 1,
        "artifact_kind": "portfolio",
        "portfolio_id": definition.portfolio_id,
        "product_id": definition.product_id,
        "max_gross_quantity": definition.max_gross_quantity,
        "exclusive_slots": definition.exclusive_slots,
        "sleeves": tuple(
            {
                **_strategy_configuration(sleeve.strategy),
                "activation_windows": sleeve.activation_windows,
            }
            for sleeve in definition.sleeves
        ),
    }
    return MarketDataDecisionCompositionIdentity.from_configuration(
        execution_scope_id=execution_scope_id,
        strategy_version=strategy_version,
        configuration=configuration,
    )
