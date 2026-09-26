"""Immutable, venue-neutral replay inputs; no valuation or migration policy.

Times are exact integer UTC timestamps in the input's common time unit. Ranges
are half-open; lookup never uses delivery time, arrival order or a fallback.
"""

from dataclasses import dataclass, fields, replace
from decimal import Decimal
from typing import Generic, TypeVar

from src.core.product_registry import CapitalModel, FeeModel, InstrumentSpec, MarketType


def _identity(value: str) -> None:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError("identity must be a nonblank normalized string")


def _timestamp(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("timestamp must be a nonnegative exact integer UTC timestamp")


def _decimal(value: Decimal, *, positive: bool = True) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("numeric input must be a finite Decimal")
    if value < 0 or (positive and value == 0):
        raise ValueError("numeric input outside positive/nonnegative domain")


@dataclass(frozen=True, slots=True)
class AccountKey:
    venue: str
    environment: str
    account_id: str
    subaccount_id: str | None = None

    def __post_init__(self) -> None:
        for value in (self.venue, self.environment, self.account_id):
            _identity(value)
        if self.subaccount_id is not None:
            _identity(self.subaccount_id)


@dataclass(frozen=True, slots=True)
class EffectiveRange:
    start: int
    end: int | None

    def __post_init__(self) -> None:
        _timestamp(self.start)
        if self.end is not None:
            _timestamp(self.end)
            if self.end <= self.start:
                raise ValueError("effective range must be nonempty and half-open")


@dataclass(frozen=True, slots=True)
class InstrumentSpecVersion:
    product_id: str
    version_id: str
    effective: EffectiveRange
    spec: InstrumentSpec

    def __post_init__(self) -> None:
        _identity(self.product_id)
        _identity(self.version_id)
        if type(self.effective) is not EffectiveRange:
            raise ValueError("effective range required")
        if type(self.spec) is not InstrumentSpec:
            raise ValueError("instrument spec required")
        if self.spec.product_id != self.product_id:
            raise ValueError("instrument product identity mismatch")
        for name in ("exchange", "symbol", "base", "quote"):
            _identity(getattr(self.spec, name))
        # Replay must not inherit product_registry's optional precision fallback.
        required = {"quantity_step", "price_tick", "min_quantity", "multiplier"}
        numeric = required | {"min_notional", "tick_value", "capital_per_contract"}
        for name in numeric:
            value = getattr(self.spec, name)
            if value is not None or name in required:
                _decimal(value, positive=name != "min_notional")
        if self.spec.session_calendar_id is not None:
            _identity(self.spec.session_calendar_id)
        for name, enum_type in (
            ("fee_model", FeeModel),
            ("capital_model", CapitalModel),
            ("market_type", MarketType),
        ):
            value = getattr(self.spec, name)
            if value is not None and type(value) is not enum_type:
                raise ValueError("instrument model must be a typed enum")
        # Detach the frozen payload from the caller's instance.
        object.__setattr__(self, "spec", replace(self.spec))


@dataclass(frozen=True, slots=True)
class MarginTier:
    """Input table row with inclusive size bounds; no tier-selection policy."""

    tier: int
    min_contracts: Decimal
    max_contracts: Decimal
    maintenance_margin_rate: Decimal
    initial_margin_rate: Decimal
    max_leverage: Decimal

    def __post_init__(self) -> None:
        if type(self.tier) is not int or self.tier <= 0:
            raise ValueError("tier number must be a positive integer")
        for field in fields(self):
            if field.name != "tier":
                _decimal(
                    getattr(self, field.name), positive=field.name != "min_contracts"
                )
        if self.max_contracts < self.min_contracts:
            raise ValueError("tier size range is reversed")


@dataclass(frozen=True, slots=True)
class RuleDataVersion:
    product_id: str
    version_id: str
    effective: EffectiveRange
    tiers: tuple[MarginTier, ...]
    pool_id: str

    def __post_init__(self) -> None:
        _identity(self.product_id)
        _identity(self.version_id)
        _identity(self.pool_id)
        if type(self.effective) is not EffectiveRange:
            raise ValueError("effective range required")
        if type(self.tiers) is not tuple or not self.tiers:
            raise ValueError("nonempty immutable tier tuple required")
        previous = None
        for tier in self.tiers:
            if type(tier) is not MarginTier:
                raise ValueError("margin tier required")
            if previous is not None and (
                tier.tier <= previous.tier
                or tier.min_contracts <= previous.max_contracts
            ):
                raise ValueError("tiers must be ordered, unique and non-overlapping")
            previous = tier


Version = TypeVar("Version", InstrumentSpecVersion, RuleDataVersion)


@dataclass(frozen=True, slots=True)
class EffectiveTimeline(Generic[Version]):
    """One product and one version kind covering exactly the requested range."""

    product_id: str
    coverage: EffectiveRange
    versions: tuple[Version, ...]

    def __post_init__(self) -> None:
        _identity(self.product_id)
        if type(self.coverage) is not EffectiveRange:
            raise ValueError("coverage range required")
        if type(self.versions) is not tuple or not self.versions:
            raise ValueError("nonempty immutable version tuple required")
        expected_start = self.coverage.start
        seen = set()
        kind = type(self.versions[0])
        for version in self.versions:
            if (
                kind not in (InstrumentSpecVersion, RuleDataVersion)
                or type(version) is not kind
            ):
                raise ValueError("timeline requires one supported version kind")
            if version.product_id != self.product_id:
                raise ValueError("wrong timeline product key")
            if isinstance(version, RuleDataVersion) and (
                version.pool_id != self.versions[0].pool_id
            ):
                raise ValueError("mixed timeline pool identity")
            if version.version_id in seen:
                raise ValueError("duplicate version identity")
            seen.add(version.version_id)
            if expected_start is None or version.effective.start != expected_start:
                raise ValueError("timeline boundary mismatch, gap or overlap")
            expected_start = version.effective.end
        if expected_start != self.coverage.end:
            raise ValueError("timeline does not exactly cover requested range")

    def resolve(self, effective_at: int) -> Version:
        _timestamp(effective_at)
        if effective_at < self.coverage.start or (
            self.coverage.end is not None and effective_at >= self.coverage.end
        ):
            raise ValueError("effective time outside timeline coverage")
        for version in self.versions:
            if version.effective.end is None or effective_at < version.effective.end:
                return version
        raise ValueError("unresolved effective time")
