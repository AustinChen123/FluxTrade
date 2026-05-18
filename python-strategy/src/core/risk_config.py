"""Configuration for risk management rules."""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class RiskConfig:
    """Validated risk thresholds loaded from environment variables."""

    max_single_order_notional_pct: Decimal = Decimal("0.05")
    daily_loss_circuit_breaker_pct: Decimal = Decimal("0.05")
    max_orders_per_minute: int = 10
    max_price_deviation_from_mid_pct: Decimal = Decimal("0.03")
    max_position_notional: Decimal = Decimal("100000")

    def __post_init__(self) -> None:
        _validate_pct(
            "max_single_order_notional_pct",
            self.max_single_order_notional_pct,
        )
        _validate_pct(
            "daily_loss_circuit_breaker_pct",
            self.daily_loss_circuit_breaker_pct,
        )
        _validate_positive_int(
            "max_orders_per_minute",
            self.max_orders_per_minute,
        )
        _validate_pct(
            "max_price_deviation_from_mid_pct",
            self.max_price_deviation_from_mid_pct,
        )
        _validate_positive_decimal(
            "max_position_notional",
            self.max_position_notional,
        )

    @classmethod
    def from_env(cls) -> "RiskConfig":
        """Build config from RISK_* environment variables with safe defaults."""
        return cls(
            max_single_order_notional_pct=_decimal_env(
                "RISK_MAX_SINGLE_ORDER_NOTIONAL_PCT",
                cls.max_single_order_notional_pct,
            ),
            daily_loss_circuit_breaker_pct=_decimal_env(
                "RISK_DAILY_LOSS_CIRCUIT_BREAKER_PCT",
                cls.daily_loss_circuit_breaker_pct,
            ),
            max_orders_per_minute=_int_env(
                "RISK_MAX_ORDERS_PER_MINUTE",
                cls.max_orders_per_minute,
            ),
            max_price_deviation_from_mid_pct=_decimal_env(
                "RISK_MAX_PRICE_DEVIATION_FROM_MID_PCT",
                cls.max_price_deviation_from_mid_pct,
            ),
            max_position_notional=_decimal_env(
                "RISK_MAX_POSITION_NOTIONAL",
                cls.max_position_notional,
            ),
        )


def _decimal_env(name: str, default: Decimal) -> Decimal:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a Decimal") from exc


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _validate_pct(name: str, value: Decimal) -> None:
    _validate_positive_decimal(name, value)
    if value > Decimal("1"):
        raise ValueError(f"{name} must be <= 1")


def _validate_positive_decimal(name: str, value: Decimal) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if value <= 0:
        raise ValueError(f"{name} must be > 0")


def _validate_positive_int(name: str, value: int) -> None:
    if not isinstance(value, int):
        raise TypeError(f"{name} must be int")
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
