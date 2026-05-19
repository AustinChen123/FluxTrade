"""Tests for risk configuration loading and validation."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from src.core.risk_config import RiskConfig


def test_risk_config_defaults_are_decimal_and_positive() -> None:
    config = RiskConfig()

    assert config.max_single_order_notional_pct == Decimal("0.05")
    assert config.daily_loss_circuit_breaker_pct == Decimal("0.05")
    assert config.max_orders_per_minute == 10
    assert config.max_price_deviation_from_mid_pct == Decimal("0.03")
    assert config.max_position_notional == Decimal("100000")


def test_risk_config_is_frozen() -> None:
    config = RiskConfig()

    with pytest.raises(FrozenInstanceError):
        config.max_orders_per_minute = 20


def test_risk_config_from_env_loads_all_values(monkeypatch) -> None:
    monkeypatch.setenv("RISK_MAX_SINGLE_ORDER_NOTIONAL_PCT", "0.10")
    monkeypatch.setenv("RISK_DAILY_LOSS_CIRCUIT_BREAKER_PCT", "0.08")
    monkeypatch.setenv("RISK_MAX_ORDERS_PER_MINUTE", "25")
    monkeypatch.setenv("RISK_MAX_PRICE_DEVIATION_FROM_MID_PCT", "0.02")
    monkeypatch.setenv("RISK_MAX_POSITION_NOTIONAL", "250000.12345678")

    config = RiskConfig.from_env()

    assert config.max_single_order_notional_pct == Decimal("0.10")
    assert config.daily_loss_circuit_breaker_pct == Decimal("0.08")
    assert config.max_orders_per_minute == 25
    assert config.max_price_deviation_from_mid_pct == Decimal("0.02")
    assert config.max_position_notional == Decimal("250000.12345678")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_single_order_notional_pct", Decimal("0")),
        ("daily_loss_circuit_breaker_pct", Decimal("-0.01")),
        ("max_price_deviation_from_mid_pct", Decimal("1.01")),
        ("max_position_notional", Decimal("0")),
    ],
)
def test_risk_config_rejects_invalid_decimal_values(field, value) -> None:
    kwargs = {field: value}

    with pytest.raises(ValueError):
        RiskConfig(**kwargs)


def test_risk_config_rejects_invalid_order_limit() -> None:
    with pytest.raises(ValueError):
        RiskConfig(max_orders_per_minute=0)


def test_risk_config_rejects_non_decimal_values() -> None:
    with pytest.raises(TypeError):
        RiskConfig(max_position_notional=100000)  # type: ignore[arg-type]


def test_risk_config_from_env_rejects_invalid_decimal(monkeypatch) -> None:
    monkeypatch.setenv("RISK_MAX_POSITION_NOTIONAL", "not-a-decimal")

    with pytest.raises(ValueError, match="RISK_MAX_POSITION_NOTIONAL"):
        RiskConfig.from_env()


def test_risk_config_from_env_rejects_invalid_integer(monkeypatch) -> None:
    monkeypatch.setenv("RISK_MAX_ORDERS_PER_MINUTE", "10.5")

    with pytest.raises(ValueError, match="RISK_MAX_ORDERS_PER_MINUTE"):
        RiskConfig.from_env()
