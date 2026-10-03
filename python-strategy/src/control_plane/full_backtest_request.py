"""Strict primitive and fee DTOs for full-strategy backtests."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, field_validator


def _strict_text(value: object, name: str, *, path_free: bool = False) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{name} must be nonblank text")
    if path_free and ("/" in value or "\\" in value):
        raise ValueError(f"{name} must not be a path")
    return value


def _decimal_input(value: object, name: str) -> Decimal:
    if type(value) is Decimal:
        result = value
    elif type(value) is str:
        try:
            result = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError(f"{name} must be a Decimal or decimal string") from exc
    else:
        raise ValueError(f"{name} must be a Decimal or decimal string")
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


class _StrictRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FullBacktestFees(_StrictRequestModel):
    maker: Decimal
    taker: Decimal

    @field_validator("maker", "taker", mode="before")
    @classmethod
    def parse_fee(cls, value: object, info) -> Decimal:
        fee = _decimal_input(value, info.field_name)
        if fee < 0:
            raise ValueError(f"{info.field_name} must be nonnegative")
        return fee
