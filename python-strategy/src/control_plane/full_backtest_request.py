"""Strict wire DTO and dispatch parser for full-strategy backtests."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from src.control_plane.models import (
    BacktestInstrumentConfig,
    BacktestJobRequest,
    _require_dated_future_rules,
)
from src.core.product_registry import (
    CapitalModel,
    FeeModel,
    InstrumentSpec,
    MarketType,
)

_MAX_UTC_MILLISECOND = 253_402_300_799_999


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


class FullBacktestInstrument(_StrictRequestModel):
    product_id: str
    exchange: str
    symbol: str
    base: str
    quote: str
    quantity_step: Decimal | None = None
    price_tick: Decimal | None = None
    min_notional: Decimal | None = None
    min_quantity: Decimal | None = None
    multiplier: Decimal | None = None
    tick_value: Decimal | None = None
    fee_model: FeeModel | None = None
    capital_model: CapitalModel | None = None
    capital_per_contract: Decimal | None = None
    session_calendar_id: str | None = None
    market_type: MarketType | None = None

    @field_validator("product_id", "exchange", "symbol", "base", "quote", mode="before")
    @classmethod
    def parse_required_text(cls, value: object, info) -> str:
        return _strict_text(value, info.field_name)

    @field_validator("session_calendar_id", mode="before")
    @classmethod
    def parse_calendar_id(cls, value: object) -> str | None:
        if value is None:
            return None
        return _strict_text(value, "session_calendar_id")

    @field_validator(
        "quantity_step",
        "price_tick",
        "min_notional",
        "min_quantity",
        "multiplier",
        "tick_value",
        "capital_per_contract",
        mode="before",
    )
    @classmethod
    def parse_optional_decimal(cls, value: object, info) -> Decimal | None:
        if value is None:
            return None
        return _decimal_input(value, info.field_name)

    @field_validator("fee_model", "capital_model", "market_type", mode="before")
    @classmethod
    def parse_optional_enum(
        cls, value: object, info
    ) -> FeeModel | CapitalModel | MarketType | None:
        if value is None:
            return None
        if type(value) is not str:
            raise ValueError(f"{info.field_name} must be an enum value string")
        enum_type = {
            "fee_model": FeeModel,
            "capital_model": CapitalModel,
            "market_type": MarketType,
        }[info.field_name]
        try:
            return enum_type(value)
        except ValueError as exc:
            raise ValueError(f"invalid {info.field_name}") from exc

    @model_validator(mode="after")
    def validate_instrument_rules(self) -> FullBacktestInstrument:
        _require_dated_future_rules(
            self.product_id,
            instrument_configured=True,
            quantity_step=self.quantity_step,
            price_tick=self.price_tick,
        )
        BacktestInstrumentConfig(
            multiplier=(Decimal("1") if self.multiplier is None else self.multiplier),
            quantity_step=self.quantity_step,
            price_tick=self.price_tick,
            fee_model=self.fee_model or FeeModel.PERCENTAGE_NOTIONAL,
            capital_model=self.capital_model or CapitalModel.NOTIONAL,
            capital_per_contract=self.capital_per_contract,
        )
        self.to_instrument_spec()
        return self

    def to_instrument_spec(self) -> InstrumentSpec:
        return InstrumentSpec(
            product_id=self.product_id,
            exchange=self.exchange,
            symbol=self.symbol,
            base=self.base,
            quote=self.quote,
            quantity_step=self.quantity_step,
            price_tick=self.price_tick,
            min_notional=self.min_notional,
            min_quantity=self.min_quantity,
            multiplier=self.multiplier,
            tick_value=self.tick_value,
            fee_model=self.fee_model,
            capital_model=self.capital_model,
            capital_per_contract=self.capital_per_contract,
            session_calendar_id=self.session_calendar_id,
            market_type=self.market_type,
        )


class FullStrategyBacktestRequest(_StrictRequestModel):
    kind: Literal["full_strategy_backtest"]
    dataset_id: str
    strategy_id: str
    artifact_version: str
    start: int
    end: int
    initial_balance: Decimal
    instrument: FullBacktestInstrument
    fees: FullBacktestFees
    drawdown_limit: Decimal | None
    execution_timeframe: str | None = None

    @field_validator("dataset_id", "strategy_id", "artifact_version", mode="before")
    @classmethod
    def parse_identity_text(cls, value: object, info) -> str:
        return _strict_text(value, info.field_name, path_free=True)

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_timestamp(cls, value: object, info) -> int:
        if type(value) is not int or not 0 <= value <= _MAX_UTC_MILLISECOND:
            raise ValueError(
                f"{info.field_name} must be a UTC millisecond in the supported range"
            )
        return value

    @field_validator("initial_balance", mode="before")
    @classmethod
    def parse_initial_balance(cls, value: object) -> Decimal:
        balance = _decimal_input(value, "initial_balance")
        if balance <= 0:
            raise ValueError("initial_balance must be positive")
        return balance

    @field_validator("drawdown_limit", mode="before")
    @classmethod
    def parse_drawdown_limit(cls, value: object) -> Decimal | None:
        if value is None:
            return None
        limit = _decimal_input(value, "drawdown_limit")
        if limit < 0:
            raise ValueError("drawdown_limit must be nonnegative")
        return limit

    @field_validator("execution_timeframe", mode="before")
    @classmethod
    def parse_execution_timeframe(cls, value: object) -> str | None:
        if value is None:
            return None
        return _strict_text(value, "execution_timeframe")

    @model_validator(mode="after")
    def validate_range_and_currency(self) -> FullStrategyBacktestRequest:
        if self.start > self.end:
            raise ValueError("start must be less than or equal to end")
        return self


BacktestRequest = BacktestJobRequest | FullStrategyBacktestRequest


def parse_backtest_request(payload: object) -> BacktestRequest:
    """Parse one existing CSV request or the strict full-strategy request."""
    if isinstance(payload, Mapping) and payload.get("kind") == "full_strategy_backtest":
        return FullStrategyBacktestRequest.model_validate(payload)
    return BacktestJobRequest.model_validate(payload)
