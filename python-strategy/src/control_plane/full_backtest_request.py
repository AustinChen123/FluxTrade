"""Strict instrument DTOs for full-strategy backtests."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from src.control_plane.models import (
    BacktestInstrumentConfig,
    _require_dated_future_rules,
)
from src.core.product_registry import (
    CapitalModel,
    FeeModel,
    InstrumentSpec,
    MarketType,
)


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
