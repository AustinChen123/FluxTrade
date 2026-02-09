from pydantic import BaseModel, ConfigDict, field_validator, field_serializer
from decimal import Decimal
from enum import Enum
from typing import Optional, Any
import re

PRODUCT_ID_REGEX = r"^[A-Z0-9]+:[A-Z0-9_]+-PERP$"

class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"

    @staticmethod
    def from_position_side(ps: "PositionSide") -> "OrderSide":
        """Convert a PositionSide to its corresponding OrderSide."""
        if ps == PositionSide.LONG:
            return OrderSide.BUY
        return OrderSide.SELL

    @staticmethod
    def closing_side(ps: "PositionSide") -> "OrderSide":
        """Return the OrderSide that closes a given PositionSide."""
        if ps == PositionSide.LONG:
            return OrderSide.SELL
        return OrderSide.BUY


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

    @staticmethod
    def from_order_side(os: "OrderSide") -> "PositionSide":
        """Convert an OrderSide to its corresponding PositionSide."""
        if os == OrderSide.BUY:
            return PositionSide.LONG
        return PositionSide.SHORT


class SignalType(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"
    NO_SIGNAL = "NO_SIGNAL"

class StrategyStatus(str, Enum):
    """Lifecycle states for a hot-pluggable strategy."""
    DISCOVERED = "DISCOVERED"
    READY = "READY"
    WARNING = "WARNING"
    ACTIVE = "ACTIVE"
    STOPPED = "STOPPED"
    ERROR = "ERROR"

class BaseFluxModel(BaseModel):
    """Base model with common configuration"""
    model_config = ConfigDict(
        # Allow population by alias/name
        populate_by_name=True
    )

    @field_serializer('*', mode='wrap', when_used='json')
    @classmethod
    def serialize_decimal(cls, value: Any, handler: Any) -> Any:
        """Serialize Decimal fields as strings for JSON compatibility."""
        if isinstance(value, Decimal):
            return str(value)
        return handler(value)

class Trade(BaseFluxModel):
    id: str
    product_id: str
    price: Decimal
    quantity: Decimal
    side: OrderSide
    timestamp: int  # Unix timestamp in milliseconds

    @field_validator('product_id')
    @classmethod
    def validate_product_id(cls, v: str) -> str:
        if not re.match(PRODUCT_ID_REGEX, v):
            raise ValueError(f"Invalid product_id format: {v}. Expected EXCHANGE:SYMBOL-PERP")
        return v

class Candlestick(BaseFluxModel):
    product_id: str
    timeframe: str
    timestamp: int  # Unix timestamp in milliseconds
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    @field_validator('product_id')
    @classmethod
    def validate_product_id(cls, v: str) -> str:
        if not re.match(PRODUCT_ID_REGEX, v):
            raise ValueError(f"Invalid product_id format: {v}. Expected EXCHANGE:SYMBOL-PERP")
        return v

class Signal(BaseFluxModel):
    strategy_id: str
    product_id: str
    timeframe: str
    timestamp: int  # Unix timestamp in milliseconds
    type: SignalType
    value: Optional[Decimal] = None
    quantity: Optional[Decimal] = None
    price: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    trailing_distance: Optional[Decimal] = None
    metadata: Optional[dict] = None

    @field_validator('product_id')
    @classmethod
    def validate_product_id(cls, v: str) -> str:
        if not re.match(PRODUCT_ID_REGEX, v):
            raise ValueError(f"Invalid product_id format: {v}. Expected EXCHANGE:SYMBOL-PERP")
        return v

class Position(BaseFluxModel):
    strategy_id: str
    product_id: str
    side: PositionSide  # "LONG" or "SHORT"
    quantity: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal

    @field_validator('product_id')
    @classmethod
    def validate_product_id(cls, v: str) -> str:
        if not re.match(PRODUCT_ID_REGEX, v):
            raise ValueError(f"Invalid product_id format: {v}. Expected EXCHANGE:SYMBOL-PERP")
        return v