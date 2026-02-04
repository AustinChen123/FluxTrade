from pydantic import BaseModel, ConfigDict, field_validator
from decimal import Decimal
from enum import Enum
from typing import Optional
import re

PRODUCT_ID_REGEX = r"^[A-Z0-9]+:[A-Z0-9_]+-PERP$"

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
        json_encoders={Decimal: str},
        # Allow population by alias/name
        populate_by_name=True
    )

class Trade(BaseFluxModel):
    id: str
    product_id: str
    price: Decimal
    quantity: Decimal
    side: str
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
    side: str  # "LONG" or "SHORT"
    quantity: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal

    @field_validator('product_id')
    @classmethod
    def validate_product_id(cls, v: str) -> str:
        if not re.match(PRODUCT_ID_REGEX, v):
            raise ValueError(f"Invalid product_id format: {v}. Expected EXCHANGE:SYMBOL-PERP")
        return v