from pydantic import BaseModel
from decimal import Decimal
from enum import Enum
from typing import Optional

class SignalType(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"
    NO_SIGNAL = "NO_SIGNAL"

class Candlestick(BaseModel):
    product_id: str
    timeframe: str
    timestamp: int  # Unix timestamp in milliseconds
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v)
        }

class Signal(BaseModel):
    strategy_id: str
    product_id: str
    timeframe: str
    timestamp: int
    type: SignalType
    value: Optional[Decimal] = None
    metadata: Optional[dict] = None

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v)
        }

class Position(BaseModel):
    strategy_id: str
    product_id: str
    side: str  # "LONG" or "SHORT"
    quantity: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v)
        }
