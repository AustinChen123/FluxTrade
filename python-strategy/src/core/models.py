from pydantic import BaseModel
from decimal import Decimal

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
