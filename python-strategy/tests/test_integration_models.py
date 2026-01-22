import pytest
from decimal import Decimal
from src.core.models import Candlestick, Trade

def test_product_id_validation():
    # Valid
    Trade(id="1", product_id="BINANCE:BTC-PERP", price=100, quantity=1, side="buy", timestamp=1000)
    
    # Invalid (missing exchange)
    with pytest.raises(ValueError):
        Trade(id="1", product_id="BTC-PERP", price=100, quantity=1, side="buy", timestamp=1000)
        
    # Invalid (missing -PERP)
    with pytest.raises(ValueError):
        Trade(id="1", product_id="BINANCE:BTC", price=100, quantity=1, side="buy", timestamp=1000)

def test_model_decimal_parsing():
    # Test string to decimal
    t = Trade(id="1", product_id="BINANCE:BTC-PERP", price="50000.50", quantity=1, side="buy", timestamp=1000)
    assert t.price == Decimal("50000.50")
    
    # Test float input (should work if pydantic allows, but we prefer string)
    t2 = Trade(id="1", product_id="BINANCE:BTC-PERP", price=50000.50, quantity=1, side="buy", timestamp=1000)
    assert t2.price == Decimal("50000.5") # Float precision might be an issue, but simple ones work
