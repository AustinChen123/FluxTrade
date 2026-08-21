from decimal import Decimal

import pytest

from src.core.models import OrderSide, Trade


def test_product_id_validation():
    # Valid
    Trade(
        id="1",
        product_id="BINANCE:BTC-PERP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=OrderSide.BUY,
        timestamp=1000,
    )

    # Invalid (missing exchange)
    with pytest.raises(ValueError):
        Trade(
            id="1",
            product_id="BTC-PERP",
            price=Decimal("100"),
            quantity=Decimal("1"),
            side=OrderSide.BUY,
            timestamp=1000,
        )

    # Invalid (missing -PERP)
    with pytest.raises(ValueError):
        Trade(
            id="1",
            product_id="BINANCE:BTC",
            price=Decimal("100"),
            quantity=Decimal("1"),
            side=OrderSide.BUY,
            timestamp=1000,
        )


def test_model_decimal_parsing():
    # Test string to decimal
    trade = Trade.model_validate(
        {
            "id": "1",
            "product_id": "BINANCE:BTC-PERP",
            "price": "50000.50",
            "quantity": "1",
            "side": "buy",
            "timestamp": 1000,
        }
    )
    assert trade.price == Decimal("50000.50")
