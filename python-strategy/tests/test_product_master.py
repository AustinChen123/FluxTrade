from decimal import Decimal

from src.core.orm_models import Exchange, Order, Product
from src.core.repositories import LiveOrderRepository


def test_live_order_write_auto_registers_product_without_csv_or_seed(
    sqlite_order_session_factory,
):
    order = Order(
        id="order",
        strategy_id="test_strategy",
        product_id="RITHMIC:MNQ-202609",
        exchange_id="RITHMIC",
        type="market",
        side="buy",
        quantity=Decimal("1"),
        status="new",
        timestamp=1,
    )
    repository = LiveOrderRepository(
        db_session_factory=sqlite_order_session_factory
    )
    repository.add_order(order)
    repository.add_order(
        Order(
            id="order-2",
            strategy_id="test_strategy",
            product_id="RITHMIC:MNQ-202609",
            exchange_id="RITHMIC",
            type="market",
            side="sell",
            quantity=Decimal("1"),
            status="new",
            timestamp=2,
        )
    )

    with sqlite_order_session_factory() as session:
        exchange = session.get(Exchange, "RITHMIC")
        product = session.get(Product, "RITHMIC:MNQ-202609")
        assert exchange is not None
        assert exchange.name == "Rithmic"
        assert product is not None
        assert product.exchange_id == "RITHMIC"
        assert product.base_asset == "MNQ"
        assert product.quote_asset == "USD"
        assert session.query(Product).count() == 1
