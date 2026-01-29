"""
Tests for src/core/adapters/simulated.py

Covers:
- Order placement (market, limit)
- Order cancellation
- Market data processing and fills
- Position tracking
- Balance management
"""

import pytest
from decimal import Decimal

from src.core.adapters.simulated import SimulatedAdapter
from src.core.orm_models import Order
from src.core.models import Candlestick


class TestSimulatedAdapterBasics:
    """Basic tests for SimulatedAdapter."""

    def test_initialization(self):
        """Should initialize with correct defaults."""
        adapter = SimulatedAdapter()

        assert adapter.get_balance("USDT") == Decimal("100000")
        assert len(adapter.open_orders) == 0
        assert len(adapter.positions) == 0

    def test_initialization_custom_balance(self):
        """Should accept custom initial balance."""
        adapter = SimulatedAdapter(initial_balance=Decimal("50000"))

        assert adapter.get_balance("USDT") == Decimal("50000")

    def test_get_balance_unknown_asset(self):
        """Should return zero for unknown asset."""
        adapter = SimulatedAdapter()

        assert adapter.get_balance("BTC") == Decimal("0")


class TestSimulatedOrderPlacement:
    """Tests for order placement in SimulatedAdapter."""

    def test_place_market_order(self, order_factory):
        """Should place market order and return exchange ID."""
        adapter = SimulatedAdapter()
        order = order_factory(order_type="market")

        exchange_id = adapter.place_order(order)

        assert exchange_id is not None
        assert exchange_id.startswith("SIM-")
        assert len(adapter.open_orders) == 1

    def test_place_limit_order(self, order_factory):
        """Should place limit order."""
        adapter = SimulatedAdapter()
        order = order_factory(order_type="limit", price=Decimal("42000"))

        exchange_id = adapter.place_order(order)

        assert exchange_id is not None
        assert len(adapter.open_orders) == 1
        assert adapter.open_orders[0].price == Decimal("42000")

    def test_place_multiple_orders(self, order_factory):
        """Should handle multiple open orders."""
        adapter = SimulatedAdapter()

        for i in range(5):
            order = order_factory(order_type="limit", price=Decimal(f"{40000 + i * 100}"))
            adapter.place_order(order)

        assert len(adapter.open_orders) == 5

    def test_order_gets_exchange_id_assigned(self, order_factory):
        """Order object should get exchange_order_id set."""
        adapter = SimulatedAdapter()
        order = order_factory()

        exchange_id = adapter.place_order(order)

        assert order.exchange_order_id == exchange_id


class TestSimulatedOrderCancellation:
    """Tests for order cancellation in SimulatedAdapter."""

    def test_cancel_existing_order(self, order_factory):
        """Should cancel existing order."""
        adapter = SimulatedAdapter()
        order = order_factory()
        exchange_id = adapter.place_order(order)

        result = adapter.cancel_order(exchange_id, order.product_id)

        assert result is True
        assert len(adapter.open_orders) == 0

    def test_cancel_nonexistent_order(self, order_factory):
        """Should return False for nonexistent order."""
        adapter = SimulatedAdapter()

        result = adapter.cancel_order("NONEXISTENT-123", "BINANCE:BTCUSDT-PERP")

        assert result is False

    def test_cancel_one_of_many_orders(self, order_factory):
        """Should cancel only the specified order."""
        adapter = SimulatedAdapter()
        orders = []
        for _ in range(3):
            order = order_factory()
            adapter.place_order(order)
            orders.append(order)

        # Cancel middle order
        result = adapter.cancel_order(orders[1].exchange_order_id, orders[1].product_id)

        assert result is True
        assert len(adapter.open_orders) == 2


class TestSimulatedMarketDataProcessing:
    """Tests for market data processing and order fills."""

    def test_market_order_fills_on_candle(self, order_factory, candlestick_factory):
        """Market order should fill when candle arrives."""
        adapter = SimulatedAdapter()
        order = order_factory(order_type="market")
        adapter.place_order(order)

        candle = candlestick_factory(close=Decimal("42100"))
        fills = adapter.on_market_data(candle)

        assert len(fills) == 1
        assert fills[0]["order"] == order
        # Market orders fill at close (with slippage)
        assert fills[0]["quantity"] == order.quantity
        assert len(adapter.open_orders) == 0

    def test_limit_buy_fills_when_low_touches_price(self, order_factory, candlestick_factory):
        """Limit buy should fill when candle low <= order price."""
        adapter = SimulatedAdapter()
        order = order_factory(order_type="limit", side="buy", price=Decimal("41500"))
        adapter.place_order(order)

        # Candle with low at 41000 (below limit price)
        candle = candlestick_factory(
            low=Decimal("41000"),
            high=Decimal("42500"),
            close=Decimal("42000")
        )
        fills = adapter.on_market_data(candle)

        assert len(fills) == 1
        assert fills[0]["price"] == Decimal("41500")  # Fills at limit price

    def test_limit_buy_not_fill_when_low_above_price(self, order_factory, candlestick_factory):
        """Limit buy should not fill when candle low > order price."""
        adapter = SimulatedAdapter()
        order = order_factory(order_type="limit", side="buy", price=Decimal("40000"))
        adapter.place_order(order)

        # Candle with low at 41000 (above limit price)
        candle = candlestick_factory(low=Decimal("41000"))
        fills = adapter.on_market_data(candle)

        assert len(fills) == 0
        assert len(adapter.open_orders) == 1

    def test_limit_sell_fills_when_high_reaches_price(self, order_factory, candlestick_factory):
        """Limit sell should fill when candle high >= order price."""
        adapter = SimulatedAdapter()
        order = order_factory(order_type="limit", side="sell", price=Decimal("43000"))
        adapter.place_order(order)

        # Candle with high at 44000 (above limit price)
        candle = candlestick_factory(high=Decimal("44000"), low=Decimal("41000"))
        fills = adapter.on_market_data(candle)

        assert len(fills) == 1
        assert fills[0]["price"] == Decimal("43000")

    def test_orders_for_different_products_unaffected(self, order_factory, candlestick_factory):
        """Orders for different products should not be affected."""
        adapter = SimulatedAdapter()

        btc_order = order_factory(product_id="BINANCE:BTCUSDT-PERP", order_type="market")
        eth_order = order_factory(product_id="BINANCE:ETHUSDT-PERP", order_type="market")
        adapter.place_order(btc_order)
        adapter.place_order(eth_order)

        # Only BTC candle
        btc_candle = candlestick_factory(product_id="BINANCE:BTCUSDT-PERP")
        fills = adapter.on_market_data(btc_candle)

        assert len(fills) == 1
        assert fills[0]["order"].product_id == "BINANCE:BTCUSDT-PERP"
        assert len(adapter.open_orders) == 1  # ETH order still open


class TestSimulatedPositionTracking:
    """Tests for position tracking in SimulatedAdapter."""

    def test_position_created_on_fill(self, order_factory, candlestick_factory):
        """Position should be created when order fills."""
        adapter = SimulatedAdapter()
        order = order_factory(order_type="market", side="buy", quantity=Decimal("0.5"))
        adapter.place_order(order)

        candle = candlestick_factory(close=Decimal("42000"))
        adapter.on_market_data(candle)

        pos = adapter.get_position(order.product_id)
        assert pos is not None
        assert pos.quantity == Decimal("0.5")
        assert pos.side == "LONG"

    def test_position_increases_on_same_direction(self, order_factory, candlestick_factory):
        """Position should increase when adding in same direction."""
        adapter = SimulatedAdapter()

        # First buy
        order1 = order_factory(order_type="market", side="buy", quantity=Decimal("0.5"))
        adapter.place_order(order1)
        adapter.on_market_data(candlestick_factory(close=Decimal("40000")))

        # Second buy
        order2 = order_factory(order_type="market", side="buy", quantity=Decimal("0.5"))
        adapter.place_order(order2)
        adapter.on_market_data(candlestick_factory(close=Decimal("44000")))

        pos = adapter.get_position(order1.product_id)
        assert pos is not None
        assert pos.quantity == Decimal("1.0")

    def test_position_reduces_on_opposite_direction(self, order_factory, candlestick_factory):
        """Position should reduce when selling partial amount."""
        adapter = SimulatedAdapter()

        # Buy 2.0
        buy_order = order_factory(order_type="market", side="buy", quantity=Decimal("2.0"))
        adapter.place_order(buy_order)
        adapter.on_market_data(candlestick_factory(close=Decimal("42000")))

        # Sell 1.0 (partial close)
        sell_order = order_factory(order_type="market", side="sell", quantity=Decimal("1.0"))
        adapter.place_order(sell_order)
        adapter.on_market_data(candlestick_factory(close=Decimal("43000")))

        pos = adapter.get_position(buy_order.product_id)
        assert pos is not None
        assert pos.quantity == Decimal("1.0")

    def test_no_position_initially(self):
        """Should have no position before any orders."""
        adapter = SimulatedAdapter()

        pos = adapter.get_position("BINANCE:BTCUSDT-PERP")

        assert pos is None


class TestSimulatedMultipleProducts:
    """Tests for handling multiple products."""

    def test_independent_positions(self, order_factory, candlestick_factory):
        """Positions for different products should be independent."""
        adapter = SimulatedAdapter()

        # BTC position
        btc_order = order_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            order_type="market",
            side="buy",
            quantity=Decimal("1.0")
        )
        adapter.place_order(btc_order)
        adapter.on_market_data(candlestick_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            close=Decimal("42000")
        ))

        # ETH position
        eth_order = order_factory(
            product_id="BINANCE:ETHUSDT-PERP",
            order_type="market",
            side="sell",
            quantity=Decimal("10.0")
        )
        adapter.place_order(eth_order)
        adapter.on_market_data(candlestick_factory(
            product_id="BINANCE:ETHUSDT-PERP",
            close=Decimal("2000")
        ))

        btc_pos = adapter.get_position("BINANCE:BTCUSDT-PERP")
        eth_pos = adapter.get_position("BINANCE:ETHUSDT-PERP")

        assert btc_pos.quantity == Decimal("1.0")
        assert eth_pos.quantity == Decimal("10.0")


class TestSimulatedEdgeCases:
    """Edge case tests for SimulatedAdapter."""

    def test_very_small_order(self, order_factory, candlestick_factory):
        """Should handle very small order quantities."""
        adapter = SimulatedAdapter()
        order = order_factory(order_type="market", quantity=Decimal("0.00001"))
        adapter.place_order(order)

        fills = adapter.on_market_data(candlestick_factory())

        assert len(fills) == 1
        assert fills[0]["quantity"] == Decimal("0.00001")

    def test_multiple_fills_same_candle(self, order_factory, candlestick_factory):
        """Should fill multiple orders on same candle."""
        adapter = SimulatedAdapter()

        for _ in range(5):
            order = order_factory(order_type="market", quantity=Decimal("0.1"))
            adapter.place_order(order)

        fills = adapter.on_market_data(candlestick_factory())

        assert len(fills) == 5
        assert len(adapter.open_orders) == 0

    def test_limit_order_at_exact_price(self, order_factory, candlestick_factory):
        """Limit order should fill when price exactly matches."""
        adapter = SimulatedAdapter()
        order = order_factory(order_type="limit", side="buy", price=Decimal("41000"))
        adapter.place_order(order)

        # Candle with low exactly at limit price
        candle = candlestick_factory(low=Decimal("41000"), high=Decimal("42000"))
        fills = adapter.on_market_data(candle)

        assert len(fills) == 1
