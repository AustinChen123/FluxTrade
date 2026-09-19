"""
Tests for src/core/adapters/simulated.py (Rust PyMatchingEngine backed)

Covers:
- Order placement (market, limit, stop_loss, take_profit, trailing_stop)
- Order cancellation
- Market data processing and fills
- Maker / taker fee model
- Position tracking (open, increase, reduce, close)
- SL / TP trigger logic with OCO
- Trailing stop dynamic update
- Balance accuracy (PnL + fees)
"""

from decimal import Decimal

import pytest

from src.core.adapters.simulated import SimulatedAdapter
from src.core.backtest.endpoint_state import build_replay_endpoint_state
from src.core.models import Candlestick
from src.core.precision import PrecisionCodec, PrecisionSpec
from src.core.interfaces.exchange import ExchangeError
from src.core.product_registry import InstrumentSpec


# ── helpers ──────────────────────────────────────────────────────

PRODUCT = "BINANCE:BTCUSDT-PERP"
TF = "15m"


def _candle(ts, o, h, low, c, vol: Decimal | str | int = 100, product=PRODUCT):
    return Candlestick(
        product_id=product,
        timeframe=TF,
        timestamp=ts,
        open=Decimal(str(o)),
        high=Decimal(str(h)),
        low=Decimal(str(low)),
        close=Decimal(str(c)),
        volume=Decimal(str(vol)),
    )


def _approx(a, b, tol=0.01):
    """Compare Decimal/float values within tolerance."""
    return abs(float(a) - float(b)) < tol


# =================================================================
# Basics
# =================================================================


class TestSimulatedAdapterBasics:
    def test_initialization_defaults(self):
        adapter = SimulatedAdapter()
        assert adapter.get_balance() == Decimal("100000")
        assert adapter.get_position(PRODUCT) is None

    def test_initialization_custom(self):
        adapter = SimulatedAdapter(
            Decimal("50000"), maker_fee=Decimal("0.001"), taker_fee=Decimal("0.002")
        )
        assert adapter.get_balance() == Decimal("50000")

    def test_exposes_only_configured_instrument_spec(self):
        spec = InstrumentSpec(
            product_id=PRODUCT,
            exchange="test",
            symbol="MNQ",
            base="MNQ",
            quote="USD",
            multiplier=Decimal("2"),
        )
        adapter = SimulatedAdapter(instrument_spec=spec)

        assert adapter.get_instrument_spec(PRODUCT) is spec
        assert adapter.get_instrument_spec("BINANCE:ETHUSDT-PERP") is None

    @pytest.mark.parametrize(
        ("configured_product", "order_product"),
        [
            ("RITHMIC:MNQ-202509", "BINANCE:BTCUSDT-PERP"),
            ("BINANCE:BTCUSDT-PERP", "RITHMIC:MNQ-202509"),
        ],
    )
    def test_configured_instrument_rejects_other_products_before_submission(
        self, order_factory, configured_product, order_product
    ):
        spec = InstrumentSpec(
            product_id=configured_product,
            exchange=configured_product.partition(":")[0].lower(),
            symbol=configured_product,
            base=configured_product,
            quote="",
            quantity_step=Decimal("1"),
            price_tick=Decimal("0.25"),
        )
        adapter = SimulatedAdapter(instrument_spec=spec)
        order = order_factory(
            product_id=order_product,
            order_type="market",
            quantity=Decimal("1"),
            price=None,
        )

        with pytest.raises(ExchangeError, match="instrument_spec_product_mismatch"):
            adapter.place_order(order)

        assert adapter.get_open_orders(order_product) == []

    def test_unconfigured_adapter_keeps_legacy_multi_product_submission(
        self, order_factory
    ):
        adapter = SimulatedAdapter()
        orders = [
            order_factory(product_id="BINANCE:BTCUSDT-PERP"),
            order_factory(product_id="BINANCE:ETHUSDT-PERP"),
        ]

        for order in orders:
            adapter.place_order(order)

        assert len(adapter.get_open_orders()) == 2

    def test_unconfigured_adapter_rejects_dated_future_before_submission(
        self, order_factory
    ):
        product_id = "RITHMIC:MNQ-202509"
        adapter = SimulatedAdapter()
        order = order_factory(
            product_id=product_id,
            order_type="market",
            quantity=Decimal("1"),
            price=None,
        )

        with pytest.raises(
            ExchangeError,
            match="instrument_spec_required_for_dated_future",
        ):
            adapter.place_order(order)

        assert adapter.get_open_orders(product_id) == []

    @pytest.mark.parametrize(
        (
            "order_type",
            "side",
            "price",
            "trigger_price",
            "expected_price",
            "expected_trigger",
        ),
        [
            ("limit", "buy", "50123.456", None, "50123.40", None),
            ("limit", "sell", "50123.456", None, "50123.50", None),
            ("stop_loss", "sell", None, "50123.456", None, "50123.50"),
            ("take_profit", "buy", None, "50123.456", None, "50123.40"),
        ],
    )
    def test_configured_crypto_submits_quantized_values_to_matcher(
        self,
        order_factory,
        order_type,
        side,
        price,
        trigger_price,
        expected_price,
        expected_trigger,
    ):
        spec = InstrumentSpec(
            product_id=PRODUCT,
            exchange="binance",
            symbol="BTC/USDT:USDT",
            base="BTC",
            quote="USDT",
            quantity_step=Decimal("0.001"),
            price_tick=Decimal("0.10"),
        )
        adapter = SimulatedAdapter(instrument_spec=spec)
        order = order_factory(
            product_id=PRODUCT,
            order_type=order_type,
            side=side,
            quantity=Decimal("0.0109"),
            price=Decimal(price) if price is not None else None,
            trigger_price=(
                Decimal(trigger_price) if trigger_price is not None else None
            ),
        )
        original_to_rust_order = adapter._to_rust_order
        submitted_values = []

        def capture_to_rust_order(order):
            submitted_values.append(
                (
                    order.quantity,
                    order.price,
                    order.trigger_price,
                )
            )
            return original_to_rust_order(order)

        adapter._to_rust_order = capture_to_rust_order

        adapter.validate_order(order)
        adapter.validate_order(order)
        adapter.place_order(order)

        expected = (
            Decimal("0.010"),
            Decimal(expected_price) if expected_price is not None else None,
            Decimal(expected_trigger) if expected_trigger is not None else None,
        )
        assert (order.quantity, order.price, order.trigger_price) == expected
        assert submitted_values == [expected]
        assert adapter.get_open_orders(PRODUCT) == [order]

    @pytest.mark.parametrize(
        ("quantity", "price", "error"),
        [
            ("1.5", "20000.00", "quantity_off_step"),
            ("1", "20000.10", "price_off_tick"),
        ],
    )
    def test_dated_future_order_validation_blocks_submission(
        self, order_factory, quantity, price, error
    ):
        product_id = "RITHMIC:MNQ-202509"
        spec = InstrumentSpec(
            product_id=product_id,
            exchange="rithmic",
            symbol="MNQ-202509",
            base="MNQ",
            quote="USD",
            quantity_step=Decimal("1"),
            price_tick=Decimal("0.25"),
        )
        adapter = SimulatedAdapter(instrument_spec=spec)
        order = order_factory(
            product_id=product_id,
            quantity=Decimal(quantity),
            price=Decimal(price),
        )

        with pytest.raises(ExchangeError, match=error):
            adapter.place_order(order)

        assert adapter.get_open_orders(product_id) == []

    @pytest.mark.parametrize("order_type", ["stop_loss", "take_profit"])
    def test_dated_future_off_tick_protection_blocks_submission(
        self, order_factory, order_type
    ):
        product_id = "RITHMIC:MNQ-202509"
        spec = InstrumentSpec(
            product_id=product_id,
            exchange="rithmic",
            symbol="MNQ-202509",
            base="MNQ",
            quote="USD",
            quantity_step=Decimal("1"),
            price_tick=Decimal("0.25"),
        )
        adapter = SimulatedAdapter(instrument_spec=spec)
        order = order_factory(
            product_id=product_id,
            order_type=order_type,
            quantity=Decimal("1"),
            trigger_price=Decimal("19999.90"),
        )

        with pytest.raises(ExchangeError, match="trigger_price_off_tick"):
            adapter.place_order(order)

        assert adapter.get_open_orders(product_id) == []

    def test_dated_future_market_order_does_not_require_price_tick(self, order_factory):
        product_id = "RITHMIC:MNQ-202509"
        spec = InstrumentSpec(
            product_id=product_id,
            exchange="rithmic",
            symbol="MNQ-202509",
            base="MNQ",
            quote="USD",
            quantity_step=Decimal("1"),
        )
        adapter = SimulatedAdapter(instrument_spec=spec)
        order = order_factory(
            product_id=product_id,
            order_type="market",
            quantity=Decimal("1"),
            price=None,
            trigger_price=None,
        )

        exchange_order_id = adapter.place_order(order)

        assert exchange_order_id.startswith("SIM-")
        assert adapter.get_open_orders(product_id) == [order]

    @pytest.mark.parametrize(
        ("distance", "accepted"),
        [("0.25", True), ("0.10", False)],
    )
    def test_dated_future_trailing_distance_validation(
        self, order_factory, distance, accepted
    ):
        product_id = "RITHMIC:MNQ-202509"
        spec = InstrumentSpec(
            product_id=product_id,
            exchange="rithmic",
            symbol="MNQ-202509",
            base="MNQ",
            quote="USD",
            quantity_step=Decimal("1"),
            price_tick=Decimal("0.25"),
        )
        adapter = SimulatedAdapter(instrument_spec=spec)
        order = order_factory(
            product_id=product_id,
            order_type="trailing_stop",
            quantity=Decimal("1"),
            price=None,
            trigger_price=None,
        )
        order._trailing_distance = Decimal(distance)

        if not accepted:
            with pytest.raises(ExchangeError, match="trailing_distance_off_tick"):
                adapter.place_order(order)
            assert order._trailing_distance == Decimal(distance)
            assert adapter.get_open_orders(product_id) == []
            return

        adapter.place_order(order)
        assert adapter.get_open_orders(product_id) == [order]


# =================================================================
# Market orders
# =================================================================


class TestMarketOrders:
    def test_market_buy_fills_at_open(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"), taker_fee=Decimal("0.0006"))
        order = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(order)

        fills = adapter.on_market_data(_candle(200, 50000, 50500, 49500, 50200))
        assert len(fills) == 1
        f = fills[0]
        assert f["price"] == Decimal("50000")
        assert f["fill_type"] == "MARKET"
        assert _approx(f["fee"], 50000 * 0.1 * 0.0006)

    def test_market_sell_opens_short(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        order = order_factory(
            order_type="market",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        adapter.place_order(order)
        adapter.on_market_data(_candle(200, 50000, 50100, 49900, 50050))

        pos = adapter.get_position(PRODUCT)
        assert pos is not None
        assert pos.side == "SHORT"
        assert _approx(pos.quantity, 0.1)

    def test_returns_orm_order_in_fill(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        order = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(order)

        fills = adapter.on_market_data(_candle(200, 50000, 50500, 49500, 50200))
        assert fills[0]["order"].id == order.id

    def test_scaled_boundary_matches_decimal_boundary_for_market_fill(
        self, order_factory
    ):
        fluxtrade_core = pytest.importorskip("fluxtrade_core")
        if not hasattr(fluxtrade_core.PyMatchingEngine, "on_scaled_candle"):
            pytest.skip("compiled Rust engine does not support scaled candle matching")

        codec = PrecisionCodec(
            PrecisionSpec(
                price_tick=Decimal("0.01"),
                quantity_step=Decimal("0.001"),
            )
        )
        decimal_adapter = SimulatedAdapter(
            Decimal("10000"), taker_fee=Decimal("0.0006")
        )
        scaled_adapter = SimulatedAdapter(
            Decimal("10000"),
            taker_fee=Decimal("0.0006"),
            precision_codec=codec,
        )
        decimal_order = order_factory(
            order_type="market",
            side="buy",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        scaled_order = order_factory(
            order_type="market",
            side="buy",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        scaled_order.id = decimal_order.id
        decimal_adapter.place_order(decimal_order)
        scaled_adapter.place_order(scaled_order)

        candle = _candle(200, "50000.12", "50500.12", "49500.12", "50200.12", "100.123")
        decimal_fills = decimal_adapter.on_market_data(candle)
        scaled_fills = scaled_adapter.on_market_data(candle)

        assert len(scaled_fills) == len(decimal_fills) == 1
        assert scaled_fills[0]["price"] == decimal_fills[0]["price"]
        assert scaled_fills[0]["fee"] == decimal_fills[0]["fee"]
        assert scaled_adapter.get_balance() == decimal_adapter.get_balance()

    def test_prepared_scaled_candle_matches_decimal_boundary_for_market_fill(
        self, order_factory
    ):
        fluxtrade_core = pytest.importorskip("fluxtrade_core")
        if not hasattr(fluxtrade_core.PyMatchingEngine, "on_scaled_candle"):
            pytest.skip("compiled Rust engine does not support scaled candle matching")

        codec = PrecisionCodec(
            PrecisionSpec(
                price_tick=Decimal("0.01"),
                quantity_step=Decimal("0.001"),
            )
        )
        decimal_adapter = SimulatedAdapter(
            Decimal("10000"), taker_fee=Decimal("0.0006")
        )
        scaled_adapter = SimulatedAdapter(
            Decimal("10000"),
            taker_fee=Decimal("0.0006"),
            precision_codec=codec,
        )
        decimal_order = order_factory(
            order_type="market",
            side="buy",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        scaled_order = order_factory(
            order_type="market",
            side="buy",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        scaled_order.id = decimal_order.id
        decimal_adapter.place_order(decimal_order)
        scaled_adapter.place_order(scaled_order)

        candle = _candle(200, "50000.12", "50500.12", "49500.12", "50200.12", "100.123")
        prepared = scaled_adapter.prepare_scaled_candle(candle)
        decimal_fills = decimal_adapter.on_market_data(candle)
        scaled_fills = scaled_adapter.on_prepared_market_data(prepared)

        assert len(scaled_fills) == len(decimal_fills) == 1
        assert scaled_fills[0]["price"] == decimal_fills[0]["price"]
        assert scaled_fills[0]["fee"] == decimal_fills[0]["fee"]
        assert scaled_adapter.get_balance() == decimal_adapter.get_balance()


# =================================================================
# Limit orders
# =================================================================


class TestLimitOrders:
    def test_limit_buy_fills_when_low_touches(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"), maker_fee=Decimal("0.0002"))
        order = order_factory(
            order_type="limit",
            side="buy",
            product_id=PRODUCT,
            price=Decimal("49000"),
            quantity=Decimal("0.1"),
        )
        adapter.place_order(order)

        # low=48900 touches 49000
        fills = adapter.on_market_data(_candle(200, 49500, 49800, 48900, 49200))
        assert len(fills) == 1
        assert fills[0]["price"] == Decimal("49000")
        assert fills[0]["fill_type"] == "LIMIT"
        assert _approx(fills[0]["fee"], 49000 * 0.1 * 0.0002)

    def test_limit_buy_no_fill_when_above(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        order = order_factory(
            order_type="limit",
            side="buy",
            product_id=PRODUCT,
            price=Decimal("40000"),
            quantity=Decimal("0.1"),
        )
        adapter.place_order(order)

        fills = adapter.on_market_data(_candle(200, 50000, 50500, 49500, 50200))
        assert len(fills) == 0

    def test_limit_sell_fills_when_high_reaches(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"), maker_fee=Decimal("0.0002"))
        order = order_factory(
            order_type="limit",
            side="sell",
            product_id=PRODUCT,
            price=Decimal("51000"),
            quantity=Decimal("0.1"),
        )
        adapter.place_order(order)

        fills = adapter.on_market_data(_candle(200, 50000, 51500, 49500, 51000))
        assert len(fills) == 1
        assert fills[0]["price"] == Decimal("51000")


# =================================================================
# Stop Loss / Take Profit
# =================================================================


class TestConditionalOrders:
    """SL/TP orders — side in ORM is the closing direction (sell/buy)."""

    def _open_long(self, adapter, order_factory):
        entry = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50200, 49900, 50100))

    def _open_short(self, adapter, order_factory):
        entry = order_factory(
            order_type="market",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50100, 49900, 50050))

    # ── SL for LONG ──────────────────────────────────────────────

    def test_sl_long_triggers_on_drop(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        self._open_long(adapter, order_factory)

        sl = order_factory(
            order_type="stop_loss",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("49000"),
        )
        adapter.place_order(sl)

        # low=48900 <= trigger 49000
        fills = adapter.on_market_data(_candle(400, 49500, 49800, 48900, 49200))
        assert len(fills) == 1
        assert fills[0]["fill_type"] == "STOP_LOSS"
        assert fills[0]["price"] == Decimal("49000")
        assert adapter.get_position(PRODUCT) is None

    def test_sl_long_no_trigger_above(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        self._open_long(adapter, order_factory)

        sl = order_factory(
            order_type="stop_loss",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("49000"),
        )
        adapter.place_order(sl)

        # low=49500 > trigger 49000
        fills = adapter.on_market_data(_candle(400, 50000, 51000, 49500, 50800))
        assert len(fills) == 0

    # ── TP for LONG ──────────────────────────────────────────────

    def test_tp_long_triggers_on_rise(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        self._open_long(adapter, order_factory)

        tp = order_factory(
            order_type="take_profit",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("52000"),
        )
        adapter.place_order(tp)

        # high=52500 >= trigger 52000
        fills = adapter.on_market_data(_candle(400, 51000, 52500, 50800, 52200))
        assert len(fills) == 1
        assert fills[0]["fill_type"] == "TAKE_PROFIT"
        assert fills[0]["price"] == Decimal("52000")

    # ── SL for SHORT ─────────────────────────────────────────────

    def test_sl_short_triggers_on_rise(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        self._open_short(adapter, order_factory)

        sl = order_factory(
            order_type="stop_loss",
            side="buy",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("51000"),
        )
        adapter.place_order(sl)

        # high=51200 >= trigger 51000
        fills = adapter.on_market_data(_candle(400, 50200, 51200, 50100, 51000))
        assert len(fills) == 1
        assert fills[0]["fill_type"] == "STOP_LOSS"
        assert fills[0]["price"] == Decimal("51000")
        assert adapter.get_position(PRODUCT) is None

    # ── TP for SHORT ─────────────────────────────────────────────

    def test_tp_short_triggers_on_drop(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        self._open_short(adapter, order_factory)

        tp = order_factory(
            order_type="take_profit",
            side="buy",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("48000"),
        )
        adapter.place_order(tp)

        # low=47800 <= trigger 48000
        fills = adapter.on_market_data(_candle(400, 49000, 49200, 47800, 48000))
        assert len(fills) == 1
        assert fills[0]["fill_type"] == "TAKE_PROFIT"
        assert fills[0]["price"] == Decimal("48000")


# =================================================================
# OCO (one-cancels-other)
# =================================================================


class TestOCO:
    def test_tp_cancels_sl(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        # open long
        entry = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50200, 49900, 50100))

        sl = order_factory(
            order_type="stop_loss",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("49000"),
        )
        tp = order_factory(
            order_type="take_profit",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("52000"),
        )
        sl._linked_order_id = tp.id
        tp._linked_order_id = sl.id
        adapter.place_order(sl)
        adapter.place_order(tp)

        # TP triggers
        fills = adapter.on_market_data(_candle(500, 51000, 52500, 50800, 52200))
        assert len(fills) == 1
        assert fills[0]["order"].id == tp.id

        # SL should have been cancelled — no longer in order map
        assert sl.id not in adapter._order_map
        assert adapter.get_position(PRODUCT) is None

    def test_sl_cancels_tp(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        entry = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50200, 49900, 50100))

        sl = order_factory(
            order_type="stop_loss",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("49000"),
        )
        tp = order_factory(
            order_type="take_profit",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("52000"),
        )
        sl._linked_order_id = tp.id
        tp._linked_order_id = sl.id
        adapter.place_order(sl)
        adapter.place_order(tp)

        # SL triggers
        fills = adapter.on_market_data(_candle(500, 49500, 49800, 48900, 49200))
        assert len(fills) == 1
        assert fills[0]["order"].id == sl.id
        assert tp.id not in adapter._order_map


# =================================================================
# Trailing Stop
# =================================================================


class TestTrailingStop:
    def test_trailing_moves_up_and_triggers(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        # open long at 50000
        entry = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50200, 49900, 50100))

        ts = order_factory(
            order_type="trailing_stop",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("49500"),
        )
        ts._trailing_distance = Decimal("500")
        adapter.place_order(ts)

        # rally: high=52000, trigger moves to 52000-500=51500
        fills = adapter.on_market_data(_candle(400, 50500, 52000, 51600, 51900))
        assert len(fills) == 0  # low 51600 > 51500

        # drop below new trigger
        fills = adapter.on_market_data(_candle(500, 51800, 51900, 51400, 51500))
        assert len(fills) == 1
        assert fills[0]["fill_type"] == "TRAILING_STOP"
        assert _approx(fills[0]["price"], 51500)
        assert adapter.get_position(PRODUCT) is None

    def test_endpoint_snapshot_uses_matcher_updated_trailing_trigger(
        self,
        order_factory,
    ):
        adapter = SimulatedAdapter(Decimal("10000"))
        entry = order_factory(
            order_type="market",
            side="buy",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50200, 49900, 50100))
        trailing = order_factory(
            order_type="trailing_stop",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("49500"),
        )
        trailing._trailing_distance = Decimal("500")
        adapter.place_order(trailing)

        fills = adapter.on_market_data(_candle(400, 50500, 52000, 51600, 51900))
        endpoint = build_replay_endpoint_state(
            positions=adapter.get_all_positions(),
            working_orders=adapter.get_matching_open_orders(),
            final_mark=Decimal("51900"),
            end_timestamp=400,
        )

        assert fills == []
        assert endpoint.protection_orders[0].trigger_price == Decimal("51500")
        assert endpoint.protection_orders[0].trailing_distance == Decimal("500")

    def test_trailing_for_short(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        entry = order_factory(
            order_type="market",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50100, 49900, 50050))

        ts = order_factory(
            order_type="trailing_stop",
            side="buy",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("50500"),
        )
        ts._trailing_distance = Decimal("500")
        adapter.place_order(ts)

        # drop: low=48000, trigger moves to 48000+500=48500
        # high must stay below 48500 to avoid triggering on this candle
        fills = adapter.on_market_data(_candle(400, 48400, 48400, 48000, 48200))
        assert len(fills) == 0

        # price rises past new trigger (48500)
        fills = adapter.on_market_data(_candle(500, 48300, 48600, 48200, 48500))
        assert len(fills) == 1
        assert fills[0]["fill_type"] == "TRAILING_STOP"
        assert _approx(fills[0]["price"], 48500)

    def test_trailing_short_no_premature_trigger(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        entry = order_factory(
            order_type="market",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50100, 49900, 50050))

        ts = order_factory(
            order_type="trailing_stop",
            side="buy",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("50500"),
        )
        ts._trailing_distance = Decimal("500")
        adapter.place_order(ts)

        # drop: low=48000, high must stay below new trigger (48500)
        fills = adapter.on_market_data(_candle(400, 49000, 48400, 48000, 48200))
        assert len(fills) == 0

        # price rises past trigger
        fills = adapter.on_market_data(_candle(500, 48300, 48600, 48200, 48500))
        assert len(fills) == 1
        assert fills[0]["fill_type"] == "TRAILING_STOP"
        assert _approx(fills[0]["price"], 48500)


# =================================================================
# Balance & PnL accuracy
# =================================================================


class TestBalanceAccuracy:
    def test_market_fee_deducted(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"), taker_fee=Decimal("0.0006"))
        order = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(order)
        adapter.on_market_data(_candle(200, 50000, 50500, 49500, 50200))

        # fee = 50000 * 0.1 * 0.0006 = 3.0
        assert _approx(adapter.get_balance(), 10000 - 3.0)

    def test_pnl_after_tp(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"), taker_fee=Decimal("0.0006"))
        # open long at 50000
        entry = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50200, 49900, 50100))

        # TP at 52000
        tp = order_factory(
            order_type="take_profit",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("52000"),
        )
        adapter.place_order(tp)
        adapter.on_market_data(_candle(400, 51000, 52500, 50800, 52200))

        # entry fee: 50000*0.1*0.0006 = 3.0
        # tp fee:    52000*0.1*0.0006 = 3.12
        # pnl:       (52000-50000)*0.1 = 200
        # expected:  10000 - 3.0 + 200 - 3.12 = 10193.88
        assert _approx(adapter.get_balance(), 10193.88)

    def test_pnl_after_sl_loss(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"), taker_fee=Decimal("0.0006"))
        entry = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(entry)
        adapter.on_market_data(_candle(200, 50000, 50200, 49900, 50100))

        sl = order_factory(
            order_type="stop_loss",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("49000"),
        )
        adapter.place_order(sl)
        adapter.on_market_data(_candle(400, 49500, 49800, 48900, 49200))

        # entry fee: 3.0,  sl fee: 49000*0.1*0.0006 = 2.94
        # pnl: (49000-50000)*0.1 = -100
        # expected: 10000 - 3.0 - 100 - 2.94 = 9894.06
        assert _approx(adapter.get_balance(), 9894.06)


# =================================================================
# Cancellation
# =================================================================


class TestCancellation:
    def test_cancel_by_exchange_id(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        order = order_factory(
            order_type="limit",
            side="buy",
            product_id=PRODUCT,
            price=Decimal("40000"),
            quantity=Decimal("0.1"),
        )
        ex_id = adapter.place_order(order)

        assert adapter.cancel_order(ex_id, PRODUCT) is True
        assert adapter.cancel_order(ex_id, PRODUCT) is False

    def test_cancel_by_client_order_id(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        order = order_factory(
            order_type="limit",
            side="buy",
            product_id=PRODUCT,
            price=Decimal("40000"),
            quantity=Decimal("0.1"),
            client_order_id="client-123",
        )
        adapter.place_order(order)

        assert adapter.cancel_order_by_client_id("client-123", PRODUCT) is True
        assert adapter.cancel_order_by_client_id("client-123", PRODUCT) is False

    def test_cancel_nonexistent(self):
        adapter = SimulatedAdapter(Decimal("10000"))
        assert adapter.cancel_order("NOPE", PRODUCT) is False

    def test_get_order_by_client_id_returns_snapshot(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        order = order_factory(
            order_type="limit",
            side="buy",
            product_id=PRODUCT,
            price=Decimal("40000"),
            quantity=Decimal("0.1"),
            status="SUBMITTED",
            client_order_id="client-123",
        )
        exchange_order_id = adapter.place_order(order)

        snapshot = adapter.get_order_by_client_id("client-123", PRODUCT)

        assert snapshot is not None
        assert snapshot.client_order_id == "client-123"
        assert snapshot.exchange_order_id == exchange_order_id
        assert snapshot.status == "SUBMITTED"

    def test_get_order_by_client_id_returns_none_when_missing(self):
        adapter = SimulatedAdapter(Decimal("10000"))

        assert adapter.get_order_by_client_id("missing", PRODUCT) is None


# =================================================================
# Position tracking
# =================================================================


class TestPositionTracking:
    def test_position_increase(self, order_factory):
        adapter = SimulatedAdapter(Decimal("100000"))
        for _ in range(3):
            o = order_factory(
                order_type="market",
                side="buy",
                product_id=PRODUCT,
                quantity=Decimal("0.1"),
            )
            adapter.place_order(o)
            adapter.on_market_data(_candle(200, 50000, 50500, 49500, 50200))

        pos = adapter.get_position(PRODUCT)
        assert pos is not None
        assert _approx(pos.quantity, 0.3)

    def test_position_close_returns_none(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        buy = order_factory(
            order_type="market", side="buy", product_id=PRODUCT, quantity=Decimal("0.1")
        )
        adapter.place_order(buy)
        adapter.on_market_data(_candle(200, 50000, 50500, 49500, 50200))

        sell = order_factory(
            order_type="market",
            side="sell",
            product_id=PRODUCT,
            quantity=Decimal("0.1"),
        )
        adapter.place_order(sell)
        adapter.on_market_data(_candle(300, 51000, 51500, 50500, 51200))

        assert adapter.get_position(PRODUCT) is None

    def test_strategy_positions_share_one_matcher_snapshot(self, order_factory):
        adapter = SimulatedAdapter(Decimal("100000"))
        for strategy_id, side in (("alpha", "buy"), ("beta", "sell")):
            adapter.place_order(
                order_factory(
                    strategy_id=strategy_id,
                    order_type="market",
                    side=side,
                    product_id=PRODUCT,
                    quantity=Decimal("0.1"),
                )
            )
        adapter.on_market_data(_candle(200, 50000, 50500, 49500, 50200))

        positions = adapter.get_strategy_positions(
            PRODUCT,
            ["alpha", "alpha", "missing", "beta"],
        )

        assert [position.strategy_id for position in positions] == [
            "alpha",
            "beta",
        ]
        assert [position.side for position in positions] == [
            "LONG",
            "SHORT",
        ]

    def test_all_positions_preserve_ownership_and_are_deterministic(
        self,
        order_factory,
    ):
        adapter = SimulatedAdapter(Decimal("100000"))
        for strategy_id, side in (("beta", "sell"), ("alpha", "buy")):
            adapter.place_order(
                order_factory(
                    strategy_id=strategy_id,
                    order_type="market",
                    side=side,
                    product_id=PRODUCT,
                    quantity=Decimal("0.1"),
                )
            )
        adapter.on_market_data(_candle(200, 50000, 50500, 49500, 50200))

        positions = adapter.get_all_positions()

        assert [
            (position.strategy_id, position.product_id, position.side)
            for position in positions
        ] == [
            ("alpha", PRODUCT, "LONG"),
            ("beta", PRODUCT, "SHORT"),
        ]
        assert adapter.get_all_positions() == positions

    def test_different_products_independent(self, order_factory):
        adapter = SimulatedAdapter(Decimal("100000"))
        btc = order_factory(
            order_type="market",
            side="buy",
            product_id="BINANCE:BTCUSDT-PERP",
            quantity=Decimal("0.1"),
        )
        eth = order_factory(
            order_type="market",
            side="sell",
            product_id="BINANCE:ETHUSDT-PERP",
            quantity=Decimal("1.0"),
        )
        adapter.place_order(btc)
        adapter.on_market_data(
            _candle(200, 50000, 50500, 49500, 50200, product="BINANCE:BTCUSDT-PERP")
        )
        adapter.place_order(eth)
        adapter.on_market_data(
            _candle(200, 2000, 2050, 1950, 2020, product="BINANCE:ETHUSDT-PERP")
        )

        btc_position = adapter.get_position("BINANCE:BTCUSDT-PERP")
        eth_position = adapter.get_position("BINANCE:ETHUSDT-PERP")
        assert btc_position is not None
        assert eth_position is not None
        assert btc_position.side == "LONG"
        assert eth_position.side == "SHORT"


# =================================================================
# Edge cases
# =================================================================


class TestEdgeCases:
    def test_fill_multiple_orders_same_candle(self, order_factory):
        adapter = SimulatedAdapter(Decimal("100000"))
        for _ in range(5):
            o = order_factory(
                order_type="market",
                side="buy",
                product_id=PRODUCT,
                quantity=Decimal("0.01"),
            )
            adapter.place_order(o)

        fills = adapter.on_market_data(_candle(200, 50000, 50500, 49500, 50200))
        assert len(fills) == 5

    def test_orders_for_different_product_untouched(self, order_factory):
        adapter = SimulatedAdapter(Decimal("10000"))
        order = order_factory(
            order_type="market",
            side="buy",
            product_id="BINANCE:ETHUSDT-PERP",
            quantity=Decimal("0.1"),
        )
        adapter.place_order(order)

        fills = adapter.on_market_data(
            _candle(200, 50000, 50500, 49500, 50200, product=PRODUCT)
        )
        assert len(fills) == 0


def _spot_spec() -> InstrumentSpec:
    return InstrumentSpec(
        product_id="BINANCE:BTCUSDT-SPOT",
        exchange="binance",
        symbol="BTC/USDT",
        base="BTC",
        quote="USDT",
        quantity_step=Decimal("0.000001"),
        price_tick=Decimal("0.01"),
        min_notional=Decimal("1"),
    )


def _with_market_reference(order, price: str):
    order.min_notional_reference_price = Decimal(price)
    return order


class TestSpotSettlement:
    def test_spot_requires_explicit_instrument_spec(self, order_factory):
        adapter = SimulatedAdapter(Decimal("100"))
        order = order_factory(
            product_id="BINANCE:BTCUSDT-SPOT",
            order_type="market",
            side="buy",
            quantity=Decimal("0.001"),
            price=None,
        )

        with pytest.raises(ExchangeError, match="instrument_spec_required_for_spot"):
            adapter.place_order(order)

    def test_spot_min_notional_uses_market_reference(self, order_factory):
        adapter = SimulatedAdapter(
            Decimal("100"),
            instrument_spec=_spot_spec(),
        )
        order = _with_market_reference(
            order_factory(
                product_id="BINANCE:BTCUSDT-SPOT",
                order_type="market",
                side="buy",
                quantity=Decimal("0.00001"),
                price=None,
            ),
            "50000",
        )

        with pytest.raises(ExchangeError, match="min_notional_not_met"):
            adapter.place_order(order)

    def test_external_funding_preserves_pending_quote_reservation(
        self, order_factory
    ):
        product_id = "BINANCE:BTCUSDT-SPOT"
        adapter = SimulatedAdapter(
            Decimal("100"),
            instrument_spec=_spot_spec(),
        )
        pending = order_factory(
            product_id=product_id,
            order_type="limit",
            side="buy",
            quantity=Decimal("1"),
            price=Decimal("60"),
        )
        adapter.place_order(pending)

        total = adapter.apply_external_funding(
            asset="USDT", amount=Decimal("50")
        )

        assert total == Decimal("150")
        assert adapter.get_asset_balance("USDT", "total") == Decimal("150")
        assert adapter.get_asset_balance("USDT", "reserved") == Decimal("60")
        assert adapter.get_asset_balance("USDT", "available") == Decimal("90")

    @pytest.mark.parametrize(
        ("asset", "amount", "message"),
        [
            ("BTC", Decimal("1"), "supports quote asset USDT only"),
            ("USDT", Decimal("0"), "finite and positive"),
            ("USDT", Decimal("-1"), "finite and positive"),
            ("USDT", Decimal("NaN"), "finite and positive"),
            ("USDT", Decimal("Infinity"), "finite and positive"),
        ],
    )
    def test_external_funding_validation_fails_closed(
        self, asset, amount, message
    ):
        adapter = SimulatedAdapter(
            Decimal("100"),
            instrument_spec=_spot_spec(),
        )

        with pytest.raises(ExchangeError, match=message):
            adapter.apply_external_funding(asset=asset, amount=amount)

        assert adapter.get_asset_balance("USDT", "total") == Decimal("100")

    def test_external_funding_rejects_non_decimal_and_derivatives(self):
        spot = SimulatedAdapter(
            Decimal("100"),
            instrument_spec=_spot_spec(),
        )
        with pytest.raises(TypeError, match="amount must be Decimal"):
            spot.apply_external_funding(asset="USDT", amount=1)  # type: ignore[arg-type]

        derivatives = SimulatedAdapter(Decimal("100"))
        with pytest.raises(ExchangeError, match="requires cash_spot settlement"):
            derivatives.apply_external_funding(
                asset="USDT", amount=Decimal("1")
            )

    def test_quote_fee_buy_sell_sequence_matches_asset_acceptance_table(
        self, order_factory
    ):
        product_id = "BINANCE:BTCUSDT-SPOT"
        adapter = SimulatedAdapter(
            Decimal("100"),
            maker_fee=Decimal("0.001"),
            taker_fee=Decimal("0.001"),
            instrument_spec=_spot_spec(),
        )
        buy = _with_market_reference(
            order_factory(
                product_id=product_id,
                order_type="market",
                side="buy",
                quantity=Decimal("0.001"),
                price=None,
            ),
            "50000",
        )

        adapter.place_order(buy)
        assert adapter.get_asset_balance("USDT", "total") == Decimal("100")
        assert adapter.get_asset_balance("USDT", "reserved") == Decimal("50.05")
        assert adapter.get_balance() == Decimal("49.95")
        fills = adapter.on_market_data(
            _candle(200, 50000, 50000, 50000, 50000, product=product_id)
        )
        assert fills[0]["fee"] == Decimal("0.05")
        assert fills[0]["fee_asset"] == "USDT"
        assert adapter.get_asset_balance("BTC", "total") == Decimal("0.001")

        sell = _with_market_reference(
            order_factory(
                product_id=product_id,
                order_type="market",
                side="sell",
                quantity=Decimal("0.0005"),
                price=None,
            ),
            "60000",
        )
        adapter.place_order(sell)
        fills = adapter.on_market_data(
            _candle(300, 60000, 60000, 60000, 60000, product=product_id)
        )

        assert fills[0]["fee"] == Decimal("0.03")
        snapshot = adapter.get_cash_spot_account_snapshot(Decimal("60000"))
        assert snapshot.quote_total == Decimal("79.92")
        assert snapshot.base_total == Decimal("0.0005")
        assert snapshot.cost_basis == Decimal("25.025")
        assert snapshot.realized_pnl == Decimal("4.945")
        assert snapshot.unrealized_pnl == Decimal("4.975")
        assert snapshot.total_equity == Decimal("109.92")

    def test_market_gap_rejects_whole_order_and_releases_quote(self, order_factory):
        product_id = "BINANCE:BTCUSDT-SPOT"
        adapter = SimulatedAdapter(
            Decimal("100"),
            taker_fee=Decimal("0.001"),
            instrument_spec=_spot_spec(),
        )
        order = _with_market_reference(
            order_factory(
                product_id=product_id,
                order_type="market",
                side="buy",
                quantity=Decimal("0.001"),
                price=None,
            ),
            "90000",
        )
        adapter.place_order(order)

        fills = adapter.on_market_data(
            _candle(200, 110000, 110000, 110000, 110000, product=product_id)
        )

        assert fills == []
        assert adapter.get_asset_balance("USDT", "total") == Decimal("100")
        assert adapter.get_asset_balance("USDT", "reserved") == Decimal("0")
        assert adapter.get_asset_balance("BTC", "total") == Decimal("0")
        rejections = adapter.drain_order_rejections()
        assert len(rejections) == 1
        assert rejections[0]["order"] is order
        assert "insufficient available USDT at fill" in rejections[0]["reason"]

    def test_same_candle_fill_does_not_hide_another_order_rejection(
        self, order_factory
    ):
        product_id = "BINANCE:BTCUSDT-SPOT"
        adapter = SimulatedAdapter(
            Decimal("100"),
            instrument_spec=_spot_spec(),
        )
        affordable = _with_market_reference(
            order_factory(
                product_id=product_id,
                order_type="market",
                side="buy",
                quantity=Decimal("0.001"),
                price=None,
            ),
            "50000",
        )
        rejected = _with_market_reference(
            order_factory(
                product_id=product_id,
                order_type="market",
                side="buy",
                quantity=Decimal("0.002"),
                price=None,
            ),
            "50000",
        )
        adapter.place_order(affordable)
        adapter.place_order(rejected)

        fills = adapter.on_market_data(
            _candle(200, 50000, 50000, 50000, 50000, product=product_id)
        )
        rejections = adapter.drain_order_rejections()

        assert [fill["order"] for fill in fills] == [affordable]
        assert len(rejections) == 1
        assert rejections[0]["order"] is rejected
        assert "insufficient available USDT at fill" in rejections[0]["reason"]
        assert adapter.get_asset_balance("USDT", "total") == Decimal("50")

    def test_cash_spot_sell_shortfall_warns_then_rejects_without_short_position(
        self, order_factory, caplog
    ):
        product_id = "BINANCE:BTCUSDT-SPOT"
        adapter = SimulatedAdapter(
            Decimal("100"),
            instrument_spec=_spot_spec(),
        )
        naked_sell = _with_market_reference(
            order_factory(
                product_id=product_id,
                order_type="market",
                side="sell",
                quantity=Decimal("0.001"),
                price=None,
            ),
            "50000",
        )
        adapter.place_order(naked_sell)
        assert "Cash-spot order accepted with advisory" in caplog.text
        assert "insufficient_available_at_submission" in caplog.text
        assert (
            adapter.on_market_data(
                _candle(100, 50000, 50000, 50000, 50000, product=product_id)
            )
            == []
        )
        assert len(adapter.drain_order_rejections()) == 1

        buy = _with_market_reference(
            order_factory(
                product_id=product_id,
                order_type="market",
                side="buy",
                quantity=Decimal("0.001"),
                price=None,
            ),
            "50000",
        )
        adapter.place_order(buy)
        adapter.on_market_data(
            _candle(200, 50000, 50000, 50000, 50000, product=product_id)
        )
        oversized_sell = _with_market_reference(
            order_factory(
                product_id=product_id,
                order_type="market",
                side="sell",
                quantity=Decimal("0.002"),
                price=None,
            ),
            "50000",
        )
        adapter.place_order(oversized_sell)
        assert (
            adapter.on_market_data(
                _candle(300, 50000, 50000, 50000, 50000, product=product_id)
            )
            == []
        )
        assert len(adapter.drain_order_rejections()) == 1

        position = adapter.get_position(product_id, strategy_id=buy.strategy_id)
        assert position is not None
        assert position.side == "LONG"
        assert position.quantity == Decimal("0.001")

    def test_base_fee_is_reported_in_base_asset(self, order_factory):
        product_id = "BINANCE:BTCUSDT-SPOT"
        adapter = SimulatedAdapter(
            Decimal("100"),
            taker_fee=Decimal("0.001"),
            instrument_spec=_spot_spec(),
            spot_fee_asset="base",
        )
        buy = _with_market_reference(
            order_factory(
                product_id=product_id,
                order_type="market",
                side="buy",
                quantity=Decimal("0.001"),
                price=None,
            ),
            "50000",
        )
        adapter.place_order(buy)
        fills = adapter.on_market_data(
            _candle(200, 50000, 50000, 50000, 50000, product=product_id)
        )

        assert fills[0]["fee"] == Decimal("0.05")
        assert fills[0]["fee_quantity"] == Decimal("0.000001")
        assert fills[0]["fee_asset"] == "BTC"
        assert adapter.get_asset_balance("USDT", "total") == Decimal("50")
        assert adapter.get_asset_balance("BTC", "total") == Decimal("0.000999")
