"""Integration test: Python ↔ Rust PyMatchingEngine binding.

Directly exercises the compiled fluxtrade_core .so without going through
SimulatedAdapter, verifying every public API of PyMatchingEngine.

Requires: compiled fluxtrade_core.so in src/
"""
import pytest
from decimal import Decimal

# Skip entire module if Rust .so is not available
try:
    from fluxtrade_core import CandleAggregator, Candlestick, Order, PyMatchingEngine
    HAS_RUST = True
except ImportError:
    HAS_RUST = False

pytestmark = [
    pytest.mark.rust,
    pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled"),
]

PRODUCT = "BINANCE:BTCUSDT-PERP"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def make_engine(balance: str = "10000", maker: str = "0.0002", taker: str = "0.0006"):
    return PyMatchingEngine(balance, maker, taker)


def make_order(
    side: str = "LONG",
    order_type: str = "MARKET",
    price: str = "50000",
    quantity: str = "0.1",
    trigger_price: str | None = None,
    trailing_distance: str | None = None,
    linked_order_id: str | None = None,
    order_id: str | None = None,
):
    return Order(
        id=order_id or f"ORD-{side}-{order_type}",
        product_id=PRODUCT,
        side=side,
        order_type=order_type,
        price=price,
        quantity=quantity,
        timestamp=1_700_000_000_000,
        trigger_price=trigger_price,
        trailing_distance=trailing_distance,
        linked_order_id=linked_order_id,
    )


def make_candle(
    open: str = "50000",
    high: str = "50100",
    low: str = "49900",
    close: str = "50050",
    ts: int = 1_700_000_900_000,
):
    return Candlestick(
        product_id=PRODUCT,
        timeframe="15m",
        timestamp=ts,
        open=open,
        high=high,
        low=low,
        close=close,
        volume="100",
    )


def make_1m_candle(
    ts: int,
    *,
    open: str,
    high: str,
    low: str,
    close: str,
    volume: str,
):
    return Candlestick(
        product_id=PRODUCT,
        timeframe="1m",
        timestamp=ts,
        open=open,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


# ===========================================================================
# Candle aggregation
# ===========================================================================
class TestCandleAggregation:
    @pytest.mark.parametrize(
        ("source_timeframe", "target_timeframe", "expected"),
        [
            ("30s", "5m", True),
            ("1m", "5m", True),
            ("3m", "15m", True),
            ("5m", "5m", True),
            ("5m", "15m", True),
            ("2m", "5m", False),
            ("6m", "5m", False),
            ("6m", "15m", False),
        ],
    )
    def test_support_matrix(self, source_timeframe, target_timeframe, expected):
        assert (
            CandleAggregator.can_aggregate(source_timeframe, target_timeframe)
            is expected
        )

    def test_aggregates_closed_5m_bucket_with_decimal_values(self):
        aggregator = CandleAggregator()
        base = 1_737_583_200_000
        rows = [
            ("100", "101", "99", "100.5", "10"),
            ("100.5", "103", "100", "102", "20"),
            ("102", "104", "98", "99", "30"),
            ("99", "100", "97", "98", "40"),
            ("98", "102", "96", "101", "50"),
        ]
        for minute, values in enumerate(rows):
            assert (
                aggregator.add_candle(
                    make_1m_candle(
                        base + minute * 60_000,
                        open=values[0],
                        high=values[1],
                        low=values[2],
                        close=values[3],
                        volume=values[4],
                    ),
                    "5m",
                )
                is None
            )

        completed = aggregator.add_candle(
            make_1m_candle(
                base + 5 * 60_000,
                open="101",
                high="102",
                low="100",
                close="101.5",
                volume="60",
            ),
            "5m",
        )

        assert completed.timestamp == base
        assert completed.timeframe == "5m"
        assert completed.open == "100"
        assert completed.high == "104"
        assert completed.low == "96"
        assert completed.close == "101"
        assert completed.volume == "150"

    def test_aggregates_5m_source_into_closed_15m_bucket(self):
        aggregator = CandleAggregator()
        base = 1_737_583_200_000
        rows = [
            ("100", "101", "99", "100.5", "10"),
            ("100.5", "103", "100", "102", "20"),
            ("102", "104", "98", "99", "30"),
        ]
        for index, values in enumerate(rows):
            candle = make_1m_candle(
                base + index * 5 * 60_000,
                open=values[0],
                high=values[1],
                low=values[2],
                close=values[3],
                volume=values[4],
            )
            candle.timeframe = "5m"
            assert aggregator.add_candle(candle, "15m") is None

        next_candle = make_1m_candle(
            base + 15 * 60_000,
            open="99",
            high="100",
            low="97",
            close="98",
            volume="40",
        )
        next_candle.timeframe = "5m"
        completed = aggregator.add_candle(next_candle, "15m")

        assert completed.timestamp == base
        assert completed.timeframe == "15m"
        assert completed.open == "100"
        assert completed.high == "104"
        assert completed.low == "98"
        assert completed.close == "99"
        assert completed.volume == "60"

    @pytest.mark.parametrize(
        ("source_timeframe", "target_timeframe"),
        [
            ("6m", "5m"),
            ("1m", ""),
            ("1m", "0m"),
            ("1m", "5x"),
            ("1m", "5分"),
        ],
    )
    def test_rejects_ambiguous_aggregation_inputs(
        self,
        source_timeframe,
        target_timeframe,
    ):
        candle = make_1m_candle(
            1_737_583_200_000,
            open="100",
            high="101",
            low="99",
            close="100",
            volume="10",
        )
        candle.timeframe = source_timeframe

        with pytest.raises(ValueError):
            CandleAggregator().add_candle(candle, target_timeframe)


# ===========================================================================
# Basic API
# ===========================================================================
class TestBasicAPI:
    def test_engine_initial_balance(self):
        engine = make_engine("10000")
        assert engine.balance == "10000"

    def test_submit_order_returns_id(self):
        engine = make_engine()
        order = make_order(order_id="ORD-1")
        oid = engine.submit_order(order)
        assert oid == "ORD-1"
        assert len(engine.open_orders) == 1

    def test_cancel_order(self):
        engine = make_engine()
        order = make_order(order_id="ORD-1")
        engine.submit_order(order)
        assert engine.cancel_order("ORD-1") is True
        assert len(engine.open_orders) == 0

    def test_cancel_nonexistent_returns_false(self):
        engine = make_engine()
        assert engine.cancel_order("NONEXISTENT") is False

    def test_get_positions_empty(self):
        engine = make_engine()
        assert len(engine.get_positions()) == 0


# ===========================================================================
# Market order fills
# ===========================================================================
class TestMarketOrders:
    def test_long_market_fills_on_candle(self):
        engine = make_engine("10000", "0", "0")
        engine.submit_order(make_order(side="LONG", order_type="MARKET", quantity="0.1", order_id="M-LONG"))
        fills = engine.on_candle(make_candle(open="50000", close="50050"))

        assert len(fills) == 1
        assert fills[0].order_id == "M-LONG"
        assert Decimal(fills[0].quantity) == Decimal("0.1")
        # After fill, should have a position
        positions = engine.get_positions()
        assert len(positions) == 1

    def test_short_market_fills_on_candle(self):
        engine = make_engine("10000", "0", "0")
        engine.submit_order(make_order(side="SHORT", order_type="MARKET", quantity="0.1"))
        fills = engine.on_candle(make_candle())

        assert len(fills) == 1
        positions = engine.get_positions()
        assert len(positions) == 1

    def test_market_fill_deducts_fee_from_balance(self):
        """Perp trading: balance only changes by fee (no notional deduction)."""
        engine = make_engine("100000", "0", "0.001")  # 0.1% taker fee
        engine.submit_order(make_order(side="LONG", order_type="MARKET", price="50000", quantity="1"))
        engine.on_candle(make_candle(open="50000"))

        # Balance should decrease by taker fee: 50000 * 1 * 0.001 = 50
        balance = Decimal(engine.balance)
        assert balance == Decimal("100000") - Decimal("50")


# ===========================================================================
# Fee model
# ===========================================================================
class TestFeeModel:
    def test_market_order_pays_taker_fee(self):
        engine = make_engine("100000", maker="0", taker="0.001")
        engine.submit_order(make_order(side="LONG", order_type="MARKET", quantity="1"))
        fills = engine.on_candle(make_candle(open="50000"))

        assert len(fills) == 1
        fee = Decimal(fills[0].fee)
        assert fee > Decimal("0")
        # taker fee on 1 * 50000 = 50
        assert fee == Decimal("50")

    def test_limit_order_pays_maker_fee(self):
        engine = make_engine("100000", maker="0.001", taker="0")
        engine.submit_order(make_order(
            side="LONG", order_type="LIMIT", price="49900", quantity="1",
        ))
        # Candle goes down to 49900 → limit fills
        fills = engine.on_candle(make_candle(open="50000", low="49800", close="49850"))

        assert len(fills) == 1
        fee = Decimal(fills[0].fee)
        assert fee > Decimal("0")


# ===========================================================================
# Limit orders
# ===========================================================================
class TestLimitOrders:
    def test_long_limit_fills_when_price_drops(self):
        engine = make_engine("100000", "0", "0")
        engine.submit_order(make_order(
            side="LONG", order_type="LIMIT", price="49500", quantity="0.1",
        ))
        # Candle doesn't reach limit
        fills = engine.on_candle(make_candle(open="50000", low="49600", close="49800"))
        assert len(fills) == 0

        # Candle reaches limit
        fills = engine.on_candle(make_candle(open="49800", low="49400", close="49600", ts=1_700_001_800_000))
        assert len(fills) == 1

    def test_short_limit_fills_when_price_rises(self):
        engine = make_engine("100000", "0", "0")
        engine.submit_order(make_order(
            side="SHORT", order_type="LIMIT", price="50500", quantity="0.1",
        ))
        fills = engine.on_candle(make_candle(open="50000", high="50600", close="50400"))
        assert len(fills) == 1


# ===========================================================================
# Stop Loss / Take Profit
# ===========================================================================
class TestSLTP:
    def test_long_stop_loss_triggers(self):
        """SL for long triggers when candle.low <= trigger_price.
        Note: Rust convention — SL side matches position side (LONG SL has side=LONG).
        """
        engine = make_engine("100000", "0", "0")
        # Open long position first
        engine.submit_order(make_order(side="LONG", order_type="MARKET", quantity="0.1"))
        engine.on_candle(make_candle(open="50000", close="50000"))

        # Place SL (side=LONG per Rust convention)
        engine.submit_order(make_order(
            side="LONG", order_type="STOP_LOSS", price="0",
            quantity="0.1", trigger_price="49000", order_id="SL-1",
        ))
        # Candle doesn't trigger SL (low=49100 > 49000)
        fills = engine.on_candle(make_candle(open="50000", low="49100", close="49500", ts=1_700_001_800_000))
        sl_fills = [f for f in fills if f.order_id == "SL-1"]
        assert len(sl_fills) == 0

        # Candle triggers SL (low=48900 <= 49000)
        fills = engine.on_candle(make_candle(open="49500", low="48900", close="49100", ts=1_700_002_700_000))
        sl_fills = [f for f in fills if f.order_id == "SL-1"]
        assert len(sl_fills) == 1

    def test_long_take_profit_triggers(self):
        """TP for long triggers when candle.high >= trigger_price.
        Note: Rust convention — TP side matches position side (LONG TP has side=LONG).
        """
        engine = make_engine("100000", "0", "0")
        engine.submit_order(make_order(side="LONG", order_type="MARKET", quantity="0.1"))
        engine.on_candle(make_candle(open="50000", close="50000"))

        engine.submit_order(make_order(
            side="LONG", order_type="TAKE_PROFIT", price="0",
            quantity="0.1", trigger_price="51000", order_id="TP-1",
        ))
        fills = engine.on_candle(make_candle(open="50500", high="51100", close="50800", ts=1_700_001_800_000))
        tp_fills = [f for f in fills if f.order_id == "TP-1"]
        assert len(tp_fills) == 1


# ===========================================================================
# OCO (One Cancels Other)
# ===========================================================================
class TestOCO:
    def test_sl_cancels_tp(self):
        """When SL fills, linked TP should be auto-cancelled."""
        engine = make_engine("100000", "0", "0")
        engine.submit_order(make_order(side="LONG", order_type="MARKET", quantity="0.1"))
        engine.on_candle(make_candle(open="50000", close="50000"))

        # SL linked to TP (side=LONG per Rust convention)
        engine.submit_order(make_order(
            side="LONG", order_type="STOP_LOSS", price="0",
            quantity="0.1", trigger_price="49000", order_id="SL-OCO",
            linked_order_id="TP-OCO",
        ))
        engine.submit_order(make_order(
            side="LONG", order_type="TAKE_PROFIT", price="0",
            quantity="0.1", trigger_price="52000", order_id="TP-OCO",
            linked_order_id="SL-OCO",
        ))
        assert len(engine.open_orders) == 2

        # Trigger SL (low=48900 <= 49000)
        engine.on_candle(make_candle(open="49500", low="48900", close="49000", ts=1_700_001_800_000))

        # TP should be cancelled
        remaining_ids = [o.id for o in engine.open_orders]
        assert "TP-OCO" not in remaining_ids
        assert "SL-OCO" not in remaining_ids


# ===========================================================================
# Trailing Stop
# ===========================================================================
class TestTrailingStop:
    def test_trailing_stop_updates_and_triggers(self):
        """Trailing stop should update trigger as price moves favorably, then trigger on reversal.
        Rust convention: trailing stop side=LONG for long position.
        """
        engine = make_engine("100000", "0", "0")
        engine.submit_order(make_order(side="LONG", order_type="MARKET", quantity="0.1"))
        engine.on_candle(make_candle(open="50000", close="50000"))

        # Trailing stop: side=LONG, initial trigger=49000, distance=1000
        engine.submit_order(make_order(
            side="LONG", order_type="TRAILING_STOP", price="0",
            quantity="0.1", trigger_price="49000",
            trailing_distance="1000", order_id="TS-1",
        ))

        # Candle 1: high=52000 → trigger updates to 52000-1000=51000, low=51200 > 51000 → no trigger
        fills1 = engine.on_candle(make_candle(open="51500", high="52000", low="51200", close="51800", ts=1_700_001_800_000))
        assert len([f for f in fills1 if f.order_id == "TS-1"]) == 0

        # Candle 2: low=50500 <= 51000 → triggers
        fills2 = engine.on_candle(make_candle(open="51200", high="51500", low="50500", close="50800", ts=1_700_002_700_000))
        ts_fills = [f for f in fills2 if f.order_id == "TS-1"]
        assert len(ts_fills) == 1
