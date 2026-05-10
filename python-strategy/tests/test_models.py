"""
Tests for src/core/models.py

Covers:
- Model validation (product_id format, required fields)
- Decimal handling
- Enum types
- Field validators
- Serialization/deserialization
"""

import pytest
from decimal import Decimal
from pydantic import ValidationError

from src.core.models import (
    SignalType, Candlestick, OrderStatus, Position, Trade
)


class TestProductIdValidation:
    """Tests for product_id validation across all models."""

    def test_valid_product_id_formats(self):
        """Valid product_id formats should pass validation."""
        valid_ids = [
            "BINANCE:BTCUSDT-PERP",
            "BYBIT:ETHUSDT-PERP",
            "BACKPACK:SOL_USDC-PERP",
            "OKX:BTC_USDT-PERP",
        ]
        for product_id in valid_ids:
            trade = Trade(
                id="1",
                product_id=product_id,
                price=Decimal("100"),
                quantity=Decimal("1"),
                side="buy",
                timestamp=1000
            )
            assert trade.product_id == product_id

    def test_invalid_product_id_missing_exchange(self):
        """Product ID without exchange prefix should fail."""
        with pytest.raises(ValidationError) as exc_info:
            Trade(
                id="1",
                product_id="BTCUSDT-PERP",
                price=Decimal("100"),
                quantity=Decimal("1"),
                side="buy",
                timestamp=1000
            )
        assert "product_id" in str(exc_info.value)

    def test_invalid_product_id_missing_perp(self):
        """Product ID without -PERP suffix should fail."""
        with pytest.raises(ValidationError) as exc_info:
            Trade(
                id="1",
                product_id="BINANCE:BTCUSDT",
                price=Decimal("100"),
                quantity=Decimal("1"),
                side="buy",
                timestamp=1000
            )
        assert "product_id" in str(exc_info.value)

    def test_invalid_product_id_lowercase(self):
        """Product ID with lowercase should fail."""
        with pytest.raises(ValidationError) as exc_info:
            Trade(
                id="1",
                product_id="binance:btcusdt-perp",
                price=Decimal("100"),
                quantity=Decimal("1"),
                side="buy",
                timestamp=1000
            )
        assert "product_id" in str(exc_info.value)


class TestTradeModel:
    """Tests for Trade model."""

    def test_trade_creation_with_decimals(self):
        """Trade should accept Decimal values."""
        trade = Trade(
            id="trade_1",
            product_id="BINANCE:BTCUSDT-PERP",
            price=Decimal("42000.50"),
            quantity=Decimal("0.001"),
            side="buy",
            timestamp=1704067200000
        )
        assert trade.price == Decimal("42000.50")
        assert trade.quantity == Decimal("0.001")

    def test_trade_creation_with_strings(self):
        """Trade should coerce string values to Decimal."""
        trade = Trade(
            id="trade_1",
            product_id="BINANCE:BTCUSDT-PERP",
            price="42000.50",
            quantity="0.001",
            side="buy",
            timestamp=1704067200000
        )
        assert trade.price == Decimal("42000.50")
        assert trade.quantity == Decimal("0.001")

    def test_trade_creation_with_floats(self):
        """Trade should accept float values (coerced to Decimal)."""
        trade = Trade(
            id="trade_1",
            product_id="BINANCE:BTCUSDT-PERP",
            price=42000.5,
            quantity=0.001,
            side="buy",
            timestamp=1704067200000
        )
        assert isinstance(trade.price, Decimal)
        assert isinstance(trade.quantity, Decimal)

    def test_trade_required_fields(self):
        """Trade should require all fields."""
        with pytest.raises(ValidationError):
            Trade(id="1", product_id="BINANCE:BTCUSDT-PERP")


class TestCandlestickModel:
    """Tests for Candlestick model."""

    def test_candlestick_creation(self, sample_candlestick):
        """Candlestick should be created with valid data."""
        assert sample_candlestick.product_id == "BINANCE:BTCUSDT-PERP"
        assert sample_candlestick.timeframe == "1m"
        assert sample_candlestick.open == Decimal("42000.00")

    def test_candlestick_ohlc_consistency(self, candlestick_factory):
        """Candlestick OHLC values should be internally consistent."""
        candle = candlestick_factory(
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105")
        )
        # high >= max(open, close, low)
        assert candle.high >= candle.open
        assert candle.high >= candle.close
        assert candle.high >= candle.low
        # low <= min(open, close, high)
        assert candle.low <= candle.open
        assert candle.low <= candle.close

    def test_candlestick_with_string_decimals(self):
        """Candlestick should accept string decimal values."""
        candle = Candlestick(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1h",
            timestamp=1704067200000,
            open="42000.00",
            high="42500.00",
            low="41500.00",
            close="42200.00",
            volume="1000.50"
        )
        assert candle.open == Decimal("42000.00")
        assert candle.volume == Decimal("1000.50")

    def test_candlestick_different_timeframes(self, candlestick_factory):
        """Candlestick should accept various timeframes."""
        timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]
        for tf in timeframes:
            candle = candlestick_factory(timeframe=tf)
            assert candle.timeframe == tf


class TestSignalModel:
    """Tests for Signal model."""

    def test_signal_long(self, sample_long_signal):
        """LONG signal should be created correctly."""
        assert sample_long_signal.type == SignalType.LONG
        assert sample_long_signal.strategy_id == "test_strategy"

    def test_signal_short(self, sample_short_signal):
        """SHORT signal should be created correctly."""
        assert sample_short_signal.type == SignalType.SHORT

    def test_signal_exit_long(self, sample_exit_long_signal):
        """EXIT_LONG signal should be created correctly."""
        assert sample_exit_long_signal.type == SignalType.EXIT_LONG

    def test_signal_exit_short(self, sample_exit_short_signal):
        """EXIT_SHORT signal should be created correctly."""
        assert sample_exit_short_signal.type == SignalType.EXIT_SHORT

    def test_signal_no_signal(self, signal_factory):
        """NO_SIGNAL type should be valid."""
        signal = signal_factory(signal_type=SignalType.NO_SIGNAL)
        assert signal.type == SignalType.NO_SIGNAL

    def test_signal_optional_fields(self, signal_factory):
        """Signal should work with optional fields."""
        signal = signal_factory(
            quantity=None,
            price=None,
            stop_loss=None,
            take_profit=None,
            metadata=None
        )
        assert signal.quantity is None
        assert signal.price is None

    def test_signal_with_stop_loss_take_profit(self, signal_factory):
        """Signal should accept stop_loss and take_profit."""
        signal = signal_factory(
            price=Decimal("42000"),
            stop_loss=Decimal("41000"),
            take_profit=Decimal("44000")
        )
        assert signal.stop_loss == Decimal("41000")
        assert signal.take_profit == Decimal("44000")

    def test_signal_with_metadata(self, signal_factory):
        """Signal should accept metadata dictionary."""
        metadata = {"reason": "golden_cross", "confidence": 0.85}
        signal = signal_factory(metadata=metadata)
        assert signal.metadata == metadata
        assert signal.metadata["reason"] == "golden_cross"


class TestOrderStatus:
    """Tests for idempotent order lifecycle status constants."""

    def test_idempotent_order_status_values(self):
        assert OrderStatus.NEW.value == "NEW"
        assert OrderStatus.SUBMITTED_UNCONFIRMED.value == "SUBMITTED_UNCONFIRMED"
        assert OrderStatus.SUBMITTED.value == "SUBMITTED"
        assert OrderStatus.FILLED.value == "FILLED"
        assert OrderStatus.CANCELLED.value == "CANCELLED"

    def test_order_status_transition_constants_are_distinct(self):
        lifecycle = [
            OrderStatus.NEW,
            OrderStatus.SUBMITTED_UNCONFIRMED,
            OrderStatus.SUBMITTED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
        ]

        assert len({status.value for status in lifecycle}) == len(lifecycle)


class TestPositionModel:
    """Tests for Position model."""

    def test_position_long(self, sample_long_position):
        """LONG position should be created correctly."""
        assert sample_long_position.side == "LONG"
        assert sample_long_position.quantity == Decimal("0.5")

    def test_position_short(self, sample_short_position):
        """SHORT position should be created correctly."""
        assert sample_short_position.side == "SHORT"

    def test_position_unrealized_pnl(self, position_factory):
        """Position should track unrealized PnL."""
        pos = position_factory(
            entry_price=Decimal("42000"),
            unrealized_pnl=Decimal("500")
        )
        assert pos.unrealized_pnl == Decimal("500")

    def test_position_product_id_validation(self):
        """Position should validate product_id format."""
        with pytest.raises(ValidationError):
            Position(
                strategy_id="test",
                product_id="invalid",
                side="LONG",
                quantity=Decimal("1"),
                entry_price=Decimal("100"),
                unrealized_pnl=Decimal("0")
            )


class TestSignalTypeEnum:
    """Tests for SignalType enum."""

    def test_signal_type_values(self):
        """SignalType enum should have expected values."""
        assert SignalType.LONG.value == "LONG"
        assert SignalType.SHORT.value == "SHORT"
        assert SignalType.EXIT_LONG.value == "EXIT_LONG"
        assert SignalType.EXIT_SHORT.value == "EXIT_SHORT"
        assert SignalType.NO_SIGNAL.value == "NO_SIGNAL"

    def test_signal_type_from_string(self):
        """SignalType should be creatable from string."""
        assert SignalType("LONG") == SignalType.LONG
        assert SignalType("SHORT") == SignalType.SHORT

    def test_signal_type_is_string_enum(self):
        """SignalType should be a string enum."""
        assert isinstance(SignalType.LONG, str)
        assert SignalType.LONG == "LONG"


class TestModelSerialization:
    """Tests for model serialization/deserialization."""

    def test_trade_to_dict(self):
        """Trade should serialize to dict correctly."""
        trade = Trade(
            id="trade_1",
            product_id="BINANCE:BTCUSDT-PERP",
            price=Decimal("42000.50"),
            quantity=Decimal("0.001"),
            side="buy",
            timestamp=1704067200000
        )
        data = trade.model_dump()
        assert data["id"] == "trade_1"
        assert data["product_id"] == "BINANCE:BTCUSDT-PERP"
        # Decimals are preserved in model_dump
        assert data["price"] == Decimal("42000.50")

    def test_trade_from_dict(self):
        """Trade should deserialize from dict correctly."""
        data = {
            "id": "trade_1",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "price": "42000.50",
            "quantity": "0.001",
            "side": "buy",
            "timestamp": 1704067200000
        }
        trade = Trade.model_validate(data)
        assert trade.price == Decimal("42000.50")

    def test_signal_json_serialization(self, sample_long_signal):
        """Signal should serialize to JSON correctly."""
        json_str = sample_long_signal.model_dump_json()
        assert "LONG" in json_str
        assert "BINANCE:BTCUSDT-PERP" in json_str
