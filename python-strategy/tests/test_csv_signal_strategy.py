"""Unit tests for CsvSignalStrategy."""
import pytest
from decimal import Decimal
from src.core.models import Candlestick, SignalType
from src.strategies.csv_signal_strategy import CsvSignalStrategy


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "15m"


def _make_candle(ts: int, close: Decimal = Decimal("50000")) -> Candlestick:
    return Candlestick(
        product_id=PRODUCT_ID, timeframe=TIMEFRAME, timestamp=ts,
        open=close, high=close + Decimal("100"), low=close - Decimal("100"),
        close=close, volume=Decimal("100"),
    )


@pytest.fixture
def csv_file(tmp_path):
    """Create a temp CSV with test signals."""
    path = tmp_path / "signals.csv"
    path.write_text(
        "timestamp,type,price,stop_loss,take_profit,trailing_distance,quantity\n"
        "1700000000000,LONG,42000,41500,43000,,0.1\n"
        "1700000900000,EXIT_LONG,,,,,\n"
        "1700001800000,SHORT,41800,42300,41000,50,0.2\n"
    )
    return str(path)


@pytest.fixture
def minimal_csv(tmp_path):
    """CSV with only required fields (timestamp + type)."""
    path = tmp_path / "minimal.csv"
    path.write_text(
        "timestamp,type\n"
        "1700000000000,LONG\n"
        "1700000900000,EXIT_LONG\n"
    )
    return str(path)


class TestCsvSignalStrategy:

    def test_signal_at_matching_timestamp(self, csv_file):
        """on_candle returns the correct signal when timestamp matches."""
        strat = CsvSignalStrategy("csv_test", csv_file, PRODUCT_ID, TIMEFRAME)
        sig = strat.on_candle(_make_candle(ts=1700000000000))

        assert sig.type == SignalType.LONG
        assert sig.price == Decimal("42000")
        assert sig.stop_loss == Decimal("41500")
        assert sig.take_profit == Decimal("43000")
        assert sig.quantity == Decimal("0.1")
        assert sig.strategy_id == "csv_test"

    def test_no_signal_at_unmatched_timestamp(self, csv_file):
        """on_candle returns NO_SIGNAL when no signal exists at timestamp."""
        strat = CsvSignalStrategy("csv_test", csv_file, PRODUCT_ID, TIMEFRAME)
        sig = strat.on_candle(_make_candle(ts=9999999999999))
        assert sig.type == SignalType.NO_SIGNAL

    def test_exit_signal(self, csv_file):
        """EXIT_LONG signal parsed correctly with empty optional fields."""
        strat = CsvSignalStrategy("csv_test", csv_file, PRODUCT_ID, TIMEFRAME)
        sig = strat.on_candle(_make_candle(ts=1700000900000))
        assert sig.type == SignalType.EXIT_LONG
        assert sig.price is None
        assert sig.quantity is None

    def test_short_with_trailing(self, csv_file):
        """SHORT signal with trailing_distance parsed correctly."""
        strat = CsvSignalStrategy("csv_test", csv_file, PRODUCT_ID, TIMEFRAME)
        sig = strat.on_candle(_make_candle(ts=1700001800000))
        assert sig.type == SignalType.SHORT
        assert sig.trailing_distance == Decimal("50")
        assert sig.quantity == Decimal("0.2")

    def test_minimal_csv_only_required_fields(self, minimal_csv):
        """CSV with only timestamp+type should work, optional fields None."""
        strat = CsvSignalStrategy("minimal", minimal_csv, PRODUCT_ID, TIMEFRAME)
        sig = strat.on_candle(_make_candle(ts=1700000000000))
        assert sig.type == SignalType.LONG
        assert sig.price is None
        assert sig.stop_loss is None

    def test_requirements(self, csv_file):
        """Requirements should match constructor params."""
        strat = CsvSignalStrategy("csv", csv_file, PRODUCT_ID, "1h")
        req = strat.requirements
        assert req.product_id == PRODUCT_ID
        assert req.timeframe == "1h"

    def test_empty_csv_raises(self, tmp_path):
        """CSV with only headers should raise ValueError."""
        path = tmp_path / "empty.csv"
        path.write_text("timestamp,type\n")
        with pytest.raises(ValueError, match="no signals"):
            CsvSignalStrategy("empty", str(path), PRODUCT_ID, TIMEFRAME)

    def test_invalid_signal_type_raises(self, tmp_path):
        """CSV with unknown signal type should raise ValueError."""
        path = tmp_path / "bad.csv"
        path.write_text("timestamp,type\n1700000000000,INVALID_TYPE\n")
        with pytest.raises(ValueError, match="Invalid signal type"):
            CsvSignalStrategy("bad", str(path), PRODUCT_ID, TIMEFRAME)

    def test_invalid_decimal_value_raises(self, tmp_path):
        """CSV with non-numeric value in Decimal field should raise ValueError."""
        path = tmp_path / "bad_decimal.csv"
        path.write_text("timestamp,type,price\n1700000000000,LONG,abc\n")
        with pytest.raises(ValueError, match="Invalid Decimal value"):
            CsvSignalStrategy("bad_dec", str(path), PRODUCT_ID, TIMEFRAME)
