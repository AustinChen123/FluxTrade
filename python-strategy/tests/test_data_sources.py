"""
Tests for src/core/data_sources/ implementations.

Covers:
- MemoryDataSource (in-memory candle storage)
- CsvDataSource (CSV file ingestion with auto-detection)
- IDataSource interface contract validation
"""

from decimal import Decimal

import pandas as pd
import pytest

from src.core.models import Candlestick
from src.core.data_sources.memory import MemoryDataSource
from src.core.data_sources.csv_source import CsvDataSource

PRODUCT = "BINANCE:BTCUSDT-PERP"
TF = "1m"


def _make_candle(
    ts: int,
    price: float = 42000.0,
    product_id: str = PRODUCT,
    timeframe: str = TF,
) -> Candlestick:
    return Candlestick(
        product_id=product_id,
        timeframe=timeframe,
        timestamp=ts,
        open=Decimal(str(price)),
        high=Decimal(str(price + 100)),
        low=Decimal(str(price - 100)),
        close=Decimal(str(price + 50)),
        volume=Decimal("500.0"),
    )


def _make_candles(count: int, start_ts: int = 1704067200000, **kwargs) -> list[Candlestick]:
    return [_make_candle(start_ts + i * 60000, **kwargs) for i in range(count)]


# =============================================================================
# MemoryDataSource
# =============================================================================

class TestMemoryDataSourceBasics:

    def test_empty_source(self):
        ds = MemoryDataSource()
        result = list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))
        assert result == []

    def test_validate_empty_returns_false(self):
        ds = MemoryDataSource()
        assert ds.validate() is False

    def test_validate_with_data_returns_true(self):
        ds = MemoryDataSource(_make_candles(1))
        assert ds.validate() is True

    def test_get_candles_returns_all(self):
        candles = _make_candles(5)
        ds = MemoryDataSource(candles)
        result = list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))
        assert len(result) == 5

    def test_get_candles_filters_by_time_range(self):
        candles = _make_candles(10)
        ds = MemoryDataSource(candles)
        start = candles[3].timestamp
        end = candles[6].timestamp
        result = list(ds.get_candles(PRODUCT, TF, start, end))
        assert len(result) == 4
        assert result[0].timestamp == start
        assert result[-1].timestamp == end

    def test_get_candles_filters_by_product_id(self):
        btc = _make_candles(3, product_id="BINANCE:BTCUSDT-PERP")
        eth = _make_candles(3, product_id="BINANCE:ETHUSDT-PERP")
        ds = MemoryDataSource(btc + eth)
        result = list(ds.get_candles("BINANCE:ETHUSDT-PERP", TF, 0, 9999999999999))
        assert len(result) == 3
        assert all(c.product_id == "BINANCE:ETHUSDT-PERP" for c in result)

    def test_get_candles_filters_by_timeframe(self):
        m1 = _make_candles(3, timeframe="1m")
        m5 = _make_candles(3, timeframe="5m")
        ds = MemoryDataSource(m1 + m5)
        result = list(ds.get_candles(PRODUCT, "5m", 0, 9999999999999))
        assert len(result) == 3
        assert all(c.timeframe == "5m" for c in result)

    def test_get_candles_order_ascending(self):
        candles = _make_candles(5)
        # Shuffle before passing
        ds = MemoryDataSource(list(reversed(candles)))
        result = list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))
        timestamps = [c.timestamp for c in result]
        assert timestamps == sorted(timestamps)


class TestMemoryDataSourceDataFrame:

    def test_get_candles_df_returns_dataframe(self):
        candles = _make_candles(5)
        ds = MemoryDataSource(candles)
        df = ds.get_candles_df(PRODUCT, TF, 0, 9999999999999)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert list(df.columns) == ["open", "high", "low", "close", "volume"]

    def test_get_candles_df_empty_result(self):
        ds = MemoryDataSource()
        df = ds.get_candles_df(PRODUCT, TF, 0, 9999999999999)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_candles_df_index_is_timestamp(self):
        candles = _make_candles(3)
        ds = MemoryDataSource(candles)
        df = ds.get_candles_df(PRODUCT, TF, 0, 9999999999999)
        assert df.index.name == "timestamp"

    def test_get_candles_df_values_are_float(self):
        candles = _make_candles(2)
        ds = MemoryDataSource(candles)
        df = ds.get_candles_df(PRODUCT, TF, 0, 9999999999999)
        for col in ("open", "high", "low", "close", "volume"):
            assert df[col].dtype == float


class TestMemoryDataSourceRange:

    def test_available_range(self):
        candles = _make_candles(10)
        ds = MemoryDataSource(candles)
        rng = ds.get_available_range(PRODUCT, TF)
        assert rng is not None
        assert rng == (candles[0].timestamp, candles[-1].timestamp)

    def test_available_range_no_data(self):
        ds = MemoryDataSource()
        assert ds.get_available_range(PRODUCT, TF) is None

    def test_available_range_filters_product(self):
        btc = _make_candles(3, product_id="BINANCE:BTCUSDT-PERP")
        eth = _make_candles(5, start_ts=1704100000000, product_id="BINANCE:ETHUSDT-PERP")
        ds = MemoryDataSource(btc + eth)
        rng = ds.get_available_range("BINANCE:ETHUSDT-PERP", TF)
        assert rng == (eth[0].timestamp, eth[-1].timestamp)


class TestMemoryDataSourceAddCandles:

    def test_add_candles_merges_and_sorts(self):
        ds = MemoryDataSource(_make_candles(3, start_ts=1704067200000))
        ds.add_candles(_make_candles(3, start_ts=1704060000000))
        result = list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))
        assert len(result) == 6
        timestamps = [c.timestamp for c in result]
        assert timestamps == sorted(timestamps)


# =============================================================================
# CsvDataSource
# =============================================================================

class TestCsvDataSourceStandard:

    @pytest.fixture
    def csv_file(self, tmp_path):
        """Create a standard CSV file for testing."""
        path = tmp_path / "candles.csv"
        lines = ["timestamp,open,high,low,close,volume"]
        base_ts = 1704067200000
        for i in range(10):
            ts = base_ts + i * 60000
            lines.append(f"{ts},42000,42100,41900,42050,{500 + i}")
        path.write_text("\n".join(lines))
        return str(path)

    def test_get_candles(self, csv_file):
        ds = CsvDataSource(csv_file, product_id=PRODUCT, timeframe=TF)
        result = list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))
        assert len(result) == 10
        assert result[0].product_id == PRODUCT
        assert result[0].timeframe == TF

    def test_get_candles_time_filter(self, csv_file):
        ds = CsvDataSource(csv_file, product_id=PRODUCT, timeframe=TF)
        base_ts = 1704067200000
        start = base_ts + 3 * 60000
        end = base_ts + 6 * 60000
        result = list(ds.get_candles(PRODUCT, TF, start, end))
        assert len(result) == 4

    def test_get_candles_df(self, csv_file):
        ds = CsvDataSource(csv_file, product_id=PRODUCT, timeframe=TF)
        df = ds.get_candles_df(PRODUCT, TF, 0, 9999999999999)
        assert len(df) == 10
        assert df.index.name == "timestamp"

    def test_available_range(self, csv_file):
        ds = CsvDataSource(csv_file, product_id=PRODUCT, timeframe=TF)
        rng = ds.get_available_range(PRODUCT, TF)
        base_ts = 1704067200000
        assert rng == (base_ts, base_ts + 9 * 60000)

    def test_validate_valid_file(self, csv_file):
        ds = CsvDataSource(csv_file)
        assert ds.validate() is True

    def test_validate_missing_file(self):
        ds = CsvDataSource("/nonexistent/path.csv")
        assert ds.validate() is False

    def test_decimal_precision(self, csv_file):
        ds = CsvDataSource(csv_file, product_id=PRODUCT, timeframe=TF)
        result = list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))
        assert isinstance(result[0].open, Decimal)
        assert isinstance(result[0].close, Decimal)


class TestCsvDataSourceTradingView:

    @pytest.fixture
    def tv_csv(self, tmp_path):
        """TradingView-style CSV with 'time' column."""
        path = tmp_path / "tv_export.csv"
        lines = ["time,open,high,low,close,Volume"]
        base_ts = 1704067200000
        for i in range(5):
            ts = base_ts + i * 60000
            lines.append(f"{ts},42000,42100,41900,42050,{1000 + i}")
        path.write_text("\n".join(lines))
        return str(path)

    def test_auto_detect_tradingview_columns(self, tv_csv):
        ds = CsvDataSource(tv_csv, product_id=PRODUCT, timeframe=TF)
        result = list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))
        assert len(result) == 5


class TestCsvDataSourceYahoo:

    @pytest.fixture
    def yahoo_csv(self, tmp_path):
        """Yahoo Finance-style CSV with date strings."""
        path = tmp_path / "yahoo.csv"
        lines = ["Date,Open,High,Low,Close,Adj Close,Volume"]
        dates = [
            "2024-01-01", "2024-01-02", "2024-01-03",
            "2024-01-04", "2024-01-05",
        ]
        for d in dates:
            lines.append(f"{d},42000,42100,41900,42050,42050,1000")
        path.write_text("\n".join(lines))
        return str(path)

    def test_auto_detect_yahoo_columns(self, yahoo_csv):
        ds = CsvDataSource(yahoo_csv, product_id=PRODUCT, timeframe="1d")
        result = list(ds.get_candles(PRODUCT, "1d", 0, 9999999999999))
        assert len(result) == 5
        # Timestamps should have been parsed from date strings
        assert all(isinstance(c.timestamp, int) for c in result)


class TestCsvDataSourceEpochSeconds:

    @pytest.fixture
    def epoch_s_csv(self, tmp_path):
        """CSV with timestamps in epoch seconds (not ms)."""
        path = tmp_path / "epoch_s.csv"
        lines = ["timestamp,open,high,low,close,volume"]
        base_ts = 1704067200  # seconds
        for i in range(5):
            ts = base_ts + i * 60
            lines.append(f"{ts},42000,42100,41900,42050,500")
        path.write_text("\n".join(lines))
        return str(path)

    def test_auto_convert_seconds_to_ms(self, epoch_s_csv):
        ds = CsvDataSource(epoch_s_csv, product_id=PRODUCT, timeframe=TF)
        result = list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))
        assert len(result) == 5
        # Should be in milliseconds
        assert result[0].timestamp >= 1e12


class TestCsvDataSourceMissingColumns:

    def test_missing_volume_raises_on_load(self, tmp_path):
        path = tmp_path / "bad.csv"
        path.write_text("timestamp,open,high,low,close\n1704067200000,42000,42100,41900,42050\n")
        ds = CsvDataSource(str(path))
        with pytest.raises(ValueError, match="missing required columns"):
            list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))

    def test_validate_returns_false_for_bad_csv(self, tmp_path):
        path = tmp_path / "bad.csv"
        path.write_text("timestamp,open,high,low,close\n1704067200000,42000,42100,41900,42050\n")
        ds = CsvDataSource(str(path))
        assert ds.validate() is False


class TestCsvDataSourceLazyLoad:

    def test_lazy_load_only_on_access(self, tmp_path):
        path = tmp_path / "lazy.csv"
        lines = ["timestamp,open,high,low,close,volume"]
        lines.append("1704067200000,42000,42100,41900,42050,500")
        path.write_text("\n".join(lines))

        ds = CsvDataSource(str(path))
        assert ds._df is None  # Not loaded yet
        list(ds.get_candles(PRODUCT, TF, 0, 9999999999999))
        assert ds._df is not None  # Now loaded
