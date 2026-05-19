"""
Tests for src/core/data_provider.py

Covers:
- timeframe_to_ms unit conversions (minutes, hours, days)
- Invalid timeframe formats
- check_data_availability with sufficient/insufficient data
- Backfill command format generation
- Product ID parsing edge cases
"""

from unittest.mock import MagicMock

import pytest

from src.core.data_provider import timeframe_to_ms, check_data_availability


# =============================================================================
# timeframe_to_ms
# =============================================================================


class TestTimeframeToMs:

    def test_one_minute(self):
        assert timeframe_to_ms("1m") == 60_000

    def test_five_minutes(self):
        assert timeframe_to_ms("5m") == 300_000

    def test_fifteen_minutes(self):
        assert timeframe_to_ms("15m") == 900_000

    def test_thirty_minutes(self):
        assert timeframe_to_ms("30m") == 1_800_000

    def test_one_hour(self):
        assert timeframe_to_ms("1h") == 3_600_000

    def test_four_hours(self):
        assert timeframe_to_ms("4h") == 14_400_000

    def test_one_day(self):
        assert timeframe_to_ms("1d") == 86_400_000

    def test_seven_days(self):
        assert timeframe_to_ms("7d") == 604_800_000


class TestTimeframeToMsErrors:

    def test_invalid_unit_raises(self):
        with pytest.raises(ValueError, match="Unknown timeframe unit"):
            timeframe_to_ms("1x")

    def test_non_numeric_value_raises(self):
        with pytest.raises(ValueError, match="Invalid timeframe format"):
            timeframe_to_ms("abcm")

    def test_empty_string_raises(self):
        with pytest.raises((ValueError, IndexError)):
            timeframe_to_ms("")

    def test_unit_only_raises(self):
        with pytest.raises(ValueError, match="Invalid timeframe format"):
            timeframe_to_ms("m")


# =============================================================================
# check_data_availability
# =============================================================================


class TestCheckDataAvailabilitySufficient:

    def test_sufficient_data_returns_true(self):
        """Should return (True, '') when count >= lookback * 0.9."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 100

        ok, cmd = check_data_availability(
            mock_db, "BINANCE:BTCUSDT-PERP", "1m", lookback=100
        )

        assert ok is True
        assert cmd == ""

    def test_exactly_90_percent_returns_true(self):
        """90% gap tolerance: 90 out of 100 should pass."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 90

        ok, cmd = check_data_availability(
            mock_db, "BINANCE:BTCUSDT-PERP", "1m", lookback=100
        )

        assert ok is True

    def test_over_lookback_returns_true(self):
        """More data than requested should still pass."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 200

        ok, cmd = check_data_availability(
            mock_db, "BINANCE:BTCUSDT-PERP", "1h", lookback=100
        )

        assert ok is True


class TestCheckDataAvailabilityInsufficient:

    def test_insufficient_data_returns_false(self):
        """Should return (False, command) when count < lookback * 0.9."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 50

        ok, cmd = check_data_availability(
            mock_db, "BINANCE:BTCUSDT-PERP", "1m", lookback=100
        )

        assert ok is False
        assert cmd != ""

    def test_zero_data_returns_false(self):
        """Should return False when no data exists."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 0

        ok, cmd = check_data_availability(
            mock_db, "BINANCE:BTCUSDT-PERP", "1m", lookback=100
        )

        assert ok is False


class TestBackfillCommand:

    def test_command_contains_exchange(self):
        """Backfill command should include parsed exchange name."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 0

        _, cmd = check_data_availability(
            mock_db, "BINANCE:BTCUSDT-PERP", "1m", lookback=100
        )

        assert "binance" in cmd.lower()

    def test_command_contains_symbol(self):
        """Backfill command should include parsed symbol (without -PERP)."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 0

        _, cmd = check_data_availability(
            mock_db, "BINANCE:BTCUSDT-PERP", "1m", lookback=100
        )

        assert "BTCUSDT" in cmd

    def test_command_format_docker_exec(self):
        """Backfill command should be a docker exec command."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 0

        _, cmd = check_data_availability(
            mock_db, "BINANCE:BTCUSDT-PERP", "1m", lookback=100
        )

        assert cmd.startswith("docker exec fluxtrade-rust")
        assert "backfill" in cmd
        assert "--exchange" in cmd
        assert "--symbol" in cmd
        assert "--start" in cmd
        assert "--end" in cmd

    def test_command_bybit_exchange(self):
        """Should correctly parse BYBIT exchange."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 0

        _, cmd = check_data_availability(
            mock_db, "BYBIT:ETHUSDT-PERP", "1h", lookback=100
        )

        assert "--exchange bybit" in cmd
        assert "--symbol ETHUSDT" in cmd

    def test_malformed_product_id_fallback(self):
        """Malformed product_id should fallback gracefully."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 0

        _, cmd = check_data_availability(
            mock_db, "NOFORMAT", "1m", lookback=100
        )

        # Should not crash; uses fallback
        assert "backfill" in cmd
