"""Integration test fixtures and configuration.

All tests in this directory are automatically marked as 'integration'.
Tests requiring the Rust .so should additionally use @pytest.mark.rust.

Run integration tests:
    uv run pytest -m integration
    uv run pytest -m "integration and not rust"   # skip Rust-dependent
    uv run pytest -m "integration and rust"        # Rust-only
"""
import pytest
from decimal import Decimal


def pytest_collection_modifyitems(items):
    """Auto-mark all tests in integration/ as integration tests."""
    for item in items:
        if "/integration/" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------
PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "15m"
INITIAL_BALANCE = Decimal("10000")


# ---------------------------------------------------------------------------
# Candle generation helpers
# ---------------------------------------------------------------------------
def make_candle(
    timestamp: int,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal = Decimal("100"),
    product_id: str = PRODUCT_ID,
    timeframe: str = TIMEFRAME,
):
    """Create a Candlestick model for integration tests."""
    from src.core.models import Candlestick

    return Candlestick(
        product_id=product_id,
        timeframe=timeframe,
        timestamp=timestamp,
        open=open,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


def make_candle_series(
    count: int = 100,
    start_timestamp: int = 1_700_000_000_000,
    start_price: Decimal = Decimal("50000"),
    product_id: str = PRODUCT_ID,
    timeframe: str = TIMEFRAME,
) -> list:
    """Generate a deterministic candle series with mild uptrend + noise."""
    import math

    candles = []
    price = float(start_price)
    interval_ms = 15 * 60 * 1000  # 15m

    for i in range(count):
        ts = start_timestamp + i * interval_ms
        # Deterministic wave pattern: uptrend with sine modulation
        drift = 10.0  # $10 per bar uptrend
        wave = 200.0 * math.sin(i * 0.15)
        noise = 50.0 * math.sin(i * 0.73)  # secondary frequency

        open_price = price
        close_price = price + drift + (wave - 200.0 * math.sin((i - 1) * 0.15))
        close_price += noise - 50.0 * math.sin((i - 1) * 0.73)

        high_price = max(open_price, close_price) + abs(30.0 * math.sin(i * 0.5))
        low_price = min(open_price, close_price) - abs(30.0 * math.sin(i * 0.3))

        candles.append(make_candle(
            timestamp=ts,
            open=Decimal(str(round(open_price, 2))),
            high=Decimal(str(round(high_price, 2))),
            low=Decimal(str(round(low_price, 2))),
            close=Decimal(str(round(close_price, 2))),
            product_id=product_id,
            timeframe=timeframe,
        ))
        price = close_price

    return candles
