from __future__ import annotations

import io
import os
import threading
import uuid
from decimal import Decimal
from typing import cast

import pytest
import redis
from fluxtrade_core import (
    CandleAggregator,  # pyright: ignore[reportAttributeAccessIssue]
    Candlestick as RustCandlestick,  # pyright: ignore[reportAttributeAccessIssue]
)

from src.core.models import Candlestick
from src.strategies.golden_cross import GoldenCrossStrategy
from src.validation.strategy_evidence import ShadowRunReport
from src.validation.strategy_evidence import run_shadow_evidence

REDIS_URL = os.getenv("FLUXTRADE_REDIS_INTEGRATION_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rust,
    pytest.mark.skipif(
        not REDIS_URL,
        reason="FLUXTRADE_REDIS_INTEGRATION_URL is not configured",
    ),
]

PRODUCT_ID = "RITHMIC:MNQ-202609"


class _ObservedRedis:
    def __init__(self, client: redis.Redis, reading: threading.Event) -> None:
        self._client = client
        self._reading = reading

    def xread(self, *args, **kwargs):
        self._reading.set()
        return self._client.xread(*args, **kwargs)


def test_redis_stream_reaches_rust_aggregate_and_strategy(monkeypatch) -> None:
    monkeypatch.setattr(
        GoldenCrossStrategy,
        "__fluxtrade_display_name__",
        "Golden Cross Fixture",
        raising=False,
    )
    monkeypatch.setattr(
        GoldenCrossStrategy,
        "__fluxtrade_artifact_version__",
        "1.0.0",
        raising=False,
    )
    monkeypatch.setattr(
        GoldenCrossStrategy,
        "__fluxtrade_readiness__",
        "RESEARCH_VALIDATED",
        raising=False,
    )
    monkeypatch.setattr(
        GoldenCrossStrategy,
        "__fluxtrade_catalog_sha256__",
        "0" * 64,
        raising=False,
    )
    client = redis.Redis.from_url(str(REDIS_URL), decode_responses=True)
    client.ping()
    stream_suffix = uuid.uuid4().hex
    source_stream_key = f"test:shadow:{stream_suffix}:1m"
    decision_stream_key = f"test:shadow:{stream_suffix}:5m"
    reading = threading.Event()
    output = io.StringIO()
    result: dict[str, object] = {}

    def consume() -> None:
        result["report"] = run_shadow_evidence(
            _ObservedRedis(client, reading),
            source_stream_key=source_stream_key,
            decision_stream_key=decision_stream_key,
            strategy=GoldenCrossStrategy(
                "golden-cross-shadow",
                PRODUCT_ID,
                short_window=2,
                long_window=3,
                timeframe="5m",
                quantity=Decimal("1"),
            ),
            output=output,
            duration_seconds=2,
        )

    consumer = threading.Thread(target=consume)
    consumer.start()
    assert reading.wait(timeout=1)

    closes = ("100", "99", "100", "110", "110")
    start_timestamp = 1_800_000_000_000
    aggregator = CandleAggregator()
    try:
        for minute in range(21):
            close = Decimal(closes[minute // 5])
            candle = Candlestick(
                product_id=PRODUCT_ID,
                timeframe="1m",
                timestamp=start_timestamp + minute * 60_000,
                open=close,
                high=close + Decimal("1"),
                low=close - Decimal("1"),
                close=close,
                volume=Decimal("1"),
            )
            client.xadd(source_stream_key, {"json": candle.model_dump_json()})
            completed = aggregator.add_candle(
                RustCandlestick(
                    candle.product_id,
                    candle.timeframe,
                    candle.timestamp,
                    str(candle.open),
                    str(candle.high),
                    str(candle.low),
                    str(candle.close),
                    str(candle.volume),
                ),
                "5m",
            )
            if completed is not None:
                completed_candle = Candlestick(
                    product_id=completed.product_id,
                    timeframe=completed.timeframe,
                    timestamp=completed.timestamp,
                    open=Decimal(completed.open),
                    high=Decimal(completed.high),
                    low=Decimal(completed.low),
                    close=Decimal(completed.close),
                    volume=Decimal(completed.volume),
                )
                client.xadd(
                    decision_stream_key,
                    {"json": completed_candle.model_dump_json()},
                )

        consumer.join(timeout=4)
        assert not consumer.is_alive()
        report = cast(ShadowRunReport, result["report"])
        assert report.source_candles == 21
        assert report.completed_candles == 4
        assert report.actionable_signal_count == 1
        assert '"type":"LONG"' in output.getvalue()
        assert client.xlen(source_stream_key) == 21
        assert client.xlen(decision_stream_key) == 4
    finally:
        if consumer.is_alive():
            consumer.join(timeout=3)
        client.delete(source_stream_key, decision_stream_key)
        client.close()
