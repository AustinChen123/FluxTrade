from __future__ import annotations

import io
import os
import threading
import uuid
from decimal import Decimal
from typing import cast
from unittest.mock import MagicMock

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
]

requires_redis = pytest.mark.skipif(
    not REDIS_URL,
    reason="FLUXTRADE_REDIS_INTEGRATION_URL is not configured",
)

PRODUCT_ID = "RITHMIC:MNQ-202609"


class _ObservedRedis:
    def __init__(
        self,
        client: redis.Redis,
        reading: threading.Event,
        *,
        source_stream_key: str | None = None,
        source_returned: threading.Event | None = None,
        source_release: threading.Event | None = None,
    ) -> None:
        self._client = client
        self._reading = reading
        self._source_stream_key = source_stream_key
        self._source_returned = source_returned
        self._source_release = source_release

    def pipeline(self, *args, **kwargs):
        return self._client.pipeline(*args, **kwargs)

    def xread(self, *args, **kwargs):
        self._reading.set()
        response = cast(
            list[list[str | list[tuple[str, dict[str, str]]]]],
            self._client.xread(*args, **kwargs),
        )
        observed_streams = {cast(str, row[0]) for row in response}
        if (
            self._source_returned is not None
            and self._source_stream_key in observed_streams
            and len(observed_streams) == 1
        ):
            self._source_returned.set()
            if self._source_release is not None:
                if not self._source_release.wait(timeout=1):
                    raise TimeoutError("source response was not released")
        return response


def test_observed_redis_sync_xread_returns_same_response() -> None:
    client = MagicMock(spec=redis.Redis)
    response = [["source", [("1-0", {"json": "{}"})]]]
    client.xread.return_value = response
    reading = threading.Event()
    observed = _ObservedRedis(client, reading)

    result = observed.xread({"source": "0-0"}, block=25)

    assert result is response
    assert reading.is_set()
    client.xread.assert_called_once_with({"source": "0-0"}, block=25)


def _mark_golden_cross_as_artifact(monkeypatch) -> None:
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


@requires_redis
def test_redis_stream_reaches_rust_aggregate_and_strategy(monkeypatch) -> None:
    _mark_golden_cross_as_artifact(monkeypatch)
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


@requires_redis
def test_shadow_does_not_skip_decision_after_source_wakes_reader(
    monkeypatch,
) -> None:
    _mark_golden_cross_as_artifact(monkeypatch)
    client = redis.Redis.from_url(str(REDIS_URL), decode_responses=True)
    client.ping()
    stream_suffix = uuid.uuid4().hex
    source_stream_key = f"test:shadow:{stream_suffix}:1m"
    decision_stream_key = f"test:shadow:{stream_suffix}:5m"
    reading = threading.Event()
    source_returned = threading.Event()
    source_release = threading.Event()
    result: dict[str, object] = {}

    def consume() -> None:
        try:
            result["report"] = run_shadow_evidence(
                _ObservedRedis(
                    client,
                    reading,
                    source_stream_key=source_stream_key,
                    source_returned=source_returned,
                    source_release=source_release,
                ),
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
                output=io.StringIO(),
                duration_seconds=2,
            )
        except Exception as exc:
            result["error"] = exc

    start_timestamp = 1_800_000_000_000

    def candle(timeframe: str, minute: int) -> Candlestick:
        return Candlestick(
            product_id=PRODUCT_ID,
            timeframe=timeframe,
            timestamp=start_timestamp + minute * 60_000,
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100"),
            volume=Decimal("1"),
        )

    consumer = threading.Thread(target=consume)
    try:
        consumer.start()
        assert reading.wait(timeout=1)
        client.xadd(
            source_stream_key,
            {"json": candle("1m", 0).model_dump_json()},
        )
        assert source_returned.wait(timeout=1)
        client.xadd(
            decision_stream_key,
            {"json": candle("5m", 0).model_dump_json()},
        )
        source_release.set()
        for minute in range(1, 6):
            client.xadd(
                source_stream_key,
                {"json": candle("1m", minute).model_dump_json()},
            )

        consumer.join(timeout=4)
        assert not consumer.is_alive()
        assert "error" not in result
        report = cast(ShadowRunReport, result["report"])
        assert report.source_candles == 6
        assert report.completed_candles == 1
    finally:
        source_release.set()
        if consumer.is_alive():
            consumer.join(timeout=3)
        client.delete(source_stream_key, decision_stream_key)
        client.close()
