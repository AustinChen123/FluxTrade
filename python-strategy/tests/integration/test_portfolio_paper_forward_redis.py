from __future__ import annotations

import io
import os
import threading
import time
import uuid
from decimal import Decimal
from typing import Any, cast

import pytest
import redis

from src.core.models import Candlestick, Signal, SignalType
from src.core.portfolio_runtime import PortfolioDefinition, PortfolioSleeve
from src.core.product_registry import to_stream_key
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.validation.portfolio_paper_forward import (
    PortfolioPaperForwardReport,
    run_portfolio_paper_forward,
)

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


class _RedisPaperStrategy(BaseStrategy):
    def __init__(self, strategy_id: str, product_id: str) -> None:
        super().__init__(strategy_id, product_id)
        self.seen = 0

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "5m", 1)

    def fresh_instance_for_replay(self) -> BaseStrategy:
        return type(self)(self.strategy_id, self.product_id)

    def replay_configuration(self) -> object:
        return ()

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        self.seen += 1
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="5m",
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )


setattr(_RedisPaperStrategy, "__fluxtrade_display_name__", "Redis Paper Fixture")
setattr(_RedisPaperStrategy, "__fluxtrade_artifact_version__", "1.0.0")
setattr(_RedisPaperStrategy, "__fluxtrade_readiness__", "RESEARCH_FROZEN")
setattr(_RedisPaperStrategy, "__fluxtrade_catalog_sha256__", "3" * 64)


def _portfolio() -> PortfolioDefinition:
    return PortfolioDefinition(
        portfolio_id="redis_paper_forward",
        product_id=PRODUCT_ID,
        sleeves=(
            PortfolioSleeve(
                _RedisPaperStrategy("redis_paper_forward.sleeve", PRODUCT_ID)
            ),
        ),
        max_gross_quantity=Decimal("1"),
        artifact_version="1.0.0",
        display_name="Redis Paper Fixture",
        readiness="RESEARCH_FROZEN",
        catalog_sha256="4" * 64,
    )


def _candle(timestamp: int, timeframe: str = "5m") -> Candlestick:
    return Candlestick(
        product_id=PRODUCT_ID,
        timeframe=timeframe,
        timestamp=timestamp,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("1"),
    )


def test_real_redis_consumer_drives_portfolio_paper_forward(
    monkeypatch,
    tmp_path,
) -> None:
    client = redis.Redis.from_url(str(REDIS_URL), decode_responses=True)
    client.ping()
    source_stream_key = to_stream_key(PRODUCT_ID, "1m")
    decision_stream_key = to_stream_key(PRODUCT_ID, "5m")
    stream_keys = (source_stream_key, decision_stream_key)
    run_id = f"redis-{uuid.uuid4().hex}"
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-integration")
    group_name = f"paper_forward:paper-forward-integration:{run_id}"
    result: dict[str, object] = {}
    message_ids: list[tuple[str, str]] = []

    monkeypatch.setattr(
        "src.core.consumer.create_redis_client",
        lambda: redis.Redis.from_url(str(REDIS_URL), decode_responses=True),
    )
    monkeypatch.setattr(
        "src.core.engine.create_redis_client",
        lambda: redis.Redis.from_url(str(REDIS_URL), decode_responses=True),
    )

    def run() -> None:
        try:
            result["report"] = run_portfolio_paper_forward(
                tmp_path / "paper",
                run_id=run_id,
                portfolio_factory=_portfolio,
                warmup_candles=[_candle(1_800_000_000_000)],
                output=io.StringIO(),
                duration_seconds=1.5,
            )
        except Exception as exc:
            result["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    try:
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            groups_by_stream = {
                stream_key: cast(
                    list[dict[str, Any]],
                    client.xinfo_groups(stream_key),
                )
                for stream_key in stream_keys
                if client.exists(stream_key)
            }
            if all(
                any(
                    group["name"] == group_name
                    for group in groups_by_stream.get(stream_key, [])
                )
                for stream_key in stream_keys
            ):
                break
            time.sleep(0.01)
        else:
            pytest.fail("paper-forward consumer group was not created")

        decision_timestamp = 1_800_000_300_000
        for timestamp in range(
            decision_timestamp,
            decision_timestamp + 300_001,
            60_000,
        ):
            message_ids.append(
                (
                    source_stream_key,
                    str(
                        cast(
                            Any,
                            client.xadd(
                                source_stream_key,
                                {
                                    "json": _candle(
                                        timestamp,
                                        "1m",
                                    ).model_dump_json()
                                },
                            ),
                        )
                    ),
                )
            )
        message_ids.append(
            (
                decision_stream_key,
                str(
                    cast(
                        Any,
                        client.xadd(
                            decision_stream_key,
                            {"json": _candle(decision_timestamp).model_dump_json()},
                        ),
                    )
                ),
            )
        )
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert "error" not in result
        report = result["report"]
        assert isinstance(report, PortfolioPaperForwardReport)
        assert report.source_candles == 6
        assert report.prospective_candles == 1
        assert report.order_count == 0
        assert report.fill_count == 0
        for stream_key in stream_keys:
            assert all(
                group["name"] != group_name
                for group in cast(
                    list[dict[str, Any]],
                    client.xinfo_groups(stream_key),
                )
            )
        assert not client.exists(
            f"fluxtrade:paper-forward-integration:consumer:{group_name}:streams"
        )
    finally:
        if thread.is_alive():
            thread.join(timeout=2)
        for stream_key in stream_keys:
            try:
                client.xgroup_destroy(stream_key, group_name)
            except redis.ResponseError:
                pass
        for stream_key, message_id in message_ids:
            client.xdel(stream_key, message_id)
        client.close()
