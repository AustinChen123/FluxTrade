"""Coordinator integration tests for StrategyEngine component wiring."""

from __future__ import annotations

from contextlib import nullcontext
from decimal import Decimal
from unittest.mock import MagicMock

from src.core.models import Candlestick, Signal, SignalType
from src.strategies.base import BaseStrategy, StrategyRequirements


class EmittingStrategy(BaseStrategy):
    def __init__(self, strategy_id: str, product_id: str = "BINANCE:BTCUSDT-PERP"):
        super().__init__(strategy_id, product_id)
        self.candles_received: list[Candlestick] = []

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 10)

    def on_candle(self, candle: Candlestick) -> Signal:
        self.candles_received.append(candle)
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            value=candle.close,
        )


def make_candle(
    product_id: str = "BINANCE:BTCUSDT-PERP",
    timeframe: str = "1m",
) -> Candlestick:
    return Candlestick(
        product_id=product_id,
        timeframe=timeframe,
        timestamp=1704067200000,
        open=Decimal("42000"),
        high=Decimal("42500"),
        low=Decimal("41500"),
        close=Decimal("42200"),
        volume=Decimal("100"),
    )


def test_full_lifecycle_routes_signal_through_wired_components(engine_factory, mock_db_session):
    engine = engine_factory()
    strategy = EmittingStrategy("s1")
    candle = make_candle()
    engine.add_strategy(strategy)
    engine.execution_engine.process_market_data = MagicMock()
    engine.execution_engine.execute_signal = MagicMock(return_value="order-1")
    engine.risk_manager.check_risk = MagicMock(return_value=(True, "ok"))

    engine.on_market_data(candle)
    engine.shutdown(timeout=0.1)

    assert strategy.candles_received == [candle]
    engine.execution_engine.process_market_data.assert_called_once_with(candle)
    engine.risk_manager.check_risk.assert_called_once()
    engine.execution_engine.execute_signal.assert_called_once()
    mock_db_session.add.assert_called_once()
    mock_db_session.commit.assert_called_once()
    assert engine.running is False


def test_command_router_lists_registered_strategies(engine_factory):
    engine = engine_factory()
    engine.add_strategy(EmittingStrategy("s1"))

    result = engine._command_router.handle({"command": "LIST"})

    assert result.success is True
    assert result.data == {
        "strategies": [
            {
                "strategy_id": "s1",
                "product_id": "BINANCE:BTCUSDT-PERP",
                "timeframe": "1m",
            }
        ]
    }


def test_health_monitoring_records_strategy_heartbeat(engine_factory):
    engine = engine_factory()
    engine.add_strategy(EmittingStrategy("s1"))
    mock_db = MagicMock()
    engine._db_session_factory = lambda: nullcontext(mock_db)

    engine._record_strategy_heartbeats(["s1"])

    assert engine._health_monitor.is_healthy("s1") is True
    assert engine._health_monitor.get_uptime("s1") >= 0.0
    mock_db.commit.assert_called_once()
