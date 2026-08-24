from decimal import Decimal
from enum import StrEnum
from typing import cast

import pytest

from src.core.models import Candlestick, SignalType
from src.core.strategy_context import RiskSnapshot, StrategyContext
from src.strategies.base import StrategyContextCapability, StrategyRequirements
from src.strategies.representative_benchmark import (
    RepresentativeBenchmarkStrategy,
    representative_strategy_factory,
)


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "5m"


def _strategy(**overrides) -> RepresentativeBenchmarkStrategy:
    values = {
        "trend_window": 3,
        "breakout_window": 3,
        "atr_window": 3,
        "rsi_window": 3,
        "volume_window": 3,
        "swing_window": 1,
        "entry_score": 2,
        "hold_bars": 2,
        "max_atr_expansion": "2",
        "quantity": "1",
    }
    values.update(overrides)
    return RepresentativeBenchmarkStrategy(
        "representative",
        PRODUCT_ID,
        timeframe=TIMEFRAME,
        **values,
    )


def _candle(
    index: int,
    close: str,
    *,
    high: str | None = None,
    low: str | None = None,
    volume: str = "100",
) -> Candlestick:
    close_value = Decimal(close)
    return Candlestick(
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        timestamp=1_700_000_000_000 + index * 300_000,
        open=close_value,
        high=Decimal(high) if high is not None else close_value + Decimal("1"),
        low=Decimal(low) if low is not None else close_value - Decimal("1"),
        close=close_value,
        volume=Decimal(volume),
    )


def test_delays_swing_confirmation_until_right_hand_bar_arrives():
    strategy = _strategy(swing_window=2)
    candles = [
        _candle(0, "100", high="101", low="99"),
        _candle(1, "101", high="102", low="100"),
        _candle(2, "104", high="110", low="103"),
        _candle(3, "103", high="105", low="102"),
        _candle(4, "102", high="104", low="101"),
    ]

    for candle in candles[:4]:
        strategy.on_candle(candle)
    assert strategy.last_confirmed_swing is None

    strategy.on_candle(candles[4])
    assert strategy.last_confirmed_swing == ("HIGH", candles[2].timestamp)


def test_emits_long_then_scheduled_exit():
    strategy = _strategy()
    signals = [
        strategy.on_candle(_candle(index, str(100 + index))) for index in range(5)
    ]

    entry = next(signal for signal in signals if signal.type == SignalType.LONG)
    assert entry.stop_loss is None
    assert entry.take_profit is None
    assert entry.quantity == Decimal("1")

    exit_signal = strategy.on_candle(_candle(5, "105"))
    assert exit_signal.type == SignalType.EXIT_LONG


def test_emits_short():
    strategy = _strategy()
    signals = [
        strategy.on_candle(_candle(index, str(105 - index))) for index in range(5)
    ]

    entry = next(signal for signal in signals if signal.type == SignalType.SHORT)
    assert entry.stop_loss is None
    assert entry.take_profit is None


def test_context_risk_gate_blocks_only_new_entries():
    strategy = _strategy(entry_score=1)
    signals = []
    for index in range(4):
        candle = _candle(index, str(100 + index))
        context = StrategyContext(
            strategy_id=strategy.strategy_id,
            product_id=PRODUCT_ID,
            timestamp=candle.timestamp,
            available_cash=Decimal("10000"),
            total_equity=Decimal("10000"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            current_drawdown=Decimal("0"),
            max_drawdown=Decimal("0"),
            risk=RiskSnapshot(trading_enabled=False, reason="benchmark_gate"),
        )
        signals.append(strategy.on_candle(candle, context))

    assert all(signal.type == SignalType.NO_SIGNAL for signal in signals)
    assert strategy.on_candle(_candle(4, "104")).type == SignalType.LONG


def test_declares_exact_entry_risk_context_capability_immutably():
    requirements = _strategy().requirements

    assert list(StrategyContextCapability) == [StrategyContextCapability.ENTRY_RISK]
    assert requirements.required_context_capabilities == frozenset(
        {StrategyContextCapability.ENTRY_RISK}
    )
    assert all(
        type(capability) is StrategyContextCapability
        for capability in requirements.required_context_capabilities
    )
    with pytest.raises(AttributeError):
        setattr(requirements, "required_context_capabilities", frozenset())


class _ForeignCapability(StrEnum):
    ENTRY_RISK = "ENTRY_RISK"


@pytest.mark.parametrize(
    "capabilities",
    [
        frozenset({"ENTRY_RISK"}),
        frozenset({_ForeignCapability.ENTRY_RISK}),
        [StrategyContextCapability.ENTRY_RISK],
        (StrategyContextCapability.ENTRY_RISK,),
    ],
)
def test_requirements_reject_non_exact_context_capability_declarations(
    capabilities: object,
):
    with pytest.raises(
        TypeError,
        match="^required context capabilities must use StrategyContextCapability$",
    ):
        StrategyRequirements(
            PRODUCT_ID,
            TIMEFRAME,
            1,
            cast(frozenset[StrategyContextCapability], capabilities),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("trend_window", 0),
        ("swing_window", -1),
        ("entry_score", 7),
        ("max_atr_expansion", "NaN"),
        ("quantity", "-1"),
    ],
)
def test_rejects_invalid_parameters(field, value):
    with pytest.raises(ValueError):
        _strategy(**{field: value})


def test_factory_requires_complete_param_pack():
    with pytest.raises(ValueError, match="candidate param_pack missing"):
        representative_strategy_factory(
            "representative",
            PRODUCT_ID,
            TIMEFRAME,
            {"trend_window": 10},
        )
