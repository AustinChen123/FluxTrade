from decimal import Decimal

import pytest

from integration.conftest import PRODUCT_ID, make_candle_series
from src.core.adapters.simulated import SimulatedAdapter
from src.core.data_sources.memory import MemoryDataSource
from src.core.models import OrderSide, PositionSide
from src.core.orm_models import Order
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.core.strategy_context import RiskSnapshot, StrategyContext
from src.core.models import Candlestick, Signal, SignalType
from src.strategies.base import BaseStrategy, StrategyRequirements

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False


def _market_order(order_id: str, *, timestamp: int) -> Order:
    return Order(
        id=order_id,
        exchange_order_id=f"sim_{order_id}",
        strategy_id="ctx_strategy",
        product_id=PRODUCT_ID,
        exchange_id="BINANCE",
        type="market",
        side=OrderSide.BUY,
        price=None,
        trigger_price=None,
        quantity=Decimal("0.01"),
        status="open",
        timestamp=timestamp,
        filled_quantity=Decimal("0"),
        filled_price=Decimal("0"),
    )


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_strategy_context_reflects_fills_before_next_decision():
    candles = make_candle_series(count=2)
    adapter = SimulatedAdapter(
        initial_balance=Decimal("10000"),
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0006"),
    )

    adapter.place_order(_market_order("ctx_1", timestamp=candles[0].timestamp))
    pre_fill_context = adapter.get_strategy_context(
        strategy_id="ctx_strategy",
        product_id=PRODUCT_ID,
        timestamp=candles[0].timestamp,
        initial_balance=Decimal("10000"),
        mark_price=candles[0].close,
        risk=RiskSnapshot(trading_enabled=True),
    )

    assert pre_fill_context.position is None
    assert len(pre_fill_context.open_orders) == 1
    assert pre_fill_context.latest_fills == ()
    assert pre_fill_context.available_cash == Decimal("10000")

    fills = adapter.on_market_data(candles[1])
    post_fill_context = adapter.get_strategy_context(
        strategy_id="ctx_strategy",
        product_id=PRODUCT_ID,
        timestamp=candles[1].timestamp,
        initial_balance=Decimal("10000"),
        mark_price=candles[1].close,
        peak_equity=Decimal("10000"),
        latest_fills=fills,
        risk=RiskSnapshot(trading_enabled=True),
    )

    assert len(fills) == 1
    assert post_fill_context.open_orders == ()
    assert len(post_fill_context.latest_fills) == 1
    assert post_fill_context.latest_fills[0].order_id == "ctx_1"
    assert post_fill_context.position is not None
    assert post_fill_context.position.side == PositionSide.LONG
    assert post_fill_context.position.quantity == Decimal("0.01")
    assert post_fill_context.position.mark_price == candles[1].close
    assert post_fill_context.position.notional == Decimal("0.01") * candles[1].close
    assert post_fill_context.available_cash < Decimal("10000")
    assert post_fill_context.total_equity == (
        post_fill_context.available_cash + post_fill_context.unrealized_pnl
    )


class ContextProbeStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__("ctx_strategy", PRODUCT_ID)
        self.contexts: list[StrategyContext] = []

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(PRODUCT_ID, "15m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal | None:
        if context is None:
            raise AssertionError("context-aware strategy must receive context")
        self.contexts.append(context)
        if len(self.contexts) == 1:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe="15m",
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.01"),
            )
        return None


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_passes_context_after_existing_fills():
    candles = make_candle_series(count=3)
    strategy = ContextProbeStrategy()
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config={"maker": 0.0002, "taker": 0.0006},
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 1
    assert strategy.contexts[0].position is None
    assert strategy.contexts[0].latest_fills == ()
    assert strategy.contexts[1].position is not None
    assert strategy.contexts[1].position.quantity == Decimal("0.01")
    assert len(strategy.contexts[1].latest_fills) == 1
    assert strategy.contexts[1].available_cash < Decimal("10000")


class ContextDrivenExitStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__("ctx_driven_exit", PRODUCT_ID)
        self.decisions: list[str] = []
        self._entered = False
        self._exited = False

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(PRODUCT_ID, "15m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal | None:
        if context is None:
            raise AssertionError("context-aware strategy must receive context")
        if (
            not self._entered
            and context.position is None
            and context.available_cash >= Decimal("10000")
        ):
            self.decisions.append("enter")
            self._entered = True
            return Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe="15m",
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.01"),
            )
        if (
            self._entered
            and not self._exited
            and context.position is not None
            and context.position.quantity > 0
        ):
            self.decisions.append("exit")
            self._exited = True
            return Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe="15m",
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=context.position.quantity,
            )
        return None


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_strategy_can_use_context_to_change_decisions():
    candles = make_candle_series(count=4)
    strategy = ContextDrivenExitStrategy()
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config={"maker": 0.0002, "taker": 0.0006},
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert strategy.decisions == ["enter", "exit"]
    assert result["raw_trade_count"] == 2
    assert result["total_trades"] == 1
