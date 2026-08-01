from decimal import Decimal

import pytest

from integration.conftest import PRODUCT_ID, make_candle, make_candle_series
from src.core.adapters.simulated import SimulatedAdapter
from src.core.capital_allocator import CapitalAllocator
from src.core.data_sources.memory import MemoryDataSource
from src.core.models import OrderSide, PositionSide
from src.core.orm_models import Order
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.core.strategy_context import CapitalSnapshot, RiskSnapshot, StrategyContext
from src.core.models import Candlestick, Signal, SignalType
from src.strategies.base import BaseStrategy, StrategyRequirements

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False


def test_strategy_context_capital_defaults_to_none():
    context = StrategyContext(
        strategy_id="ctx_strategy",
        product_id=PRODUCT_ID,
        timestamp=1,
        available_cash=Decimal("10000"),
        total_equity=Decimal("10000"),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        current_drawdown=Decimal("0"),
        max_drawdown=Decimal("0"),
    )

    assert context.capital is None


def test_capital_snapshot_is_immutable():
    snapshot = CapitalSnapshot(
        allocated=Decimal("5000"),
        used=Decimal("1500"),
        available=Decimal("3500"),
        unallocated=Decimal("95000"),
    )

    with pytest.raises(AttributeError):
        snapshot.available = Decimal("0")  # type: ignore[misc]


def test_research_runner_rejects_capital_allocator_without_strategy_positions():
    class LegacyPositionAdapter:
        supports_strategy_positions = False

        def get_open_orders(self):
            return []

        def get_position(self, product_id: str, strategy_id: str | None = None):
            raise AssertionError("legacy product-level positions must not be used")

    candles = make_candle_series(count=1)
    strategy = SingleEntryStrategy()
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("5000"))
    allocator.set_usage(strategy.strategy_id, Decimal("1000"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[0].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    with pytest.raises(RuntimeError, match="strategy-scoped positions"):
        runner._sync_capital_usage(LegacyPositionAdapter(), candles[0])  # type: ignore[arg-type]


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


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_strategy_context_drawdown_is_loss_magnitude():
    candles = make_candle_series(count=1)
    adapter = SimulatedAdapter(initial_balance=Decimal("10000"))

    context = adapter.get_strategy_context(
        strategy_id="ctx_strategy",
        product_id=PRODUCT_ID,
        timestamp=candles[0].timestamp,
        initial_balance=Decimal("10000"),
        mark_price=candles[0].close,
        peak_equity=Decimal("11000"),
        max_drawdown=Decimal("1000"),
    )

    assert context.total_equity == Decimal("10000")
    assert context.current_drawdown == Decimal("1000")
    assert context.max_drawdown == Decimal("1000")


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_strategy_context_account_fields_follow_research_contract():
    candles = make_candle_series(count=2)
    adapter = SimulatedAdapter(
        initial_balance=Decimal("10000"),
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0006"),
    )

    empty_context = adapter.get_strategy_context(
        strategy_id="ctx_strategy",
        product_id=PRODUCT_ID,
        timestamp=candles[0].timestamp,
        initial_balance=Decimal("10000"),
        mark_price=candles[0].close,
    )

    assert empty_context.available_cash == Decimal("10000")
    assert empty_context.unrealized_pnl == Decimal("0")
    assert empty_context.total_equity == (
        empty_context.available_cash + empty_context.unrealized_pnl
    )
    assert empty_context.realized_pnl == Decimal("0")

    adapter.place_order(_market_order("ctx_account", timestamp=candles[0].timestamp))
    fills = adapter.on_market_data(candles[1])
    filled_context = adapter.get_strategy_context(
        strategy_id="ctx_strategy",
        product_id=PRODUCT_ID,
        timestamp=candles[1].timestamp,
        initial_balance=Decimal("10000"),
        mark_price=candles[1].close,
        latest_fills=fills,
    )

    assert filled_context.available_cash < Decimal("10000")
    assert filled_context.total_equity == (
        filled_context.available_cash + filled_context.unrealized_pnl
    )
    assert filled_context.realized_pnl == (
        filled_context.total_equity - Decimal("10000")
    )


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_strategy_context_exposes_capital_snapshot_without_redefining_cash():
    candles = make_candle_series(count=1)
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate("ctx_strategy", Decimal("5000"))
    allocator.record_usage("ctx_strategy", Decimal("1500"))
    adapter = SimulatedAdapter(initial_balance=Decimal("10000"))

    context = adapter.get_strategy_context(
        strategy_id="ctx_strategy",
        product_id=PRODUCT_ID,
        timestamp=candles[0].timestamp,
        initial_balance=Decimal("10000"),
        mark_price=candles[0].close,
        capital_allocator=allocator,
    )

    assert context.available_cash == Decimal("10000")
    assert context.capital is not None
    assert context.capital.allocated == Decimal("5000")
    assert context.capital.used == Decimal("1500")
    assert context.capital.available == Decimal("3500")
    assert context.capital.unallocated == Decimal("95000")


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


class SingleEntryStrategy(BaseStrategy):
    def __init__(self, quantity: Decimal = Decimal("0.01")) -> None:
        super().__init__("single_entry", PRODUCT_ID)
        self.quantity = quantity
        self.contexts: list[StrategyContext] = []
        self._sent = False

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(PRODUCT_ID, "15m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal | None:
        if context is not None:
            self.contexts.append(context)
        if self._sent:
            return None
        self._sent = True
        return Signal(
            strategy_id=self.strategy_id,
            product_id=PRODUCT_ID,
            timeframe="15m",
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            quantity=self.quantity,
        )


class RejectedEntryThenExitStrategy(BaseStrategy):
    def __init__(self, quantity: Decimal = Decimal("0.01")) -> None:
        super().__init__("rejected_entry_then_exit", PRODUCT_ID)
        self.quantity = quantity
        self._index = 0

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(PRODUCT_ID, "15m", 1)

    def on_candle(self, candle: Candlestick) -> Signal | None:
        self._index += 1
        if self._index == 1:
            signal_type = SignalType.LONG
        elif self._index == 2:
            signal_type = SignalType.EXIT_LONG
        else:
            return None
        return Signal(
            strategy_id=self.strategy_id,
            product_id=PRODUCT_ID,
            timeframe="15m",
            timestamp=candle.timestamp,
            type=signal_type,
            quantity=self.quantity,
        )


class MultiEntryStrategy(BaseStrategy):
    def __init__(self, quantity: Decimal = Decimal("0.006")) -> None:
        super().__init__("multi_entry", PRODUCT_ID)
        self.quantity = quantity
        self.contexts: list[StrategyContext] = []
        self._sent = False

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(PRODUCT_ID, "15m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> list[Signal] | None:
        if context is not None:
            self.contexts.append(context)
        if self._sent:
            return None
        self._sent = True
        return [
            Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe="15m",
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=self.quantity,
            ),
            Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe="15m",
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=self.quantity,
            ),
        ]


class PendingLimitThenEntryStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__("pending_limit_then_entry", PRODUCT_ID)
        self.contexts: list[StrategyContext] = []
        self._count = 0

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(PRODUCT_ID, "15m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal | None:
        if context is not None:
            self.contexts.append(context)
        self._count += 1
        if self._count == 1:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe="15m",
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.006"),
                price=Decimal("49000"),
            )
        if self._count == 2:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe="15m",
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.006"),
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
    assert strategy.contexts[0].current_drawdown == Decimal("0")
    assert strategy.contexts[0].max_drawdown == Decimal("0")
    assert strategy.contexts[1].position is not None
    assert strategy.contexts[1].position.quantity == Decimal("0.01")
    assert len(strategy.contexts[1].latest_fills) == 1
    assert strategy.contexts[1].available_cash < Decimal("10000")
    assert strategy.contexts[1].current_drawdown >= Decimal("0")
    assert strategy.contexts[1].max_drawdown >= strategy.contexts[1].current_drawdown
    assert strategy.contexts[2].max_drawdown >= strategy.contexts[1].max_drawdown


class ContextDrivenExitStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__("ctx_driven_exit", PRODUCT_ID)
        self.decisions: list[str] = []
        self.contexts: list[StrategyContext] = []
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
        self.contexts.append(context)
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


class ContextDrawdownRoundTripStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__("ctx_drawdown_round_trip", PRODUCT_ID)
        self.contexts: list[StrategyContext] = []
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
        self.contexts.append(context)
        if not self._entered:
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
            not self._exited
            and context.position is not None
            and candle.close >= Decimal("105")
        ):
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


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_reports_bar_level_drawdown_for_open_positions():
    base_ts = 1_700_000_000_000
    interval_ms = 15 * 60 * 1000
    candles = [
        make_candle(base_ts, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")),
        make_candle(base_ts + interval_ms, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")),
        make_candle(base_ts + 2 * interval_ms, Decimal("60"), Decimal("60"), Decimal("60"), Decimal("60")),
        make_candle(base_ts + 3 * interval_ms, Decimal("105"), Decimal("105"), Decimal("105"), Decimal("105")),
        make_candle(base_ts + 4 * interval_ms, Decimal("105"), Decimal("105"), Decimal("105"), Decimal("105")),
    ]
    strategy = ContextDrawdownRoundTripStrategy()
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 2
    assert result["total_pnl"] > Decimal("0")
    assert result["max_drawdown"] == Decimal("0.40")
    assert max(context.max_drawdown for context in strategy.contexts) == Decimal("0.40")


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
@pytest.mark.parametrize("include_unprocessed_candle", [False, True])
def test_research_runner_reports_triggered_drawdown_halt_policy(
    include_unprocessed_candle,
):
    base_ts = 1_700_000_000_000
    interval_ms = 15 * 60 * 1000
    candles = [
        make_candle(base_ts, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")),
        make_candle(base_ts + interval_ms, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")),
        make_candle(base_ts + 2 * interval_ms, Decimal("60"), Decimal("60"), Decimal("60"), Decimal("60")),
    ]
    if include_unprocessed_candle:
        candles.append(
            make_candle(
                base_ts + 3 * interval_ms,
                Decimal("105"),
                Decimal("105"),
                Decimal("105"),
                Decimal("105"),
            )
        )
    strategy = SingleEntryStrategy(quantity=Decimal("1"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        max_drawdown_limit=0.004,
        balance_check_interval=1,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["candle_count"] == 3
    assert result["raw_trade_count"] == 1
    assert result["max_drawdown"] == Decimal("40")
    assert len(strategy.contexts) == 3
    assert strategy.contexts[-1].current_drawdown == Decimal("40")
    endpoint_state = result["endpoint_state"]
    assert endpoint_state.halted_early is True
    assert endpoint_state.final_mark == candles[2].close
    assert endpoint_state.end_timestamp == candles[2].timestamp
    assert len(endpoint_state.positions) == 1
    endpoint_position = endpoint_state.positions[0]
    assert endpoint_position.strategy_id == strategy.strategy_id
    assert endpoint_position.product_id == PRODUCT_ID
    assert endpoint_position.side == PositionSide.LONG
    assert endpoint_position.quantity == Decimal("1")
    assert endpoint_position.average_entry_price == Decimal("100")


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_passes_capital_snapshot_to_context_strategy():
    candles = make_candle_series(count=2)
    strategy = ContextProbeStrategy()
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("5000"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    runner.run()

    assert strategy.contexts
    assert strategy.contexts[0].available_cash == Decimal("10000")
    assert strategy.contexts[0].capital is not None
    assert strategy.contexts[0].capital.allocated == Decimal("5000")
    assert strategy.contexts[0].capital.used == Decimal("0")
    assert strategy.contexts[0].capital.available == Decimal("5000")


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_syncs_capital_usage_from_positions():
    candles = make_candle_series(count=4)
    strategy = ContextDrivenExitStrategy()
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("5000"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 2
    assert strategy.decisions == ["enter", "exit"]
    assert strategy.contexts[1].capital is not None
    assert strategy.contexts[1].capital.used == Decimal("0.01") * candles[1].close
    assert strategy.contexts[1].capital.available == (
        Decimal("5000") - strategy.contexts[1].capital.used
    )
    assert strategy.contexts[2].capital is not None
    assert strategy.contexts[2].capital.used == Decimal("0")
    assert strategy.contexts[2].capital.available == Decimal("5000")
    assert allocator.get_used(strategy.strategy_id) == Decimal("0")


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_rejects_entry_when_capital_is_insufficient():
    candles = make_candle_series(count=3)
    strategy = SingleEntryStrategy(quantity=Decimal("0.01"))
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("100"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 0
    assert allocator.get_used(strategy.strategy_id) == Decimal("0")
    assert strategy.contexts[0].capital is not None
    assert strategy.contexts[0].capital.available == Decimal("100")
    assert strategy.contexts[0].latest_rejections == ()
    assert len(strategy.contexts[1].latest_rejections) == 1
    assert "capital_allocation_rejected" in strategy.contexts[1].latest_rejections[0].reason
    assert "required=" in strategy.contexts[1].latest_rejections[0].reason
    assert "available=100" in strategy.contexts[1].latest_rejections[0].reason
    assert strategy.contexts[2].latest_rejections == ()


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_skips_exit_when_rejected_entry_left_no_position():
    candles = make_candle_series(count=3)
    strategy = RejectedEntryThenExitStrategy(quantity=Decimal("0.01"))
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("100"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 0
    assert allocator.get_used(strategy.strategy_id) == Decimal("0")


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_allows_entry_when_capital_is_sufficient():
    candles = make_candle_series(count=3)
    strategy = SingleEntryStrategy(quantity=Decimal("0.01"))
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("1000"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 1
    assert allocator.get_used(strategy.strategy_id) == Decimal("0.01") * candles[-1].close


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_reserves_capital_between_same_candle_entries():
    candles = make_candle_series(count=3)
    strategy = MultiEntryStrategy(quantity=Decimal("0.006"))
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("500"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 1
    assert allocator.get_used(strategy.strategy_id) == Decimal("0.006") * candles[-1].close
    assert len(strategy.contexts[1].latest_rejections) == 1
    assert "capital_allocation_rejected" in strategy.contexts[1].latest_rejections[0].reason


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_reserves_capital_for_pending_entry_orders():
    candles = make_candle_series(count=3)
    strategy = PendingLimitThenEntryStrategy()
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("500"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 0
    assert allocator.get_used(strategy.strategy_id) == Decimal("0.006") * Decimal("49000")
    assert strategy.contexts[1].capital is not None
    assert strategy.contexts[1].capital.available == (
        Decimal("500") - Decimal("0.006") * Decimal("49000")
    )
    assert strategy.contexts[1].latest_rejections == ()
    assert len(strategy.contexts[2].latest_rejections) == 1
    assert "capital_allocation_rejected" in strategy.contexts[2].latest_rejections[0].reason


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_runner_allows_exit_when_capital_is_exhausted():
    candles = make_candle_series(count=4)
    strategy = ContextDrivenExitStrategy()
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("501"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe="15m",
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert strategy.decisions == ["enter", "exit"]
    assert result["raw_trade_count"] == 2
    assert strategy.contexts[1].capital is not None
    assert strategy.contexts[1].capital.available < Decimal("0")
    assert allocator.get_used(strategy.strategy_id) == Decimal("0")
