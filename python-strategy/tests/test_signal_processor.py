"""Tests for src/core/signal_processor.py."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, sentinel

import pytest

from src.core.models import Candlestick, OrderSide, Signal, SignalType, Trade
from src.core.portfolio_runtime import (
    PortfolioCoordinator,
    PortfolioDecisionRejected,
    PortfolioDefinition,
    PortfolioExclusiveSlot,
    PortfolioExposureSnapshot,
    PortfolioSleeve,
)
from src.core.signal_processor import SignalObserverError, SignalProcessor
from src.core.strategy_context import StrategyContext
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy, StrategyRequirements


class DummyStrategy(BaseStrategy):
    def __init__(
        self,
        strategy_id: str,
        product_id: str = "BINANCE:BTCUSDT-PERP",
        timeframe: str = "1m",
        result: Signal | list[Signal] | None = None,
        trade_result: Signal | None = None,
        should_raise: bool = False,
    ) -> None:
        super().__init__(strategy_id, product_id)
        self._timeframe = timeframe
        self.result = result
        self.trade_result = trade_result
        self.should_raise = should_raise
        self.candles_received: list[Candlestick] = []
        self.trades_received: list[Trade] = []

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, self._timeframe, 10)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal | list[Signal] | None:
        self.candles_received.append(candle)
        if self.should_raise:
            raise RuntimeError("strategy failed")
        return self.result

    def on_trade(self, trade: Trade) -> Signal | None:
        self.trades_received.append(trade)
        if self.should_raise:
            raise RuntimeError("strategy failed")
        return self.trade_result

    def snapshot_walk_forward_trade_state(self) -> object:
        return None

    def restore_walk_forward_trade_state(self, state: object) -> None:
        assert state is None


class StatefulEntryStrategy(DummyStrategy):
    def __init__(self, strategy_id: str) -> None:
        super().__init__(strategy_id)
        self._in_position = False
        self.restore_calls = 0

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        self.candles_received.append(candle)
        signal_type = SignalType.NO_SIGNAL
        if not self._in_position:
            self._in_position = True
            signal_type = SignalType.LONG
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=self.requirements.timeframe,
            timestamp=candle.timestamp,
            type=signal_type,
            quantity=Decimal("1"),
        )

    def snapshot_walk_forward_trade_state(self) -> object:
        return self._in_position

    def restore_walk_forward_trade_state(self, state: object) -> None:
        assert isinstance(state, bool)
        self.restore_calls += 1
        self._in_position = state


class RestoreFailingEntryStrategy(StatefulEntryStrategy):
    def restore_walk_forward_trade_state(self, state: object) -> None:
        self.restore_calls += 1
        raise RuntimeError("restore failed")


class DummyStateManager:
    def __init__(self, running: set[str]):
        self.running = running

    def is_running(self, strategy_id: str) -> bool:
        return strategy_id in self.running


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


def make_trade(
    product_id: str = "BINANCE:BTCUSDT-PERP",
) -> Trade:
    return Trade(
        id="t1",
        product_id=product_id,
        price=Decimal("42200"),
        quantity=Decimal("0.1"),
        side=OrderSide.BUY,
        timestamp=1704067200000,
    )


def make_signal(
    strategy_id: str = "s1",
    signal_type: SignalType = SignalType.LONG,
) -> Signal:
    return Signal(
        strategy_id=strategy_id,
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=1704067200000,
        type=signal_type,
        value=Decimal("42200"),
    )


def test_on_candle_routes_matching_strategy() -> None:
    signal = make_signal()
    strategy = DummyStrategy("s1", result=signal)
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    processor = SignalProcessor(registry, execution)
    candle = make_candle()

    processor.on_candle(candle)

    assert strategy.candles_received == [candle]
    execution.execute_signal.assert_called_once_with(signal, candle)


def test_on_candle_coordinates_portfolio_before_emitting_any_signal() -> None:
    strategy_a = DummyStrategy(
        "sleeve_a",
        result=make_signal("sleeve_a", SignalType.LONG),
    )
    strategy_b = DummyStrategy(
        "sleeve_b",
        result=make_signal("sleeve_b", SignalType.SHORT),
    )
    registry = StrategyRegistry()
    registry.register(strategy_a)
    registry.register(strategy_b)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy_a.product_id,
            sleeves=(
                PortfolioSleeve(strategy_a),
                PortfolioSleeve(strategy_b),
            ),
            max_gross_quantity=Decimal("2"),
        )
    )
    execution = MagicMock()
    execution.default_quantity = Decimal("1")
    handler = MagicMock()
    processor = SignalProcessor(
        registry,
        execution,
        signal_handler=handler,
        position_loader=lambda *_args: None,
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
    )

    with pytest.raises(PortfolioDecisionRejected, match="opposing_exposure"):
        processor.on_candle(make_candle())

    handler.assert_not_called()


def test_on_candle_emits_only_selected_exclusive_slot_owner() -> None:
    strategy_b = DummyStrategy(
        "sleeve_b",
        result=make_signal("sleeve_b", SignalType.SHORT),
    )
    strategy_a = DummyStrategy(
        "sleeve_a",
        result=make_signal("sleeve_a", SignalType.LONG),
    )
    registry = StrategyRegistry()
    registry.register(strategy_b)
    registry.register(strategy_a)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy_a.product_id,
            sleeves=(
                PortfolioSleeve(strategy_b),
                PortfolioSleeve(strategy_a),
            ),
            max_gross_quantity=Decimal("1"),
            exclusive_slots=(
                PortfolioExclusiveSlot(
                    slot_id="shared",
                    strategy_ids=("sleeve_a", "sleeve_b"),
                ),
            ),
        )
    )
    execution = MagicMock()
    execution.default_quantity = Decimal("1")
    handler = MagicMock(return_value=True)
    observed: list[tuple[Signal, ...]] = []

    SignalProcessor(
        registry,
        execution,
        signal_handler=handler,
        position_loader=lambda *_args: None,
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
        signal_batch_observer=observed.append,
    ).on_candle(make_candle())

    handler.assert_called_once()
    emitted_signal, emitted_candle = handler.call_args.args
    assert emitted_signal.strategy_id == "sleeve_a"
    assert emitted_signal.type == SignalType.LONG
    assert emitted_candle == make_candle()
    assert len(observed) == 1
    assert observed[0] == (emitted_signal,)
    assert observed[0][0] is emitted_signal


def test_exclusive_slot_restores_suppressed_stateful_sleeve() -> None:
    strategy_b = StatefulEntryStrategy("sleeve_b")
    strategy_a = StatefulEntryStrategy("sleeve_a")
    registry = StrategyRegistry()
    registry.register(strategy_b)
    registry.register(strategy_a)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy_a.product_id,
            sleeves=(
                PortfolioSleeve(strategy_b),
                PortfolioSleeve(strategy_a),
            ),
            max_gross_quantity=Decimal("1"),
            exclusive_slots=(
                PortfolioExclusiveSlot(
                    slot_id="shared",
                    strategy_ids=("sleeve_a", "sleeve_b"),
                ),
            ),
        )
    )
    execution = MagicMock()
    execution.default_quantity = Decimal("1")
    handler = MagicMock(return_value=True)
    processor = SignalProcessor(
        registry,
        execution,
        signal_handler=handler,
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
    )

    processor.on_candle(make_candle())

    assert strategy_a._in_position is True
    assert strategy_b._in_position is False
    strategy_a._in_position = True
    second_candle = make_candle().model_copy(
        update={"timestamp": make_candle().timestamp + 60_000}
    )
    processor.on_candle(second_candle)

    assert strategy_b._in_position is True
    assert [call.args[0].strategy_id for call in handler.call_args_list] == [
        "sleeve_a",
        "sleeve_b",
    ]


def test_exclusive_slot_restores_state_when_coordination_rejects() -> None:
    strategy_a = StatefulEntryStrategy("sleeve_a")
    strategy_b = StatefulEntryStrategy("sleeve_b")
    registry = StrategyRegistry()
    registry.register(strategy_a)
    registry.register(strategy_b)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy_a.product_id,
            sleeves=(
                PortfolioSleeve(strategy_a),
                PortfolioSleeve(strategy_b),
            ),
            max_gross_quantity=Decimal("0.5"),
            exclusive_slots=(
                PortfolioExclusiveSlot(
                    slot_id="shared",
                    strategy_ids=("sleeve_a", "sleeve_b"),
                ),
            ),
        )
    )
    execution = MagicMock()
    execution.default_quantity = Decimal("1")
    processor = SignalProcessor(
        registry,
        execution,
        signal_handler=MagicMock(return_value=True),
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
    )

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_gross_limit_exceeded",
    ):
        processor.on_candle(make_candle())

    assert strategy_a._in_position is False
    assert strategy_b._in_position is False


def test_exclusive_slot_restore_failure_prevents_signal_emission() -> None:
    strategy_a = StatefulEntryStrategy("sleeve_a")
    strategy_b = RestoreFailingEntryStrategy("sleeve_b")
    registry = StrategyRegistry()
    registry.register(strategy_a)
    registry.register(strategy_b)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy_a.product_id,
            sleeves=(
                PortfolioSleeve(strategy_a),
                PortfolioSleeve(strategy_b),
            ),
            max_gross_quantity=Decimal("1"),
            exclusive_slots=(
                PortfolioExclusiveSlot(
                    slot_id="shared",
                    strategy_ids=("sleeve_a", "sleeve_b"),
                ),
            ),
        )
    )
    execution = MagicMock()
    execution.default_quantity = Decimal("1")
    handler = MagicMock(return_value=True)
    processor = SignalProcessor(
        registry,
        execution,
        signal_handler=handler,
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
    )

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_decision_trade_state_restore_failed",
    ):
        processor.on_candle(make_candle())

    handler.assert_not_called()
    assert strategy_a.restore_calls == 1
    assert strategy_a._in_position is False
    assert strategy_b.restore_calls >= 1


def test_portfolio_sleeves_share_parent_lifecycle_state() -> None:
    strategy = DummyStrategy("sleeve_a", result=make_signal("sleeve_a"))
    registry = StrategyRegistry()
    registry.register(strategy)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy.product_id,
            sleeves=(PortfolioSleeve(strategy),),
            max_gross_quantity=Decimal("1"),
        )
    )
    execution = MagicMock()
    execution.default_quantity = Decimal("1")
    state_manager = DummyStateManager({"portfolio_v1"})
    handler = MagicMock()

    SignalProcessor(
        registry,
        execution,
        state_manager,
        signal_handler=handler,
        position_loader=lambda *_args: None,
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
    ).on_candle(make_candle())

    handler.assert_called_once()


def test_portfolio_submission_rejection_stops_remaining_sleeves() -> None:
    strategy_a = DummyStrategy(
        "sleeve_a",
        result=make_signal("sleeve_a", SignalType.LONG),
    )
    strategy_b = DummyStrategy(
        "sleeve_b",
        result=make_signal("sleeve_b", SignalType.LONG),
    )
    registry = StrategyRegistry()
    registry.register(strategy_a)
    registry.register(strategy_b)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy_a.product_id,
            sleeves=(
                PortfolioSleeve(strategy_a),
                PortfolioSleeve(strategy_b),
            ),
            max_gross_quantity=Decimal("2"),
        )
    )
    execution = MagicMock(default_quantity=Decimal("1"))
    handler = MagicMock(side_effect=[False, True])
    admission = MagicMock(return_value=True)
    processor = SignalProcessor(
        registry,
        execution,
        signal_handler=handler,
        position_loader=lambda *_args: None,
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
        entry_admission_handler=admission,
    )

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_submission_rejected:strategy_id=sleeve_a",
    ):
        processor.on_candle(make_candle())

    assert admission.call_count == 2
    handler.assert_called_once()


def test_portfolio_admission_checks_complete_batch_before_submission() -> None:
    strategies = tuple(
        DummyStrategy(
            f"sleeve_{index}",
            result=make_signal(f"sleeve_{index}", SignalType.LONG),
        )
        for index in range(2)
    )
    registry = StrategyRegistry()
    for strategy in strategies:
        registry.register(strategy)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategies[0].product_id,
            sleeves=tuple(PortfolioSleeve(strategy) for strategy in strategies),
            max_gross_quantity=Decimal("2"),
        )
    )
    handler = MagicMock(return_value=True)
    admission = MagicMock(side_effect=[True, False])

    SignalProcessor(
        registry,
        MagicMock(default_quantity=Decimal("1")),
        signal_handler=handler,
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
        entry_admission_handler=admission,
    ).on_candle(make_candle())

    assert [call.args[0].strategy_id for call in admission.call_args_list] == [
        "sleeve_0",
        "sleeve_1",
    ]
    handler.assert_not_called()


def test_entry_admission_suppression_preserves_exit_and_observer_batch() -> None:
    entry = DummyStrategy(
        "entry",
        result=make_signal("entry", SignalType.LONG),
    )
    exiting = DummyStrategy(
        "exit",
        result=make_signal("exit", SignalType.EXIT_LONG),
    )
    idle = DummyStrategy(
        "idle",
        result=make_signal("idle", SignalType.NO_SIGNAL),
    )
    registry = StrategyRegistry()
    registry.register(entry)
    registry.register(idle)
    registry.register(exiting)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=entry.product_id,
            sleeves=(
                PortfolioSleeve(entry),
                PortfolioSleeve(exiting),
            ),
            max_gross_quantity=Decimal("1"),
        )
    )
    handler = MagicMock(return_value=True)
    observer = MagicMock()

    SignalProcessor(
        registry,
        MagicMock(default_quantity=Decimal("1")),
        signal_handler=handler,
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
        signal_batch_observer=observer,
        entry_admission_handler=MagicMock(return_value=False),
    ).on_candle(make_candle())

    handler.assert_called_once()
    processed = handler.call_args.args[0]
    assert processed.type == SignalType.EXIT_LONG
    observer.assert_called_once()
    observed = observer.call_args.args[0]
    assert tuple(signal.type for signal in observed) == (
        SignalType.EXIT_LONG,
        SignalType.NO_SIGNAL,
    )
    assert observed[0] is processed


@pytest.mark.parametrize("portfolio_owned", [False, True])
def test_entry_admission_suppression_restores_nonexclusive_trade_state(
    portfolio_owned: bool,
) -> None:
    strategy = StatefulEntryStrategy("sleeve")
    registry = StrategyRegistry()
    registry.register(strategy)
    coordinator = None
    exposure_loader = None
    if portfolio_owned:

        def load_exposure(*_args: object) -> PortfolioExposureSnapshot:
            return PortfolioExposureSnapshot({})

        coordinator = PortfolioCoordinator()
        coordinator.register(
            PortfolioDefinition(
                portfolio_id="portfolio_v1",
                product_id=strategy.product_id,
                sleeves=(PortfolioSleeve(strategy),),
                max_gross_quantity=Decimal("1"),
            )
        )
        exposure_loader = load_exposure
    handler = MagicMock(return_value=True)
    admission = MagicMock(side_effect=[False, True])
    processor = SignalProcessor(
        registry,
        MagicMock(default_quantity=Decimal("1")),
        signal_handler=handler,
        exposure_loader=exposure_loader,
        portfolio_coordinator=coordinator,
        entry_admission_handler=admission,
    )

    processor.on_candle(make_candle())

    assert strategy._in_position is False
    assert strategy.restore_calls == 1
    handler.assert_not_called()

    processor.on_candle(
        make_candle().model_copy(update={"timestamp": make_candle().timestamp + 60_000})
    )

    handler.assert_called_once()
    assert handler.call_args.args[0].type == SignalType.LONG
    assert admission.call_count == 2


def test_admission_error_propagates_and_rolls_back_exclusive_slot_state() -> None:
    strategy_a = StatefulEntryStrategy("sleeve_a")
    strategy_b = StatefulEntryStrategy("sleeve_b")
    registry = StrategyRegistry()
    registry.register(strategy_a)
    registry.register(strategy_b)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy_a.product_id,
            sleeves=(PortfolioSleeve(strategy_a), PortfolioSleeve(strategy_b)),
            max_gross_quantity=Decimal("1"),
            exclusive_slots=(
                PortfolioExclusiveSlot(
                    slot_id="shared",
                    strategy_ids=("sleeve_a", "sleeve_b"),
                ),
            ),
        )
    )
    cause = RuntimeError("admission unavailable")
    handler = MagicMock()

    def fail_admission(_signal: Signal) -> bool:
        raise cause

    with pytest.raises(RuntimeError, match="admission unavailable") as caught:
        SignalProcessor(
            registry,
            MagicMock(default_quantity=Decimal("1")),
            signal_handler=handler,
            exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
            portfolio_coordinator=coordinator,
            entry_admission_handler=fail_admission,
        ).on_candle(make_candle())

    assert caught.value is cause
    assert strategy_a._in_position is False
    assert strategy_b._in_position is False
    handler.assert_not_called()


def test_portfolio_crash_replay_does_not_double_count_persisted_intent() -> None:
    strategy_a = DummyStrategy(
        "sleeve_a",
        result=make_signal("sleeve_a", SignalType.LONG),
    )
    strategy_b = DummyStrategy(
        "sleeve_b",
        result=make_signal("sleeve_b", SignalType.LONG),
    )
    registry = StrategyRegistry()
    registry.register(strategy_a)
    registry.register(strategy_b)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy_a.product_id,
            sleeves=(
                PortfolioSleeve(strategy_a),
                PortfolioSleeve(strategy_b),
            ),
            max_gross_quantity=Decimal("2"),
        )
    )
    execution = MagicMock(default_quantity=Decimal("1"))
    handler = MagicMock(return_value=True)
    observed_requested_intents = {}

    def load_exposure(_strategy_ids, _product_id, requested_intents):
        nonlocal observed_requested_intents
        observed_requested_intents = requested_intents
        client_order_ids = tuple(requested_intents)
        return PortfolioExposureSnapshot(
            quantities={"sleeve_a": Decimal("1")},
            existing_client_order_ids=frozenset({client_order_ids[0]}),
        )

    SignalProcessor(
        registry,
        execution,
        signal_handler=handler,
        position_loader=lambda *_args: None,
        exposure_loader=load_exposure,
        portfolio_coordinator=coordinator,
    ).on_candle(make_candle())

    assert set(observed_requested_intents.values()) == {
        "sleeve_a",
        "sleeve_b",
    }
    assert handler.call_count == 2
    assert {
        call.args[0].metadata["client_order_id"] for call in handler.call_args_list
    } == set(observed_requested_intents)


@pytest.mark.parametrize(
    ("signal_types", "max_gross_quantity"),
    [
        ((SignalType.LONG, SignalType.SHORT), Decimal("2")),
        ((SignalType.LONG, SignalType.LONG), Decimal("1")),
    ],
)
def test_portfolio_trade_signals_fail_before_any_submission(
    signal_types,
    max_gross_quantity,
) -> None:
    strategies = tuple(
        DummyStrategy(
            f"sleeve_{index}",
            trade_result=make_signal(
                f"sleeve_{index}",
                signal_type,
            ),
        )
        for index, signal_type in enumerate(signal_types)
    )
    on_trade_mocks: list[MagicMock] = []
    for strategy in strategies:
        on_trade = MagicMock(return_value=strategy.trade_result)
        object.__setattr__(strategy, "on_trade", on_trade)
        on_trade_mocks.append(on_trade)
    registry = StrategyRegistry()
    for strategy in strategies:
        registry.register(strategy)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategies[0].product_id,
            sleeves=tuple(PortfolioSleeve(strategy) for strategy in strategies),
            max_gross_quantity=max_gross_quantity,
        )
    )
    handler = MagicMock()
    processor = SignalProcessor(
        registry,
        MagicMock(default_quantity=Decimal("1")),
        signal_handler=handler,
        position_loader=lambda *_args: None,
        exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
        portfolio_coordinator=coordinator,
    )

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_trade_signals_unsupported",
    ):
        processor.on_trade(make_trade())

    handler.assert_not_called()
    for on_trade in on_trade_mocks:
        on_trade.assert_not_called()


def test_warm_up_routes_target_strategy_without_emitting_signals() -> None:
    signal = make_signal()
    strategy = DummyStrategy("s1", result=signal)
    other = DummyStrategy("s2", result=make_signal("s2"))
    registry = StrategyRegistry()
    registry.register(strategy)
    registry.register(other)
    execution = MagicMock()
    state_manager = DummyStateManager(running=set())
    processor = SignalProcessor(registry, execution, state_manager)
    candle = make_candle()

    processor.warm_up(strategy, [candle])

    assert strategy.candles_received == [candle]
    assert other.candles_received == []
    execution.execute_signal.assert_not_called()


def test_warm_up_restores_trade_state_after_dropped_signals() -> None:
    class StatefulStrategy(DummyStrategy):
        def __init__(self) -> None:
            super().__init__("s1")
            self.position = 0
            self._in_position = False

        def on_candle(
            self,
            candle: Candlestick,
            context: StrategyContext | None = None,
        ) -> Signal:
            self.candles_received.append(candle)
            self.position = 1
            self._in_position = True
            return make_signal("s1", SignalType.LONG)

    strategy = StatefulStrategy()
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    processor = SignalProcessor(registry, execution)

    processor.warm_up(strategy, [make_candle()])

    assert strategy.candles_received == [make_candle()]
    assert strategy.position == 0
    assert strategy._in_position is False
    execution.execute_signal.assert_not_called()


def test_warm_up_failure_restores_trade_state_and_propagates() -> None:
    class FailingWarmupStrategy(DummyStrategy):
        def __init__(self) -> None:
            super().__init__("s1")
            self.position = 0
            self._in_position = False

        def on_candle(
            self,
            candle: Candlestick,
            context: StrategyContext | None = None,
        ) -> Signal:
            self.candles_received.append(candle)
            self.position = 1
            self._in_position = True
            raise RuntimeError("warm-up replay failed")

    strategy = FailingWarmupStrategy()
    processor = SignalProcessor(StrategyRegistry(), MagicMock())

    try:
        processor.warm_up(strategy, [make_candle()])
    except RuntimeError as exc:
        assert str(exc) == "warm-up replay failed"
    else:
        raise AssertionError("warm-up failure should propagate")

    assert strategy.position == 0
    assert strategy._in_position is False


def test_set_position_state_maps_common_strategy_flags() -> None:
    class StatefulStrategy(DummyStrategy):
        def __init__(self):
            super().__init__("s1")
            self.position = 99
            self._in_position = True

    processor = SignalProcessor(StrategyRegistry(), MagicMock())

    cases = [
        (None, 0, False),
        ("LONG", 1, True),
        ("SHORT", -1, True),
    ]
    for position_side, expected_position, expected_in_position in cases:
        strategy = StatefulStrategy()

        processor.set_position_state(strategy, position_side)

        assert strategy.position == expected_position
        assert strategy._in_position is expected_in_position


def test_set_position_state_prefers_explicit_strategy_hook() -> None:
    class HookStrategy(DummyStrategy):
        def __init__(self):
            super().__init__("s1")
            self.position = 99
            self.synced_side = None

        def sync_position_state(self, position_side: str | None) -> bool:
            self.synced_side = position_side
            return True

    strategy = HookStrategy()
    processor = SignalProcessor(StrategyRegistry(), MagicMock())

    applied = processor.set_position_state(strategy, "LONG")

    assert applied is True
    assert strategy.synced_side == "LONG"
    assert strategy.position == 99


def test_on_candle_syncs_strategy_only_when_authoritative_side_changes() -> None:
    class HookStrategy(DummyStrategy):
        def __init__(self):
            super().__init__("s1")
            self.synced_sides: list[str | None] = []

        def sync_position_state(self, position_side: str | None) -> bool:
            self.synced_sides.append(position_side)
            return True

    strategy = HookStrategy()
    registry = StrategyRegistry()
    registry.register(strategy)
    position = MagicMock(side="LONG")
    current_position: list[MagicMock | None] = [position]
    processor = SignalProcessor(
        registry,
        MagicMock(),
        position_loader=lambda _strategy_id, _product_id: current_position[0],
    )

    processor.on_candle(make_candle())
    processor.on_candle(make_candle())
    current_position[0] = None
    processor.on_candle(make_candle())

    assert strategy.synced_sides == ["LONG", None]
    assert len(strategy.candles_received) == 3


# ---------------------------------------------------------------------------
# Parametrized decision-table for set_position_state
# ---------------------------------------------------------------------------

_SENTINEL = sentinel.untouched


class _HookTrueStrategy(BaseStrategy):
    """Hook always returns True; exposes attrs that would be mutated by fallback."""

    def __init__(self):
        super().__init__("hook_true", "BINANCE:BTCUSDT-PERP")
        self._in_position = _SENTINEL

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> None:
        return None

    def sync_position_state(self, position_side: str | None) -> bool:
        return True


class _HookFalseStrategy(BaseStrategy):
    """Hook always returns False; _in_position sentinel must be untouched."""

    def __init__(self):
        super().__init__("hook_false", "BINANCE:BTCUSDT-PERP")
        self._in_position = _SENTINEL

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> None:
        return None

    def sync_position_state(self, position_side: str | None) -> bool:
        return False


class _HookNoneFullStrategy(BaseStrategy):
    """Hook returns None (inherits default) + has both position and _in_position."""

    def __init__(self):
        super().__init__("hook_none_full", "BINANCE:BTCUSDT-PERP")
        self.position = 99
        self._in_position = _SENTINEL

    # sync_position_state NOT overridden → BaseStrategy.sync_position_state → None

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> None:
        return None


class _InPositionOnlyStrategy(BaseStrategy):
    """No ``position`` attr; only ``_in_position``. Cannot represent direction."""

    def __init__(self):
        super().__init__("in_pos_only", "BINANCE:BTCUSDT-PERP")
        self._in_position = _SENTINEL

    # sync_position_state NOT overridden → None

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> None:
        return None


class _NoAttrsStrategy(BaseStrategy):
    """No state attrs at all; hook returns None."""

    def __init__(self):
        super().__init__("no_attrs", "BINANCE:BTCUSDT-PERP")

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> None:
        return None


@pytest.mark.parametrize(
    "strategy_factory, side, expected_result, checks",
    [
        # hook returns True → result True, fallback attrs untouched
        pytest.param(
            _HookTrueStrategy,
            "LONG",
            True,
            {"_in_position": _SENTINEL},  # fallback must NOT have run
            id="hook_returns_true",
        ),
        # hook returns False → result False, fallback attrs untouched (P1 regression)
        pytest.param(
            _HookFalseStrategy,
            "LONG",
            False,
            {"_in_position": _SENTINEL},  # fallback must NOT have run
            id="hook_returns_false_attrs_untouched",
        ),
        # hook returns None (base default) + both attrs + LONG → True, position=1
        pytest.param(
            _HookNoneFullStrategy,
            "LONG",
            True,
            {"position": 1, "_in_position": True},
            id="hook_none_full_attrs_LONG",
        ),
        # hook returns None + both attrs + SHORT → True, position=-1
        pytest.param(
            _HookNoneFullStrategy,
            "SHORT",
            True,
            {"position": -1, "_in_position": True},
            id="hook_none_full_attrs_SHORT",
        ),
        # hook returns None + both attrs + None → True, position=0
        pytest.param(
            _HookNoneFullStrategy,
            None,
            True,
            {"position": 0, "_in_position": False},
            id="hook_none_full_attrs_flat",
        ),
        # hook returns None + _in_position only + LONG → False, attr untouched (direction-safety)
        pytest.param(
            _InPositionOnlyStrategy,
            "LONG",
            False,
            {"_in_position": _SENTINEL},
            id="in_position_only_LONG_direction_safety",
        ),
        # hook returns None + _in_position only + None → True, _in_position=False
        pytest.param(
            _InPositionOnlyStrategy,
            None,
            True,
            {"_in_position": False},
            id="in_position_only_flat",
        ),
        # hook returns None + no attrs at all + LONG → False
        pytest.param(
            _NoAttrsStrategy,
            "LONG",
            False,
            {},
            id="no_attrs_LONG",
        ),
    ],
)
def test_set_position_state_decision_matrix(
    strategy_factory, side, expected_result, checks
) -> None:
    """Full decision table for set_position_state hook contract + fallback."""
    strategy = strategy_factory()
    processor = SignalProcessor(StrategyRegistry(), MagicMock())

    result = processor.set_position_state(strategy, side)

    assert result is expected_result
    for attr, expected_val in checks.items():
        assert (
            getattr(strategy, attr) is expected_val
            or getattr(strategy, attr) == expected_val
        ), f"{attr}: expected {expected_val!r}, got {getattr(strategy, attr)!r}"


def test_on_candle_skips_timeframe_mismatch() -> None:
    strategy = DummyStrategy("s1", timeframe="5m", result=make_signal())
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()

    SignalProcessor(registry, execution).on_candle(make_candle(timeframe="1m"))

    assert strategy.candles_received == []
    execution.execute_signal.assert_not_called()


def test_on_candle_skips_product_mismatch() -> None:
    strategy = DummyStrategy(
        "s1",
        product_id="BINANCE:ETHUSDT-PERP",
        result=make_signal(),
    )
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()

    SignalProcessor(registry, execution).on_candle(make_candle())

    assert strategy.candles_received == []
    execution.execute_signal.assert_not_called()


def test_on_candle_skips_stopped_strategy() -> None:
    strategy = DummyStrategy("s1", result=make_signal())
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    state_manager = DummyStateManager(running=set())

    SignalProcessor(registry, execution, state_manager).on_candle(make_candle())

    assert strategy.candles_received == []
    execution.execute_signal.assert_not_called()


def test_on_trade_routes_matching_strategy_without_timeframe_filter() -> None:
    signal = make_signal()
    strategy = DummyStrategy("s1", timeframe="15m", trade_result=signal)
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    signal_handler = MagicMock()

    SignalProcessor(
        registry,
        execution,
        signal_handler=signal_handler,
    ).on_trade(make_trade())

    assert strategy.trades_received == [make_trade()]
    emitted_signal = signal_handler.call_args.args[0]
    assert emitted_signal.model_copy(update={"metadata": None}) == signal
    assert emitted_signal.metadata["client_order_id"]
    assert signal_handler.call_args.args[1] is None
    execution.execute_signal.assert_not_called()


def test_on_trade_admission_rejection_skips_submission() -> None:
    class StatefulTradeStrategy(DummyStrategy):
        def __init__(self):
            super().__init__("s1")
            self._in_position = False
            self.restore_calls = 0

        def on_trade(self, trade):
            self._in_position = True
            return make_signal()

        def snapshot_walk_forward_trade_state(self):
            return self._in_position

        def restore_walk_forward_trade_state(self, state):
            self.restore_calls += 1
            self._in_position = state

    strategy = StatefulTradeStrategy()
    registry = StrategyRegistry()
    registry.register(strategy)
    signal_handler = MagicMock()
    admission = MagicMock(side_effect=[False, True])
    processor = SignalProcessor(
        registry,
        MagicMock(),
        signal_handler=signal_handler,
        entry_admission_handler=admission,
    )

    processor.on_trade(make_trade())

    assert strategy._in_position is False
    assert strategy.restore_calls == 1
    signal_handler.assert_not_called()

    processor.on_trade(make_trade())

    assert admission.call_count == 2
    assert admission.call_args.args[0].metadata["client_order_id"]
    signal_handler.assert_called_once()


def test_on_trade_admission_error_propagates_and_restores_state() -> None:
    class StatefulTradeStrategy(DummyStrategy):
        def __init__(self):
            super().__init__("s1")
            self._in_position = False
            self.restore_calls = 0

        def on_trade(self, trade):
            self._in_position = True
            return make_signal()

        def snapshot_walk_forward_trade_state(self):
            return self._in_position

        def restore_walk_forward_trade_state(self, state):
            self.restore_calls += 1
            self._in_position = state

    strategy = StatefulTradeStrategy()
    registry = StrategyRegistry()
    registry.register(strategy)
    signal_handler = MagicMock()
    cause = RuntimeError("admission unavailable")

    def fail_admission(_signal: Signal) -> bool:
        raise cause

    processor = SignalProcessor(
        registry,
        MagicMock(),
        signal_handler=signal_handler,
        entry_admission_handler=fail_admission,
    )

    with pytest.raises(RuntimeError, match="admission unavailable") as caught:
        processor.on_trade(make_trade())

    assert caught.value is cause
    assert strategy._in_position is False
    assert strategy.restore_calls == 1
    signal_handler.assert_not_called()


def test_on_trade_admission_checks_all_entries_before_preserving_exit() -> None:
    strategies = (
        DummyStrategy("entry_0", trade_result=make_signal("entry_0")),
        DummyStrategy("entry_1", trade_result=make_signal("entry_1")),
        DummyStrategy(
            "exit",
            trade_result=make_signal("exit", SignalType.EXIT_LONG),
        ),
    )
    registry = StrategyRegistry()
    for strategy in strategies:
        registry.register(strategy)
    handler = MagicMock(return_value=True)
    admission = MagicMock(side_effect=[True, False])

    SignalProcessor(
        registry,
        MagicMock(),
        signal_handler=handler,
        entry_admission_handler=admission,
    ).on_trade(make_trade())

    assert [call.args[0].strategy_id for call in admission.call_args_list] == [
        "entry_0",
        "entry_1",
    ]
    handler.assert_called_once()
    assert handler.call_args.args[0].type == SignalType.EXIT_LONG


def test_on_trade_skips_product_mismatch() -> None:
    strategy = DummyStrategy(
        "s1",
        product_id="BINANCE:ETHUSDT-PERP",
        trade_result=make_signal(),
    )
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()

    SignalProcessor(registry, execution).on_trade(make_trade())

    assert strategy.trades_received == []
    execution.execute_signal.assert_not_called()


def test_on_trade_skips_stopped_strategy() -> None:
    strategy = DummyStrategy("s1", trade_result=make_signal())
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    state_manager = DummyStateManager(running=set())

    SignalProcessor(registry, execution, state_manager).on_trade(make_trade())

    assert strategy.trades_received == []
    execution.execute_signal.assert_not_called()


def test_trade_strategy_exception_blocks_all_signal_side_effects() -> None:
    good = DummyStrategy("good", trade_result=make_signal("good"))
    failing = DummyStrategy("bad", should_raise=True)
    registry = StrategyRegistry()
    registry.register(good)
    registry.register(failing)
    signal_handler = MagicMock()

    with pytest.raises(RuntimeError, match="strategy failed"):
        SignalProcessor(
            registry,
            MagicMock(),
            signal_handler=signal_handler,
        ).on_trade(make_trade())

    assert good.trades_received == [make_trade()]
    assert failing.trades_received == [make_trade()]
    signal_handler.assert_not_called()


def test_market_signal_id_is_stable_when_pending_trade_is_replayed() -> None:
    strategy = DummyStrategy("s1", trade_result=make_signal("s1"))
    registry = StrategyRegistry()
    registry.register(strategy)
    signal_handler = MagicMock()
    processor = SignalProcessor(
        registry,
        MagicMock(),
        signal_handler=signal_handler,
    )
    trade = make_trade()

    processor.on_trade(trade)
    processor.on_trade(trade)

    first_signal = signal_handler.call_args_list[0].args[0]
    second_signal = signal_handler.call_args_list[1].args[0]
    assert (
        first_signal.metadata["client_order_id"]
        == second_signal.metadata["client_order_id"]
    )


@pytest.mark.parametrize(
    ("trade_id", "timestamp"),
    [
        ("", 1704067200000),
        ("  ", 1704067200000),
        ("unknown", 1704067200000),
        ("UNKNOWN", 1704067200000),
        ("t1", 0),
    ],
)
def test_trade_without_stable_identity_fails_before_strategy_dispatch(
    trade_id,
    timestamp,
) -> None:
    strategy = DummyStrategy("s1", trade_result=make_signal("s1"))
    registry = StrategyRegistry()
    registry.register(strategy)
    trade = make_trade().model_copy(update={"id": trade_id, "timestamp": timestamp})

    with pytest.raises(ValueError, match="stable id and positive timestamp"):
        SignalProcessor(registry, MagicMock()).on_trade(trade)

    assert strategy.trades_received == []


def test_dispatch_normalizes_none_signal_and_list() -> None:
    processor = SignalProcessor(StrategyRegistry(), MagicMock())
    candle = make_candle()
    single = make_signal()
    multiple = [make_signal("s1"), make_signal("s1", SignalType.SHORT)]

    assert (
        processor._dispatch_to_strategy(DummyStrategy("s1", result=None), candle) == []
    )
    assert processor._dispatch_to_strategy(
        DummyStrategy("s1", result=single), candle
    ) == [single]
    assert (
        processor._dispatch_to_strategy(DummyStrategy("s1", result=multiple), candle)
        == multiple
    )


def test_process_signals_skips_no_signal() -> None:
    execution = MagicMock()
    processor = SignalProcessor(StrategyRegistry(), execution)

    processor._process_signals("s1", [make_signal(signal_type=SignalType.NO_SIGNAL)])

    execution.execute_signal.assert_not_called()


def test_process_signals_executes_multiple_actionable_signals() -> None:
    execution = MagicMock()
    processor = SignalProcessor(StrategyRegistry(), execution)
    candle = make_candle()
    signals = [make_signal("s1", SignalType.LONG), make_signal("s1", SignalType.SHORT)]

    processor._process_signals("s1", signals, candle)

    assert execution.execute_signal.call_count == 2
    execution.execute_signal.assert_any_call(signals[0], candle)
    execution.execute_signal.assert_any_call(signals[1], candle)


def test_process_signals_uses_signal_handler_when_provided() -> None:
    execution = MagicMock()
    signal_handler = MagicMock()
    processor = SignalProcessor(
        StrategyRegistry(), execution, signal_handler=signal_handler
    )
    candle = make_candle()
    signal = make_signal("s1", SignalType.LONG)

    processor._process_signals("s1", [signal], candle)

    signal_handler.assert_called_once_with(signal, candle)
    execution.execute_signal.assert_not_called()


def test_finalized_signal_observer_preserves_batch_order_identity_and_timing() -> None:
    class RecordingProcessor(SignalProcessor):
        processed: list[list[Signal]] = []

        def _process_signals(self, strategy_id, signals, candle=None) -> None:
            self.processed.append(signals)
            super()._process_signals(strategy_id, signals, candle)

    first_signals = [
        make_signal("s1", SignalType.LONG),
        make_signal("s1", SignalType.NO_SIGNAL),
    ]
    second_signals = [
        make_signal("s2", SignalType.SHORT),
        make_signal("s2", SignalType.EXIT_SHORT),
    ]
    registry = StrategyRegistry()
    registry.register(DummyStrategy("s1", result=first_signals))
    registry.register(DummyStrategy("s2", result=second_signals))
    events: list[tuple[str, object]] = []
    observed: list[tuple[Signal, ...]] = []

    def observer(batch: tuple[Signal, ...]) -> None:
        observed.append(batch)
        events.append(("observer", batch))

    def handler(signal: Signal, _candle: Candlestick | None) -> bool:
        events.append(("handler", signal))
        return True

    processor = RecordingProcessor(
        registry,
        MagicMock(),
        signal_handler=handler,
        signal_batch_observer=observer,
    )
    processor.on_candle(make_candle())

    assert len(observed) == 1
    assert [signal.type for signal in observed[0]] == [
        SignalType.LONG,
        SignalType.NO_SIGNAL,
        SignalType.SHORT,
        SignalType.EXIT_SHORT,
    ]
    assert events[0][0] == "observer"
    assert events[1][1] is observed[0][0]
    assert events[2][1] is observed[0][2]
    assert events[3][1] is observed[0][3]
    processed = tuple(signal for signals in processor.processed for signal in signals)
    assert all(
        observed_signal is processed_signal
        for observed_signal, processed_signal in zip(
            observed[0], processed, strict=True
        )
    )
    assert observed[0][0].metadata is not None
    assert observed[0][0].metadata["client_order_id"]


@pytest.mark.parametrize("explicit_none", [False, True])
def test_finalized_signal_observer_default_none_is_equivalent(
    explicit_none: bool,
) -> None:
    class ExactSignals(list[Signal]):
        iterations = 0

        def __iter__(self):
            self.iterations += 1
            return super().__iter__()

    signal = make_signal()
    signals = ExactSignals([signal])
    strategy = DummyStrategy("s1", result=signals)
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    kwargs = {"signal_batch_observer": None} if explicit_none else {}

    SignalProcessor(registry, execution, **kwargs).on_candle(make_candle())

    execution.execute_signal.assert_called_once_with(signal, make_candle())
    assert signals.iterations == 1
    assert execution.execute_signal.call_args.args[0] is signal
    assert signal.metadata is None


def test_finalized_signal_observer_does_not_call_for_pure_empty_batch() -> None:
    registry = StrategyRegistry()
    registry.register(DummyStrategy("empty", result=[]))
    observer = MagicMock()

    SignalProcessor(
        registry,
        MagicMock(),
        signal_batch_observer=observer,
    ).on_candle(make_candle())

    observer.assert_not_called()


def test_finalized_signal_observer_skips_empty_but_observes_no_signal() -> None:
    strategies = [
        DummyStrategy("empty", result=[]),
        DummyStrategy("none", result=None),
        DummyStrategy(
            "no_signal", result=make_signal("no_signal", SignalType.NO_SIGNAL)
        ),
    ]
    registry = StrategyRegistry()
    for strategy in strategies:
        registry.register(strategy)
    observed: list[tuple[Signal, ...]] = []
    execution = MagicMock()

    SignalProcessor(
        registry,
        execution,
        signal_batch_observer=observed.append,
    ).on_candle(make_candle())

    assert len(observed) == 1
    assert observed[0][0].type == SignalType.NO_SIGNAL
    execution.execute_signal.assert_not_called()


def test_finalized_signal_observer_ignores_false_return_value() -> None:
    signal = make_signal()
    registry = StrategyRegistry()
    registry.register(DummyStrategy("s1", result=signal))
    observer = MagicMock(return_value=False)
    handler = MagicMock(return_value=True)

    SignalProcessor(
        registry,
        MagicMock(),
        signal_handler=handler,
        signal_batch_observer=observer,
    ).on_candle(make_candle())

    observer.assert_called_once()
    handler.assert_called_once()
    assert handler.call_args.args[0] is observer.call_args.args[0][0]


def test_finalized_signal_observer_failure_has_stage_cause_and_no_effects() -> None:
    class HostileObserverFailure(Exception):
        def __str__(self) -> str:
            raise AssertionError("must not render observer failure")

        def __repr__(self) -> str:
            raise AssertionError("must not render observer failure")

    cause = HostileObserverFailure()
    strategy = DummyStrategy("s1", result=make_signal())
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    handler = MagicMock()

    def observer(_batch: tuple[Signal, ...]) -> None:
        raise cause

    with pytest.raises(SignalObserverError) as caught:
        SignalProcessor(
            registry,
            execution,
            signal_handler=handler,
            signal_batch_observer=observer,
        ).on_candle(make_candle())

    assert caught.value.stage == "post_coordination_pre_execution"
    assert caught.value.args == ("signal observer failed",)
    assert caught.value.__cause__ is cause
    handler.assert_not_called()
    execution.execute_signal.assert_not_called()


def test_finalized_signal_observer_failure_rolls_back_exclusive_slot_state() -> None:
    strategy_a = StatefulEntryStrategy("sleeve_a")
    strategy_b = StatefulEntryStrategy("sleeve_b")
    registry = StrategyRegistry()
    registry.register(strategy_a)
    registry.register(strategy_b)
    coordinator = PortfolioCoordinator()
    coordinator.register(
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=strategy_a.product_id,
            sleeves=(PortfolioSleeve(strategy_a), PortfolioSleeve(strategy_b)),
            max_gross_quantity=Decimal("1"),
            exclusive_slots=(
                PortfolioExclusiveSlot(
                    slot_id="shared",
                    strategy_ids=("sleeve_a", "sleeve_b"),
                ),
            ),
        )
    )
    execution = MagicMock(default_quantity=Decimal("1"))
    handler = MagicMock()

    def observer(_batch: tuple[Signal, ...]) -> None:
        raise RuntimeError("observer failed")

    with pytest.raises(Exception, match="signal observer failed"):
        SignalProcessor(
            registry,
            execution,
            signal_handler=handler,
            exposure_loader=lambda *_args: PortfolioExposureSnapshot({}),
            portfolio_coordinator=coordinator,
            signal_batch_observer=observer,
        ).on_candle(make_candle())

    assert strategy_a._in_position is False
    assert strategy_b._in_position is False
    handler.assert_not_called()


def test_strategy_exception_blocks_all_signal_side_effects() -> None:
    good_signal = make_signal("good")
    failing = DummyStrategy("bad", should_raise=True)
    good = DummyStrategy("good", result=good_signal)
    registry = StrategyRegistry()
    registry.register(failing)
    registry.register(good)
    execution = MagicMock()

    with pytest.raises(RuntimeError, match="strategy failed"):
        SignalProcessor(registry, execution).on_candle(make_candle())

    execution.execute_signal.assert_not_called()
    assert good.candles_received == []


def test_market_signal_id_is_stable_when_pending_candle_is_replayed() -> None:
    signal = make_signal("s1")
    strategy = DummyStrategy("s1", result=signal)
    registry = StrategyRegistry()
    registry.register(strategy)
    signal_handler = MagicMock()
    processor = SignalProcessor(
        registry,
        MagicMock(),
        signal_handler=signal_handler,
    )
    candle = make_candle()

    processor.on_candle(candle)
    processor.on_candle(candle)

    first_signal = signal_handler.call_args_list[0].args[0]
    second_signal = signal_handler.call_args_list[1].args[0]
    assert (
        first_signal.metadata["client_order_id"]
        == second_signal.metadata["client_order_id"]
    )


def test_pending_candle_replay_overrides_strategy_client_order_id() -> None:
    first = make_signal("s1").model_copy(
        update={"metadata": {"client_order_id": "strategy-attempt-1"}}
    )
    second = make_signal("s1").model_copy(
        update={"metadata": {"client_order_id": "strategy-attempt-2"}}
    )
    candle = make_candle()

    first_replay = SignalProcessor._with_market_idempotency(
        first,
        product_id=candle.product_id,
        event_scope=candle.timeframe,
        event_timestamp=candle.timestamp,
        ordinal=0,
    )
    second_replay = SignalProcessor._with_market_idempotency(
        second,
        product_id=candle.product_id,
        event_scope=candle.timeframe,
        event_timestamp=candle.timestamp,
        ordinal=0,
    )

    first_metadata = first_replay.metadata
    second_metadata = second_replay.metadata
    assert first_metadata is not None
    assert second_metadata is not None
    assert first_metadata["client_order_id"] == second_metadata["client_order_id"]
    assert first_metadata["client_order_id"] != "strategy-attempt-1"
    assert first_metadata["requested_client_order_id"] == "strategy-attempt-1"
    assert second_metadata["requested_client_order_id"] == "strategy-attempt-2"
