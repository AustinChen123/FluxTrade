from dataclasses import replace
from unittest.mock import MagicMock, Mock

import pytest

from src.core.market_data.profiles.decision_batch_builder import (
    DecisionBatchBuildError,
)
from src.core.market_data.profiles.decision_context import StrategyMarketDataContext
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.market_data.profiles.decision_input_store import (
    DecisionInputPinResult,
    DecisionInputPinStatus,
    DecisionInputRecord,
)
from src.core.market_data.profiles.decision_owner import (
    MarketDataDecisionOwner,
    MarketDataDecisionOwnerError,
)
from src.core.market_data.profiles.snapshot_cache import ProfileSnapshotCache
from test_profile_decision_owner import DAY, NOW, Strategy, base_context, decision
from test_signal_processor import make_candle


def confirmed(value):
    return DecisionInputPinResult(
        DecisionInputPinStatus.CONFIRMED,
        DecisionInputRecord(value, NOW, False),
    )


def test_confirmed_wrong_valid_record_is_rejected_before_callback():
    store = MagicMock()
    store.get.return_value = None

    def wrong(value):
        other = replace(
            value,
            requirements=(),
            context=StrategyMarketDataContext(value.decision_time_ms, ()),
        )
        return DecisionInputPinResult(
            DecisionInputPinStatus.CONFIRMED,
            DecisionInputRecord(other, NOW, False),
        )

    store.pin_confirmed.side_effect = wrong
    strategy, (candle, scope) = Strategy("profile_strategy"), decision(store)
    callbacks = []
    with pytest.raises(MarketDataDecisionOwnerError):
        manager = scope(strategy, candle, base_context(strategy))
        with manager:
            callbacks.append(True)
    assert callbacks == [] and scope.build() is None


def test_non_profile_strategy_uses_zero_profile_dependencies():
    class Legacy(Strategy):
        @property
        def requirements(self):
            return replace(super().requirements, profile_requirements=())

    identity, cache, store = Mock(), MagicMock(), MagicMock()
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="live-berlin-1",
        identity_resolver=identity,
        cache=cache,
        input_store=store,
        utc_ms=lambda: DAY + 3,
        monotonic_ms=lambda: 10,
    )
    strategy, candle = Legacy("legacy"), make_candle()
    context = base_context(strategy)
    scope = owner.begin_candle(candle)
    with scope(strategy, candle, context) as received:
        assert received is context
    assert scope.build() is None
    identity.assert_not_called()
    cache.live_requests.assert_not_called()
    cache.decision_many.assert_not_called()
    store.pin_confirmed.assert_not_called()


def test_one_candle_samples_clocks_once_and_rejects_equal_copy():
    utc, mono = Mock(return_value=DAY + 3), Mock(return_value=10)
    cache = ProfileSnapshotCache(Mock(), utc_ms=utc, monotonic_ms=mono)
    store = MagicMock()
    store.get.return_value = None
    store.pin_confirmed.side_effect = confirmed
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="live-berlin-1",
        identity_resolver=lambda strategy: strategy_decision_composition(
            "live-berlin-1", strategy
        ),
        cache=cache,
        input_store=store,
        utc_ms=utc,
        monotonic_ms=mono,
    )
    candle = make_candle()
    scope = owner.begin_candle(candle)
    first, second = Strategy("first"), Strategy("second")
    with pytest.raises(MarketDataDecisionOwnerError):
        scope(first, candle.model_copy(), base_context(first))
    for strategy in (first, second):
        with scope(strategy, candle, base_context(strategy)):
            pass
    batch = scope.build()
    assert batch is not None and len(batch.outcomes) == 2
    assert utc.call_count == mono.call_count == 1


@pytest.mark.parametrize("failure", [RuntimeError("callback"), KeyboardInterrupt()])
def test_callback_failure_propagates_and_keeps_batch_incomplete(failure):
    store = MagicMock()
    store.get.return_value = None
    store.pin_confirmed.side_effect = confirmed
    strategy, (candle, scope) = Strategy("profile_strategy"), decision(store)
    manager = scope(strategy, candle, base_context(strategy))
    with pytest.raises(type(failure)) as caught:
        with manager:
            raise failure
    assert caught.value is failure
    with pytest.raises(DecisionBatchBuildError):
        scope.build()


def test_applied_exists_only_after_normal_scope_exit():
    store = MagicMock()
    store.get.return_value = None
    store.pin_confirmed.side_effect = confirmed
    strategy, (candle, scope) = Strategy("profile_strategy"), decision(store)
    manager = scope(strategy, candle, base_context(strategy))
    manager.__enter__()
    with pytest.raises(DecisionBatchBuildError):
        scope.build()
    assert manager.__exit__(None, None, None) is False
    batch = scope.build()
    assert batch is not None and batch.outcomes[0].disposition == "APPLIED"
