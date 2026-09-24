from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from src.core.market_data.profiles.decision_context import StrategyMarketDataContext
from src.core.market_data.profiles.decision_input_store import DecisionInputRecord
from src.core.market_data.profiles.decision_owner import (
    MarketDataDecisionOwnerError,
)
from src.core.signal_processor import StrategyDecisionSkipped
from test_profile_decision_input import sample
from test_profile_decision_owner import DAY, NOW, Strategy, base_context, decision


def test_existing_input_reuses_recorded_context_without_current_dependencies():
    store = MagicMock()
    strategy = Strategy("profile_strategy")
    candle, scope = decision(store, clock=2 * DAY + 3)
    base = base_context(strategy)
    recorded = []

    def existing(key):
        value = replace(sample(), key=key)
        recorded.append(value)
        return DecisionInputRecord(value, NOW, True)

    store.get.side_effect = existing
    cache = scope._owner._cache
    cache.live_requests = MagicMock(side_effect=AssertionError("cache forbidden"))
    cache.decision_many = MagicMock(side_effect=AssertionError("cache forbidden"))
    with scope(strategy, candle, base) as enriched:
        assert enriched is not None and enriched.market_data is not None
        assert enriched.market_data == sample().context
    batch = scope.build()
    assert batch is not None and batch.outcomes[0].disposition == "APPLIED"
    assert batch.outcomes[0].input_digest == recorded[0].input_digest
    store.pin_confirmed.assert_not_called()
    store.get.assert_called_once()
    cache.live_requests.assert_not_called()
    cache.decision_many.assert_not_called()


def test_existing_wrong_requirements_is_rejected_before_callback():
    store = MagicMock()
    strategy = Strategy("profile_strategy")
    candle, scope = decision(store)

    def wrong(key):
        value = replace(
            sample(),
            key=key,
            requirements=(),
            context=StrategyMarketDataContext(sample().decision_time_ms, ()),
        )
        return DecisionInputRecord(value, NOW, True)

    store.get.side_effect = wrong
    with pytest.raises(MarketDataDecisionOwnerError):
        scope(strategy, candle, base_context(strategy))
    assert scope.build() is None
    store.pin_confirmed.assert_not_called()


def test_existing_wrong_key_is_rejected_before_callback_or_dependencies():
    store = MagicMock()
    strategy = Strategy("profile_strategy")
    candle, scope = decision(store)
    cache = scope._owner._cache
    cache.live_requests = MagicMock(side_effect=AssertionError("cache forbidden"))
    cache.decision_many = MagicMock(side_effect=AssertionError("cache forbidden"))

    def wrong(key):
        value = replace(sample(), key=replace(key, strategy_id="other"))
        return DecisionInputRecord(value, NOW, True)

    store.get.side_effect = wrong
    callbacks = []
    with pytest.raises(MarketDataDecisionOwnerError):
        manager = scope(strategy, candle, base_context(strategy))
        with manager:
            callbacks.append(True)
    assert callbacks == [] and scope.build() is None
    cache.live_requests.assert_not_called()
    cache.decision_many.assert_not_called()
    store.pin_confirmed.assert_not_called()


def test_input_lookup_failure_is_local_skip_without_cache_or_pin():
    store = MagicMock()
    store.get.side_effect = RuntimeError("SECRET")
    strategy = Strategy("profile_strategy")
    candle, scope = decision(store)
    cache = scope._owner._cache
    cache.live_requests = MagicMock(side_effect=AssertionError("cache forbidden"))
    cache.decision_many = MagicMock(side_effect=AssertionError("cache forbidden"))
    manager = scope(strategy, candle, base_context(strategy))
    with pytest.raises(StrategyDecisionSkipped):
        with manager:
            pytest.fail("callback must not start")
    batch = scope.build()
    assert batch is not None
    assert batch.outcomes[0].reason == "INPUT_STORE_FAILED"
    store.pin_confirmed.assert_not_called()


def test_input_lookup_base_exception_propagates_without_terminal_outcome():
    failure = KeyboardInterrupt()
    store = MagicMock()
    store.get.side_effect = failure
    strategy = Strategy("profile_strategy")
    candle, scope = decision(store)
    with pytest.raises(KeyboardInterrupt) as caught:
        scope(strategy, candle, base_context(strategy))
    assert caught.value is failure and scope.build() is None
