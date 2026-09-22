from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from src.core.market_data.profiles.decision_identity import strategy_decision_composition
from src.core.market_data.profiles.decision_input_store import DecisionInputRecord
from src.core.market_data.profiles.decision_owner import (
    MarketDataDecisionOwner,
    MarketDataDecisionOwnerError,
)
from test_profile_context_enrichment import S
from test_profile_decision_input import sample
from test_profile_recorded_decision_owner import (
    NOW,
    Strategy,
    base_context,
    recorded,
)
from test_signal_processor import DummyStrategy, make_candle


@pytest.mark.parametrize("mutation", ["missing", "wrong_input", "wrong_key"])
def test_recorded_applied_rejects_incomplete_or_conflicting_input(mutation):
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, value, batch, _cache, _utc, _monotonic = recorded(store, strategy)
    if mutation == "missing":
        store.get.return_value = None
    elif mutation == "wrong_input":
        store.get.return_value = DecisionInputRecord(
            replace(value, context=sample(status=S.MISSING).context),
            NOW,
            True,
        )
    else:
        other_key = replace(value.key, strategy_id="other")
        store.get.return_value = DecisionInputRecord(
            replace(value, key=other_key), NOW, True
        )

    with pytest.raises(MarketDataDecisionOwnerError):
        owner.replay_candle(candle, batch)(strategy, candle, base_context(strategy))


def test_recorded_profile_requires_terminal_outcome():
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, _value, batch, _cache, _utc, _monotonic = recorded(store, strategy)
    empty = replace(batch, participants=(), outcomes=())

    with pytest.raises(MarketDataDecisionOwnerError):
        owner.replay_candle(candle, empty)(strategy, candle, base_context(strategy))

    store.get.assert_not_called()


def test_recorded_batch_identity_is_exact():
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, _value, batch, cache, _utc, _monotonic = recorded(store, strategy)
    other_environment = MarketDataDecisionOwner(
        environment="paper",
        execution_scope_id="live-berlin-1",
        identity_resolver=owner._identity_resolver,
        cache=cache,
        input_store=store,
        utc_ms=MagicMock(),
        monotonic_ms=MagicMock(),
    )

    with pytest.raises(MarketDataDecisionOwnerError):
        owner.replay_candle(make_candle(timeframe="5m"), batch)
    with pytest.raises(MarketDataDecisionOwnerError):
        other_environment.replay_candle(candle, batch)


@pytest.mark.parametrize("drift", ["version", "config"])
def test_recorded_skip_rejects_current_composition_drift_before_io(drift):
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, _owner, _value, batch, cache, utc_ms, monotonic_ms = recorded(
        store, strategy, disposition="SKIPPED"
    )
    original = strategy_decision_composition("live-berlin-1", strategy)
    changed = replace(
        original,
        **(
            {"strategy_version": "1.2.1"}
            if drift == "version"
            else {"config_hash": "0" * 64}
        ),
    )
    drifted = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="live-berlin-1",
        identity_resolver=lambda _strategy: changed,
        cache=cache,
        input_store=store,
        utc_ms=utc_ms,
        monotonic_ms=monotonic_ms,
    )

    with pytest.raises(MarketDataDecisionOwnerError):
        drifted.replay_candle(candle, batch)(strategy, candle, None)

    store.get.assert_not_called()
    store.pin_confirmed.assert_not_called()
    cache.live_requests.assert_not_called()
    cache.decision_many.assert_not_called()
    utc_ms.assert_not_called()
    monotonic_ms.assert_not_called()


def test_recorded_removed_profile_requirement_rejects_historical_outcome():
    recorded_strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, _value, batch, _cache, _utc, _monotonic = recorded(
        store, recorded_strategy
    )
    current_without_profile = DummyStrategy("profile_strategy")

    with pytest.raises(MarketDataDecisionOwnerError):
        owner.replay_candle(candle, batch)(current_without_profile, candle, None)

    store.get.assert_not_called()


def test_recorded_legacy_strategy_without_outcome_passes_through():
    recorded_strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, _value, batch, _cache, _utc, _monotonic = recorded(
        store, recorded_strategy
    )
    legacy = DummyStrategy("legacy")
    context = base_context(legacy)

    with owner.replay_candle(candle, batch)(legacy, candle, context) as replayed:
        assert replayed is context

    store.get.assert_not_called()
