from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.core.market_data.profiles.decision_application import (
    MarketDataDecisionBatch,
    MarketDataDecisionKey,
    MarketDataDecisionOutcome,
)
from src.core.market_data.profiles.decision_identity import strategy_decision_composition
from src.core.market_data.profiles.decision_input import MarketDataDecisionInput
from src.core.market_data.profiles.decision_input_store import DecisionInputRecord
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwner
from src.core.signal_processor import StrategyDecisionSkipped
from test_profile_context_enrichment import context as example_context
from test_profile_decision_input import sample
from test_signal_processor import DummyStrategy, make_candle

DAY = 86_400_000
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class Strategy(DummyStrategy):
    __fluxtrade_artifact_version__ = "1.2.0"

    @property
    def requirements(self):
        return replace(
            super().requirements,
            profile_requirements=sample().requirements,
        )

    def replay_configuration(self):
        return {"profile": True}


def base_context(strategy):
    candle = make_candle()
    return replace(
        example_context(),
        strategy_id=strategy.strategy_id,
        product_id=candle.product_id,
        timestamp=candle.timestamp,
    )


def recorded(store, strategy, *, disposition="APPLIED"):
    cache = MagicMock()
    utc_ms = MagicMock()
    monotonic_ms = MagicMock()
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="live-berlin-1",
        identity_resolver=lambda value: strategy_decision_composition(
            "live-berlin-1", value
        ),
        cache=cache,
        input_store=store,
        utc_ms=utc_ms,
        monotonic_ms=monotonic_ms,
    )
    candle = make_candle()
    identity = strategy_decision_composition("live-berlin-1", strategy)
    key = MarketDataDecisionKey.for_candle(
        environment="live",
        execution_scope_id=identity.execution_scope_id,
        strategy_id=strategy.strategy_id,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        product_id=candle.product_id,
        timeframe=candle.timeframe,
        bar_start_ms=candle.timestamp,
    )
    value = MarketDataDecisionInput(
        key,
        strategy.requirements.profile_requirements,
        DAY + 3,
        sample().context,
    )
    outcome = MarketDataDecisionOutcome(
        key,
        disposition,
        value.input_id if disposition == "APPLIED" else None,
        value.input_digest if disposition == "APPLIED" else None,
        None if disposition == "APPLIED" else "INPUT_STORE_FAILED",
    )
    batch = MarketDataDecisionBatch(
        "live",
        "live-berlin-1",
        candle.product_id,
        candle.timeframe,
        candle.timestamp,
        (key,),
        (outcome,),
    )
    return candle, owner, value, batch, cache, utc_ms, monotonic_ms


def test_recorded_applied_reuses_exact_input_without_cache_or_clocks():
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, value, batch, cache, utc_ms, monotonic_ms = recorded(
        store, strategy
    )
    store.get.return_value = DecisionInputRecord(value, NOW, True)

    with owner.replay_candle(candle, batch)(
        strategy, candle, base_context(strategy)
    ) as enriched:
        assert enriched is not None
        assert enriched.market_data == value.context

    store.get.assert_called_once_with(value.key)
    store.pin_confirmed.assert_not_called()
    cache.live_requests.assert_not_called()
    cache.decision_many.assert_not_called()
    utc_ms.assert_not_called()
    monotonic_ms.assert_not_called()


def test_recorded_skipped_never_reads_input_or_starts_callback():
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, _value, batch, cache, utc_ms, monotonic_ms = recorded(
        store, strategy, disposition="SKIPPED"
    )

    with pytest.raises(StrategyDecisionSkipped):
        with owner.replay_candle(candle, batch)(strategy, candle, None):
            pytest.fail("recorded skip must remain skipped")

    store.get.assert_not_called()
    store.pin_confirmed.assert_not_called()
    cache.live_requests.assert_not_called()
    cache.decision_many.assert_not_called()
    utc_ms.assert_not_called()
    monotonic_ms.assert_not_called()
