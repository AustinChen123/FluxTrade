from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.core.market_data.profiles.decision_application import (
    MarketDataDecisionBatch,
    MarketDataDecisionKey,
    MarketDataDecisionOutcome,
)
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.market_data.profiles.decision_input import MarketDataDecisionInput
from src.core.market_data.profiles.decision_input_store import DecisionInputRecord
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwner
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwnerError
from src.core.market_data.profiles.decision_owner import PreparedRecordedDecision
from decimal import Decimal
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
    candle, owner, value, batch, cache, utc_ms, monotonic_ms = recorded(store, strategy)
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


@pytest.mark.parametrize("disposition", ["APPLIED", "SKIPPED"])
def test_prepared_scope_detached_from_all_live_dependencies(disposition):
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, value, batch, cache, utc, mono = recorded(
        store, strategy, disposition=disposition
    )
    store.get.return_value = DecisionInputRecord(value, NOW, True)
    prepared = owner.prepare_replay_candle(strategy, candle, batch)
    assert prepared.outcome is batch.outcomes[0]
    assert prepared.pinned_input is (value if disposition == "APPLIED" else None)
    assert store.get.call_count == (1 if disposition == "APPLIED" else 0)
    for method in (
        store.get,
        store.pin_confirmed,
        cache.live_requests,
        cache.decision_many,
        utc,
        mono,
    ):
        method.side_effect = AssertionError("dependency called after preparation")
    owner._identity_resolver = lambda _: pytest.fail("identity resolver after prepare")
    if disposition == "APPLIED":
        with prepared(strategy, candle, base_context(strategy)) as enriched:
            assert enriched is not None and enriched.market_data is value.context
    else:
        with pytest.raises(StrategyDecisionSkipped):
            with prepared(strategy, candle, None):
                pytest.fail("callback must not start")
    assert not hasattr(prepared, "__dict__")


def test_prepare_missing_input_fails_before_returning_scope():
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, _, batch, _, _, _ = recorded(store, strategy)
    store.get.return_value = None
    with pytest.raises(MarketDataDecisionOwnerError):
        owner.prepare_replay_candle(strategy, candle, batch)


@pytest.mark.parametrize(
    "damage", ["strategy", "candle", "context", "product", "timestamp"]
)
def test_prepared_call_identity_and_context(damage):
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, value, batch, _, _, _ = recorded(store, strategy)
    store.get.return_value = DecisionInputRecord(value, NOW, True)
    prepared = owner.prepare_replay_candle(strategy, candle, batch)
    context = base_context(strategy)
    if damage == "strategy":
        strategy = Strategy("profile_strategy")
    elif damage == "candle":
        candle = candle.model_copy()
    elif damage == "context":
        context = None
    elif damage == "product":
        context = replace(context, product_id="BINANCE:BTCUSDT-SPOT")
    else:
        context = replace(context, timestamp=context.timestamp + 1)
    with pytest.raises(MarketDataDecisionOwnerError):
        prepared(strategy, candle, context)


def test_direct_prepared_construction_rejected():
    strategy = Strategy("profile_strategy")
    candle, _, value, batch, _, _, _ = recorded(MagicMock(), strategy)
    other = replace(
        value,
        decision_time_ms=value.decision_time_ms + 1,
        context=replace(
            value.context,
            decision_time_ms=value.decision_time_ms + 1,
            profiles=tuple(
                replace(item, decision_time_ms=item.decision_time_ms + 1)
                for item in value.context.profiles
            ),
        ),
    )
    for pinned in (value, other, None):
        with pytest.raises(MarketDataDecisionOwnerError):
            PreparedRecordedDecision(strategy, candle, batch.outcomes[0], pinned)


@pytest.mark.parametrize(
    "field,value",
    [
        ("timestamp", 1),
        ("timeframe", "5m"),
        ("product_id", "BINANCE:BTCUSDT-SPOT"),
        *(
            (name, Decimal("123"))
            for name in ("open", "high", "low", "close", "volume")
        ),
    ],
)
def test_mutated_same_candle_rejected(field, value):
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, pinned, batch, _, _, _ = recorded(store, strategy)
    store.get.return_value = DecisionInputRecord(pinned, NOW, True)
    prepared = owner.prepare_replay_candle(strategy, candle, batch)
    setattr(candle, field, value)
    with pytest.raises(MarketDataDecisionOwnerError):
        prepared(strategy, candle, base_context(strategy))


@pytest.mark.parametrize(
    "damage", ["id", "product", "requirements", "configuration", "version"]
)
def test_mutated_same_strategy_rejected(damage, monkeypatch):
    strategy = Strategy("profile_strategy")
    store = MagicMock()
    candle, owner, pinned, batch, _, _, _ = recorded(store, strategy)
    store.get.return_value = DecisionInputRecord(pinned, NOW, True)
    prepared = owner.prepare_replay_candle(strategy, candle, batch)
    if damage == "id":
        strategy.strategy_id = "other"
    elif damage == "product":
        strategy.product_id = "BINANCE:BTCUSDT-SPOT"
    elif damage == "requirements":
        requirements = replace(strategy.requirements, profile_requirements=())
        monkeypatch.setattr(Strategy, "requirements", property(lambda _: requirements))
    elif damage == "configuration":
        monkeypatch.setattr(strategy, "replay_configuration", lambda: {"changed": True})
    else:
        monkeypatch.setattr(Strategy, "__fluxtrade_artifact_version__", "2.0.0")
    with pytest.raises(MarketDataDecisionOwnerError):
        prepared(strategy, candle, base_context(strategy))
