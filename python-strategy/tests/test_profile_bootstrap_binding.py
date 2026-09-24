from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from src.core.market_data.profiles.bootstrap_hydration import BoundBootstrapHydration
from src.core.market_data.profiles.bootstrap_seed import BootstrapSeedError
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwner
from src.core.market_data.profiles.decision_input_store import DecisionInputRecord
from src.core.market_data.profiles.decision_application import MarketDataDecisionBatch
from src.core.market_data.profiles.decision_context import ProfileDecisionStatus
from src.core.signal_processor import StrategyDecisionSkipped
from src.strategies.base import StrategyRequirements
from test_profile_bootstrap_seed import seed, suffix
from test_profile_bootstrap_hydration import plan
from test_profile_recorded_decision_owner import Strategy, NOW
from test_profile_context_enrichment import context
from test_signal_processor import make_candle


def setup(status=ProfileDecisionStatus.FRESH, count=0):
    value = seed(status)

    class Target(Strategy):
        @property
        def requirements(self):
            return StrategyRequirements(
                self.product_id, "1m", 2, profile_requirements=value.requirements
            )

    strategy = Target("s", value.key.product_id)
    identity = strategy_decision_composition("deployment", strategy)
    value = replace(
        value,
        key=replace(
            value.key,
            strategy_version=identity.strategy_version,
            config_hash=identity.config_hash,
        ),
        max_seed_candles=10,
    )
    rows = tuple(suffix(value, index=i, skipped=i == 1) for i in range(count))
    store, cache = MagicMock(), MagicMock()
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="deployment",
        identity_resolver=lambda _: identity,
        cache=cache,
        input_store=store,
        utc_ms=lambda: pytest.fail("clock"),
        monotonic_ms=lambda: pytest.fail("clock"),
    )
    prepared = []
    for outcome, pinned in rows:
        candle = make_candle().model_copy(
            update=dict(
                product_id=value.key.product_id,
                timestamp=int(outcome.key.trigger_id.split(":")[1]),
            )
        )
        batch = MarketDataDecisionBatch(
            "live",
            "deployment",
            candle.product_id,
            candle.timeframe,
            candle.timestamp,
            (outcome.key,),
            (outcome,),
        )
        store.get.return_value = (
            DecisionInputRecord(pinned, NOW, True) if pinned else None
        )
        prepared.append((candle, owner.prepare_replay_candle(strategy, candle, batch)))
    store.get.side_effect = AssertionError("read after preparation")
    return plan(value, rows), strategy, identity, tuple(prepared), store, cache


@pytest.mark.parametrize("status", list(ProfileDecisionStatus))
@pytest.mark.parametrize("count", [0, 1, 3])
def test_fixed_ordered_scopes_and_unavailable(status, count):
    value, strategy, identity, suffixes, store, cache = setup(status, count)
    reads = store.get.call_count
    bound = BoundBootstrapHydration(value, strategy, identity, suffixes)
    assert len(bound.candles) == 2 + count
    assert [c.timestamp for c in bound.candles] == [
        value.seed.cutover_ms + (i - 2) * 60000 for i in range(2 + count)
    ]
    for i, candle in enumerate(bound.candles):
        base = replace(
            context(),
            strategy_id=strategy.strategy_id,
            product_id=candle.product_id,
            timestamp=candle.timestamp,
        )
        scope = bound.decision_scope_loader(candle)
        if i == 3:
            with pytest.raises(StrategyDecisionSkipped):
                with scope(strategy, candle, base):
                    pytest.fail("skip callback")
        else:
            with scope(strategy, candle, base) as enriched:
                assert enriched is not None
                if i < 2:
                    expected = value.seed.candles[i].context
                else:
                    pinned = value.recorded[i - 2][1]
                    assert pinned is not None
                    expected = pinned.context
                assert enriched.market_data is expected
    assert store.get.call_count == reads
    cache.assert_not_called()


@pytest.mark.parametrize(
    "damage", ["scope", "version", "config", "missing", "reverse", "copy", "outcome"]
)
def test_bind_mismatch_before_hydration(damage):
    value, strategy, identity, suffixes, _, _ = setup(count=3)
    if damage in ("scope", "version", "config"):
        field = {
            "scope": "execution_scope_id",
            "version": "strategy_version",
            "config": "config_hash",
        }[damage]
        identity = replace(
            identity, **{field: "d" * 64 if damage == "config" else "other"}
        )
    elif damage == "missing":
        suffixes = suffixes[:-1]
    elif damage == "reverse":
        suffixes = suffixes[::-1]
    elif damage == "copy":
        suffixes = ((suffixes[0][0].model_copy(), suffixes[0][1]), *suffixes[1:])
    else:
        suffixes = ((suffixes[0][0], suffixes[2][1]), *suffixes[1:])
    with pytest.raises(BootstrapSeedError):
        BoundBootstrapHydration(value, strategy, identity, suffixes)


def test_unknown_and_drifted_candle_and_configuration(monkeypatch):
    value, strategy, identity, suffixes, _, _ = setup()
    bound = BoundBootstrapHydration(value, strategy, identity, suffixes)
    with pytest.raises(BootstrapSeedError):
        bound.decision_scope_loader(bound.candles[0].model_copy())
    monkeypatch.setattr(strategy, "replay_configuration", lambda: {"changed": True})
    with pytest.raises(BootstrapSeedError):
        bound.decision_scope_loader(bound.candles[0])
    monkeypatch.undo()
    bound.candles[0].timestamp += 1
    with pytest.raises(BootstrapSeedError):
        bound.decision_scope_loader(bound.candles[0])


@pytest.mark.parametrize(
    "field,value",
    [
        ("strategy_id", "other"),
        ("product_id", "BINANCE:BTCUSDT-SPOT"),
        ("timeframe", "5m"),
        ("lookback_window", 3),
        ("profile_requirements", ()),
    ],
)
def test_strategy_contract_mismatch(field, value, monkeypatch):
    plan_value, strategy, identity, suffixes, _, _ = setup()
    if field in ("strategy_id", "product_id"):
        setattr(strategy, field, value)
    else:
        changed = replace(strategy.requirements, **{field: value})
        monkeypatch.setattr(type(strategy), "requirements", property(lambda _: changed))
    with pytest.raises(BootstrapSeedError):
        BoundBootstrapHydration(plan_value, strategy, identity, suffixes)


def test_other_valid_pinned_input_is_not_substituted():
    original, strategy, identity, suffixes, _, _ = setup(
        ProfileDecisionStatus.MISSING, 1
    )
    outcome, pinned = original.recorded[0]
    assert pinned is not None
    changed = replace(
        pinned,
        context=replace(
            pinned.context,
            profiles=(
                replace(pinned.context.profiles[0], reason="BACKEND_UNAVAILABLE"),
            ),
        ),
    )
    changed_outcome = replace(outcome, input_digest=changed.input_digest)
    changed_plan = plan(original.seed, ((changed_outcome, changed),))
    with pytest.raises(BootstrapSeedError):
        BoundBootstrapHydration(changed_plan, strategy, identity, suffixes)


@pytest.mark.parametrize("pin_mode", ["absent", "orphan", "referenced"])
def test_skipped_pin_modes_without_input_read(pin_mode):
    original, strategy, identity, _, store, cache = setup(count=0)
    skipped, _ = suffix(original.seed, skipped=True)
    _, orphan = suffix(original.seed)
    assert orphan is not None
    if pin_mode == "referenced":
        skipped = replace(
            skipped, input_id=orphan.input_id, input_digest=orphan.input_digest
        )
        with pytest.raises(BootstrapSeedError):
            plan(original.seed, ((replace(skipped, input_digest="d" * 64), orphan),))
    rows = ((skipped, orphan if pin_mode != "absent" else None),)
    selected = plan(original.seed, rows)
    candle = make_candle().model_copy(update={"timestamp": original.seed.cutover_ms})
    batch = MarketDataDecisionBatch(
        "live",
        "deployment",
        candle.product_id,
        candle.timeframe,
        candle.timestamp,
        (skipped.key,),
        (skipped,),
    )
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="deployment",
        identity_resolver=lambda _: identity,
        cache=cache,
        input_store=store,
        utc_ms=lambda: pytest.fail("clock"),
        monotonic_ms=lambda: pytest.fail("clock"),
    )
    prepared = owner.prepare_replay_candle(strategy, candle, batch)
    assert prepared.pinned_input is None
    bound = BoundBootstrapHydration(selected, strategy, identity, ((candle, prepared),))
    with pytest.raises(StrategyDecisionSkipped):
        with bound.decision_scope_loader(candle)(strategy, candle, None):
            pytest.fail("skipped callback must never start")
    store.get.assert_not_called()
    store.pin_confirmed.assert_not_called()
