"""Initial preparation rejects drift without activating or executing strategies."""

import ast
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.core import bootstrap_hydration_reader as owner
from src.core.market_data.profiles.bootstrap_seed import (
    BootstrapSeed,
    BootstrapSeedError,
)
from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedRecord,
    BootstrapSeedPinResult,
    BootstrapSeedPinStatus,
)
from test_bootstrap_initial_seed import setup
from test_bootstrap_hydration_reader import harness
from test_profile_context_enrichment import B, S, Collection, fixture
from test_profile_recorded_decision_owner import NOW


def shifted(value):
    candles = []
    for candle in value.candles:
        decision = candle.context.decision_time_ms + 60000
        item = candle.context.profiles[0]
        item = replace(
            item,
            decision_time_ms=decision,
            request=replace(item.request, as_of_ms=decision),
        )
        candles.append(
            replace(
                candle,
                bar_start_ms=candle.bar_start_ms + 60000,
                context=Collection(decision, (item,)),
            )
        )
    return replace(
        value,
        cutover_ms=value.cutover_ms + 60000,
        candles=tuple(candles),
        max_seed_candles=10,
    )


@pytest.mark.parametrize(
    "field", ["type", "key", "cutover", "requirements", "lookback"]
)
def test_valid_candidate_independent_mismatch_never_pins(field):
    call, seed, store, _, factory, *_ = setup()
    candidate = seed
    if field == "type":
        candidate = None
    elif field == "key":
        candidate = replace(
            seed, key=replace(seed.key, strategy_id="other"), max_seed_candles=10
        )
    elif field == "cutover":
        candidate = shifted(seed)
    elif field == "lookback":
        candidate = replace(
            seed, lookback=1, candles=seed.candles[-1:], max_seed_candles=10
        )
    else:
        candles = []
        for candle in seed.candles:
            item = candle.context.profiles[0]
            item = replace(
                item,
                request=replace(item.request, product_id="BINANCE:ETHUSDT-SPOT"),
                status=S.MISSING,
                reason="PROFILE_NOT_READY",
                profile=None,
                available_at_ms=None,
            )
            candles.append(
                replace(candle, context=Collection(item.decision_time_ms, (item,)))
            )
        candidate = replace(
            seed,
            requirements=(
                replace(seed.requirements[0], product_id="BINANCE:ETHUSDT-SPOT"),
            ),
            candles=tuple(candles),
            max_seed_candles=10,
        )
    if candidate is not None:
        assert (
            BootstrapSeed.from_canonical_bytes(
                candidate.canonical_bytes, max_seed_candles=10
            )
            == candidate
        )
        identities = ("key", "cutover_ms", "requirements", "lookback")
        changed = "cutover_ms" if field == "cutover" else field
        assert [
            name
            for name in identities
            if getattr(candidate, name) != getattr(seed, name)
        ] == [changed]
    factory.return_value = candidate
    with pytest.raises(owner.BootstrapHydrationReaderError):
        call()
    store.pin_confirmed.assert_not_called()


class Halt(BaseException):
    pass


@pytest.mark.parametrize("phase", ["lookup", "history", "factory"])
@pytest.mark.parametrize("error_type", [RuntimeError, Halt])
def test_dependency_errors_propagate_identity_without_pin(phase, error_type):
    call, _, store, history, factory, *_ = setup()
    error = error_type("SECRET")
    {"lookup": store.get, "history": history, "factory": factory}[
        phase
    ].side_effect = error
    with pytest.raises(error_type) as caught:
        call()
    assert caught.value is error
    store.pin_confirmed.assert_not_called()
    if phase == "lookup":
        history.assert_not_called()
    if phase != "factory":
        factory.assert_not_called()


@pytest.mark.parametrize("different", [False, True])
def test_confirmed_canonical_winner_preserves_modeled_provenance(different):
    call, seed, store, *_ = setup()
    winner = BootstrapSeed.from_canonical_bytes(
        seed.canonical_bytes, max_seed_candles=10
    )
    assert winner is not seed
    if different:
        winner = replace(winner, dataset_digest="f" * 64, max_seed_candles=10)
    record = BootstrapSeedRecord(winner, NOW, True)
    store.pin_confirmed.return_value = BootstrapSeedPinResult(
        BootstrapSeedPinStatus.CONFIRMED, record
    )
    if different:
        with pytest.raises(owner.BootstrapHydrationReaderError):
            call()
    else:
        actual = call()
        assert actual is record
        assert actual.value.canonical_bytes == seed.canonical_bytes
        assert actual.value.availability_policy_id == seed.availability_policy_id
        assert (
            actual.value.availability_policy_digest == seed.availability_policy_digest
        )
        assert actual.value.dataset_digest == seed.dataset_digest
        assert tuple(c.context.canonical_bytes for c in actual.value.candles) == tuple(
            c.context.canonical_bytes for c in seed.candles
        )


def test_live_evidence_cannot_become_seed_or_reach_pin():
    call, seed, store, _, factory, *_ = setup()
    live, _ = fixture(B.LIVE_OBSERVED, S.MISSING)
    candle = seed.candles[0]
    live = replace(live, decision_time_ms=candle.context.decision_time_ms)

    def invalid_factory(key):
        assert key == seed.key
        return replace(
            seed,
            candles=(
                replace(candle, context=Collection(live.decision_time_ms, (live,))),
                *seed.candles[1:],
            ),
            max_seed_candles=10,
        )

    factory.side_effect = invalid_factory
    with pytest.raises(BootstrapSeedError):
        call()
    store.pin_confirmed.assert_not_called()


def test_no_profile_requirements_do_not_lookup_or_create_seed(monkeypatch):
    reader, plan, strategy, store, *_ = harness(count=0)
    requirements = replace(strategy.requirements, profile_requirements=())
    monkeypatch.setattr(
        type(strategy), "requirements", property(lambda _: requirements)
    )
    history, factory = MagicMock(), MagicMock()
    with pytest.raises(owner.BootstrapHydrationReaderError):
        reader.prepare_initial_seed(
            strategy, plan.seed.cutover_ms, history_reader=history, factory=factory
        )
    assert not store.mock_calls and not history.mock_calls and not factory.mock_calls


def test_initial_method_has_no_execution_or_clock_dependencies():
    tree = ast.parse(Path(owner.__file__).read_text())
    method = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "prepare_initial_seed"
    )
    forbidden = {
        "hydrate_candles",
        "warm_up",
        "publish",
        "on_candle",
        "prepare",
        "live_requests",
        "decision_many",
        "latest",
        "utc_ms",
        "monotonic_ms",
        "time",
        "time_ns",
        "monotonic",
        "Thread",
        "start",
    }
    assert not any(
        isinstance(n, ast.Attribute)
        and n.attr in forbidden
        or isinstance(n, ast.Name)
        and n.id in forbidden
        for n in ast.walk(method)
    )
