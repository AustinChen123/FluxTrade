import ast
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typing import Any, cast

from src.core.bootstrap_hydration_reader import (
    BootstrapHydrationReader,
    BootstrapHydrationReaderError,
)
from src.core.market_data.profiles.bootstrap_seed_store import BootstrapSeedRecord
from src.core.market_data.profiles.decision_application import MarketDataDecisionBatch
from src.core.market_data.profiles.decision_application_store import DecisionBatchRecord
from src.core.market_data.profiles.decision_input_store import DecisionInputRecord
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwner
from src.core.signal_processor import StrategyDecisionSkipped
from test_profile_bootstrap_binding import setup
from test_profile_bootstrap_seed import suffix as recorded_suffix
from test_profile_recorded_decision_owner import NOW
from test_profile_context_enrichment import context


def harness(count=3, referenced=False):
    plan, strategy, identity, suffix, inputs, cache = setup(count=count)
    seed_store, application, db = MagicMock(), MagicMock(), MagicMock()
    seed_store.get.return_value = BootstrapSeedRecord(plan.seed, NOW, True)
    records = []
    by_key = {}
    for index, ((outcome, pinned), (candle, _)) in enumerate(
        zip(plan.recorded, suffix, strict=True)
    ):
        if outcome.disposition == "SKIPPED" and referenced:
            _, pinned = recorded_suffix(plan.seed, index=index)
            assert pinned is not None
            outcome = replace(
                outcome, input_id=pinned.input_id, input_digest=pinned.input_digest
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
        records.append((candle, DecisionBatchRecord(batch, NOW, True, (pinned,))))
        if pinned is not None:
            by_key[pinned.key] = DecisionInputRecord(pinned, NOW, True)
    inputs.get.side_effect = lambda key: by_key.get(key)
    inputs.reset_mock()
    application.read_applied_candle.side_effect = records
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="deployment",
        identity_resolver=lambda _: identity,
        cache=cache,
        input_store=inputs,
        utc_ms=lambda: pytest.fail("clock"),
        monotonic_ms=lambda: pytest.fail("clock"),
    )
    events = []

    @contextmanager
    def sessions():
        events.append("enter")
        try:
            yield db
        finally:
            events.append("closed")

    reader = BootstrapHydrationReader(
        db_session_factory=sessions,
        seed_store=seed_store,
        application=application,
        decision_owner=owner,
        environment="live",
        identity_resolver=lambda _: identity,
        max_seed_candles=10,
        max_recorded_candles=3,
    )
    return reader, plan, strategy, seed_store, application, inputs, events


@pytest.mark.parametrize(
    "mode,count",
    [
        ("BEFORE_PENDING", 0),
        ("BEFORE_PENDING", 1),
        ("BEFORE_PENDING", 3),
        ("THROUGH_APPLIED", 1),
        ("THROUGH_APPLIED", 3),
    ],
)
@pytest.mark.parametrize("referenced", [False, True])
def test_boundaries_and_detached_scopes(mode, count, referenced):
    reader, plan, strategy, seeds, application, inputs, events = harness(
        count, referenced
    )
    boundary = plan.seed.cutover_ms + (count - (mode == "THROUGH_APPLIED")) * 60000
    bound = reader.prepare(strategy, boundary, mode)
    assert len(bound.candles) == 2 + count
    assert events == (["enter", "closed"] if count else [])
    seeds.get.assert_called_once_with(plan.seed.key)
    seeds.pin.assert_not_called()
    assert application.read_applied_candle.call_count == count
    assert inputs.get.call_count == count - (count > 1)
    for index, call in enumerate(application.read_applied_candle.call_args_list):
        assert call.kwargs["bar_start_ms"] == plan.seed.cutover_ms + index * 60000
    seeds.get.side_effect = inputs.get.side_effect = (
        application.read_applied_candle.side_effect
    ) = AssertionError("I/O after prepare")
    for index, candle in enumerate(bound.candles):
        base = replace(
            context(),
            strategy_id=strategy.strategy_id,
            product_id=candle.product_id,
            timestamp=candle.timestamp,
        )
        if index == 3:
            with pytest.raises(StrategyDecisionSkipped):
                with bound.decision_scope_loader(candle)(strategy, candle, base):
                    pytest.fail("skip")
        else:
            with bound.decision_scope_loader(candle)(
                strategy, candle, base
            ) as enriched:
                assert enriched is not None and enriched.market_data is not None


@pytest.mark.parametrize("delta", [-60000, -1, 1, 4 * 60000])
def test_boundary_limit_before_suffix_io(delta):
    reader, plan, strategy, _, application, _, events = harness()
    with pytest.raises(BootstrapHydrationReaderError):
        reader.prepare(strategy, plan.seed.cutover_ms + delta)
    application.read_applied_candle.assert_not_called()
    assert events == []


@pytest.mark.parametrize("damage", ["missing", "key", "requirements", "lookback"])
def test_seed_mismatch_before_suffix_io(damage, monkeypatch):
    reader, plan, strategy, seeds, application, _, events = harness()
    if damage == "missing":
        seeds.get.return_value = None
    else:
        seed = plan.seed
        if damage == "key":
            seed = replace(
                seed, key=replace(seed.key, strategy_id="other"), max_seed_candles=10
            )
        elif damage == "lookback":
            seed = replace(seed, lookback=0, candles=(), max_seed_candles=10)
        else:
            changed = replace(
                strategy.requirements,
                profile_requirements=(
                    replace(seed.requirements[0], output_grid_id="other"),
                ),
            )
            monkeypatch.setattr(
                type(strategy), "requirements", property(lambda _: changed)
            )
            assert seed.lookback == strategy.requirements.lookback_window
            assert seed.requirements != strategy.requirements.profile_requirements
        seeds.get.return_value = BootstrapSeedRecord(seed, NOW, True)
    with pytest.raises(BootstrapHydrationReaderError):
        reader.prepare(strategy, plan.seed.cutover_ms)
    application.read_applied_candle.assert_not_called()
    assert events == []


@pytest.mark.parametrize("target_present", [True, False])
def test_multi_strategy_record_pairs_target_at_nonzero_index(target_present):
    reader, plan, strategy, _, application, _, events = harness(count=1)
    original = next(application.read_applied_candle.side_effect)
    candle, record = original
    target = record.batch.outcomes[0]
    pinned = record.verified_inputs[0]
    assert pinned is not None
    other_key = replace(target.key, strategy_id="a_before_target")
    other_input = replace(pinned, key=other_key)
    other = replace(
        target,
        key=other_key,
        input_id=other_input.input_id,
        input_digest=other_input.input_digest,
    )
    if target_present:
        outcomes = (other, target)
        inputs = (other_input, pinned)
    else:
        second_key = replace(other_key, strategy_id="b_also_not_target")
        second_input = replace(pinned, key=second_key)
        second = replace(
            target,
            key=second_key,
            input_id=second_input.input_id,
            input_digest=second_input.input_digest,
        )
        outcomes = (other, second)
        inputs = (other_input, second_input)
    multi = replace(
        record.batch,
        participants=tuple(item.key for item in outcomes),
        outcomes=outcomes,
    )
    assert multi.outcomes == outcomes
    multi_record = DecisionBatchRecord(multi, NOW, True, inputs)
    application.read_applied_candle.side_effect = [(candle, multi_record)]
    if not target_present:
        with pytest.raises(BootstrapHydrationReaderError):
            reader.prepare(strategy, plan.seed.cutover_ms + 60000)
    else:
        assert multi.outcomes[1] is target
        bound = reader.prepare(strategy, plan.seed.cutover_ms + 60000)
        assert bound.plan.recorded == ((target, pinned),)
        assert bound.plan.recorded[0][0] is target
        assert bound.plan.recorded[0][1] is pinned
        assert bound.suffix[0][1].outcome is target
        assert bound.suffix[0][1].pinned_input is pinned
    assert application.read_applied_candle.call_count == 1
    assert events == ["enter", "closed"]


def test_suffix_failure_closes_session_without_fallback():
    reader, plan, strategy, seeds, application, _, events = harness()
    error = RuntimeError("corrupt receipt")
    application.read_applied_candle.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        reader.prepare(strategy, plan.seed.cutover_ms + 60000)
    assert caught.value is error and events == ["enter", "closed"]
    seeds.pin.assert_not_called()


def test_dependency_boundary():
    source = (
        Path(__file__).resolve().parents[1] / "src/core/bootstrap_hydration_reader.py"
    )
    tree = ast.parse(source.read_text())
    imports = {
        name
        for node in ast.walk(tree)
        for name in (
            [node.module or "", *(alias.name for alias in node.names)]
            if isinstance(node, ast.ImportFrom)
            else [alias.name for alias in node.names]
            if isinstance(node, ast.Import)
            else []
        )
    }
    assert not any(
        any(
            word in name.split(".")
            for word in (
                "engine",
                "main",
                "strategy_registry",
                "snapshot_cache",
                "snapshot_client",
                "time",
                "asyncio",
            )
        )
        for name in imports
    )
    initial = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "prepare_initial_seed"
    )
    pins = [
        node
        for node in ast.walk(initial)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "pin_confirmed"
    ]
    assert (
        len(pins) == 1
        and ast.unparse(pins[0]) == "self._seeds.pin_confirmed(candidate)"
    )
    allowed_pin = pins[0].func
    assert not any(
        isinstance(node, ast.Attribute)
        and node is not allowed_pin
        and node.attr
        in ("pin", "pin_confirmed", "hydrate_candles", "warm_up", "publish")
        for node in ast.walk(tree)
    )


@pytest.mark.parametrize(
    "boundary,mode",
    [
        (True, "BEFORE_PENDING"),
        (-1, "BEFORE_PENDING"),
        (2**63, "BEFORE_PENDING"),
        (0, "OTHER"),
        (0, True),
    ],
)
def test_invalid_caller_before_seed_read(boundary, mode):
    reader, _, strategy, seeds, application, _, events = harness()
    with pytest.raises(BootstrapHydrationReaderError):
        reader.prepare(strategy, boundary, cast(Any, mode))
    seeds.get.assert_not_called()
    application.read_applied_candle.assert_not_called()
    assert events == []


def test_wrong_identity_before_seed_read():
    reader, plan, strategy, seeds, application, _, events = harness()
    reader._identity = MagicMock(return_value=None)
    with pytest.raises(BootstrapHydrationReaderError):
        reader.prepare(strategy, plan.seed.cutover_ms)
    seeds.get.assert_not_called()
    application.read_applied_candle.assert_not_called()
    assert events == []
