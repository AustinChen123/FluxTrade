from contextlib import AbstractContextManager
from dataclasses import replace

import pytest

from src.core.market_data.profiles.decision_application import MarketDataDecisionKey
from src.core.market_data.profiles.decision_batch_builder import (
    DecisionBatchBuildError,
    DecisionBatchBuilder,
)
from test_signal_processor import make_candle


def key(strategy_id="strategy_v1"):
    candle = make_candle()
    return MarketDataDecisionKey.for_candle(
        environment="live",
        execution_scope_id="live-berlin-1",
        strategy_id=strategy_id,
        strategy_version="1.2.0",
        config_hash="a" * 64,
        product_id=candle.product_id,
        timeframe=candle.timeframe,
        bar_start_ms=candle.timestamp,
    )


def builder():
    return DecisionBatchBuilder(
        environment="live",
        execution_scope_id="live-berlin-1",
        candle=make_candle(),
    )


def applied_scope(owner, value, context="enriched") -> AbstractContextManager[str]:
    return owner.applied_scope(
        key=value,
        input_id=value.input_id,
        input_digest="b" * 64,
        context=context,
    )


def test_empty_builder_returns_none_and_seals():
    owner = builder()
    assert owner.build() is None
    with pytest.raises(DecisionBatchBuildError):
        owner.build()
    with pytest.raises(DecisionBatchBuildError):
        owner.record_skipped(key=key(), reason="INPUT_STORE_FAILED")


def test_applied_is_recorded_only_after_normal_callback_exit():
    owner = builder()
    value = key()
    events = []
    with applied_scope(owner, value) as context:
        events.append(context)
        with pytest.raises(DecisionBatchBuildError):
            owner.build()
    batch = owner.build()
    assert events == ["enriched"]
    assert batch is not None
    assert batch.participants == (value,)
    assert batch.outcomes[0].disposition == "APPLIED"
    assert batch.outcomes[0].input_id == value.input_id


@pytest.mark.parametrize("failure", [RuntimeError("failure"), KeyboardInterrupt()])
def test_started_callback_failure_never_becomes_terminal(failure):
    owner = builder()
    value = key()
    with pytest.raises(type(failure)) as caught:
        with applied_scope(owner, value):
            raise failure
    assert caught.value is failure
    with pytest.raises(DecisionBatchBuildError):
        owner.build()


@pytest.mark.parametrize(
    "reason,input_refs",
    [
        ("INPUT_STORE_FAILED", False),
        ("INPUT_COMMIT_UNCONFIRMED", False),
        ("INPUT_STORE_FAILED", True),
        ("INPUT_COMMIT_UNCONFIRMED", True),
    ],
)
def test_explicit_skip_is_terminal_without_callback(reason, input_refs):
    owner = builder()
    value = key()
    owner.record_skipped(
        key=value,
        reason=reason,
        input_id=value.input_id if input_refs else None,
        input_digest="b" * 64 if input_refs else None,
    )
    batch = owner.build()
    assert batch is not None
    assert batch.outcomes[0].disposition == "SKIPPED"
    assert batch.outcomes[0].reason == reason


def test_mixed_outcomes_are_complete_and_canonical():
    owner = builder()
    first, second = key("z_strategy"), key("a_strategy")
    with applied_scope(owner, first):
        pass
    owner.record_skipped(key=second, reason="INPUT_STORE_FAILED")
    batch = owner.build()
    assert batch is not None
    assert [value.strategy_id for value in batch.participants] == [
        "a_strategy",
        "z_strategy",
    ]
    assert [value.key.strategy_id for value in batch.outcomes] == [
        "a_strategy",
        "z_strategy",
    ]


@pytest.mark.parametrize(
    "changed",
    [
        {"environment": "paper"},
        {"execution_scope_id": "other"},
        {"product_id": "BINANCE:ETHUSDT-SPOT"},
        {"trigger_id": "5m:0"},
    ],
)
def test_scope_mismatch_is_rejected_before_callback(changed):
    owner = builder()
    with pytest.raises(DecisionBatchBuildError):
        applied_scope(owner, replace(key(), **changed))
    assert owner.build() is None


def test_duplicate_strategy_and_invalid_outcome_do_not_overwrite():
    owner = builder()
    value = key()
    with applied_scope(owner, value):
        pass
    with pytest.raises(DecisionBatchBuildError):
        owner.record_skipped(key=value, reason="INPUT_STORE_FAILED")
    batch = owner.build()
    assert batch is not None and batch.outcomes[0].disposition == "APPLIED"

    other = builder()
    with pytest.raises(DecisionBatchBuildError):
        other.record_skipped(key=value, reason="PROFILE_NOT_READY")
    assert other.build() is None


@pytest.mark.parametrize(
    "input_id,input_digest",
    [
        ("c" * 64, "b" * 64),
        ("bad", "b" * 64),
        (key().input_id, "bad"),
        (type("HostileString", (str,), {})(key().input_id), "b" * 64),
    ],
)
def test_invalid_applied_input_is_rejected_before_callback_admission(
    input_id, input_digest
):
    owner = builder()
    value = key()
    callbacks = []
    with pytest.raises(DecisionBatchBuildError):
        scope = owner.applied_scope(
            key=value,
            input_id=input_id,
            input_digest=input_digest,
            context="must-not-run",
        )
        with scope:
            callbacks.append("callback")
    assert callbacks == []
    assert owner.build() is None


def test_participant_limit_is_rejected_before_extra_callback():
    owner = builder()
    callback_count = 0
    for index in range(256):
        value = key(f"strategy_{index}")
        with applied_scope(owner, value):
            callback_count += 1
    with pytest.raises(DecisionBatchBuildError):
        applied_scope(owner, key("strategy_256"))
    batch = owner.build()
    assert batch is not None
    assert callback_count == len(batch.participants) == 256
