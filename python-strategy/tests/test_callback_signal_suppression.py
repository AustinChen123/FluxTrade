"""Post-callback suppression preserves state but removes downstream signals."""

from contextlib import nullcontext
import json
from unittest.mock import MagicMock

import pytest

from src.core.signal_processor import SignalProcessor
from src.core.strategy_registry import StrategyRegistry
from src.core.market_data.profiles.decision_batch_builder import DecisionBatchBuildError
from test_profile_decision_batch_builder import builder, key
from test_signal_processor import DummyStrategy, make_candle, make_signal


def processor(strategy, handler):
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock(default_quantity=1)
    return SignalProcessor(registry, execution, signal_handler=handler)


@pytest.mark.parametrize("suppressed", [False, True])
def test_builder_suppresses_before_idempotency_portfolio_and_handler(
    monkeypatch, suppressed
):
    strategy = DummyStrategy("s1", result=make_signal())
    handler = MagicMock()
    owner, value = builder(), key("s1")
    scope = owner.applied_scope(
        key=value,
        input_id=value.input_id,
        input_digest="b" * 64,
        context=None,
        signal_suppressed=suppressed,
        suppression_reason="SNAPSHOT_REVOKED" if suppressed else None,
    )
    runner = processor(strategy, handler)
    identities = MagicMock(wraps=runner._with_market_idempotency)
    monkeypatch.setattr(runner, "_with_market_idempotency", identities)
    portfolio = MagicMock()
    portfolio.decision_state_transaction.return_value = nullcontext()
    portfolio.coordinate_candle_decisions.side_effect = (
        lambda _, decisions, **kwargs: decisions
    )
    runner.portfolio_coordinator = portfolio
    candle = make_candle()
    runner.on_candle(candle, decision_scope=lambda *_: scope)
    assert strategy.candles_received == [candle]  # Callback state survives suppression.
    assert identities.call_count == handler.call_count == (0 if suppressed else 1)
    assert len(portfolio.coordinate_candle_decisions.call_args.args[1][0][1]) == (
        0 if suppressed else 1
    )
    runner.execution_engine.execute_signal.assert_not_called()
    batch = owner.build()
    assert batch is not None
    outcome = batch.outcomes[0]
    assert outcome.disposition == "APPLIED" and outcome.signal_suppressed is suppressed
    encoded = json.loads(batch.canonical_bytes)["outcomes"][0]
    assert encoded["signal_suppressed"] is suppressed
    assert encoded["suppression_reason"] == ("SNAPSHOT_REVOKED" if suppressed else None)


class Stop(BaseException):
    pass


@pytest.mark.parametrize("failure", [None, RuntimeError("callback"), Stop("callback")])
def test_applied_scope_is_one_shot_even_after_failure(failure):
    owner, value = builder(), key()
    scope = owner.applied_scope(
        key=value, input_id=value.input_id, input_digest="b" * 64, context="context"
    )
    with pytest.raises(DecisionBatchBuildError):
        scope.__exit__(None, None, None)
    with pytest.raises(DecisionBatchBuildError):
        owner.build()  # Exit before enter cannot manufacture APPLIED.
    assert scope.__enter__() == "context"
    with pytest.raises(DecisionBatchBuildError):
        scope.__enter__()
    assert scope.__exit__(type(failure) if failure else None, failure, None) is False
    with pytest.raises(DecisionBatchBuildError):
        scope.__exit__(None, None, None)  # Failure cannot be rewritten as success.
    with pytest.raises(DecisionBatchBuildError):
        scope.__enter__()
    if failure is not None:
        with pytest.raises(DecisionBatchBuildError):
            owner.build()
    else:
        batch = owner.build()
        assert batch is not None and len(batch.outcomes) == 1
        assert batch.outcomes[0].disposition == "APPLIED"


@pytest.mark.parametrize("error", [RuntimeError("callback"), Stop("callback")])
def test_callback_failure_never_records_or_filters(error):
    owner, value = builder(), key("s1")
    manager = owner.applied_scope(
        key=value,
        input_id=value.input_id,
        input_digest="b" * 64,
        context=None,
        signal_suppressed=True,
        suppression_reason="SNAPSHOT_REVOKED",
    )
    policy = MagicMock(side_effect=AssertionError("policy after failure"))
    setattr(manager, "filter_callback_signals", policy)
    strategy = DummyStrategy("s1")
    strategy.on_candle = MagicMock(side_effect=error)
    with pytest.raises(type(error)) as caught:
        processor(strategy, MagicMock()).on_candle(
            make_candle(), decision_scope=lambda *_: manager
        )
    assert caught.value is error
    policy.assert_not_called()
    with pytest.raises(DecisionBatchBuildError):
        owner.build()


@pytest.mark.parametrize(
    "result",
    [
        None,
        True,
        {},
        [None],
        (make_signal(), None),
        type("Rows", (list,), {})([make_signal()]),
    ],
)
def test_invalid_policy_output_fails_before_downstream(result):
    manager = MagicMock()
    manager.__enter__.return_value = None
    manager.filter_callback_signals.return_value = result
    handler = MagicMock()
    strategy = DummyStrategy("s1", result=make_signal())
    with pytest.raises(TypeError, match="invalid callback signal policy result"):
        processor(strategy, handler).on_candle(
            make_candle(), decision_scope=lambda *_: manager
        )
    assert len(strategy.candles_received) == 1
    manager.__exit__.assert_called_once_with(None, None, None)
    handler.assert_not_called()


@pytest.mark.parametrize("stage", ["exit", "policy"])
def test_successful_callback_then_base_exception_is_not_swallowed(stage):
    events, failure = [], Stop("stop")

    class Scope:
        def __enter__(self):
            events.append("enter")

        def __exit__(self, *_):
            events.append("exit")
            if stage == "exit":
                raise failure

        def filter_callback_signals(self, signals):
            events.append("policy")
            raise failure

    strategy, handler = DummyStrategy("s1", result=make_signal()), MagicMock()
    with pytest.raises(Stop) as caught:
        processor(strategy, handler).on_candle(
            make_candle(), decision_scope=lambda *_: Scope()
        )
    assert caught.value is failure and len(strategy.candles_received) == 1
    assert events == (
        ["enter", "exit"] if stage == "exit" else ["enter", "exit", "policy"]
    )
    handler.assert_not_called()


@pytest.mark.parametrize("kind", ["none", "nullcontext", "tuple"])
def test_default_and_exact_tuple_policy_preserve_signal(kind):
    handler = MagicMock()
    runner = processor(DummyStrategy("s1", result=make_signal()), handler)
    manager = nullcontext()
    if kind == "tuple":
        setattr(manager, "filter_callback_signals", tuple)
    runner.on_candle(
        make_candle(), decision_scope=None if kind == "none" else lambda *_: manager
    )
    handler.assert_called_once()


@pytest.mark.parametrize(
    "suppressed,reason",
    [(1, None), (True, None), (False, "SNAPSHOT_REVOKED"), (True, "SECRET")],
)
def test_builder_closed_suppression_contract(suppressed, reason):
    owner, value = builder(), key()
    with pytest.raises(DecisionBatchBuildError):
        owner.applied_scope(
            key=value,
            input_id=value.input_id,
            input_digest="b" * 64,
            context=None,
            signal_suppressed=suppressed,
            suppression_reason=reason,
        )
    assert owner.build() is None
