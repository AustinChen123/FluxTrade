from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from src.core.market_data.profiles.modeled_input import (
    ModeledProfileInput,
    ModeledProfileInputError,
    modeled_profile_warmup_scope_loader,
)
from src.core.market_data.profiles.decision_context import (
    StrategyMarketDataContext,
    ProfileDecisionStatus,
)
from src.core.signal_processor import SignalProcessor
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import StrategyRequirements
from test_profile_modeled_input import modeled, Provider, POLICY, DAY
from test_profile_context_enrichment import context
from test_signal_processor import DummyStrategy, make_candle, make_signal


def setup(status=ProfileDecisionStatus.FRESH, timeframe="1m", timestamp=DAY):
    item, requirement = modeled(status)
    duration = 60000 if timeframe == "1m" else 300000
    decision = timestamp + duration
    item = replace(
        item,
        decision_time_ms=decision,
        request=replace(item.request, as_of_ms=decision),
        available_at_ms=DAY if status is ProfileDecisionStatus.FRESH else None,
    )
    provider = Provider(StrategyMarketDataContext(decision, (item,)))

    class Strategy(DummyStrategy):
        @property
        def requirements(self):
            return StrategyRequirements(
                self.product_id, timeframe, 1, profile_requirements=(requirement,)
            )

        def on_candle(self, candle, context=None):
            seen.append(context)
            if failure:
                raise failure[0]
            return make_signal()

    seen, failure = [], []
    strategy = Strategy("s1")
    candle = make_candle().model_copy(
        update=dict(timestamp=timestamp, timeframe=timeframe)
    )
    original = replace(
        context(), strategy_id="s1", product_id=candle.product_id, timestamp=timestamp
    )
    execution = MagicMock()
    processor = SignalProcessor(
        StrategyRegistry(), execution, strategy_context_loader=lambda *_: original
    )
    loader = modeled_profile_warmup_scope_loader(ModeledProfileInput(provider, POLICY))
    return (
        strategy,
        candle,
        original,
        provider,
        processor,
        execution,
        loader,
        seen,
        failure,
    )


@pytest.mark.parametrize("status", list(ProfileDecisionStatus))
@pytest.mark.parametrize(
    "timeframe,timestamp", [("1m", DAY), ("1m", DAY - 60000), ("5m", DAY - 300000)]
)
def test_real_warmup_enrichment_and_no_signals(status, timeframe, timestamp):
    strategy, candle, original, provider, processor, execution, loader, seen, _ = setup(
        status, timeframe, timestamp
    )
    processor.warm_up(strategy, [candle], decision_scope_loader=loader)
    assert len(seen) == 1 and seen[0].market_data is provider.result
    assert seen[0].market_data.profiles[0].status is status
    assert (
        replace(seen[0], market_data=None) == original and original.market_data is None
    )
    execution.execute_signal.assert_not_called()
    first = seen[0].market_data.canonical_bytes
    processor.warm_up(strategy, [candle], decision_scope_loader=loader)
    assert seen[1].market_data.canonical_bytes == first
    assert len(provider.calls) == 2


def test_no_profile_passthrough_zero_provider_and_callback_failure():
    (
        strategy,
        candle,
        original,
        provider,
        processor,
        execution,
        loader,
        seen,
        failure,
    ) = setup()
    plain = DummyStrategy("plain")
    with loader(candle)(plain, candle, original) as received:
        assert received is original
    assert provider.calls == []
    error = RuntimeError("callback")
    failure.append(error)
    with pytest.raises(RuntimeError) as caught:
        processor.warm_up(strategy, [candle], decision_scope_loader=loader)
    assert caught.value is error and len(seen) == 1
    execution.execute_signal.assert_not_called()
    assert original.market_data is None


@pytest.mark.parametrize(
    "damage",
    ["copy", "context_id", "context_time", "existing", "policy", "dataset", "coverage"],
)
def test_fail_closed_before_callback(damage):
    strategy, candle, original, provider, processor, execution, loader, seen, _ = (
        setup()
    )
    if damage == "copy":
        with pytest.raises(ModeledProfileInputError):
            loader(candle)(strategy, candle.model_copy(), original)
        assert provider.calls == []
        return
    if damage == "context_id":
        original = replace(original, strategy_id="other")
    elif damage == "context_time":
        original = replace(original, timestamp=original.timestamp + 1)
    elif damage == "existing":
        original = replace(original, market_data=provider.result)
    elif damage == "policy":
        provider.availability_policy_id = "other"
    elif damage == "dataset":
        provider.dataset_digest = "b" * 64
    elif damage == "coverage":
        assert isinstance(provider.result, StrategyMarketDataContext)
        provider.result = StrategyMarketDataContext(
            provider.result.decision_time_ms, ()
        )
    processor.strategy_context_loader = lambda *_: original
    with pytest.raises(ModeledProfileInputError):
        processor.warm_up(strategy, [candle], decision_scope_loader=loader)
    assert seen == []
    execution.execute_signal.assert_not_called()


@pytest.mark.parametrize("timestamp", [-1, True, 2**63 - 1])
def test_invalid_candle_time_before_provider(timestamp):
    _, candle, _, provider, _, _, loader, _, _ = setup()
    with pytest.raises(ModeledProfileInputError):
        loader(candle.model_copy(update={"timestamp": timestamp}))
    assert provider.calls == []


def test_provider_exception_and_strategy_scope_fail_before_callback():
    strategy, candle, original, provider, processor, execution, loader, seen, _ = (
        setup()
    )
    wrong = candle.model_copy(update={"product_id": "BINANCE:ETHUSDT-PERP"})
    with pytest.raises(ModeledProfileInputError):
        loader(wrong)(strategy, wrong, original)
    assert provider.calls == []
    error = RuntimeError("provider failure")
    provider.result = error
    with pytest.raises(RuntimeError) as caught:
        processor.warm_up(strategy, [candle], decision_scope_loader=loader)
    assert caught.value is error and seen == []
    execution.execute_signal.assert_not_called()


def test_no_profile_real_warmup_does_not_resolve():
    _, candle, _, provider, processor, execution, loader, _, _ = setup()
    plain = DummyStrategy("plain", result=make_signal())
    processor.warm_up(plain, [candle], decision_scope_loader=loader)
    assert plain.candles_received == [candle] and provider.calls == []
    execution.execute_signal.assert_not_called()
