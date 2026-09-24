from dataclasses import replace
from unittest.mock import MagicMock, call

import pytest

from src.core.runtime_environment import RuntimeEnvironment
from test_profile_decision_input import sample
from test_signal_processor import DummyStrategy, make_candle


class _ProfileStrategy(DummyStrategy):
    __fluxtrade_readiness__ = "LIVE_APPROVED"

    @property
    def requirements(self):
        return replace(
            super().requirements,
            profile_requirements=sample().requirements,
        )


def test_decision_owner_requires_base_context_loader(engine_factory):
    with pytest.raises(
        ValueError, match="^market data decisions require a context loader$"
    ):
        engine_factory(market_data_decision_owner=MagicMock())


def test_bootstrap_reader_capability_forwarded_unchanged(engine_factory):
    reader = MagicMock()
    engine = engine_factory(bootstrap_hydration_reader=reader)
    assert engine._pending_market_replay._bootstrap_hydration_reader is reader
    reader.prepare.assert_not_called()


def test_live_profile_activation_waits_for_modeled_warmup_contract(engine_factory):
    engine = engine_factory(
        strategy_context_loader=MagicMock(),
        market_data_decision_owner=MagicMock(),
    )
    engine.runtime_environment = RuntimeEnvironment("live")
    engine._strategy_hydration.fresh_instance_for_replay = MagicMock()
    strategy = _ProfileStrategy("profile", "BINANCE:BTCUSDT-PERP")

    with pytest.raises(
        RuntimeError,
        match="^strategy_profile_activation_requires_modeled_warmup: profile$",
    ):
        engine.add_strategy(strategy)

    engine._strategy_hydration.fresh_instance_for_replay.assert_not_called()
    assert "profile" not in engine.strategy_instances


def test_non_live_profile_registration_is_not_blocked_by_live_gate(engine_factory):
    engine = engine_factory()
    engine.runtime_environment = RuntimeEnvironment("test")
    strategy = _ProfileStrategy("profile", "BINANCE:BTCUSDT-PERP")

    engine.add_strategy(strategy)

    assert engine.strategy_instances["profile"] is strategy


def test_unpersisted_candle_passes_scope_and_returns_pending_batch(engine_factory):
    events = []
    owner, lifecycle, pending = MagicMock(), MagicMock(), MagicMock()
    owner.begin_candle.side_effect = lambda candle: (
        events.append(("begin", candle)) or lifecycle
    )
    lifecycle.build.side_effect = lambda: events.append(("build",)) or pending
    context_loader = MagicMock()
    engine = engine_factory(
        strategy_context_loader=context_loader,
        market_data_decision_owner=owner,
    )
    candle = make_candle()
    fill = {"order": MagicMock()}
    engine.execution_engine.process_market_data = MagicMock(
        side_effect=lambda value: events.append(("market", value)) or [fill]
    )
    engine._signal_processor.on_candle = MagicMock(
        side_effect=lambda *args, **kwargs: events.append(("dispatch", args, kwargs))
    )

    assert engine._apply_unpersisted_candle(candle) is pending

    assert events == [
        ("market", candle),
        ("begin", candle),
        (
            "dispatch",
            (candle,),
            {
                "latest_fills": (
                    {"order": fill["order"], "timestamp": candle.timestamp},
                ),
                "decision_scope": lifecycle,
            },
        ),
        ("build",),
    ]


@pytest.mark.parametrize("stage", ["market", "begin", "dispatch", "build"])
def test_decision_failure_stage_never_advances_later_stage(engine_factory, stage):
    failure = RuntimeError(stage)
    owner, lifecycle = MagicMock(), MagicMock()
    engine = engine_factory(
        strategy_context_loader=MagicMock(),
        market_data_decision_owner=owner,
    )
    candle = make_candle()
    engine.execution_engine.process_market_data = MagicMock(return_value=[])
    if stage == "market":
        engine.execution_engine.process_market_data.side_effect = failure
    owner.begin_candle.return_value = lifecycle
    if stage == "begin":
        owner.begin_candle.side_effect = failure
    engine._signal_processor.on_candle = MagicMock(
        side_effect=failure if stage == "dispatch" else None
    )
    lifecycle.build.side_effect = failure if stage == "build" else None

    with pytest.raises(RuntimeError) as caught:
        engine._apply_unpersisted_candle(candle)

    assert caught.value is failure
    assert owner.begin_candle.call_count == (stage != "market")
    assert engine._signal_processor.on_candle.call_count == (
        stage not in ("market", "begin")
    )
    assert lifecycle.build.call_count == (stage == "build")


def test_live_application_receives_decision_batch_from_engine(engine_factory):
    owner, lifecycle, pending = MagicMock(), MagicMock(), MagicMock()
    owner.begin_candle.return_value = lifecycle
    lifecycle.build.return_value = pending
    engine = engine_factory(
        strategy_context_loader=MagicMock(),
        market_data_decision_owner=owner,
    )
    engine._signal_processor.on_candle = MagicMock()
    engine._live_candle_application = MagicMock()
    candle = make_candle()

    engine.on_market_data(candle)

    engine._live_candle_application.application_fence.assert_called_once_with(candle)
    engine._live_candle_application.apply.assert_called_once()
    kwargs = engine._live_candle_application.apply.call_args.kwargs
    assert kwargs["apply_new"] == engine._apply_unpersisted_candle
    assert kwargs["apply_new"](candle) is pending
    assert engine._signal_processor.on_candle.call_args_list[-1] == call(
        candle,
        latest_fills=(),
        decision_scope=lifecycle,
    )
