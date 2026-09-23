from dataclasses import replace
from decimal import Decimal

import pytest

from src.core.backtest_runner import BacktestRunner
import src.core.backtest_runner as backtest_runner_module
from src.core.data_sources.memory import MemoryDataSource
from src.core.portfolio_runtime import PortfolioDefinition, PortfolioSleeve
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.core.models import Candlestick
from src.core.market_data.profiles.decision_context import (
    ProfileDecisionBasis,
    ProfileDecisionContext,
    ProfileDecisionStatus,
    StrategyMarketDataContext,
)
from src.core.market_data.profiles.modeled_input import ModeledProfileInput
from src.core.market_data.profiles.read_types import ProfileQueryRequest
from test_profile_decision_input import sample
from test_signal_processor import DummyStrategy

PRODUCT = "BINANCE:BTCUSDT-PERP"
DAY = 86_400_000
POLICY = "utc_day_0020_conservative_v1"


class _ProfileStrategy(DummyStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.contexts = []

    @property
    def requirements(self):
        return replace(
            super().requirements,
            profile_requirements=sample().requirements,
        )

    def on_candle(self, candle, context=None):
        self.contexts.append(context)
        return None


class _NoContextProfileStrategy(_ProfileStrategy):
    def on_candle(self, candle):
        return None


class _Provider:
    def __init__(self):
        self.calls = []

    def context_for(self, requirements, *, decision_time_ms, availability_policy_id):
        self.calls.append((requirements, decision_time_ms, availability_policy_id))
        end = decision_time_ms // DAY * DAY
        profiles = tuple(
            ProfileDecisionContext(
                ProfileQueryRequest(
                    requirement.product_id,
                    requirement.base_grid_id,
                    requirement.output_grid_id,
                    requirement.algorithm_version,
                    end - requirement.window_days * DAY,
                    end,
                    "MODELED_RESEARCH",
                    availability_policy_id=availability_policy_id,
                    as_of_ms=decision_time_ms,
                ),
                decision_time_ms,
                ProfileDecisionBasis.MODELED,
                ProfileDecisionStatus.MISSING,
                "PROFILE_NOT_READY",
            )
            for requirement in requirements
        )
        return StrategyMarketDataContext(decision_time_ms, profiles)


def _full(provider=None) -> BacktestRunner:
    modeled = None if provider is None else ModeledProfileInput(provider, POLICY)
    return BacktestRunner(0, 60_000, PRODUCT, "1m", modeled_profile_input=modeled)


def _research(provider=None) -> ResearchBacktestRunner:
    modeled = None if provider is None else ModeledProfileInput(provider, POLICY)
    return ResearchBacktestRunner(
        0, 60_000, PRODUCT, "1m", modeled_profile_input=modeled
    )


def test_full_runner_rejects_profile_strategy_before_registration():
    runner = _full()

    with pytest.raises(
        RuntimeError,
        match="^profile_market_data_provider_required: "
        "runner=full strategy_id=profile$",
    ):
        runner.add_strategy(_ProfileStrategy("profile"))

    assert runner._primary_runtime_id is None
    assert runner._strategies_buffer == []


def test_full_runner_accepts_profile_strategy_with_preloaded_input():
    runner = _full(_Provider())
    strategy = _ProfileStrategy("profile")

    runner.add_strategy(strategy)

    assert runner._primary_runtime_id == "profile"
    assert runner._strategies_buffer == [strategy]


def test_full_runner_rejects_profile_strategy_without_context_contract():
    runner = _full(_Provider())

    with pytest.raises(
        RuntimeError,
        match="^profile_strategy_context_required: runner=full strategy_id=profile$",
    ):
        runner.add_strategy(_NoContextProfileStrategy("profile"))

    assert runner._primary_runtime_id is None
    assert runner._strategies_buffer == []


@pytest.mark.rust
def test_full_runner_split_path_and_provenance_are_causally_wired(
    tmp_path, monkeypatch
):
    pytest.importorskip("fluxtrade_core")
    from integration.test_research_backtest_runner import (
        _sqlite_backtest_session_factory,
    )

    bucket = 2 * DAY
    candles = [
        Candlestick(
            product_id=PRODUCT,
            timeframe="1m",
            timestamp=bucket + minute * 60_000,
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("1"),
        )
        for minute in range(6)
    ]
    captured = []
    build = backtest_runner_module.build_backtest_run_provenance

    def capture(**kwargs):
        captured.append(kwargs["configuration"])
        return build(**kwargs)

    monkeypatch.setattr(
        backtest_runner_module, "build_backtest_run_provenance", capture
    )

    def run(name, strategy, modeled_input=None):
        directory = tmp_path / name
        directory.mkdir()
        runner = BacktestRunner(
            bucket,
            candles[-1].timestamp,
            PRODUCT,
            "5m",
            data_source=MemoryDataSource(candles),
            execution_timeframe="1m",
            report_config={
                key: False
                for key in (
                    "csv_trades",
                    "markdown_report",
                    "equity_curve",
                    "journal_export",
                )
            },
            db_session_factory=_sqlite_backtest_session_factory(directory, PRODUCT),
            modeled_profile_input=modeled_input,
        )
        runner.add_strategy(strategy)
        return runner.run()

    provider = _Provider()
    strategy = _ProfileStrategy("profile", timeframe="5m")
    first = run("profile-a", strategy, ModeledProfileInput(provider, POLICY))
    plain_provider = _Provider()
    run(
        "plain-configured",
        DummyStrategy("plain", timeframe="5m"),
        ModeledProfileInput(plain_provider, POLICY),
    )
    run("plain-unset", DummyStrategy("plain", timeframe="5m"))
    alternate = "utc_day_0030_conservative_v1"
    second = run(
        "profile-b",
        _ProfileStrategy("profile", timeframe="5m"),
        ModeledProfileInput(_Provider(), alternate),
    )

    assert first is not None and second is not None
    assert len(strategy.contexts) == 1 and strategy.contexts[0] is not None
    assert strategy.contexts[0].timestamp == bucket
    assert strategy.contexts[0].market_data.decision_time_ms == bucket + 300_000
    assert provider.calls == [
        (strategy.requirements.profile_requirements, bucket + 300_000, POLICY)
    ]
    assert plain_provider.calls == []
    assert [item.get("profile_availability_policy_id") for item in captured] == [
        POLICY,
        POLICY,
        None,
        alternate,
    ]
    assert "profile_availability_policy_id" not in captured[2]
    assert (
        first["provenance"].configuration_sha256
        != second["provenance"].configuration_sha256
    )


@pytest.mark.rust
def test_full_and_research_runners_share_exact_modeled_input_semantics(tmp_path):
    pytest.importorskip("fluxtrade_core")
    from integration.test_research_backtest_runner import (
        _sqlite_backtest_session_factory,
    )

    bucket = 2 * DAY
    execution = [
        Candlestick(
            product_id=PRODUCT,
            timeframe="1m",
            timestamp=bucket + minute * 60_000,
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("1"),
        )
        for minute in range(6)
    ]
    decision = Candlestick(
        product_id=PRODUCT,
        timeframe="5m",
        timestamp=bucket,
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        volume=Decimal("5"),
    )
    full_provider, research_provider = _Provider(), _Provider()
    full_strategy = _ProfileStrategy("profile", timeframe="5m")
    research_strategy = _ProfileStrategy("profile", timeframe="5m")
    full = BacktestRunner(
        bucket,
        bucket + 300_000,
        PRODUCT,
        "5m",
        data_source=MemoryDataSource(execution),
        max_drawdown_limit=None,
        execution_timeframe="1m",
        report_config={
            key: False
            for key in (
                "csv_trades",
                "markdown_report",
                "equity_curve",
                "journal_export",
            )
        },
        db_session_factory=_sqlite_backtest_session_factory(tmp_path, PRODUCT),
        modeled_profile_input=ModeledProfileInput(full_provider, POLICY),
    )
    research = ResearchBacktestRunner(
        bucket,
        bucket + 300_000,
        PRODUCT,
        "5m",
        data_source=MemoryDataSource([decision]),
        modeled_profile_input=ModeledProfileInput(research_provider, POLICY),
    )
    full.add_strategy(full_strategy)
    research.add_strategy(research_strategy)

    full_result, research_result = full.run(), research.run()

    assert full_result is not None
    full_context = full_strategy.contexts[0]
    research_context = research_strategy.contexts[0]
    assert full_context.market_data is not None
    assert research_context.market_data is not None
    assert (
        full_context.market_data.canonical_bytes
        == research_context.market_data.canonical_bytes
    )
    assert full_context.market_data.digest == research_context.market_data.digest
    assert full_result["decision_snapshots"] == research_result["decision_snapshots"]
    assert (
        full_result["provenance"].configuration_sha256
        == research_result["provenance"].configuration_sha256
    )
    expected = (
        full_strategy.requirements.profile_requirements,
        bucket + 300_000,
        POLICY,
    )
    assert full_provider.calls == research_provider.calls == [expected]


def test_research_runner_rejects_profile_strategy_before_registration():
    runner = _research()

    with pytest.raises(
        RuntimeError,
        match="^profile_market_data_provider_required: "
        "runner=research strategy_id=profile$",
    ):
        runner.add_strategy(_ProfileStrategy("profile"))

    assert runner._strategies == []


def test_research_runner_accepts_preloaded_input_and_requires_context_contract():
    runner = _research(_Provider())
    strategy = _ProfileStrategy("profile")
    runner.add_strategy(strategy)
    assert runner._strategies == [strategy]

    rejected = _research(_Provider())
    with pytest.raises(
        RuntimeError,
        match="^profile_strategy_context_required: runner=research strategy_id=bad$",
    ):
        rejected.add_strategy(_NoContextProfileStrategy("bad"))
    assert rejected._strategies == []


def test_full_portfolio_profile_rejection_is_atomic_and_sorted():
    runner = _full()
    definition = PortfolioDefinition(
        portfolio_id="portfolio",
        product_id=PRODUCT,
        sleeves=(
            PortfolioSleeve(DummyStrategy("plain")),
            PortfolioSleeve(_ProfileStrategy("z-profile")),
            PortfolioSleeve(_ProfileStrategy("a-profile")),
        ),
        max_gross_quantity=Decimal("3"),
    )

    with pytest.raises(
        RuntimeError,
        match="^profile_market_data_provider_required: "
        "runner=full strategy_id=a-profile,z-profile$",
    ):
        runner.add_portfolio(definition)

    assert runner._primary_runtime_id is None
    assert runner._portfolios_buffer == []
    assert runner._strategies_buffer == []


def test_non_profile_strategies_preserve_both_runner_paths():
    full = _full()
    research = _research()
    full_strategy = DummyStrategy("full")
    research_strategy = DummyStrategy("research")

    full.add_strategy(full_strategy)
    research.add_strategy(research_strategy)

    assert full._strategies_buffer == [full_strategy]
    assert research._strategies == [research_strategy]
