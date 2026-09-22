from dataclasses import replace
from decimal import Decimal

import pytest

from src.core.backtest_runner import BacktestRunner
from src.core.portfolio_runtime import PortfolioDefinition, PortfolioSleeve
from src.core.research_backtest_runner import ResearchBacktestRunner
from test_profile_decision_input import sample
from test_signal_processor import DummyStrategy

PRODUCT = "BINANCE:BTCUSDT-PERP"


class _ProfileStrategy(DummyStrategy):
    @property
    def requirements(self):
        return replace(
            super().requirements,
            profile_requirements=sample().requirements,
        )


def _full() -> BacktestRunner:
    return BacktestRunner(0, 60_000, PRODUCT, "1m")


def _research() -> ResearchBacktestRunner:
    return ResearchBacktestRunner(0, 60_000, PRODUCT, "1m")


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


def test_research_runner_rejects_profile_strategy_before_registration():
    runner = _research()

    with pytest.raises(
        RuntimeError,
        match="^profile_market_data_provider_required: "
        "runner=research strategy_id=profile$",
    ):
        runner.add_strategy(_ProfileStrategy("profile"))

    assert runner._strategies == []


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
