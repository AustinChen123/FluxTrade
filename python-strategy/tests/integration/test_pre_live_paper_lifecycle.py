from __future__ import annotations

from datetime import UTC, datetime, time as wall_time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from src.core.execution import ExecutionEngine
from src.core.models import Candlestick, Signal, SignalType
from src.core.portfolio_runtime import PortfolioDefinition, PortfolioSleeve
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.validation.paper_lifecycle import run_paper_lifecycle
from src.validation.portfolio_paper_lifecycle import (
    run_portfolio_paper_lifecycle,
)

ET = ZoneInfo("America/New_York")
PRODUCT_ID = "RITHMIC:MNQ-202609"
STRATEGY_ID = "paper_probe"


class HardFlatProbeStrategy(BaseStrategy):
    def __init__(
        self,
        strategy_id: str,
        product_id: str,
        exit_quantity: Decimal = Decimal("1"),
    ) -> None:
        super().__init__(strategy_id, product_id)
        self.exit_quantity = exit_quantity

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "5m", 0)

    def fresh_instance_for_replay(self) -> BaseStrategy:
        return type(self)(
            self.strategy_id,
            self.product_id,
            self.exit_quantity,
        )

    def replay_configuration(self) -> object:
        return (str(self.exit_quantity),)

    def sync_position_state(self, position_side: str | None) -> bool:
        return position_side in {None, "LONG"}

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        decision_time = datetime.fromtimestamp(
            candle.timestamp / 1_000, UTC
        ).astimezone(ET) + timedelta(minutes=5)
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="5m",
            timestamp=candle.timestamp,
            type=(
                SignalType.EXIT_LONG
                if decision_time.time() >= wall_time(16, 40)
                else SignalType.NO_SIGNAL
            ),
            quantity=self.exit_quantity,
        )


setattr(HardFlatProbeStrategy, "__fluxtrade_display_name__", "Paper Fixture")
setattr(HardFlatProbeStrategy, "__fluxtrade_artifact_version__", "1.0.0")
setattr(HardFlatProbeStrategy, "__fluxtrade_readiness__", "RESEARCH_VALIDATED")
setattr(HardFlatProbeStrategy, "__fluxtrade_catalog_sha256__", "0" * 64)


def _portfolio() -> PortfolioDefinition:
    return PortfolioDefinition(
        portfolio_id="paper_portfolio",
        product_id=PRODUCT_ID,
        sleeves=(
            PortfolioSleeve(
                HardFlatProbeStrategy("paper_portfolio.sleeve_a", PRODUCT_ID)
            ),
            PortfolioSleeve(
                HardFlatProbeStrategy("paper_portfolio.sleeve_b", PRODUCT_ID)
            ),
        ),
        max_gross_quantity=Decimal("2"),
        artifact_version="1.0.0",
        display_name="Paper Portfolio Fixture",
        readiness="RESEARCH_VALIDATED",
        catalog_sha256="1" * 64,
    )


def _sized_portfolio() -> PortfolioDefinition:
    definition = _portfolio()
    return PortfolioDefinition(
        portfolio_id=definition.portfolio_id,
        product_id=definition.product_id,
        sleeves=(
            PortfolioSleeve(
                HardFlatProbeStrategy(
                    "paper_portfolio.sleeve_a",
                    PRODUCT_ID,
                    Decimal("2"),
                )
            ),
            PortfolioSleeve(
                HardFlatProbeStrategy(
                    "paper_portfolio.sleeve_b",
                    PRODUCT_ID,
                )
            ),
        ),
        max_gross_quantity=Decimal("3"),
        artifact_version=definition.artifact_version,
        display_name=definition.display_name,
        readiness=definition.readiness,
        catalog_sha256=definition.catalog_sha256,
    )


def test_paper_lifecycle_finishes_every_scenario_flat_and_reconciled(tmp_path):
    report = run_paper_lifecycle(
        tmp_path,
        product_id=PRODUCT_ID,
        strategy_id=STRATEGY_ID,
        hard_flat_strategy_factory=lambda: HardFlatProbeStrategy(
            STRATEGY_ID,
            PRODUCT_ID,
        ),
    )

    assert [scenario.scenario for scenario in report.scenarios] == [
        "stop_loss",
        "take_profit",
        "hard_flat_1640_et",
    ]
    assert report.instrument.product_id == PRODUCT_ID
    assert report.instrument.quantity_step == "1"
    assert report.instrument.price_tick == "0.25"
    assert report.instrument.multiplier == "2"
    assert report.instrument.fee_model == "per_contract"
    for scenario in report.scenarios:
        assert scenario.restart_unresolved_count == 0
        assert scenario.restart_verification_blocked_count == 0
        assert scenario.final_unresolved_count == 0
        assert scenario.final_verification_blocked_count == 0
        assert scenario.final_position_count == 0
        assert scenario.final_working_order_count == 0
        assert scenario.orders
        assert scenario.fills
        assert all(
            order.status not in {"NEW", "SUBMITTED"} for order in scenario.orders
        )
        assert all(fill.fee == "0" for fill in scenario.fills)

    stop_loss, take_profit, hard_flat = report.scenarios
    assert stop_loss.driver == "synthetic_protected_entry"
    assert stop_loss.strategy is None
    assert {order.order_type for order in stop_loss.orders} == {
        "market",
        "stop_loss",
        "take_profit",
    }
    assert any(fill.order_type == "stop_loss" for fill in stop_loss.fills)
    assert any(
        order.order_type == "take_profit" and order.status == "CANCELLED"
        for order in stop_loss.orders
    )
    assert any(fill.order_type == "take_profit" for fill in take_profit.fills)
    assert hard_flat.driver == "strategy"
    assert hard_flat.strategy is not None
    assert hard_flat.strategy.strategy_id == STRATEGY_ID
    assert any(
        order.order_type == "market" and order.side == "sell"
        for order in hard_flat.orders
    )


def test_paper_lifecycle_rejects_non_mnq_instrument_before_creating_workspace(
    tmp_path,
):
    workspace = tmp_path / "paper"

    with pytest.raises(ValueError, match="only dated Rithmic MNQ"):
        run_paper_lifecycle(
            workspace,
            product_id="RITHMIC:ES-202609",
            strategy_id=STRATEGY_ID,
            hard_flat_strategy_factory=lambda: HardFlatProbeStrategy(
                STRATEGY_ID,
                "RITHMIC:ES-202609",
            ),
        )

    assert not workspace.exists()


def test_portfolio_paper_lifecycle_preserves_sleeve_ownership_and_finishes_flat(
    tmp_path,
):
    report = run_portfolio_paper_lifecycle(
        tmp_path,
        portfolio_factory=_portfolio,
    )

    assert [scenario.scenario for scenario in report.scenarios] == [
        "stop_loss:paper_portfolio.sleeve_a",
        "take_profit:paper_portfolio.sleeve_a",
        "stop_loss:paper_portfolio.sleeve_b",
        "take_profit:paper_portfolio.sleeve_b",
        "working_entry_restart",
        "hard_flat_1640_et",
    ]
    for scenario in report.scenarios:
        assert scenario.strategy is not None
        assert scenario.strategy.strategy_id == "paper_portfolio"
        assert scenario.restart_unresolved_count == 0
        assert scenario.restart_verification_blocked_count == 0
        assert scenario.final_unresolved_count == 0
        assert scenario.final_verification_blocked_count == 0
        assert scenario.final_position_count == 0
        assert scenario.final_working_order_count == 0
        assert scenario.orders
        assert {
            order.strategy_id for order in scenario.orders
        } <= {
            "paper_portfolio.sleeve_a",
            "paper_portfolio.sleeve_b",
        }

    working_restart = report.scenarios[-2]
    assert working_restart.driver == "portfolio_working_entry"
    assert {
        order.strategy_id
        for order in working_restart.orders
        if order.order_type == "limit"
    } == {
        "paper_portfolio.sleeve_a",
        "paper_portfolio.sleeve_b",
    }
    assert {
        order.status
        for order in working_restart.orders
        if order.order_type == "limit"
    } == {"CANCELLED"}

    hard_flat = report.scenarios[-1]
    assert hard_flat.driver == "portfolio_strategy_engine"
    assert {
        fill.strategy_id
        for fill in hard_flat.fills
        if fill.order_type == "market" and fill.side == "sell"
    } == {
        "paper_portfolio.sleeve_a",
        "paper_portfolio.sleeve_b",
    }


def test_portfolio_paper_lifecycle_exercises_configured_sleeve_quantities(
    tmp_path,
):
    report = run_portfolio_paper_lifecycle(
        tmp_path,
        portfolio_factory=_sized_portfolio,
        scenario_quantities={
            "paper_portfolio.sleeve_a": Decimal("2"),
            "paper_portfolio.sleeve_b": Decimal("1"),
        },
    )

    for scenario in report.scenarios:
        assert scenario.final_position_count == 0
        assert scenario.final_working_order_count == 0
    hard_flat = report.scenarios[-1]
    entry_quantities = {
        fill.strategy_id: fill.quantity
        for fill in hard_flat.fills
        if fill.order_type == "market" and fill.side == "buy"
    }
    assert entry_quantities == {
        "paper_portfolio.sleeve_a": "2",
        "paper_portfolio.sleeve_b": "1",
    }


def test_portfolio_paper_lifecycle_rejects_verification_blocked_restart(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        ExecutionEngine,
        "reconcile_recoverable_client_orders",
        lambda _self: {
            "unresolved_count": 0,
            "verification_blocked_count": 1,
        },
    )

    with pytest.raises(
        AssertionError,
        match="restart reconciliation was not resolved and verified",
    ):
        run_portfolio_paper_lifecycle(
            tmp_path,
            portfolio_factory=_portfolio,
        )


@pytest.mark.parametrize(
    "quantities, error",
    [
        (
            {"paper_portfolio.sleeve_a": Decimal("1")},
            "name every sleeve exactly",
        ),
        (
            {
                "paper_portfolio.sleeve_a": Decimal("1.5"),
                "paper_portfolio.sleeve_b": Decimal("1"),
            },
            "positive integer",
        ),
        (
            {
                "paper_portfolio.sleeve_a": Decimal("2"),
                "paper_portfolio.sleeve_b": Decimal("2"),
            },
            "max_gross_quantity",
        ),
    ],
)
def test_portfolio_paper_lifecycle_rejects_invalid_scenario_quantities(
    tmp_path,
    quantities,
    error,
):
    with pytest.raises(ValueError, match=error):
        run_portfolio_paper_lifecycle(
            tmp_path,
            portfolio_factory=_sized_portfolio,
            scenario_quantities=quantities,
        )
