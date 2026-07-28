from __future__ import annotations

from datetime import UTC, datetime, time as wall_time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from src.core.models import Candlestick, Signal, SignalType
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.validation.paper_lifecycle import run_paper_lifecycle

ET = ZoneInfo("America/New_York")
PRODUCT_ID = "RITHMIC:MNQ-202609"
STRATEGY_ID = "paper_probe"


class HardFlatProbeStrategy(BaseStrategy):
    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "5m", 0)

    def fresh_instance_for_replay(self) -> BaseStrategy:
        return type(self)(self.strategy_id, self.product_id)

    def replay_configuration(self) -> object:
        return ()

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
            quantity=Decimal("1"),
        )


setattr(HardFlatProbeStrategy, "__fluxtrade_display_name__", "Paper Fixture")
setattr(HardFlatProbeStrategy, "__fluxtrade_artifact_version__", "1.0.0")
setattr(HardFlatProbeStrategy, "__fluxtrade_readiness__", "RESEARCH_VALIDATED")
setattr(HardFlatProbeStrategy, "__fluxtrade_catalog_sha256__", "0" * 64)


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
        assert scenario.final_unresolved_count == 0
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
