"""Closed selection of detached protocol literals; no execution or admission."""

from src.core.backtest import spider_scenario_plan_liquidation as _liquidation
from src.core.backtest import spider_scenario_plan_o03 as _o03
from src.core.backtest import spider_scenario_plan_scheduled as _scheduled

PLAN_IDS = (
    "SPIDER_P1_SCHEDULED_MTM_V1",
    "SPIDER_P1_LEGAL_LIQUIDATION_V1",
    "SPIDER_P1_O03_DURABLE_V1",
)
CLI_PLAN_IDS = PLAN_IDS[:2]


def plan_bundle(plan_id: object) -> dict[str, object]:
    """Select one of the three fixed internal evidence identities."""
    if type(plan_id) is not str or plan_id not in PLAN_IDS:
        raise ValueError("UNSUPPORTED_CONFIGURATION")
    if plan_id == PLAN_IDS[0]:
        return _scheduled.detached_bundle()
    if plan_id == PLAN_IDS[1]:
        return _liquidation.detached_bundle()
    return _o03.detached_bundle()


def cli_plan_bundle(plan_id: object) -> dict[str, object]:
    """Select only a CLI-approved identity; O03 remains evidence-only."""
    if type(plan_id) is not str or plan_id not in CLI_PLAN_IDS:
        raise ValueError("UNSUPPORTED_CONFIGURATION")
    return plan_bundle(plan_id)
