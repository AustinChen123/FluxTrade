"""Closed selector authorization and detached ownership boundaries."""

import ast
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_scenario_plans as plans
from src.core.backtest.spider_run_artifacts import canonical_bytes

IDS = ("SPIDER_P1_SCHEDULED_MTM_V1", "SPIDER_P1_LEGAL_LIQUIDATION_V1", "SPIDER_P1_O03_DURABLE_V1")
VECTORS = [(1451, "8fb335d6a98afb6bb08fa837386347c4db90f0b00e8661e305599618f05f1520"),
           (598, "4e149110d228fc7422b6149efe31de1dc036eabf648340049ca95ce12dda2512"),
           (1200, "439906a52cba32b5d1d1bcc1cbd4439270aad175f58a118568fcb754dd42eef8")]


class TextSubclass(str):
    pass


def test_exact_public_surface_and_order():
    assert {name for name in vars(plans) if not name.startswith("_")} == {"PLAN_IDS", "CLI_PLAN_IDS", "plan_bundle", "cli_plan_bundle"}
    assert type(plans.PLAN_IDS) is tuple and plans.PLAN_IDS == IDS
    assert type(plans.CLI_PLAN_IDS) is tuple and plans.CLI_PLAN_IDS == IDS[:2]


@pytest.mark.parametrize("index", range(3))
def test_internal_plan_vectors(index):
    bundle = cast(dict[str, Any], plans.plan_bundle(IDS[index]))
    assert bundle["plan"]["scenario_plan_id"] == IDS[index]
    raw = canonical_bytes(bundle["plan"])
    assert (len(raw), sha256(raw).hexdigest()) == VECTORS[index]
    assert bundle["plan_sha256"] == VECTORS[index][1]
    if index < 2:
        assert plans.cli_plan_bundle(IDS[index]) == bundle
    else:
        with pytest.raises(ValueError, match="^UNSUPPORTED_CONFIGURATION$"):
            plans.cli_plan_bundle(IDS[index])


@pytest.mark.parametrize("select", [plans.plan_bundle, plans.cli_plan_bundle])
@pytest.mark.parametrize("value", [None, True, False, 1, b"SPIDER_P1_SCHEDULED_MTM_V1", [], {},
                                  "", "UNKNOWN", " SPIDER_P1_SCHEDULED_MTM_V1", "SPIDER_P1_SCHEDULED_MTM_V1\n",
                                  TextSubclass(IDS[0]), TextSubclass(IDS[1]), TextSubclass(IDS[2])])
def test_invalid_types_and_unknown_ids(select, value):
    with pytest.raises(ValueError, match="^UNSUPPORTED_CONFIGURATION$"):
        select(value)


@pytest.mark.parametrize("index", range(3))
def test_repeated_cross_selector_nested_isolation(index):
    select = plans.cli_plan_bundle if index < 2 else plans.plan_bundle
    first = cast(dict[str, Any], plans.plan_bundle(IDS[index]))
    second = cast(dict[str, Any], select(IDS[index]))
    before = canonical_bytes(second)
    first["journal"][0]["payload"]["owner_evidence_before"]["inspection"]["cash"] = "99"
    first["plan"]["account_key"]["account"] = "changed"
    first["report"].clear()
    assert canonical_bytes(second) == before
    assert canonical_bytes(plans.plan_bundle(IDS[index])) == before
    second["endpoint"]["final_owner_evidence"]["trading_fact"]["immutable_payload"]["equity"] = "88"
    assert canonical_bytes(select(IDS[index])) == before
    for other in range(3):
        test_internal_plan_vectors(other)


def test_import_boundary():
    tree = ast.parse(Path(plans.__file__).read_text())
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))
    imports = [node for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    assert all(node.module == "src.core.backtest" for node in imports)
    assert {alias.name for node in imports for alias in node.names} == {
        "spider_scenario_plan_scheduled", "spider_scenario_plan_liquidation", "spider_scenario_plan_o03"}
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and
                   node.func.id in {"open", "eval", "exec", "__import__"} for node in ast.walk(tree))
