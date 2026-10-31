"""Native mixed-ownership authority seam and paid-input admission tests."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from typing import Any, cast

import pytest

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_gross_execution_input import validate_spider_gross_execution_input
from src.core.backtest.spider_gross_response import propose_reductions
from src.core.backtest.spider_historical_replay import _prepare_historical_replay
from src.core.backtest.spider_run_artifacts import decode_canonical
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_gross_execution_input import _over_cap_gross_input
from test_spider_historical_input import _initial_account_state, _run_with_configuration


def _native_seed_with_units(request):
    prepared = _prepare_historical_replay(request.execution_input.historical_input)
    configuration = cast(dict[str, Any], deepcopy(prepared.configuration))
    configuration["seed_effective_at"] *= 16
    for product in configuration["products"]:
        for field in ("specs", "tiers", "marks"):
            for version in product[field]:
                for key in ("valid_from", "valid_to"):
                    if version[key] is not None:
                        version[key] *= 16
    return prepared.account, configuration


def test_native_projection_and_proposal_do_not_authorize_foreign_lots():
    request = _over_cap_gross_input()
    account, configuration = _native_seed_with_units(request)
    strategy_id = request.authorization_scope.strategy_id
    product_b = next(row for row in configuration["positions"]
                     if row["product_id"] == "B-USDT-SWAP")
    product_b["lots"][0]["strategy_id"] = "foreign-strategy"
    codec = wire.ScenarioCodec(
        "SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1", account, configuration,
    )
    before_financial = codec.inspect_state()
    native = codec.gross_response_projection()
    gross = native["gross_admission"]
    assert D(gross["gross"]) == D("200")
    assert D(gross["effective_equity"]) == D("50")
    products = {row["product_id"]: row for row in native["products"]}
    assert D(products["A-USDT-SWAP"]["signed_contracts"]) > 0
    assert D(products["B-USDT-SWAP"]["signed_contracts"]) < 0
    assert len(products["A-USDT-SWAP"]["lots"]) == len(products["B-USDT-SWAP"]["lots"]) == 1
    assert products["B-USDT-SWAP"]["lots"][0]["strategy_id"] == "foreign-strategy"

    proposal = propose_reductions(
        native, strategy_id=strategy_id,
        ordered_products=request.authorization_scope.ordered_products,
    )
    assert proposal.state == "INCOMPLETE"
    assert proposal.reason == "AUTHORIZATION_SCOPE_INSUFFICIENT"
    assert proposal.rows == ()
    assert codec.inspect_state() == before_financial
    assert codec.gross_response_projection() == native


def test_gross_paid_admission_rejects_mixed_initial_lot_before_native_owner(monkeypatch):
    request = _over_cap_gross_input()
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    position_b = next(row for row in configuration["positions"]
                      if row["product_id"] == "B-USDT-SWAP")
    position_b["lots"][0]["strategy_id"] = "foreign-strategy"
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        initial_account_state=_initial_account_state(
            cash="50", positions=configuration["positions"], orders=configuration["orders"],
        ),
    )
    mixed = replace(request, execution_input=replace(execution, historical_input=history))
    owner_constructions = []
    original_init = wire.ScenarioCodec.__init__

    def spy_init(self, *args, **kwargs):
        owner_constructions.append(True)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(wire.ScenarioCodec, "__init__", spy_init)
    with pytest.raises(ValueError):
        validate_spider_gross_execution_input(mixed)
    with pytest.raises(ValueError):
        _ReplayComposition._from_gross_response_run(mixed)
    assert owner_constructions == []
