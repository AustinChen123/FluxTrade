"""Causal tests for private remaining-cost floating exit decisions."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, ROUND_DOWN, localcontext
from typing import cast

import pytest

from src.core.backtest import spider_bayes_model as model
from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_bayes_replay import (
    Closed15mSample,
    PRODUCTS,
    PROJECTION_ID,
)
from src.core.backtest.spider_floating_exit_policy import (
    BAYES_REMAINING_COST_RISK_V1,
    FIXED_REMAINING_COST_TP_V1,
    decide_floating_exits,
)
from test_synthetic_scenario_codec import KEY, _configured_input, _historical_node


_ZERO = Decimal("0")
_PRICE = Decimal("10")
_FEE_RATE = Decimal("0.0006")
_OBSERVED_AT = 14_400_006


def _sample(
    *,
    prices: tuple[Decimal, ...] | None = None,
    posterior: tuple[Decimal, ...] = model.PRIOR,
    cutoff_ms: int = 900_000,
    observation: Decimal = Decimal("0.01"),
) -> Closed15mSample:
    return Closed15mSample(
        cutoff_ms=cutoff_ms,
        visible_at=cutoff_ms * 16 + 6,
        prices=prices or (_PRICE,) * len(PRODUCTS),
        observation=observation,
        posterior=posterior,
    )


def _cost_projection(
    *,
    quantity: Decimal = Decimal("1"),
    average: Decimal = Decimal("10"),
    lifecycle: str = "RISK_STABLE",
    overrides: dict[str, tuple[Decimal, Decimal]] | None = None,
) -> dict[str, object]:
    override_rows = overrides or {}
    return {
        "schema_version": "spider_source_cost_projection_v1",
        "model_id": "SOURCE_WEIGHTED_OPEN_COST_V1",
        "account_key": deepcopy(KEY),
        "account_version": 7,
        "valuation_context_id": "a" * 64,
        "lifecycle": lifecycle,
        "seed_fee_provenance": "MODELED_CONFIGURED_TAKER_RATE_V1",
        "rows": [
            {
                "product_id": product,
                "signed_base_quantity": override_rows.get(product, (quantity, average))[
                    0
                ],
                "average_open_price": override_rows.get(product, (quantity, average))[
                    1
                ],
                "estimated_exit_fee_rate": _FEE_RATE,
            }
            for product in PRODUCTS
        ],
    }


def _decide(
    policy_id: str = FIXED_REMAINING_COST_TP_V1,
    *,
    sample: Closed15mSample | None = None,
    cost: dict[str, object] | None = None,
    active_products: tuple[str, ...] | list[str] | None = None,
    observed_at: int = _OBSERVED_AT,
):
    return decide_floating_exits(
        policy_id,
        sample or _sample(),
        cost or _cost_projection(),
        PRODUCTS if active_products is None else active_products,
        KEY,
        observed_at,
    )


def _average_for_ratio(ratio: Decimal, *, quantity: Decimal = Decimal("1")) -> Decimal:
    gross = quantity.copy_abs() * _PRICE
    fee = _FEE_RATE * gross
    return _PRICE - (gross * ratio + fee) / quantity


def _first_row(batch):
    return batch.rows[0]


@pytest.mark.parametrize(
    ("policy_id", "quantity", "ratio", "threshold"),
    [
        (FIXED_REMAINING_COST_TP_V1, Decimal("1"), Decimal("0.03"), Decimal("0.03")),
        (FIXED_REMAINING_COST_TP_V1, Decimal("-1"), Decimal("0.03"), Decimal("0.03")),
        (
            BAYES_REMAINING_COST_RISK_V1,
            Decimal("1"),
            Decimal("0.015"),
            Decimal("0.015"),
        ),
        (BAYES_REMAINING_COST_RISK_V1, Decimal("1"), Decimal("0.09"), Decimal("0.09")),
        (BAYES_REMAINING_COST_RISK_V1, Decimal("1"), Decimal("0.05"), Decimal("0.05")),
    ],
)
def test_tp_is_inclusive_at_the_selected_remaining_cost_threshold(
    policy_id, quantity, ratio, threshold
):
    posterior = (
        tuple(map(Decimal, ("0", "0", ".60", ".071", ".329")))
        if ratio == Decimal("0.015")
        else tuple(map(Decimal, ("0", ".541", "0", "0", ".459")))
        if ratio == Decimal("0.09")
        else tuple(map(Decimal, (".581", "0", "0", "0", ".419")))
        if ratio == Decimal("0.05")
        else model.PRIOR
    )
    cost = _cost_projection(
        quantity=quantity,
        average=_average_for_ratio(ratio, quantity=quantity),
    )
    row = _first_row(_decide(policy_id, sample=_sample(posterior=posterior), cost=cost))

    assert row.gross == Decimal("10")
    assert row.estimated_exit_fee == Decimal("0.0060")
    assert row.unrealized == row.gross * ratio
    assert row.return_ratio == ratio
    assert row.threshold == threshold
    assert row.reason == "TP"


@pytest.mark.parametrize(
    ("weights", "expected"),
    [
        (("0", "0", ".60", ".07", ".33"), ".03"),
        (("0", "0", ".60", ".071", ".329"), ".015"),
        (("0", ".54", "0", "0", ".46"), ".03"),
        (("0", ".541", "0", "0", ".459"), ".09"),
        ((".58", "0", "0", "0", ".42"), ".03"),
        ((".581", "0", "0", "0", ".419"), ".05"),
    ],
)
def test_bayes_threshold_uses_strict_source_boundaries(weights, expected):
    sample = _sample(posterior=tuple(map(Decimal, weights)))
    row = _first_row(_decide(BAYES_REMAINING_COST_RISK_V1, sample=sample))
    assert row.threshold == Decimal(expected)


@pytest.mark.parametrize(
    ("bad", "ratio", "expected"),
    [
        (Decimal("0.77"), Decimal("-0.12"), None),
        (Decimal("0.771"), Decimal("-0.115"), None),
        (Decimal("0.771"), Decimal("-0.1151"), "RISK"),
        (Decimal("0.77"), Decimal("-0.1151"), None),
    ],
)
def test_risk_exit_requires_both_strict_conditions(bad, ratio, expected):
    posterior = (Decimal("0"), Decimal("0"), bad, Decimal("0"), Decimal("1") - bad)
    cost = _cost_projection(average=_average_for_ratio(ratio))
    row = _first_row(
        _decide(
            BAYES_REMAINING_COST_RISK_V1,
            sample=_sample(posterior=posterior),
            cost=cost,
        )
    )
    assert row.return_ratio == ratio
    assert row.reason == expected


def test_fixed_policy_never_applies_the_bayes_risk_reason():
    posterior = tuple(map(Decimal, ("0", "0", ".80", ".10", ".10")))
    cost = _cost_projection(average=_average_for_ratio(Decimal("-0.20")))
    row = _first_row(
        _decide(
            FIXED_REMAINING_COST_TP_V1,
            sample=_sample(posterior=posterior),
            cost=cost,
        )
    )
    assert row.threshold == Decimal("0.03")
    assert row.return_ratio == Decimal("-0.20")
    assert row.reason is None


@pytest.mark.parametrize("lifecycle", ["LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"])
def test_native_liquidation_lifecycle_never_proposes_another_exit(lifecycle):
    cost = _cost_projection(
        quantity=Decimal("1"),
        average=Decimal("9.694"),
        lifecycle=lifecycle,
    )
    batch = _decide(cost=cost)
    assert batch.lifecycle == lifecycle
    assert _first_row(batch).reason is None


def test_awaiting_cancel_effective_is_nonterminal_and_policy_does_not_change_owner_inputs():
    cost = _cost_projection(
        quantity=Decimal("1"),
        average=Decimal("9.694"),
        lifecycle="AWAITING_CANCEL_EFFECTIVE",
    )
    before = deepcopy(cost)
    batch = _decide(cost=cost)
    assert batch.lifecycle == "AWAITING_CANCEL_EFFECTIVE"
    assert _first_row(batch).reason == "TP"
    assert cost == before


def test_flat_and_inactive_products_have_no_reason_and_zero_quantity_has_no_ratio():
    cost = _cost_projection(
        quantity=Decimal("1"),
        average=Decimal("9.694"),
        overrides={PRODUCTS[1]: (_ZERO, _ZERO)},
    )
    batch = _decide(cost=cost, active_products=(PRODUCTS[1],))

    assert _first_row(batch).reason is None
    flat = batch.rows[1]
    assert flat.active is True
    assert flat.gross == flat.unrealized == flat.estimated_exit_fee == _ZERO
    assert flat.return_ratio is None
    inactive = batch.rows[0]
    assert inactive.active is False
    assert inactive.reason is None


def test_cost_u_uses_only_remaining_position_and_estimated_close_fee():
    full = _cost_projection(quantity=Decimal("1"), average=Decimal("9"))
    half = _cost_projection(quantity=Decimal("0.5"), average=Decimal("9"))
    full_row = _first_row(_decide(cost=full))
    half_row = _first_row(_decide(cost=half))

    assert full_row.unrealized == Decimal("0.9940")
    assert half_row.unrealized == Decimal("0.49700")
    assert full_row.return_ratio == half_row.return_ratio == Decimal("0.0994")
    assert full_row.unrealized != Decimal(
        "1"
    )  # Includes estimated close fee, not realized PnL.


def test_posterior_normalizes_once_and_decision_batch_is_detached_immutable_and_repeatable():
    cost = _cost_projection()
    original = deepcopy(cost)
    active = [PRODUCTS[0], PRODUCTS[2]]
    sample = _sample(posterior=tuple(Decimal("2") * weight for weight in model.PRIOR))
    batch = _decide(sample=sample, cost=cost, active_products=active)
    repeated = _decide(sample=sample, cost=cost, active_products=active)

    assert batch == repeated
    assert batch.posterior == model.PRIOR
    assert batch.policy_id == FIXED_REMAINING_COST_TP_V1
    assert batch.model_id == model.MODEL_ID
    assert batch.observation_projection_id == PROJECTION_ID
    assert (batch.cutoff_ms, batch.visible_at, batch.observed_at) == (
        sample.cutoff_ms,
        sample.visible_at,
        _OBSERVED_AT,
    )
    assert batch.observation == sample.observation
    assert batch.account_identity == (
        KEY["venue"],
        KEY["environment"],
        KEY["account"],
        None,
    )
    assert batch.account_version == 7
    assert batch.valuation_context_id == "a" * 64
    assert batch.lifecycle == "RISK_STABLE"
    assert tuple(row.product_id for row in batch.rows) == PRODUCTS
    assert tuple(row.active for row in batch.rows) == tuple(
        product in active for product in PRODUCTS
    )
    assert batch.rows is not repeated.rows
    assert cost == original
    active.clear()
    cast(list[dict[str, object]], cost["rows"])[0]["average_open_price"] = Decimal(
        "999"
    )
    assert _first_row(batch).average_open_price == Decimal("10")
    with pytest.raises(FrozenInstanceError):
        batch.rows[0].reason = "TP"  # type: ignore[misc]


def test_nonzero_decision_is_independent_of_ambient_decimal_context():
    sample = _sample(prices=(Decimal("10.123456789"),) + (_PRICE,) * 13)
    cost = _cost_projection(quantity=Decimal("-2.5"), average=Decimal("10.001234567"))
    expected = _decide(BAYES_REMAINING_COST_RISK_V1, sample=sample, cost=cost)

    with localcontext() as context:
        context.prec = 7
        context.rounding = ROUND_DOWN
        actual = _decide(BAYES_REMAINING_COST_RISK_V1, sample=sample, cost=cost)
    assert actual == expected


def test_visible_at_is_inclusive_but_earlier_observation_is_rejected():
    sample = _sample(cutoff_ms=900_000)
    batch = _decide(sample=sample, observed_at=sample.visible_at)
    assert batch.observed_at == sample.visible_at
    with pytest.raises(ValueError, match="not yet been observed"):
        _decide(sample=sample, observed_at=sample.visible_at - 1)


@pytest.mark.parametrize(
    ("sample", "observed_at", "error"),
    [
        (replace(_sample(), cutoff_ms=900_001), _OBSERVED_AT, ValueError),
        (replace(_sample(), visible_at=_OBSERVED_AT + 1), _OBSERVED_AT + 1, ValueError),
        (replace(_sample(), prices=(Decimal("1"),)), _OBSERVED_AT, ValueError),
        (replace(_sample(), prices=(1.0,) + (_PRICE,) * 13), _OBSERVED_AT, TypeError),
        (replace(_sample(), prices=(True,) + (_PRICE,) * 13), _OBSERVED_AT, TypeError),
        (
            replace(_sample(), prices=(Decimal("NaN"),) + (_PRICE,) * 13),
            _OBSERVED_AT,
            ValueError,
        ),
        (
            replace(_sample(), prices=(Decimal("0"),) + (_PRICE,) * 13),
            _OBSERVED_AT,
            ValueError,
        ),
        (replace(_sample(), posterior=(Decimal("1"),) * 4), _OBSERVED_AT, ValueError),
        (
            replace(_sample(), posterior=(True,) + (Decimal("1"),) * 4),
            _OBSERVED_AT,
            TypeError,
        ),
        (
            replace(_sample(), posterior=(Decimal("NaN"),) + (Decimal("1"),) * 4),
            _OBSERVED_AT,
            ValueError,
        ),
        (
            replace(_sample(), posterior=(Decimal("-1"),) + (Decimal("1"),) * 4),
            _OBSERVED_AT,
            ValueError,
        ),
        (replace(_sample(), posterior=(_ZERO,) * 5), _OBSERVED_AT, ValueError),
        (replace(_sample(), observation=True), _OBSERVED_AT, TypeError),
        (replace(_sample(), observation=Decimal("Infinity")), _OBSERVED_AT, ValueError),
        (_sample(), True, ValueError),
    ],
)
def test_invalid_sample_shape_numeric_type_or_visibility_is_rejected(
    sample, observed_at, error
):
    with pytest.raises(error):
        _decide(sample=sample, observed_at=observed_at)


@pytest.mark.parametrize(
    ("policy_id", "error"),
    [
        ("UNNAMED_POLICY", ValueError),
        (None, TypeError),
    ],
)
def test_only_two_named_private_policies_are_accepted(policy_id, error):
    with pytest.raises(error):
        _decide(policy_id)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda cost: cost.update(schema_version="spider_source_cost_projection_v2"),
        lambda cost: cost.update(model_id="OTHER"),
        lambda cost: cost.update(seed_fee_provenance="UNKNOWN"),
        lambda cost: cost.update(account_key={**KEY, "account": "OTHER"}),
        lambda cost: cost.update(account_version=True),
        lambda cost: cost.update(valuation_context_id=""),
        lambda cost: cost.update(lifecycle="UNKNOWN"),
        lambda cost: cost.update(extra="field"),
        lambda cost: cost["rows"].pop(),
        lambda cost: cost["rows"].append(deepcopy(cost["rows"][0])),
        lambda cost: cost["rows"][0].update(signed_base_quantity=1.0),
        lambda cost: cost["rows"][0].update(signed_base_quantity=True),
        lambda cost: cost["rows"][0].update(signed_base_quantity=Decimal("Infinity")),
        lambda cost: cost["rows"][0].update(average_open_price=Decimal("NaN")),
        lambda cost: cost["rows"][0].update(estimated_exit_fee_rate=Decimal("0.001")),
        lambda cost: cost["rows"][0].update(estimated_exit_fee_rate=0.0006),
        lambda cost: cost["rows"][0].update(average_open_price=Decimal("0")),
        lambda cost: cost["rows"][0].update(product_id="UNKNOWN"),
        lambda cost: cost["rows"][0].update(extra="field"),
    ],
)
def test_invalid_cost_projection_schema_identity_or_numeric_value_is_rejected(mutation):
    cost = _cost_projection()
    mutation(cost)
    with pytest.raises((TypeError, ValueError)):
        _decide(cost=cost)


def test_invalid_cost_account_and_active_product_order_are_rejected():
    with pytest.raises(ValueError, match="account does not match"):
        _decide(cost={**_cost_projection(), "account_key": {**KEY, "account": "B"}})
    with pytest.raises(ValueError, match="preserve source order"):
        _decide(active_products=(PRODUCTS[2], PRODUCTS[0]))
    with pytest.raises(ValueError, match="unique"):
        _decide(active_products=(PRODUCTS[0], PRODUCTS[0]))
    with pytest.raises(ValueError, match="outside the source basket"):
        _decide(active_products=("UNKNOWN",))


def _native_fourteen_product_configuration() -> dict[str, object]:
    configuration = deepcopy(_configured_input(1))
    template = cast(list[dict[str, object]], configuration["products"])[0]
    products = []
    for index, product_id in enumerate(PRODUCTS, start=1):
        product = deepcopy(template)
        product["product_id"] = product_id
        product["instrument_code"] = index
        product["taker_fee_rate"] = Decimal("0.0006")
        products.append(product)
    configuration["products"] = products
    configuration["orders"] = [
        {
            "intent_id": "source-cost-intent",
            "order_id": "source-cost-order",
            "client_order_id": "source-cost-client",
            "strategy_id": "source-cost-strategy",
            "product_id": PRODUCTS[0],
            "side": "SHORT",
            "limit_price": Decimal("1"),
            "reduce_only": False,
            "original_quantity_contracts": Decimal("1"),
            "filled_quantity_contracts": Decimal("0"),
            "canceled_quantity_contracts": Decimal("0"),
            "remaining_quantity_contracts": Decimal("1"),
            "status": "OPEN",
        },
        {
            "intent_id": "source-cost-long-intent",
            "order_id": "source-cost-long-order",
            "client_order_id": "source-cost-long-client",
            "strategy_id": "source-cost-strategy",
            "product_id": PRODUCTS[1],
            "side": "LONG",
            "limit_price": Decimal("1"),
            "reduce_only": False,
            "original_quantity_contracts": Decimal("1"),
            "filled_quantity_contracts": Decimal("0"),
            "canceled_quantity_contracts": Decimal("0"),
            "remaining_quantity_contracts": Decimal("1"),
            "status": "OPEN",
        },
    ]
    return configuration


def test_real_native_multifill_cost_projection_flows_through_policy_without_owner_mutation():
    configuration = _native_fourteen_product_configuration()
    codec = wire.ScenarioCodec(
        "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", KEY, configuration
    )
    open_orders = cast(
        list[wire.HistoricalWorkingOrder], codec.historical_working_orders()
    )
    assert len(open_orders) == 2

    first = codec.historical_market_step(
        _historical_node(
            configuration,
            step_index=0,
            bar_open_ms=3,
            working_orders=open_orders,
            volume=Decimal("1"),
        )
    )
    remaining = cast(
        list[wire.HistoricalWorkingOrder], codec.historical_working_orders()
    )
    second = codec.historical_market_step(
        _historical_node(
            configuration,
            step_index=1,
            bar_open_ms=3,
            working_orders=remaining,
            volume=Decimal("1"),
        )
    )
    assert first["effective_at"] < second["effective_at"]
    assert tuple(row["product_id"] for row in first["products"][:2]) == PRODUCTS[:2]
    assert tuple(row["product_id"] for row in second["products"][:2]) == PRODUCTS[:2]
    for result in (first, second):
        assert [
            row["fills"][0]["quantity_contracts"] for row in result["products"][:2]
        ] == [
            Decimal("0.5"),
            Decimal("0.5"),
        ]

    owner_before = codec.inspect_state()
    assert owner_before["total_fees"] == Decimal("0.0012")
    projection = codec.historical_cost_projection()
    owner_after_projection = codec.inspect_state()
    assert owner_after_projection == owner_before
    source_rows = {cast(str, row["product_id"]): row for row in projection["rows"]}
    assert source_rows[PRODUCTS[0]]["signed_base_quantity"] == Decimal("-1")
    assert source_rows[PRODUCTS[1]]["signed_base_quantity"] == Decimal("1")
    assert all(
        source_rows[product]["estimated_exit_fee_rate"] == _FEE_RATE
        for product in PRODUCTS[:2]
    )
    assert source_rows[PRODUCTS[0]]["average_open_price"] == Decimal("0.9994")
    assert source_rows[PRODUCTS[1]]["average_open_price"] == Decimal("1.0006")
    assert projection["schema_version"] == "spider_source_cost_projection_v1"
    assert projection["model_id"] == "SOURCE_WEIGHTED_OPEN_COST_V1"
    assert projection["seed_fee_provenance"] == "MODELED_CONFIGURED_TAKER_RATE_V1"
    assert projection["valuation_context_id"] == owner_before["valuation_context_id"]
    assert projection["account_version"] == owner_before["account_version"]

    sample = _sample(prices=(Decimal("1"),) * len(PRODUCTS), cutoff_ms=900_000)
    assert sample.visible_at > second["effective_at"]
    batch = decide_floating_exits(
        BAYES_REMAINING_COST_RISK_V1,
        sample,
        projection,
        PRODUCTS[:2],
        KEY,
        sample.visible_at,
    )
    for index, product in enumerate(PRODUCTS[:2]):
        projected_row = batch.rows[index]
        expected_quantity = Decimal("-1") if index == 0 else Decimal("1")
        expected_open_fee = abs(expected_quantity) * Decimal("1") * _FEE_RATE
        expected_average = (
            Decimal("1") - expected_open_fee
            if expected_quantity < 0
            else Decimal("1") + expected_open_fee
        )
        expected_gross = abs(expected_quantity) * Decimal("1")
        expected_exit_fee = expected_gross * _FEE_RATE
        expected_unrealized = (
            expected_quantity * (Decimal("1") - expected_average) - expected_exit_fee
        )
        assert projected_row.product_id == product
        assert projected_row.signed_base_quantity == expected_quantity
        assert projected_row.average_open_price == expected_average
        assert projected_row.estimated_exit_fee == expected_exit_fee
        assert projected_row.gross == expected_gross
        assert projected_row.unrealized == expected_unrealized
        assert projected_row.return_ratio == expected_unrealized / expected_gross
        assert projected_row.reason is None
        assert owner_before["total_fees"] >= expected_open_fee
    assert codec.inspect_state() == owner_before
