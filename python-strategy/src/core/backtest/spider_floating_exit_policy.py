"""Pure source-derived remaining-cost exit decisions for Spider research."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Literal, cast

from src.core.backtest import spider_bayes_model
from src.core.backtest.spider_bayes_replay import (
    Closed15mSample,
    PRODUCTS,
    PROJECTION_ID,
)


FIXED_REMAINING_COST_TP_V1 = "FIXED_REMAINING_COST_TP_V1"
BAYES_REMAINING_COST_RISK_V1 = "BAYES_REMAINING_COST_RISK_V1"
_POLICY_IDS = frozenset((FIXED_REMAINING_COST_TP_V1, BAYES_REMAINING_COST_RISK_V1))
_LIFECYCLES = frozenset(
    (
        "RISK_STABLE",
        "AWAITING_CANCEL_EFFECTIVE",
        "LIQUIDATED_FLAT",
        "LIQUIDATED_INSOLVENT",
    )
)
_TERMINAL_LIFECYCLES = frozenset(("LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"))
_ZERO = Decimal("0")
_FIXED_THRESHOLD = Decimal("0.03")
_EXPECTED_EXIT_FEE_RATE = Decimal("0.0006")
_FIFTEEN_MINUTES_MS = 900_000


@dataclass(frozen=True, slots=True)
class FloatingExitProductDecision:
    product_id: str
    active: bool
    signed_base_quantity: Decimal
    price: Decimal
    average_open_price: Decimal
    estimated_exit_fee_rate: Decimal
    estimated_exit_fee: Decimal
    gross: Decimal
    unrealized: Decimal
    return_ratio: Decimal | None
    threshold: Decimal
    reason: Literal["TP", "RISK"] | None


@dataclass(frozen=True, slots=True)
class FloatingExitDecisionBatch:
    policy_id: str
    model_id: str
    observation_projection_id: str
    cutoff_ms: int
    visible_at: int
    observed_at: int
    observation: Decimal
    account_identity: tuple[str, str, str, str | None]
    account_version: int
    valuation_context_id: str
    lifecycle: str
    posterior: tuple[Decimal, ...]
    rows: tuple[FloatingExitProductDecision, ...]


def _finite_decimal(value: object, name: str) -> Decimal:
    if type(value) is not Decimal:
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _account_identity(value: object, name: str) -> tuple[str, str, str, str | None]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an account mapping")
    required = {"venue", "environment", "account"}
    keys = set(value)
    if keys not in (required, required | {"subaccount"}):
        raise ValueError(f"{name} has unsupported account fields")
    venue, environment, account = (
        value[key] for key in ("venue", "environment", "account")
    )
    if any(type(part) is not str for part in (venue, environment, account)):
        raise TypeError(f"{name} identity fields must be strings")
    subaccount = value.get("subaccount")
    if subaccount is not None and type(subaccount) is not str:
        raise TypeError(f"{name} subaccount must be a string or null")
    return venue, environment, account, subaccount


def _cost_rows(value: object) -> dict[str, Mapping[str, object]]:
    if not isinstance(value, Mapping):
        raise TypeError("cost projection must be a mapping")
    required = {
        "schema_version",
        "model_id",
        "account_key",
        "account_version",
        "valuation_context_id",
        "lifecycle",
        "seed_fee_provenance",
        "rows",
    }
    if set(value) != required:
        raise ValueError("cost projection fields are invalid")
    if value["schema_version"] != "spider_source_cost_projection_v1":
        raise ValueError("unsupported cost projection schema")
    if value["model_id"] != "SOURCE_WEIGHTED_OPEN_COST_V1":
        raise ValueError("unsupported cost projection model")
    if value["seed_fee_provenance"] != "MODELED_CONFIGURED_TAKER_RATE_V1":
        raise ValueError("unsupported seed fee provenance")
    version, context_id, lifecycle = (
        value["account_version"],
        value["valuation_context_id"],
        value["lifecycle"],
    )
    if type(version) is not int or version < 0:
        raise ValueError("cost account version is invalid")
    if type(context_id) is not str or not context_id:
        raise ValueError("valuation context is invalid")
    if type(lifecycle) is not str or lifecycle not in _LIFECYCLES:
        raise ValueError("unsupported native lifecycle")
    rows = value["rows"]
    if type(rows) is not list:
        raise TypeError("cost projection rows must be a list")
    required_row = {
        "product_id",
        "signed_base_quantity",
        "average_open_price",
        "estimated_exit_fee_rate",
    }
    result: dict[str, Mapping[str, object]] = {}
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != required_row:
            raise ValueError("cost projection row fields are invalid")
        product_id = row["product_id"]
        if (
            type(product_id) is not str
            or product_id not in PRODUCTS
            or product_id in result
        ):
            raise ValueError("cost projection products are invalid")
        quantity = _finite_decimal(row["signed_base_quantity"], "signed base quantity")
        average = _finite_decimal(row["average_open_price"], "average open price")
        rate = _finite_decimal(
            row["estimated_exit_fee_rate"], "estimated exit fee rate"
        )
        if rate != _EXPECTED_EXIT_FEE_RATE:
            raise ValueError("unsupported estimated exit fee rate")
        if (quantity == _ZERO and average != _ZERO) or (
            quantity != _ZERO and average <= _ZERO
        ):
            raise ValueError("cost quantity and average are inconsistent")
        result[product_id] = row
    if set(result) != set(PRODUCTS):
        raise ValueError("cost projection must cover the source basket")
    return result


def decide_floating_exits(
    policy_id: str,
    sample: Closed15mSample,
    cost_projection: Mapping[str, object],
    active_products: Sequence[str],
    expected_account: Mapping[str, object],
    observed_at: int,
) -> FloatingExitDecisionBatch:
    """Return a detached, immutable decision batch without changing native state."""
    if type(policy_id) is not str:
        raise TypeError("policy id must be a string")
    if policy_id not in _POLICY_IDS:
        raise ValueError("unsupported floating exit policy")
    if type(sample) is not Closed15mSample:
        raise TypeError("sample must be a Closed15mSample")
    if type(sample.cutoff_ms) is not int or sample.cutoff_ms < 0:
        raise ValueError("sample cutoff is invalid")
    if sample.cutoff_ms % _FIFTEEN_MINUTES_MS:
        raise ValueError("sample cutoff must be 15-minute aligned")
    visible_at = sample.cutoff_ms * 16 + 6
    if type(sample.visible_at) is not int or sample.visible_at != visible_at:
        raise ValueError("sample visibility is invalid")
    if type(observed_at) is not int or observed_at < visible_at:
        raise ValueError("sample has not yet been observed")
    if type(sample.prices) is not tuple or len(sample.prices) != len(PRODUCTS):
        raise ValueError("sample must contain the source basket")
    if type(sample.posterior) is not tuple or len(sample.posterior) != len(
        spider_bayes_model.STATES
    ):
        raise ValueError("sample posterior must contain five weights")
    if not isinstance(active_products, (tuple, list)):
        raise TypeError("active products must be an ordered sequence")
    active = tuple(active_products)
    if any(type(product) is not str or product not in PRODUCTS for product in active):
        raise ValueError("active products are outside the source basket")
    if len(set(active)) != len(active):
        raise ValueError("active products must be unique")
    source_indexes = tuple(PRODUCTS.index(product) for product in active)
    if source_indexes != tuple(sorted(source_indexes)):
        raise ValueError("active products must preserve source order")

    expected_identity = _account_identity(expected_account, "expected account")
    rows_by_product = _cost_rows(cost_projection)
    cost_identity = _account_identity(
        cost_projection.get("account_key"), "cost account"
    )
    if cost_identity != expected_identity:
        raise ValueError("cost projection account does not match")
    observation = _finite_decimal(sample.observation, "sample observation")

    with localcontext(spider_bayes_model._model_context()):
        prices = tuple(
            _finite_decimal(price, "sample price") for price in sample.prices
        )
        if any(price <= _ZERO for price in prices):
            raise ValueError("sample prices must be positive")
        posterior = spider_bayes_model._normalized(sample.posterior, "posterior")
        bad_probability = posterior[2] + posterior[3]
        if policy_id == FIXED_REMAINING_COST_TP_V1:
            threshold = _FIXED_THRESHOLD
        elif bad_probability > Decimal("0.67"):
            threshold = Decimal("0.015")
        elif posterior[1] > Decimal("0.54"):
            threshold = Decimal("0.09")
        elif posterior[0] > Decimal("0.58"):
            threshold = Decimal("0.05")
        else:
            threshold = _FIXED_THRESHOLD

        decisions = []
        terminal = cost_projection["lifecycle"] in _TERMINAL_LIFECYCLES
        active_set = frozenset(active)
        for index, product_id in enumerate(PRODUCTS):
            row = rows_by_product[product_id]
            quantity = cast(Decimal, row["signed_base_quantity"])
            average = cast(Decimal, row["average_open_price"])
            fee_rate = cast(Decimal, row["estimated_exit_fee_rate"])
            price = prices[index]
            gross = quantity.copy_abs() * price
            exit_fee = fee_rate * gross
            unrealized = quantity * (price - average) - exit_fee
            ratio = unrealized / gross if gross > _ZERO else None
            is_active = product_id in active_set
            reason: Literal["TP", "RISK"] | None = None
            if not terminal and is_active and ratio is not None:
                if unrealized > _ZERO and ratio >= threshold:
                    reason = "TP"
                elif (
                    policy_id == BAYES_REMAINING_COST_RISK_V1
                    and bad_probability > Decimal("0.77")
                    and ratio < Decimal("-0.115")
                ):
                    reason = "RISK"
            decisions.append(
                FloatingExitProductDecision(
                    product_id=product_id,
                    active=is_active,
                    signed_base_quantity=quantity,
                    price=price,
                    average_open_price=average,
                    estimated_exit_fee_rate=fee_rate,
                    estimated_exit_fee=exit_fee,
                    gross=gross,
                    unrealized=unrealized,
                    return_ratio=ratio,
                    threshold=threshold,
                    reason=reason,
                )
            )

        version = cast(int, cost_projection["account_version"])
        context_id = cast(str, cost_projection["valuation_context_id"])
        lifecycle = cast(str, cost_projection["lifecycle"])
        return FloatingExitDecisionBatch(
            policy_id=policy_id,
            model_id=spider_bayes_model.MODEL_ID,
            observation_projection_id=PROJECTION_ID,
            cutoff_ms=sample.cutoff_ms,
            visible_at=visible_at,
            observed_at=observed_at,
            observation=observation,
            account_identity=expected_identity,
            account_version=version,
            valuation_context_id=context_id,
            lifecycle=lifecycle,
            posterior=posterior,
            rows=tuple(decisions),
        )
