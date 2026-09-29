import ast
from copy import deepcopy
from itertools import product
from pathlib import Path
from typing import Any, Callable, cast

import pytest

from src.core.backtest import spider_run_native_schema as n
from src.core.backtest.spider_run_artifacts import configuration_context

H = "a" * 64
A = {"venue": "v", "environment": "test", "account": "A"}
S = dict(event_id="e", effective_at=1, causal_parent_ids=[], ordering_contract_id="S_order_v1", scenario_ordinal=20)
R = {"namespace": "SOURCE", "fact_id": "e"}
P = {"product_id": "P_A", "margin_mode": "cross", "position_contracts": "-1"}
ORDER = dict(order_id="o", client_order_id="c", product_id="P_A", state="live", side="buy",
         limit_price="1", original_size_contracts="1", cumulative_filled_size_contracts="0", created_at=0)
M = dict(product_id="P_A", price="1", contract_value="1", lot_size="1", minimum_size="1",
         price_increment="1", high_low_ratio="0.1", state="live", instrument_code=1)
INPUTS = {
    "CONTEXT_MARKS": dict(expected_before=H, expected_after=H, rows=[
        dict(product_id="BTC-USDT-SWAP", valid_from=0, valid_to=2, mark="1")]),
    "EXECUTION": dict(namespace="s", product_id="P_A", external_execution_id="x", order_id="o",
                      side="LONG", price="1", quantity_contracts="1", liquidity="SYNTHETIC_TAKER",
                      matching_effective_at=1, candidate_id="c", source_id="s", visible_at=1,
                      expected_account_version=0, expected_order_version=0, spec_version="v", rule_data_version="v"),
    "INTENT": dict(intent_id="i", client_order_id="c", config_id="c", product_id="P_A", strategy_id="s",
                   side="SHORT", order_type="MARKET", quantity_contracts="1", reduce_only=False, requested_at=0),
    "CANCEL_REQUEST": {"targets": [{"target_order_id": "o", "reason": "EXPLICIT_SCENARIO"}]},
    "CANCEL_EFFECT": {"effects": [{"target_order_id": "o", "reason": "SPEC_MIGRATION", "detecting_event_id": "e"}]},
}
PAYLOADS = {
    "MARKET": {"markets": [M]}, "EARN": {"outcome": "SUCCESS", "earn": "0"},
    "TRADING": {"outcome": "SUCCESS", "equity": "-1", "available_equity": "-2"},
    "POSITIONS": {"outcome": "SUCCESS", "rows": [P]},
    "OPEN_ORDERS": {"outcome": "SUCCESS", "rows": [ORDER]},
}
RESULT = dict(schema_version="group_result_v1", classification="COMMITTED", group_id="g",
              committed_references=[R], rejections=[{"event_id": "e", "reason": "CODE"}],
              account_version_before=0, account_version_after=1, gate_after="RUNNING",
              lifecycle_after="RISK_STABLE", owner_state_digest=H, result_digest=H)
REQUEST = dict(schema_version="snapshot_request_v1", account_key=A, snapshot_id="s", snapshot_kind="TRADING",
               capture_mode="OWNER_CURRENT", captured_at=0)
EXECUTION = dict(order_id="o", owner_client_order_id="c", policy_client_order_id="c", product_id="P_A",
                 state="filled", side="sell", limit_price="1", fill_price="1", original_size_contracts="1",
                 cumulative_filled_size_contracts="1", contract_value="1", execution_effective_at=0,
                 commit_account_version=1, spec_version="v", rule_data_version="v")
TRANSPORT = dict(route="REST", operation="ORDER", client_order_id="c", code="")
INSPECTION = dict(schema_version="inspect_state_v1", account_key=A, profile_id="SYNTHETIC_P1_O03_V1",
                  config_id="other-config", account_version=0, valuation_context_id=H, gate="RUNNING",
                  lifecycle="RISK_STABLE", cash="-1", gross_realized="0", total_fees="1",
                  positions_digest=H, orders_digest=H, reservations_digest=H, owner_state_digest=H)


def member(kind: str) -> dict[str, object]:
    return deepcopy(dict(kind=kind, stamp=S, payload=INPUTS[kind]))


def group() -> dict[str, object]:
    return dict(schema_version="scenario_group_v1", group_id="g", account_key=deepcopy(A),
                ordering_contract_id="S_order_v1", group_effective_at=1, declared_member_count=1,
                members=[member("INTENT")])


def fact(kind: str) -> dict[str, object]:
    return deepcopy(dict(schema_version="snapshot_fact_v1", reference=R, request_digest=H,
                         snapshot_kind=kind, snapshot_as_of=0, immutable_payload=PAYLOADS[kind], payload_digest=H))


def delivery(kind: str, payload: object) -> dict[str, object]:
    return deepcopy(dict(account_key=A, delivery_id="d", source_fact_id="s", source_namespace="SOURCE",
                         payload_kind=kind, occurrence_index=0, schedule_sequence=0, immutable_payload=payload,
                         payload_digest=H, visible_at=0))


DELIVERY_KINDS = ["MARKET_SNAPSHOT", "EARN_SNAPSHOT", "TRADING_SNAPSHOT", "POSITION_SNAPSHOT", "OPEN_ORDER_SNAPSHOT"]

CONFIGURED_PRODUCTS = ("CFG-FIRST", "CFG-MIDDLE", "CFG-LAST")


def configured_context(config_id="configured-v1"):
    return configuration_context(
        {
            "schema_version": "spider_configuration_context_v1",
            "config_id": config_id,
            "configuration_sha256": H,
            "products": list(CONFIGURED_PRODUCTS),
        }
    )


CASES: list[tuple[Callable[[object], None], object]] = [
    (n.account, A), (n.stamp, S), (n.reference, R), (n.rejection, {"event_id": "e", "reason": "CODE"}),
    (n.group, group()), (n.group_result, RESULT), (n.snapshot_request, REQUEST), (n.position, P),
    (n.open_order, ORDER), (n.execution_fact, EXECUTION), (n.transport, TRANSPORT), (n.inspection, INSPECTION),
    (n.delivery, delivery("EXECUTION_FACT", EXECUTION)), (n.delivery, delivery("TRANSPORT_ACK", TRANSPORT)),
]
CASES += [(n.member, member(kind)) for kind in INPUTS]
CASES += [(n.snapshot_fact, fact(kind)) for kind in PAYLOADS]
CASES += [(n.delivery, delivery(kind, payload)) for kind, payload in zip(DELIVERY_KINDS, PAYLOADS.values(), strict=True)]


def objects(value: object, path: tuple[object, ...] = ()):
    if isinstance(value, dict):
        yield path, value
        for key, item in value.items():
            yield from objects(item, path + (key,))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from objects(item, path + (index,))


def at(value: Any, path) -> Any:
    for key in path:
        value = value[key]
    return value


@pytest.mark.parametrize("check,fixture", CASES)
def test_all_graph_branches_are_closed_and_do_not_mutate(check, fixture):
    before = deepcopy(fixture)
    check(fixture)
    assert fixture == before
    for path, node in objects(fixture):
        mutated = deepcopy(fixture)
        at(mutated, path)["extra"] = None
        with pytest.raises(ValueError):
            check(mutated)
        for field in node:
            mutated = deepcopy(fixture)
            del at(mutated, path)[field]
            with pytest.raises(ValueError):
                check(mutated)
            mutated = deepcopy(fixture)
            at(mutated, path)[field] = None
            with pytest.raises(ValueError):
                check(mutated)


@pytest.mark.parametrize("check,fixture", CASES)
def test_scalar_types_are_checked_at_every_nested_boundary(check, fixture):
    for path, node in objects(fixture):
        for field, value in node.items():
            bad = True if type(value) is int else 1 if type(value) is bool else 1.5
            mutated = deepcopy(fixture)
            at(mutated, path)[field] = bad
            with pytest.raises(ValueError):
                check(mutated)


@pytest.mark.parametrize("kind,other", list(product(INPUTS, repeat=2)))
def test_member_discriminant_cannot_relabel_a_payload(kind, other):
    value = member(kind)
    value["payload"] = deepcopy(INPUTS[other])
    if kind == other:
        n.member(value)
    else:
        with pytest.raises(ValueError):
            n.member(value)


@pytest.mark.parametrize("kind,other", list(product(PAYLOADS, repeat=2)))
def test_snapshot_payload_cross_wiring(kind, other):
    value = fact(kind)
    value["immutable_payload"] = deepcopy(PAYLOADS[other])
    if kind == other:
        n.snapshot_fact(value)
    else:
        with pytest.raises(ValueError):
            n.snapshot_fact(value)


@pytest.mark.parametrize("kind", list(PAYLOADS))
def test_failure_snapshot_union(kind):
    value = fact(kind)
    value["immutable_payload"] = {"outcome": "FAILURE", "reason": "SYNTHETIC_FAILURE"}
    n.snapshot_fact(value)
    for payload in [{"outcome": "UNKNOWN", "reason": "SYNTHETIC_FAILURE"},
                    {"outcome": "FAILURE", "reason": "OTHER"}, {"outcome": "SUCCESS", "reason": "SYNTHETIC_FAILURE"}]:
        value["immutable_payload"] = payload
        with pytest.raises(ValueError):
            n.snapshot_fact(value)


OPTIONALS = [
    (n.account, A, "subaccount", "sub", True), (n.stamp, S, "source_sequence", 1, True),
    (n.group_result, RESULT, "group_digest", H, False), (n.group_result, RESULT, "failure", "CODE", False),
    (n.group_result, RESULT, "gate_failure", "CODE", False),
    (n.snapshot_request, REQUEST, "fixture_key", "fixture", True),
    (n.snapshot_request, REQUEST, "continuation_id", "c", True),
    (n.position, P, "last_price", "1", False), (n.position, P, "notional_usd", "1", False),
    (n.snapshot_fact, fact("EARN"), "captured_account_version", 1, False),
    (n.snapshot_fact, fact("EARN"), "continuation_id", "c", False),
    (n.delivery, delivery("EXECUTION_FACT", EXECUTION), "snapshot_version", 1, False),
    (n.delivery, delivery("EXECUTION_FACT", EXECUTION), "snapshot_as_of", 1, False),
    (n.delivery, delivery("EXECUTION_FACT", EXECUTION), "continuation_id", "c", False),
    (n.inspection, INSPECTION, "gate_failure", "CODE", False),
]
OPTIONALS += [(n.transport, TRANSPORT, key, value, True) for key, value in
              dict(order_id="o", message="", product_id="P_A", side="buy", limit_price="1", size_contracts="1").items()]


@pytest.mark.parametrize("check,fixture,key,value,nullable", OPTIONALS)
def test_optional_presence_null_and_wrong_type(check, fixture, key, value, nullable):
    check(fixture)
    for replacement in [value, None, {}]:
        row = deepcopy(fixture)
        row[key] = replacement
        if replacement == value or replacement is None and nullable:
            check(row)
        else:
            with pytest.raises(ValueError):
                check(row)


@pytest.mark.parametrize("kind,key", [("INTENT", "limit_price"), ("EXECUTION", "reported_fee"), ("EXECUTION", "fee_asset")])
def test_nested_optional_input_fields(kind, key):
    for replacement in [None, "1", True]:
        row = member(kind)
        at(row, ("payload",))[key] = replacement
        if replacement is True:
            with pytest.raises(ValueError):
                n.member(row)
        else:
            n.member(row)


@pytest.mark.parametrize("classification,gate,lifecycle", product(
    ["COMMITTED", "REJECTED", "FAULT"], ["RUNNING", "FAILED"],
    ["RISK_STABLE", "AWAITING_CANCEL_EFFECTIVE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"]))
def test_result_values_are_structural_not_financial_predicates(classification, gate, lifecycle):
    for optional in [{}, {"group_digest": H, "failure": "CODE", "gate_failure": "OTHER_CODE"}]:
        n.group_result({**RESULT, **optional, "classification": classification, "gate_after": gate, "lifecycle_after": lifecycle})


@pytest.mark.parametrize("value", ["1.0", "1e2", "-0", "NaN", "Infinity", "01", "+1", " 1"])
def test_canonical_decimal_text_is_required(value):
    with pytest.raises(ValueError):
        n.position({**P, "position_contracts": value})


def test_ordering_duplicates_and_preserved_group_input_order():
    for parents in [["b", "a"], ["a", "a"]]:
        with pytest.raises(ValueError):
            n.stamp({**S, "causal_parent_ids": parents})
    for kind, first, second in [("POSITIONS", P, {**P, "product_id": "Z"}),
                                ("OPEN_ORDERS", ORDER, {**ORDER, "order_id": "z"})]:
        n.snapshot_payload({"outcome": "SUCCESS", "rows": [first, second]}, kind)
        for rows in [[second, first], [first, first]]:
            with pytest.raises(ValueError):
                n.snapshot_payload({"outcome": "SUCCESS", "rows": rows}, kind)
    row = group()
    with pytest.raises(ValueError):
        n.group({**row, "declared_member_count": 2})
    with pytest.raises(ValueError):
        n.group({**row, "declared_member_count": 2, "members": [member("INTENT"), member("INTENT")]})
    other = member("EXECUTION")
    at(other, ("stamp",))["event_id"] = "other"
    at(other, ("stamp",))["scenario_ordinal"] = 30
    intent = member("INTENT")
    at(intent, ("stamp",))["scenario_ordinal"] = 60
    descending = {**row, "declared_member_count": 2, "members": [intent, other]}
    before = deepcopy(descending)
    n.group(descending)
    assert descending == before
    first = {**ORDER, "product_id": "BTC-USDT-SWAP", "order_id": "z"}
    second = {**ORDER, "product_id": "ETH-USDT-SWAP", "order_id": "a"}
    n.snapshot_payload({"outcome": "SUCCESS", "rows": [first, second]}, "OPEN_ORDERS")
    with pytest.raises(ValueError):
        n.snapshot_payload({"outcome": "SUCCESS", "rows": [second, first]}, "OPEN_ORDERS")


def test_delivery_union_and_unknown_enums_reject():
    for kind in ["EXECUTION_FACT", "TRANSPORT_ACK", *DELIVERY_KINDS, "UNKNOWN"]:
        if kind != "EXECUTION_FACT":
            with pytest.raises(ValueError):
                n.delivery(delivery(kind, EXECUTION))
    for check, fixture, field in [(n.group_result, RESULT, "classification"), (n.inspection, INSPECTION, "gate"),
                                  (n.inspection, INSPECTION, "lifecycle"), (n.inspection, INSPECTION, "profile_id"),
                                  (n.snapshot_request, REQUEST, "capture_mode"), (n.open_order, ORDER, "side")]:
        with pytest.raises(ValueError):
            check({**fixture, field: "UNKNOWN"})


def test_architecture_is_pure_and_does_not_import_native():
    source = Path(n.__file__).read_text()
    imports = [node for node in ast.walk(ast.parse(source)) if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert all(isinstance(node, ast.ImportFrom) and node.module in {
        "collections.abc", "typing", "src.core.backtest.spider_run_artifacts"
    } for node in imports)
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                   and node.func.id in {"open", "eval", "exec", "__import__"} for node in ast.walk(ast.parse(source)))


def test_closed_enum_and_numeric_boundaries_through_all_graphs():
    enum_fields = {
        "kind", "ordering_contract_id", "side", "liquidity", "order_type", "classification",
        "gate_after", "lifecycle_after", "snapshot_kind", "capture_mode", "margin_mode", "state",
        "route", "operation", "source_namespace", "payload_kind", "profile_id", "gate", "lifecycle", "outcome",
    }
    money_fields = {
        "price", "mark", "quantity_contracts", "position_contracts", "limit_price", "fill_price",
        "original_size_contracts", "cumulative_filled_size_contracts", "contract_value", "lot_size",
        "minimum_size", "price_increment", "high_low_ratio", "earn", "equity", "available_equity",
        "cash", "gross_realized", "total_fees",
    }
    for check, fixture in CASES:
        for path, node in objects(fixture):
            for field, value in node.items():
                bad_values = []
                if field in enum_fields or field == "schema_version":
                    bad_values.append("UNKNOWN")
                if type(value) is int:
                    bad_values.append(-1)
                if field in money_fields:
                    bad_values.append("1.0")
                if field.endswith("digest") or field == "valuation_context_id":
                    bad_values.extend(["f" * 63, "F" * 64])
                for bad in bad_values:
                    mutated = deepcopy(fixture)
                    at(mutated, path)[field] = bad
                    with pytest.raises(ValueError):
                        check(mutated)


@pytest.mark.parametrize("kind,key", [("CANCEL_REQUEST", "targets"), ("CANCEL_EFFECT", "effects"), ("CONTEXT_MARKS", "rows")])
def test_duplicate_member_collection_identities_reject(kind, key):
    value = member(kind)
    rows = at(value, ("payload", key))
    rows.append(deepcopy(rows[0]))
    with pytest.raises(ValueError):
        n.member(value)


@pytest.mark.parametrize("reason", ["lowercase", "CODE WITH SPACE", "A" * 129, ""])
def test_diagnostic_reasons_are_bounded_codes_not_a_duplicate_registry(reason):
    with pytest.raises(ValueError):
        n.rejection({"event_id": "e", "reason": reason})
    n.rejection({"event_id": "e", "reason": "NEW_UPSTREAM_CODE_1"})


def test_all_native_profile_and_ordering_tokens_remain_structurally_supported():
    for profile in ["SYNTHETIC_BTC_ETH_V1", "SYNTHETIC_GOLDEN_CANCEL_V1", "SYNTHETIC_MIN_CASH_V1",
                    "SYNTHETIC_P1_LIQUIDATION_V1", "SYNTHETIC_P1_O03_V1"]:
        n.inspection({**INSPECTION, "profile_id": profile})
    n.group({**group(), "ordering_contract_id": "S_order_v1_reverse_execution_cancel_effective"})


@pytest.mark.parametrize("product", CONFIGURED_PRODUCTS)
def test_configured_products_reach_every_product_bearing_native_boundary(product):
    context = configured_context()

    marks = member("CONTEXT_MARKS")
    at(marks, ("payload", "rows", 0))["product_id"] = product
    n.member(marks, context=context)

    for kind in ("EXECUTION", "INTENT"):
        value = member(kind)
        at(value, ("payload",))["product_id"] = product
        n.member(value, context=context)

    configured_group = group()
    at(configured_group, ("members", 0, "payload"))["product_id"] = product
    n.group(configured_group, context=context)

    position_row = {**P, "product_id": product}
    order_row = {**ORDER, "product_id": product}
    market_row = {**M, "product_id": product}
    n.position(position_row, context=context)
    n.open_order(order_row, context=context)
    n.snapshot_payload({"markets": [market_row]}, "MARKET", context=context)
    n.snapshot_payload(
        {"outcome": "SUCCESS", "rows": [position_row]}, "POSITIONS", context=context
    )
    n.snapshot_payload(
        {"outcome": "SUCCESS", "rows": [order_row]}, "OPEN_ORDERS", context=context
    )

    execution = {**EXECUTION, "product_id": product}
    n.execution_fact(execution, context=context)
    transport_payload = {**TRANSPORT, "product_id": product}
    n.transport(transport_payload, context=context)

    for snapshot_kind, payload_kind, snapshot_payload_value in (
        ("MARKET", "MARKET_SNAPSHOT", {"markets": [market_row]}),
        (
            "POSITIONS",
            "POSITION_SNAPSHOT",
            {"outcome": "SUCCESS", "rows": [position_row]},
        ),
        (
            "OPEN_ORDERS",
            "OPEN_ORDER_SNAPSHOT",
            {"outcome": "SUCCESS", "rows": [order_row]},
        ),
    ):
        snapshot = fact(snapshot_kind)
        snapshot["immutable_payload"] = snapshot_payload_value
        n.snapshot_fact(snapshot, context=context)
        n.delivery(delivery(payload_kind, snapshot_payload_value), context=context)
    n.delivery(delivery("EXECUTION_FACT", execution), context=context)
    n.delivery(delivery("TRANSPORT_ACK", transport_payload), context=context)


@pytest.mark.parametrize("product", ["BTC-USDT-SWAP", "CFG-OTHER"])
def test_configured_context_rejects_nonmembers_at_every_product_boundary(product):
    context = configured_context()
    marks = member("CONTEXT_MARKS")
    at(marks, ("payload", "rows", 0))["product_id"] = product
    intent = member("INTENT")
    at(intent, ("payload",))["product_id"] = product
    execution_member = member("EXECUTION")
    at(execution_member, ("payload",))["product_id"] = product
    configured_group = {**group(), "members": [intent]}
    position = {**P, "product_id": product}
    order = {**ORDER, "product_id": product}
    market = {**M, "product_id": product}
    execution = {**EXECUTION, "product_id": product}
    transport_payload = {**TRANSPORT, "product_id": product}
    position_payload = {"outcome": "SUCCESS", "rows": [position]}
    order_payload = {"outcome": "SUCCESS", "rows": [order]}
    market_payload = {"markets": [market]}
    boundaries = [
        (n.member, marks),
        (n.member, execution_member),
        (n.member, intent),
        (n.group, configured_group),
        (n.position, position),
        (n.open_order, order),
        (n.transport, transport_payload),
        (
            lambda value, context: n.snapshot_payload(value, "MARKET", context=context),
            market_payload,
        ),
        (
            lambda value, context: n.snapshot_payload(value, "POSITIONS", context=context),
            position_payload,
        ),
        (
            lambda value, context: n.snapshot_payload(value, "OPEN_ORDERS", context=context),
            order_payload,
        ),
    ]
    for kind, payload in (
        ("MARKET", market_payload),
        ("POSITIONS", position_payload),
        ("OPEN_ORDERS", order_payload),
    ):
        snapshot = fact(kind)
        snapshot["immutable_payload"] = payload
        boundaries.append((n.snapshot_fact, snapshot))
    for kind, payload in (
        ("TRANSPORT_ACK", transport_payload),
        ("MARKET_SNAPSHOT", market_payload),
        ("POSITION_SNAPSHOT", position_payload),
        ("OPEN_ORDER_SNAPSHOT", order_payload),
        ("EXECUTION_FACT", execution),
    ):
        boundaries.append((n.delivery, delivery(kind, payload)))

    for check, fixture in boundaries:
        with pytest.raises(ValueError):
            check(deepcopy(fixture), context=context)


@pytest.mark.parametrize(
    "product", ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "P_A", "CFG-OTHER"]
)
def test_configured_product_context_replaces_p1_membership(product):
    context = configured_context()
    value = member("INTENT")
    at(value, ("payload",))["product_id"] = product
    with pytest.raises(ValueError):
        n.member(value, context=context)
    with pytest.raises(ValueError):
        n.group({**group(), "members": [value]}, context=context)


def test_configured_nested_context_is_required_through_group_delivery_and_snapshot():
    context = configured_context()
    configured_intent = member("INTENT")
    at(configured_intent, ("payload",))["product_id"] = "CFG-MIDDLE"
    configured_group = {**group(), "members": [configured_intent]}
    n.group(configured_group, context=context)
    with pytest.raises(ValueError):
        n.group(configured_group)

    execution = {**EXECUTION, "product_id": "CFG-MIDDLE"}
    execution_delivery = delivery("EXECUTION_FACT", execution)
    n.delivery(execution_delivery, context=context)
    with pytest.raises(ValueError):
        n.delivery(execution_delivery)

    snapshot = fact("POSITIONS")
    snapshot["immutable_payload"] = {
        "outcome": "SUCCESS",
        "rows": [{**P, "product_id": "CFG-MIDDLE"}],
    }
    n.snapshot_fact(snapshot, context=context)
    positions = cast(dict[str, object], snapshot["immutable_payload"])
    rows = cast(list[dict[str, object]], positions["rows"])
    rows[0]["product_id"] = "CFG-OTHER"
    with pytest.raises(ValueError):
        n.snapshot_fact(snapshot, context=context)

    position_delivery = delivery(
        "POSITION_SNAPSHOT",
        {"outcome": "SUCCESS", "rows": [{**P, "product_id": "CFG-OTHER"}]},
    )
    with pytest.raises(ValueError):
        n.delivery(position_delivery, context=context)


def test_configured_inspection_binds_profile_and_config_id_not_product_membership():
    first = configured_context("config-one")
    second = configured_context("config-two")
    assert first.products == second.products == CONFIGURED_PRODUCTS
    assert first.config_id != second.config_id
    configured = {
        **INSPECTION,
        "profile_id": "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1",
        "config_id": "config-one",
    }
    n.inspection(configured, context=first)
    with pytest.raises(ValueError):
        n.inspection(configured, context=second)
    with pytest.raises(ValueError):
        n.inspection(configured)
    with pytest.raises(ValueError):
        n.inspection(INSPECTION, context=first)
    n.inspection({**INSPECTION, "config_id": "any-other-nonempty-id"})
    assert n._profile_config_identity(
        INSPECTION["profile_id"], "any-other-nonempty-id", None
    )
    assert not n._profile_config_identity(
        INSPECTION["profile_id"], "scenario-v1", first,
        p1_config_id="scenario-v1",
    )


@pytest.mark.parametrize("profile", n._PROFILES)
def test_every_p1_profile_rejects_a_configured_validation_context(profile):
    context = configured_context()
    n.inspection({**INSPECTION, "profile_id": profile})
    with pytest.raises(ValueError):
        n.inspection({**INSPECTION, "profile_id": profile}, context=context)
