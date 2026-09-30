"""Closed structural validation of persisted native DTOs; no runtime owner."""

from collections.abc import Callable
from decimal import Decimal
from typing import cast

from src.core.backtest.spider_run_artifacts import (
    ConfigurationContext,
    _boolean,
    _decimal_text,
    _enum,
    _integer,
    _list,
    _object,
    _require,
    _text,
    configuration_context,
)
from src.core.backtest.spider_historical_input import historical_market_step_clock

_PRODUCTS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "P_A"]
_KINDS = ["MARKET", "EARN", "TRADING", "POSITIONS", "OPEN_ORDERS"]
_ORDERING = ["S_order_v1", "S_order_v1_reverse_execution_cancel_effective"]
_LIFECYCLES = ["RISK_STABLE", "AWAITING_CANCEL_EFFECTIVE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"]
_PROFILES = ["SYNTHETIC_BTC_ETH_V1", "SYNTHETIC_GOLDEN_CANCEL_V1", "SYNTHETIC_MIN_CASH_V1", "SYNTHETIC_P1_LIQUIDATION_V1", "SYNTHETIC_P1_O03_V1"]
_CONFIGURED_PROFILE = "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1"
_HISTORICAL_MODELS = ["OHLC4_OPEN_HIGH_LOW_CLOSE_V1", "OHLC4_OPEN_LOW_HIGH_CLOSE_V1"]


def _fields(row: dict[str, object], names: str, check: Callable[[object], object]) -> None:
    for name in names.split():
        check(row[name])


def _optional(row: dict[str, object], names: str, check: Callable[[object], object], *, nullable: bool = False) -> None:
    for name in names.split():
        if name in row and not (nullable and row[name] is None):
            check(row[name])


def _hash(value: object) -> str:
    return _text(value, "[0-9a-f]{64}")


def _context(value: ConfigurationContext | None) -> ConfigurationContext | None:
    return None if value is None else configuration_context(value)


def _product_id(
    value: object,
    context: ConfigurationContext | None,
    default_domain: list[str] | None,
) -> str:
    if context is not None:
        return _enum(value, context.products)
    return _text(value) if default_domain is None else _enum(value, default_domain)


def _profile_config_identity(
    profile_id: object,
    config_id: object,
    context: ConfigurationContext | None,
    *,
    p1_config_id: str | None = None,
) -> bool:
    profile = _text(profile_id)
    config = _text(config_id)
    context = _context(context)
    if context is not None:
        return profile == _CONFIGURED_PROFILE and config == context.config_id
    return profile in _PROFILES and (p1_config_id is None or config == p1_config_id)


def _reason(value: object) -> str:
    return _text(value, "[A-Z][A-Z0-9_]{0,127}")


def account(value: object) -> None:
    row = _object(value, "venue environment account", "subaccount")
    _fields(row, "venue environment account", _text)
    _optional(row, "subaccount", _text, nullable=True)


def stamp(value: object, *, historical: bool = False) -> None:
    row = _object(value, "event_id effective_at causal_parent_ids ordering_contract_id scenario_ordinal", "source_sequence")
    _text(row["event_id"])
    _fields(row, "effective_at scenario_ordinal", _integer)
    _optional(row, "source_sequence", _integer, nullable=True)
    ordering = _ORDERING + (["HISTORICAL_ORDER_V1"] if historical else [])
    _enum(row["ordering_contract_id"], ordering)
    parents = [_text(item) for item in _list(row["causal_parent_ids"])]
    _require(parents == sorted(set(parents)))


def reference(value: object) -> None:
    row = _object(value, "namespace fact_id")
    _enum(row["namespace"], ["SOURCE", "LIQUIDATION", "SNAPSHOT"])
    _text(row["fact_id"])


def rejection(value: object) -> None:
    row = _object(value, "event_id reason")
    _text(row["event_id"])
    _reason(row["reason"])


def member(value: object, *, context: ConfigurationContext | None = None, historical: bool = False) -> None:
    context = _context(context)
    row = _object(value, "kind stamp payload")
    stamp(row["stamp"], historical=historical)
    kind = _enum(row["kind"], ["CONTEXT_MARKS", "EXECUTION", "INTENT", "CANCEL_REQUEST", "CANCEL_EFFECT"])
    if kind == "CONTEXT_MARKS":
        payload = _object(row["payload"], "expected_before expected_after rows")
        _fields(payload, "expected_before expected_after", _hash)
        products: list[str] = []
        for item in _list(payload["rows"]):
            mark = _object(item, "product_id valid_from valid_to mark")
            products.append(_product_id(mark["product_id"], context, _PRODUCTS[:2]))
            _fields(mark, "valid_from valid_to", _integer)
            _decimal_text(mark["mark"])
        _require(len(products) == len(set(products)))
    elif kind in ("EXECUTION", "INTENT"):
        if kind == "EXECUTION":
            payload = _object(row["payload"], "namespace product_id external_execution_id order_id side price quantity_contracts liquidity matching_effective_at candidate_id source_id visible_at expected_account_version expected_order_version spec_version rule_data_version", "fee_asset reported_fee")
            _fields(payload, "namespace external_execution_id order_id candidate_id source_id spec_version rule_data_version", _text)
            _fields(payload, "matching_effective_at visible_at expected_account_version expected_order_version", _integer)
            _decimal_text(payload["price"])
            _enum(payload["liquidity"], ["SYNTHETIC_TAKER"])
            _optional(payload, "fee_asset", _text, nullable=True)
            _optional(payload, "reported_fee", _decimal_text, nullable=True)
        else:
            payload = _object(row["payload"], "intent_id client_order_id config_id product_id strategy_id side order_type quantity_contracts reduce_only requested_at", "limit_price")
            _fields(payload, "intent_id client_order_id config_id strategy_id", _text)
            _enum(payload["order_type"], ["LIMIT", "MARKET"])
            _boolean(payload["reduce_only"])
            _integer(payload["requested_at"])
            _optional(payload, "limit_price", _decimal_text, nullable=True)
        _product_id(payload["product_id"], context, _PRODUCTS)
        _enum(payload["side"], ["LONG", "SHORT"])
        _decimal_text(payload["quantity_contracts"])
    else:
        key = "targets" if kind == "CANCEL_REQUEST" else "effects"
        payload = _object(row["payload"], key)
        targets: list[str] = []
        for item in _list(payload[key]):
            target = _object(item, "target_order_id reason" + (" detecting_event_id" if kind == "CANCEL_EFFECT" else ""))
            targets.append(_text(target["target_order_id"]))
            _enum(target["reason"], ["EXPLICIT_SCENARIO", "RISK_SHORTFALL", "MMR_BREACH", "SPEC_MIGRATION", "UNSUPPORTED"])
            if kind == "CANCEL_EFFECT":
                _text(target["detecting_event_id"])
        _require(len(targets) == len(set(targets)))


def group(value: object, *, context: ConfigurationContext | None = None, historical: bool = False) -> None:
    context = _context(context)
    row = _object(value, "schema_version group_id account_key ordering_contract_id group_effective_at declared_member_count members")
    _enum(row["schema_version"], ["scenario_group_v1"])
    _text(row["group_id"])
    account(row["account_key"])
    ordering = _ORDERING + (["HISTORICAL_ORDER_V1"] if historical else [])
    _enum(row["ordering_contract_id"], ordering)
    _integer(row["group_effective_at"])
    members = _list(row["members"])
    _require(_integer(row["declared_member_count"]) == len(members))
    for item in members:
        member(item, context=context, historical=historical)
    events = [cast(dict[str, object], cast(dict[str, object], item)["stamp"])["event_id"] for item in members]
    _require(len(events) == len(set(events)))


def group_result(value: object) -> None:
    row = _object(value, "schema_version classification group_id committed_references rejections account_version_before account_version_after gate_after lifecycle_after owner_state_digest result_digest", "group_digest failure gate_failure")
    _enum(row["schema_version"], ["group_result_v1"])
    _enum(row["classification"], ["COMMITTED", "REJECTED", "FAULT"])
    _text(row["group_id"])
    _fields(row, "account_version_before account_version_after", _integer)
    _enum(row["gate_after"], ["RUNNING", "FAILED"])
    _enum(row["lifecycle_after"], _LIFECYCLES)
    _fields(row, "owner_state_digest result_digest", _hash)
    _optional(row, "group_digest", _hash)
    _optional(row, "failure gate_failure", _reason)
    for item in _list(row["committed_references"]):
        reference(item)
    for item in _list(row["rejections"]):
        rejection(item)


def snapshot_request(value: object) -> None:
    row = _object(value, "schema_version account_key snapshot_id snapshot_kind capture_mode captured_at", "fixture_key continuation_id")
    _enum(row["schema_version"], ["snapshot_request_v1"])
    account(row["account_key"])
    _text(row["snapshot_id"])
    _enum(row["snapshot_kind"], _KINDS)
    _enum(row["capture_mode"], ["OWNER_CURRENT", "FROZEN_POLL_FIXTURE"])
    _integer(row["captured_at"])
    _optional(row, "fixture_key continuation_id", _text, nullable=True)


def position(value: object, *, context: ConfigurationContext | None = None) -> None:
    context = _context(context)
    row = _object(value, "product_id margin_mode position_contracts", "last_price notional_usd")
    _product_id(row["product_id"], context, None)
    _enum(row["margin_mode"], ["cross"])
    _decimal_text(row["position_contracts"])
    _optional(row, "last_price notional_usd", _decimal_text)


def open_order(value: object, *, context: ConfigurationContext | None = None) -> None:
    context = _context(context)
    row = _object(value, "order_id client_order_id product_id state side limit_price original_size_contracts cumulative_filled_size_contracts created_at")
    _fields(row, "order_id client_order_id", _text)
    _product_id(row["product_id"], context, None)
    _enum(row["state"], ["live", "partially_filled"])
    _enum(row["side"], ["buy", "sell"])
    _fields(row, "limit_price original_size_contracts cumulative_filled_size_contracts", _decimal_text)
    _integer(row["created_at"])


def snapshot_payload(value: object, kind: str, *, context: ConfigurationContext | None = None) -> None:
    context = _context(context)
    _enum(kind, _KINDS)
    if type(value) is dict and value.get("outcome") == "FAILURE":
        row = _object(value, "outcome reason")
        _enum(row["outcome"], ["FAILURE"])
        _enum(row["reason"], ["SYNTHETIC_FAILURE"])
    elif kind == "MARKET":
        row = _object(value, "markets")
        for item in _list(row["markets"]):
            market = _object(item, "product_id price contract_value lot_size minimum_size price_increment high_low_ratio state instrument_code")
            _product_id(market["product_id"], context, None)
            _fields(market, "price contract_value lot_size minimum_size price_increment high_low_ratio", _decimal_text)
            _enum(market["state"], ["live"])
            _integer(market["instrument_code"])
    elif kind in ("EARN", "TRADING"):
        names = "earn" if kind == "EARN" else "equity available_equity"
        row = _object(value, "outcome " + names)
        _enum(row["outcome"], ["SUCCESS"])
        _fields(row, names, _decimal_text)
    else:
        row = _object(value, "outcome rows")
        _enum(row["outcome"], ["SUCCESS"])
        identities: list[tuple[str, ...]] = []
        for item in _list(row["rows"]):
            (position if kind == "POSITIONS" else open_order)(item, context=context)
            native_row = cast(dict[str, object], item)
            keys = ("product_id",) if kind == "POSITIONS" else ("product_id", "order_id")
            identities.append(tuple(_product_id(native_row[key], context, None) if key == "product_id" else _text(native_row[key]) for key in keys))
        _require(identities == sorted(set(identities)))


def execution_fact(value: object, *, context: ConfigurationContext | None = None) -> None:
    context = _context(context)
    row = _object(value, "order_id owner_client_order_id policy_client_order_id product_id state side limit_price fill_price original_size_contracts cumulative_filled_size_contracts contract_value execution_effective_at commit_account_version spec_version rule_data_version")
    _fields(row, "order_id owner_client_order_id policy_client_order_id spec_version rule_data_version", _text)
    _product_id(row["product_id"], context, _PRODUCTS)
    _enum(row["state"], ["partially_filled", "filled"])
    _enum(row["side"], ["buy", "sell"])
    _fields(row, "limit_price fill_price original_size_contracts cumulative_filled_size_contracts contract_value", _decimal_text)
    _fields(row, "execution_effective_at commit_account_version", _integer)


def transport(value: object, *, context: ConfigurationContext | None = None) -> None:
    context = _context(context)
    row = _object(value, "route operation client_order_id code", "order_id message product_id side limit_price size_contracts")
    _enum(row["route"], ["REST", "WS"])
    _enum(row["operation"], ["ORDER", "CANCEL"])
    _text(row["client_order_id"])
    _require(type(row["code"]) is str)
    _optional(row, "order_id", _text, nullable=True)
    if "product_id" in row and row["product_id"] is not None:
        _product_id(row["product_id"], context, None)
    _optional(row, "message", lambda item: _require(type(item) is str), nullable=True)
    _optional(row, "side", lambda item: _enum(item, ["buy", "sell"]), nullable=True)
    _optional(row, "limit_price size_contracts", _decimal_text, nullable=True)


def snapshot_fact(value: object, *, context: ConfigurationContext | None = None) -> None:
    context = _context(context)
    row = _object(value, "schema_version reference request_digest snapshot_kind snapshot_as_of immutable_payload payload_digest", "captured_account_version continuation_id")
    _enum(row["schema_version"], ["snapshot_fact_v1"])
    reference(row["reference"])
    _fields(row, "request_digest payload_digest", _hash)
    _integer(row["snapshot_as_of"])
    _optional(row, "captured_account_version", _integer)
    _optional(row, "continuation_id", _text)
    snapshot_payload(row["immutable_payload"], _enum(row["snapshot_kind"], _KINDS), context=context)


def delivery(value: object, *, context: ConfigurationContext | None = None) -> None:
    context = _context(context)
    row = _object(value, "account_key delivery_id source_fact_id source_namespace payload_kind occurrence_index schedule_sequence immutable_payload payload_digest visible_at", "snapshot_version snapshot_as_of continuation_id")
    account(row["account_key"])
    _fields(row, "delivery_id source_fact_id", _text)
    _enum(row["source_namespace"], ["SOURCE", "LIQUIDATION", "SNAPSHOT"])
    _fields(row, "occurrence_index schedule_sequence visible_at", _integer)
    _optional(row, "snapshot_version snapshot_as_of", _integer)
    _optional(row, "continuation_id", _text)
    _hash(row["payload_digest"])
    kind = _enum(row["payload_kind"], ["EXECUTION_FACT", "TRANSPORT_ACK", "MARKET_SNAPSHOT", "EARN_SNAPSHOT", "TRADING_SNAPSHOT", "POSITION_SNAPSHOT", "OPEN_ORDER_SNAPSHOT"])
    if kind in ("EXECUTION_FACT", "TRANSPORT_ACK"):
        (execution_fact if kind == "EXECUTION_FACT" else transport)(row["immutable_payload"], context=context)
    else:
        native_kind = {"POSITION_SNAPSHOT": "POSITIONS", "OPEN_ORDER_SNAPSHOT": "OPEN_ORDERS"}.get(kind, kind.removesuffix("_SNAPSHOT"))
        snapshot_payload(row["immutable_payload"], native_kind, context=context)


def inspection(value: object, *, context: ConfigurationContext | None = None) -> None:
    context = _context(context)
    row = _object(value, "schema_version account_key profile_id config_id account_version valuation_context_id gate lifecycle cash gross_realized total_fees positions_digest orders_digest reservations_digest owner_state_digest", "gate_failure")
    _enum(row["schema_version"], ["inspect_state_v1"])
    account(row["account_key"])
    _require(_profile_config_identity(row["profile_id"], row["config_id"], context))
    _integer(row["account_version"])
    _fields(row, "valuation_context_id positions_digest orders_digest reservations_digest owner_state_digest", _hash)
    _enum(row["gate"], ["RUNNING", "FAILED"])
    _enum(row["lifecycle"], _LIFECYCLES)
    _fields(row, "cash gross_realized total_fees", _decimal_text)
    _optional(row, "gate_failure", _reason)


def historical_market_result(
    value: object,
    *,
    context: ConfigurationContext | None,
    historical_context: object,
) -> None:
    context = _context(context)
    _require(context is not None)
    configured = cast(ConfigurationContext, context)
    historical = _object(
        historical_context,
        "schema_version research_classification historical_input_sha256 path_pair_sha256 source_sha256 model_sha256 assumption_sha256 coverage_sha256 model_id model_version",
    )
    row = _object(value, "account_key request result working_orders_snapshot owner_inspection_before owner_inspection_after")
    account(row["account_key"])
    account_key = cast(dict[str, object], row["account_key"])
    _require(account_key["venue"] == "SPIDER_HISTORICAL_RESEARCH"
             and account_key["environment"] == "RESEARCH_ONLY")
    request = _object(
        row["request"],
        "schema_version model_id model_version run_contract_hash bar_open_ms bar_duration_ms step_index market_slippage_bps bars working_orders",
    )
    model = _enum(request["model_id"], _HISTORICAL_MODELS)
    _require(model == historical["model_id"] and request["model_version"] == str(historical["model_version"]))
    _require(request["schema_version"] == "historical_node_v1")
    _require(_text(request["run_contract_hash"], r"[0-9a-f]{64}") == historical["historical_input_sha256"])
    raw_open = _integer(request["bar_open_ms"])
    _require(raw_open < 2**59 and request["bar_duration_ms"] == 60_000)
    step = _integer(request["step_index"])
    _require(step <= 3)
    raw_time, effective_at = historical_market_step_clock(raw_open, step)
    slippage = Decimal(_decimal_text(request["market_slippage_bps"]))
    _require(slippage >= 0)
    bars = _list(request["bars"])
    _require(len(bars) == len(configured.products))
    for product, item in zip(configured.products, bars, strict=True):
        bar = _object(item, "product_id trade mark")
        _require(bar["product_id"] == product)
        for field, trade in (("trade", True), ("mark", False)):
            values = _object(bar[field], "open high low close" + (" volume_contracts confirmed source_row_hash" if trade else " confirmed source_row_hash"))
            _require(values["confirmed"] is True)
            _text(values["source_row_hash"], r"[0-9a-f]{64}")
            prices = [Decimal(_decimal_text(values[name])) for name in ("open", "high", "low", "close")]
            _require(all(price > 0 for price in prices)
                     and prices[1] >= max(prices[0], prices[3])
                     and prices[2] <= min(prices[0], prices[3]))
            if trade:
                _require(Decimal(_decimal_text(values["volume_contracts"])) >= 0)
    working = _list(request["working_orders"])
    _require(row["working_orders_snapshot"] == request["working_orders"])
    orders: dict[str, tuple[str, Decimal]] = {}
    for item in working:
        order = _object(item, "order_id product_id order_version status remaining_quantity_contracts accepted_at accepted_source_sequence order_kind side limit_price risk_cancel_pending")
        order_id = _text(order["order_id"])
        product = _product_id(order["product_id"], configured, None)
        _require(order_id not in orders)
        _fields(order, "order_version accepted_at accepted_source_sequence", _integer)
        quantity = Decimal(_decimal_text(order["remaining_quantity_contracts"]))
        _require(quantity > 0)
        _enum(order["status"], ["OPEN", "PARTIALLY_FILLED"])
        _enum(order["order_kind"], ["LIMIT", "MARKET"])
        _enum(order["side"], ["LONG", "SHORT"])
        _optional(order, "limit_price", _decimal_text, nullable=True)
        if order["limit_price"] is not None:
            _require(Decimal(_decimal_text(order["limit_price"])) > 0)
        _boolean(order["risk_cancel_pending"])
        orders[order_id] = (product, quantity)
    result = _object(row["result"], "schema_version raw_time_ms effective_at products owner_evidence")
    _require(result["schema_version"] == "historical_node_result_v1"
             and result["raw_time_ms"] == raw_time and result["effective_at"] == effective_at)
    products = _list(result["products"])
    _require(len(products) == len(configured.products))
    filled: dict[str, Decimal] = {}
    executions: set[str] = set()
    sources: set[str] = set()
    for product, item in zip(configured.products, products, strict=True):
        outcome = _object(item, "product_id capacity discarded_volume fills")
        _require(outcome["product_id"] == product)
        _require(Decimal(_decimal_text(outcome["capacity"])) >= 0
                 and Decimal(_decimal_text(outcome["discarded_volume"])) >= 0)
        for fill_value in _list(outcome["fills"]):
            fill = _object(fill_value, "order_id quantity_contracts price execution_id source_event_id occurrence_index")
            order_id = _text(fill["order_id"])
            quantity = Decimal(_decimal_text(fill["quantity_contracts"]))
            _require(order_id in orders and orders[order_id][0] == product and quantity > 0
                     and Decimal(_decimal_text(fill["price"])) > 0)
            _text(fill["execution_id"], r"[0-9a-f]{64}")
            source = _text(fill["source_event_id"])
            _require(fill["execution_id"] not in executions and source not in sources)
            executions.add(cast(str, fill["execution_id"]))
            sources.add(source)
            _integer(fill["occurrence_index"])
            filled[order_id] = filled.get(order_id, Decimal(0)) + quantity
    _require(all(quantity <= orders[order_id][1] for order_id, quantity in filled.items()))
    before = row["owner_inspection_before"]
    after = row["owner_inspection_after"]
    for inspection_value in (before, after):
        inspection(inspection_value, context=context)
        inspection_row = cast(dict[str, object], inspection_value)
        _require(inspection_row["account_key"] == account_key
                 and inspection_row["profile_id"] == _CONFIGURED_PROFILE)
    _require(result["owner_evidence"] == after)
    before_row, after_row = cast(dict[str, object], before), cast(dict[str, object], after)
    _require(_integer(after_row["account_version"]) >= _integer(before_row["account_version"]))
