"""Closed structural validation for the gross-only paid observation artifact."""

from __future__ import annotations

from decimal import Decimal
from typing import Callable, cast

from src.core.backtest.spider_run_artifacts import (
    _decimal_text, _enum, _integer, _list, _object, _require, _text,
)
from src.core.backtest import spider_run_envelope_schema as envelope
from src.core.backtest.spider_run_artifacts import (
    ConfigurationContext, GrossExecutionContext, gross_execution_context,
    configuration_context, historical_context,
)
from src.core.backtest.spider_run_artifacts import _boundary

_STATES = ("IDLE", "RESTRICTED", "INCOMPLETE", "RECOVERED",
           "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED")
_QUEUES = ("SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY",
           "HISTORICAL_MARKET_STEP", "HISTORICAL_MARKET_CACHE",
           "HISTORICAL_TIMER", "PAID_ENDPOINT_DRAIN")


def gross_group_control(value: object) -> None:
    control = _object(value, "schema_version restricted")
    _enum(control["schema_version"], ["spider_gross_response_group_control_v1"])
    _require(type(control["restricted"]) is bool)


def _decimal(value: object, *, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if type(value) is Decimal:
        _require(value.is_finite())
    else:
        _decimal_text(value)


def scheduler_key(value: object, *, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    row = _object(value, "visible_at queue_class schedule_sequence stable_id")
    _integer(row["visible_at"])
    _enum(row["queue_class"], _QUEUES)
    _integer(row["schedule_sequence"])
    _text(row["stable_id"])


def _source_state(value: object) -> None:
    row = _object(value, "policy_rows policy_markets policy_last_earn_ms cache_records")
    _integer(row["policy_last_earn_ms"])
    for item in _list(row["policy_rows"]):
        policy = _object(item, "active hold hold上限 hold下限 leverage name product_id 單數 歩差")
        for name in ("active", "hold上限", "hold下限", "name", "product_id", "單數", "歩差"):
            _text(policy[name])
        _decimal_or_text(policy["hold"])
        _decimal_or_text(policy["leverage"])
    markets = row["policy_markets"]
    _require(type(markets) is dict)
    for key, item in cast(dict[object, object], markets).items():
        _text(key)
        _policy_market(item)
    for item in _list(row["cache_records"]):
        cache = _object(item, "key snapshot result")
        _scheduler_key_list(cache["key"])
        snapshot = _object(cache["snapshot"], "close_ms markets")
        _integer(snapshot["close_ms"])
        for market in _list(snapshot["markets"]):
            row_market = _object(market, "product_id ctVal increment instIdCode lotSz minSz price ratioHL state")
            _text(row_market["product_id"])
            _policy_market({name: value for name, value in row_market.items() if name != "product_id"})
        _scheduler_result(cache["result"], expected_kind="HISTORICAL_MARKET_CACHE")


def _decimal_or_text(value: object) -> None:
    if type(value) is Decimal:
        _require(value.is_finite())
    else:
        _text(value)


def _policy_market(value: object) -> None:
    market = _object(value, "ctVal increment instIdCode lotSz minSz price ratioHL state")
    for name in ("ctVal", "increment", "lotSz", "minSz", "price", "ratioHL"):
        _decimal_or_text(market[name])
    _integer(market["instIdCode"])
    _enum(market["state"], ["live"])


def _scheduler_result(value: object, *, expected_kind: str | None = None,
                      gross_cut_evidence: bool = False) -> None:
    row = _object_keys(value, "kind stable_id classification",
                       "market_snapshot group_result gross_group_control events callbacks reason native_failure")
    kind = _text(row["kind"])
    if expected_kind is not None:
        _enum(kind, [expected_kind])
    _text(row["stable_id"])
    _enum(row["classification"], ["PENDING", "SUCCESS", "TERMINAL"])
    if "market_snapshot" in row:
        snapshot = _object(row["market_snapshot"], "close_ms markets")
        _integer(snapshot["close_ms"])
        for item in _list(snapshot["markets"]):
            market = _object(item, "product_id ctVal increment instIdCode lotSz minSz price ratioHL state")
            _text(market["product_id"])
            _policy_market({name: value for name, value in market.items() if name != "product_id"})
    if "group_result" in row:
        from src.core.backtest.spider_run_native_schema import group_result
        group_result(row["group_result"])
    if "gross_group_control" in row:
        gross_group_control(row["gross_group_control"])
    if "events" in row:
        _list(row["events"])
    if "callbacks" in row:
        callbacks = row["callbacks"]
        if gross_cut_evidence and row["classification"] == "TERMINAL":
            _require(type(callbacks) is dict)
            for delivery_id, audit_events in cast(dict[object, object], callbacks).items():
                _text(delivery_id, r"[0-9a-f]{64}")
                for event in _list(audit_events):
                    _audit_event(event)
        else:
            _list(callbacks)
    if "reason" in row:
        _text(row["reason"])
    if "native_failure" in row and row["native_failure"] is not None:
        _object(row["native_failure"], "kind reason")


def _audit_event(value: object) -> None:
    event = _object(value, "event", "actions disposition")
    envelope._policy_event(event["event"])
    if "actions" in event:
        for action in _list(event["actions"]):
            row = _object(action, "status group_id", "group_result native_failure")
            _enum(row["status"], ["UNSUBMITTED", "SUBMITTED"])
            if row["group_id"] is not None:
                _text(row["group_id"])
            if row.get("group_result") is not None:
                from src.core.backtest.spider_run_native_schema import group_result
                group_result(row["group_result"])
            if row.get("native_failure") is not None:
                failure = _object(row["native_failure"], "reason poisoned")
                _text(failure["reason"])
                _require(type(failure["poisoned"]) is bool)
    if "disposition" in event:
        _enum(event["disposition"], ["NONE", "ALL", "PARTIAL"])


def _send_dependency(value: object) -> None:
    dep = _object(value, "client_order_id product_id strategy_id group_id event_id state binding role")
    for name in ("client_order_id", "product_id", "strategy_id", "group_id", "event_id"):
        _text(dep[name])
    _enum(dep["state"], ["PENDING", "COMMITTED", "REJECTED"])
    _enum(dep["role"], ["GRID", "EXIT", "GROSS_REDUCTION"])
    if dep["binding"] is not None:
        binding = _object(dep["binding"], "client_order_id order_id product_id strategy_id")
        for name in ("client_order_id", "order_id", "product_id", "strategy_id"):
            _text(binding[name])


def _product_pending_delivery(value: object) -> None:
    row = _object(value, "delivery_id role cancel_action_id")
    _text(row["delivery_id"])
    if row["role"] is not None:
        _enum(row["role"], ["GRID", "EXIT", "DRAIN_CANCEL", "GROSS_CANCEL", "GROSS_REDUCTION"])
    if row["cancel_action_id"] is not None:
        _text(row["cancel_action_id"])


def _cancel_effect(value: object) -> None:
    row = _object_keys(value,
                       "parent_raw request_event_id effect_event_id effect_group_id target_order_id product_id strategy_id reason",
                       "paid_action_id paid_action_role")
    _integer(row["parent_raw"])
    for name in ("request_event_id", "effect_event_id", "effect_group_id", "target_order_id",
                 "product_id", "strategy_id", "reason"):
        _text(row[name])
    if "paid_action_id" in row and row["paid_action_id"] is not None:
        _text(row["paid_action_id"])
    if "paid_action_role" in row and row["paid_action_role"] is not None:
        _enum(row["paid_action_role"], ["DRAIN_CANCEL", "GROSS_CANCEL"])


def _delivery(value: object) -> None:
    row = _object(value,
                   "account_key delivery_id source_fact_id source_namespace payload_kind occurrence_index schedule_sequence immutable_payload payload_digest visible_at",
                   "snapshot_version snapshot_as_of continuation_id")
    from src.core.backtest.spider_run_native_schema import account, open_order, position
    account(row["account_key"])
    _text(row["delivery_id"])
    _text(row["source_fact_id"])
    _enum(row["source_namespace"], ["SOURCE", "LIQUIDATION", "SNAPSHOT"])
    for name in ("occurrence_index", "schedule_sequence", "visible_at"):
        _integer(row[name])
    for name in ("snapshot_version", "snapshot_as_of"):
        if name in row:
            _integer(row[name])
    if "continuation_id" in row:
        _text(row["continuation_id"])
    _text(row["payload_digest"], r"[0-9a-f]{64}")
    kind = _enum(row["payload_kind"], ["EXECUTION_FACT", "TRANSPORT_ACK", "MARKET_SNAPSHOT",
                                      "EARN_SNAPSHOT", "TRADING_SNAPSHOT", "POSITION_SNAPSHOT",
                                      "OPEN_ORDER_SNAPSHOT"])
    payload = row["immutable_payload"]
    if kind == "EXECUTION_FACT":
        fact = _object(payload, "order_id owner_client_order_id policy_client_order_id product_id state side limit_price fill_price original_size_contracts cumulative_filled_size_contracts contract_value execution_effective_at commit_account_version spec_version rule_data_version")
        for name in ("order_id", "owner_client_order_id", "policy_client_order_id", "product_id", "spec_version", "rule_data_version"):
            _text(fact[name])
        _enum(fact["state"], ["partially_filled", "filled"])
        _enum(fact["side"], ["buy", "sell"])
        if fact["limit_price"] is not None:
            _decimal_text(fact["limit_price"])
        for name in ("fill_price", "original_size_contracts", "cumulative_filled_size_contracts", "contract_value"):
            _decimal_text(fact[name])
        _integer(fact["execution_effective_at"])
        _integer(fact["commit_account_version"])
    elif kind == "TRANSPORT_ACK":
        ack = _object_keys(payload, "route operation client_order_id code",
                           "order_id message product_id side limit_price size_contracts")
        _enum(ack["route"], ["REST", "WS"])
        _enum(ack["operation"], ["ORDER", "CANCEL"])
        _text(ack["client_order_id"])
        _text(ack["code"])
        for name in ("order_id", "product_id", "message"):
            if name in ack and ack[name] is not None:
                _text(ack[name])
        if "side" in ack and ack["side"] is not None:
            _enum(ack["side"], ["buy", "sell"])
        for name in ("limit_price", "size_contracts"):
            if name in ack and ack[name] is not None:
                _decimal_text(ack[name])
    else:
        native_kind = {"POSITION_SNAPSHOT": "POSITIONS", "OPEN_ORDER_SNAPSHOT": "OPEN_ORDERS"}.get(
            kind, kind.removesuffix("_SNAPSHOT"),
        )
        fields = "outcome " + ("earn" if native_kind == "EARN" else "equity available_equity") if native_kind in ("EARN", "TRADING") else "outcome rows"
        snapshot = _object(payload, fields)
        _enum(snapshot["outcome"], ["SUCCESS"])
        if native_kind in ("EARN", "TRADING"):
            for name in (("earn",) if native_kind == "EARN" else ("equity", "available_equity")):
                _decimal_text(snapshot[name])
        elif native_kind == "MARKET":
            from src.core.backtest.spider_run_native_schema import snapshot_payload
            snapshot_payload(payload, "MARKET")
        else:
            for item in _list(snapshot["rows"]):
                (position if native_kind == "POSITIONS" else open_order)(item)


def _json_object(value: object) -> None:
    _require(type(value) is dict)
    for key, item in cast(dict[object, object], value).items():
        _text(key)
        _json_value(item)


def _json_value(value: object) -> None:
    _require(value is None or type(value) in (str, int, bool, list, dict))
    if type(value) is list:
        for item in cast(list[object], value):
            _json_value(item)
    elif type(value) is dict:
        _json_object(value)


def _scheduler_key_list(value: object) -> None:
    key = _list(value)
    _require(len(key) == 4)
    _integer(key[0])
    _integer(key[1])
    _integer(key[2])
    _text(key[3])


def _operation_state(value: object, *, gross_cut_evidence: bool = False) -> None:
    row = _object(value, "actions product_operations pending_delivery_ids pending_deliveries delivery_roles cancel_action_ids send_dependencies cancel_effects cancel_pending_order_ids gross_response")
    for action in _list(row["actions"]):
        item = _object(action, "action_id parent_action_id role product_id order_id group_id effective_at classification result")
        _text(item["action_id"])
        for name in ("parent_action_id", "product_id", "order_id", "group_id"):
            if item[name] is not None:
                _text(item[name])
        _enum(item["role"], ["GRID", "EXIT", "DRAIN_CANCEL", "GROSS_CANCEL", "GROSS_REDUCTION"])
        _integer(item["effective_at"])
        _enum(item["classification"], ["PENDING", "COMMITTED", "REJECTED", "FAULT"])
        if item["result"] is not None:
            from src.core.backtest.spider_run_native_schema import group_result
            group_result(item["result"])
    products = row["product_operations"]
    _require(type(products) is dict)
    for product_id, value in cast(dict[object, object], products).items():
        _text(product_id)
        op = _object_keys(value,
                          "product_id first_reason requested_at status completed_at positions working_orders",
                          "position_capture_key pending_sends pending_deliveries")
        _require(op["product_id"] == product_id)
        _text(op["first_reason"])
        _integer(op["requested_at"])
        _enum(op["status"], ["EXITING", "EXITED", "INCOMPLETE", "FAILED", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"])
        _require(op["completed_at"] is None or type(op["completed_at"]) is int)
        from src.core.backtest.spider_run_native_schema import open_order, position
        if op["positions"] is not None:
            for position_row in _list(op["positions"]):
                position(position_row)
        if op["working_orders"] is not None:
            for order in _list(op["working_orders"]):
                open_order(order)
        if "position_capture_key" in op and op["position_capture_key"] is not None:
            capture = _list(op["position_capture_key"])
            _require(len(capture) == 2)
            _integer(capture[0])
            if capture[1] is not None:
                _scheduler_key_list(capture[1])
        for name in ("pending_sends", "pending_deliveries"):
            if name in op:
                for item in _list(op[name]):
                    _send_dependency(item) if name == "pending_sends" else _product_pending_delivery(item)
    pending_ids = [_text(item) for item in _list(row["pending_delivery_ids"])]
    _require(pending_ids == sorted(set(pending_ids)))
    for item in _list(row["pending_deliveries"]):
        delivery = _object(item, "key delivery result role cancel_action_id")
        _scheduler_key_list(delivery["key"])
        _delivery(delivery["delivery"])
        _scheduler_result(delivery["result"], gross_cut_evidence=gross_cut_evidence)
        if delivery["role"] is not None:
            _enum(delivery["role"], ["GRID", "EXIT", "DRAIN_CANCEL", "GROSS_CANCEL", "GROSS_REDUCTION"])
        if delivery["cancel_action_id"] is not None:
            _text(delivery["cancel_action_id"])
    roles = row["delivery_roles"]
    _require(type(roles) is dict)
    for key, value in cast(dict[object, object], roles).items():
        _text(key)
        _enum(value, ["GRID", "EXIT", "DRAIN_CANCEL", "GROSS_CANCEL", "GROSS_REDUCTION"])
    cancels = _object(row["cancel_action_ids"], "requests effects acks")
    for name in ("requests", "effects", "acks"):
        values = cancels[name]
        _require(type(values) is dict)
        for key, value in cast(dict[object, object], values).items():
            _text(key)
            if value is not None:
                _text(value)
    dependencies = row["send_dependencies"]
    _require(type(dependencies) is dict)
    for value in cast(dict[object, object], dependencies).values():
        _send_dependency(value)
    effects = row["cancel_effects"]
    _require(type(effects) is dict)
    for group_id, value in cast(dict[object, object], effects).items():
        _text(group_id)
        _cancel_effect(value)
    _text_list(row["cancel_pending_order_ids"])
    gross = row["gross_response"]
    if gross is not None:
        state = _object(gross, "episode_sequence state active rejected_products current_round_sequence current_round_action_ids")
        _integer(state["episode_sequence"])
        _text(state["state"])
        _require(type(state["active"]) is bool)
        _text_list(state["rejected_products"])
        _require(state["current_round_sequence"] is None or type(state["current_round_sequence"]) is int)
        _text_list(state["current_round_action_ids"])


def _text_list(value: object) -> None:
    for item in _list(value):
        _text(item)


def _object_keys(value: object, required: str, optional: str = "") -> dict[str, object]:
    _require(type(value) is dict)
    row = cast(dict[str, object], value)
    required_names = set(required.split())
    allowed = required_names | set(optional.split())
    _require(required_names <= row.keys() and row.keys() <= allowed)
    return row


def _native_state(value: object) -> None:
    row = _object(value, "schema_version account_key account_version available_margin valuation_context_id valuation_effective_at owner_state_digest lifecycle gate maintenance_margin gross_admission products orders profile_id current_admission_state_ready")
    _enum(row["schema_version"], ["spider_gross_response_native_state_v1"])
    from src.core.backtest.spider_run_native_schema import account
    account(row["account_key"])
    _integer(row["account_version"])
    _text(row["valuation_context_id"], r"[0-9a-f]{64}")
    _integer(row["valuation_effective_at"])
    _text(row["owner_state_digest"], r"[0-9a-f]{64}")
    _enum(row["lifecycle"], ["RISK_STABLE", "AWAITING_CANCEL_EFFECTIVE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"])
    _enum(row["gate"], ["RUNNING", "FAILED"])
    _decimal(row["available_margin"])
    _decimal(row["maintenance_margin"])
    _enum(row["profile_id"], ["SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1"])
    admission = _object(row["gross_admission"], "schema_version account_key account_version bound_reason bound_scope cap effective_equity equity_floor fee_hold gross gross_ratio net order_loss_hold risk_policy_id valuation_context_id worst_gross")
    _enum(admission["schema_version"], ["spider_gross_admission_projection_v1"])
    account(admission["account_key"])
    _integer(admission["account_version"])
    if admission["bound_reason"] is not None:
        _text(admission["bound_reason"])
    _text(admission["bound_scope"])
    _enum(admission["risk_policy_id"], ["CURRENT_MARK_GROSS_CAP_TWO_V1"])
    _text(admission["valuation_context_id"], r"[0-9a-f]{64}")
    for name in ("cap", "effective_equity", "fee_hold", "gross", "net", "order_loss_hold", "worst_gross"):
        _decimal(admission[name])
    effective_equity = Decimal(str(admission["effective_equity"]))
    if effective_equity <= 0:
        _require(admission["gross_ratio"] is None)
    else:
        ratio = _object(admission["gross_ratio"], "numerator denominator")
        _decimal(ratio["numerator"])
        _decimal(ratio["denominator"])
    _decimal(admission["equity_floor"], nullable=True)
    _require(type(row["current_admission_state_ready"]) is bool)
    products = _list(row["products"])
    for item in products:
        product = _object(item, "product_id closing_capacity contract_value gross lots mark minimum_quantity multiplier quantity_step signed_contracts spec_version taker_fee_rate tier_version")
        _text(product["product_id"])
        for name in ("closing_capacity", "contract_value", "gross", "mark", "minimum_quantity", "multiplier", "quantity_step", "signed_contracts", "taker_fee_rate"):
            _decimal(product[name])
        _text(product["spec_version"])
        _text(product["tier_version"])
        for lot_value in _list(product["lots"]):
            lot = _object(lot_value, "contracts strategy_id")
            _decimal(lot["contracts"])
            _text(lot["strategy_id"])
    for order in _list(row["orders"]):
        item = _object(order, "order_id client_order_id strategy_id product_id side order_type reduce_only remaining_contracts status cancel_pending verified_reducing")
        for name in ("order_id", "client_order_id", "strategy_id", "product_id"):
            _text(item[name])
        _enum(item["side"], ["LONG", "SHORT"])
        _enum(item["order_type"], ["LIMIT", "MARKET"])
        _require(type(item["reduce_only"]) is bool)
        _decimal(item["remaining_contracts"])
        _text(item["status"])
        _require(type(item["cancel_pending"]) is bool and type(item["verified_reducing"]) is bool)


def _proposal(value: object, *, gross_cut_evidence: bool = False) -> None:
    row = _object(value, "policy_id parameters scope native_state_digest account_version valuation_context_id state estimated_fee_rate estimated_reduction_gross estimated_fee estimated_equity_after rows reason proposal_context")
    _enum(row["policy_id"], ["PROPORTIONAL_PAID_GROSS_TARGET_V1"])
    params = _object(row["parameters"], "trigger_cap target_cap allocation quantity_rounding")
    _decimal(params["trigger_cap"])
    _decimal(params["target_cap"])
    _enum(params["allocation"], ["GROSS_PROPORTIONAL_V1"])
    _enum(params["quantity_rounding"], ["CEIL_STEP_NO_MINIMUM_UPLIFT_V1"])
    scope = _object(row["scope"], "kind strategy_id ordered_products")
    _enum(scope["kind"], ["RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1"])
    _text(scope["strategy_id"])
    _require(bool(_list(scope["ordered_products"])))
    for item in cast(list[object], scope["ordered_products"]):
        _text(item)
    _text(row["native_state_digest"], r"[0-9a-f]{64}")
    _integer(row["account_version"])
    _text(row["valuation_context_id"], r"[0-9a-f]{64}")
    _text(row["state"])
    for name in ("estimated_fee_rate", "estimated_reduction_gross", "estimated_fee", "estimated_equity_after"):
        _decimal(row[name], nullable=True)
    if row["reason"] is not None:
        _text(row["reason"])
    for item in _list(row["rows"]):
        result = _object(item, "product_id side gross raw_quantity quantity rounding_excess capacity reason")
        _text(result["product_id"])
        _enum(result["side"], ["LONG", "SHORT"])
        for name in ("gross", "raw_quantity", "quantity", "rounding_excess", "capacity"):
            _decimal(result[name], nullable=True)
        if result["reason"] is not None:
            _text(result["reason"])
    context = _object(row["proposal_context"], "source_state operation_state")
    _source_state(context["source_state"])
    _operation_state(context["operation_state"], gross_cut_evidence=gross_cut_evidence)


def _episode(value: object, *, gross_cut_evidence: bool = False) -> None:
    row = _object(value, "sequence trigger_key trigger_state rounds state reason recovered_key recovery_state remaining_obligations")
    _integer(row["sequence"])
    scheduler_key(row["trigger_key"], nullable=True)
    if row["trigger_state"] is not None:
        _native_state(row["trigger_state"])
    _enum(row["state"], _STATES)
    if row["reason"] is not None:
        _text(row["reason"])
    scheduler_key(row["recovered_key"], nullable=True)
    if row["recovery_state"] is not None:
        _native_state(row["recovery_state"])
    for item in _list(row["rounds"]):
        round_row = _object(item, "sequence proposal_key proposal action_ids completion_key reason")
        if round_row["sequence"] is not None:
            _integer(round_row["sequence"])
        scheduler_key(round_row["proposal_key"], nullable=True)
        _proposal(round_row["proposal"], gross_cut_evidence=gross_cut_evidence)
        for action_id in _list(round_row["action_ids"]):
            _text(action_id)
        scheduler_key(round_row["completion_key"], nullable=True)
        if round_row["reason"] is not None:
            _text(round_row["reason"])
    if row["remaining_obligations"] is not None:
        for item in _list(row["remaining_obligations"]):
            obligation = _object(item, "operation_state native_state")
            _operation_state(obligation["operation_state"], gross_cut_evidence=gross_cut_evidence)
            _native_state(obligation["native_state"])


def gross_observation(value: object, *, expected_hash: str | None = None) -> None:
    _require(type(value) is dict)
    raw = cast(dict[str, object], value)
    schema_version = raw.get("schema_version")
    cut_evidence = schema_version == "spider_gross_response_observation_v2"
    fields = "schema_version run_contract_sha256 policy_id response_parameters authorization_scope observations episodes peak_ratio overlimit_intervals overlimit_duration_ms current_state reason"
    if cut_evidence:
        fields += " final_obligation_state"
    row = _object(value, fields)
    _enum(row["schema_version"], ["spider_gross_response_observation_v1",
                                  "spider_gross_response_observation_v2"])
    run_hash = _text(row["run_contract_sha256"], r"[0-9a-f]{64}")
    _require(expected_hash is None or run_hash == expected_hash)
    _enum(row["policy_id"], ["PROPORTIONAL_PAID_GROSS_TARGET_V1"])
    params = _object(row["response_parameters"], "trigger_cap target_cap allocation quantity_rounding")
    _decimal(params["trigger_cap"])
    _decimal(params["target_cap"])
    _enum(params["allocation"], ["GROSS_PROPORTIONAL_V1"])
    _enum(params["quantity_rounding"], ["CEIL_STEP_NO_MINIMUM_UPLIFT_V1"])
    scope = _object(row["authorization_scope"], "kind strategy_id ordered_products")
    _enum(scope["kind"], ["RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1"])
    _text(scope["strategy_id"])
    products = [_text(item) for item in _list(scope["ordered_products"])]
    _require(bool(products) and len(products) == len(set(products)))
    for item in _list(row["observations"]):
        observation_fields = "key observed_at native_state before_state after_state reason episode_sequence"
        if cut_evidence:
            observation_fields += " operation_state"
        observation = _object(item, observation_fields)
        scheduler_key(observation["key"], nullable=True)
        _integer(observation["observed_at"])
        _native_state(observation["native_state"])
        _enum(observation["before_state"], _STATES)
        _enum(observation["after_state"], _STATES)
        if observation["reason"] is not None:
            _text(observation["reason"])
        if observation["episode_sequence"] is not None:
            _integer(observation["episode_sequence"])
        if cut_evidence and observation["operation_state"] is not None:
            _operation_state(observation["operation_state"], gross_cut_evidence=cut_evidence)
    sequences = []
    for episode in _list(row["episodes"]):
        _episode(episode, gross_cut_evidence=cut_evidence)
        sequences.append(cast(dict[str, object], episode)["sequence"])
    _require(sequences == sorted(set(sequences)))
    peak = row["peak_ratio"]
    if peak is not None:
        peak_row = _object(peak, "numerator denominator key")
        _decimal(peak_row["numerator"])
        _decimal(peak_row["denominator"])
        scheduler_key(peak_row["key"], nullable=True)
    for item in _list(row["overlimit_intervals"]):
        interval = _object(item, "start_key end_key end_cut open")
        scheduler_key(interval["start_key"], nullable=True)
        scheduler_key(interval["end_key"], nullable=True)
        if interval["end_cut"] is not None:
            _integer(interval["end_cut"])
        _require(type(interval["open"]) is bool)
    _integer(row["overlimit_duration_ms"])
    _enum(row["current_state"], _STATES)
    if row["reason"] is not None:
        _text(row["reason"])
    if cut_evidence:
        _final_obligation_state(row["final_obligation_state"])


def _final_obligation_state(value: object) -> None:
    if value is None:
        return
    final = _object(value, "operation_state native_state observed_at key")
    _operation_state(final["operation_state"], gross_cut_evidence=True)
    _native_state(final["native_state"])
    _integer(final["observed_at"])
    scheduler_key(final["key"], nullable=True)


def execution_report(value: object, *, execution: GrossExecutionContext,
                     configuration: ConfigurationContext) -> None:
    row = _object(value, "schema_version run_id initial_owner_evidence scheduler_observation paid_observation paid_endpoint source_evidence_refs gross_observation")
    _enum(row["schema_version"], ["spider_gross_execution_report_v1"])
    _text(row["run_id"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    if row["initial_owner_evidence"] is not None:
        envelope.owner_evidence(row["initial_owner_evidence"], context=configuration,
                                gross_profile=True)
    envelope.scheduler_observation(row["scheduler_observation"], historical=True,
                                   paid_execution=True)
    if row["gross_observation"] is None:
        _require(type(row["paid_endpoint"]) is dict)
        paid_endpoint = cast(dict[str, object], row["paid_endpoint"])
        _enum(paid_endpoint["status"], ["FAILED"])
        _require(row["initial_owner_evidence"] is None
                 and paid_endpoint["owner_evidence"] is None)
    else:
        gross_observation(row["gross_observation"], expected_hash=execution.execution_input_sha256)
    refs = [_text(item, r"journal:[1-9][0-9]*") for item in _list(row["source_evidence_refs"])]
    _require(refs == sorted(set(refs), key=lambda item: int(item.split(":", 1)[1])))


def reconciliation(value: object, run_id: str, *, journal_rows: int | None,
                   validate_refs: Callable[[object, int | None], None]) -> None:
    checks_order = ("INPUT_IDENTITY", "JOURNAL_FRONTIER", "OWNER_SCOPE_AND_CHAIN",
                    "SNAPSHOT_DIGESTS", "ACTION_BINDINGS", "ENDPOINT_CLASSIFICATION",
                    "REPORT_PROJECTION")
    row = _object(value, "schema_version run_id result evidence_scope checks")
    _enum(row["schema_version"], ["spider_gross_execution_reconciliation_v1"])
    _require(_text(row["run_id"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}") == run_id)
    _enum(row["result"], ["OK", "FAILED"])
    _enum(row["evidence_scope"], ["COMPLETE_RUN", "FAILED_PREFIX"])
    checks = _list(row["checks"])
    _require(len(checks) == len(checks_order))
    for item, name in zip(checks, checks_order, strict=True):
        check = _object(item, "name result evidence_refs")
        _enum(check["name"], [name])
        _enum(check["result"], ["OK", "FAILED"])
        validate_refs(check["evidence_refs"], journal_rows)


def completion(value: object, *, execution: GrossExecutionContext, run_id: str,
               artifact_schemas: tuple[tuple[str, str, int | None], ...]) -> None:
    row = _object(value, "schema_version run_id state terminal_reason input_contract_hashes planned_coverage processed_boundary persisted_boundary endpoint_state_digest reconciliation_digest artifacts configuration_context historical_context execution_context endpoint_status evidence_scope")
    _enum(row["schema_version"], ["spider_gross_execution_completion_v1"])
    _require(_text(row["run_id"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}") == run_id)
    _enum(row["state"], ["COMPLETE"])
    _enum(row["terminal_reason"], ["PAID_CLOSE_RESEARCH_V1"])
    _text(row["endpoint_state_digest"], r"[0-9a-f]{64}")
    _text(row["reconciliation_digest"], r"[0-9a-f]{64}")
    _boundary(row["processed_boundary"])
    _boundary(row["persisted_boundary"])
    hashes = _list(row["input_contract_hashes"])
    _require(len(hashes) == 4)
    names = ("SCENARIO_PLAN", "PROGRAM", "NATIVE_ARTIFACT", "POLICY_SOURCE_MANIFEST")
    for item, name in zip(hashes, names, strict=True):
        entry = _object(item, "name sha256")
        _enum(entry["name"], [name])
        _text(entry["sha256"], r"[0-9a-f]{64}")
    coverage = _list(row["planned_coverage"])
    _require(bool(coverage))
    seen = set()
    for ordinal, item in enumerate(coverage, 1):
        entry = _object(item, "ordinal barrier_id record_kind")
        _require(_integer(entry["ordinal"]) == ordinal)
        barrier = _text(entry["barrier_id"])
        _require(barrier not in seen)
        seen.add(barrier)
        _enum(entry["record_kind"], ["HISTORICAL_MARKET_STEP", "HISTORICAL_MARKET_CACHE",
                                     "HISTORICAL_TIMER", "PAID_ENDPOINT_DRAIN"])
    _require(gross_execution_context(row["execution_context"]) == execution)
    configuration_context(row["configuration_context"])
    historical_context(row["historical_context"])
    _enum(row["endpoint_status"], ["PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT",
                                   "LIQUIDATED_INSOLVENT", "FAILED"])
    _enum(row["evidence_scope"], ["COMPLETE_RUN", "FAILED_PREFIX"])
    artifacts = _list(row["artifacts"])
    _require(len(artifacts) == len(artifact_schemas))
    for item, (path, schema, fixed_rows) in zip(artifacts, artifact_schemas, strict=True):
        artifact = _object(item, "path schema_version sha256 byte_count row_count")
        _enum(artifact["path"], [path])
        _enum(artifact["schema_version"], [schema])
        _text(artifact["sha256"], r"[0-9a-f]{64}")
        _integer(artifact["byte_count"])
        rows = _integer(artifact["row_count"])
        if fixed_rows is not None:
            _require(rows == fixed_rows)
