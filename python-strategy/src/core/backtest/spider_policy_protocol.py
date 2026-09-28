"""Closed source-policy typed protocol encoding; no policy or owner dependency."""

import hashlib
import re
from decimal import Decimal
from typing import Callable, Literal, NotRequired, TypedDict, cast


def _event_integer(value: object) -> bytes:
    if type(value) is not int or not -(1 << 63) <= value < (1 << 63):
        raise ValueError("POLICY_EMISSION_MISMATCH")
    return value.to_bytes(8, "big", signed=True)


def _event_text(value: object) -> bytes:
    if not isinstance(value, str):
        raise ValueError("POLICY_EMISSION_MISMATCH")
    raw = value.encode("utf-8")
    return _event_integer(len(raw)) + raw


def _event_decimal(value: object) -> bytes:
    if not isinstance(value, str) or re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?", value) is None:
        raise ValueError("POLICY_EMISSION_MISMATCH")
    if value == "-0":
        raise ValueError("POLICY_EMISSION_MISMATCH")
    return _event_text(value)


def _event_id(value: object) -> bytes:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError("POLICY_EMISSION_MISMATCH")
    return _event_text(value)


def _event_object(value: object, required: str, optional: str = "") -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("POLICY_EMISSION_MISMATCH")
    fields, extras = set(required.split()), set(optional.split())
    if not fields <= value.keys() or value.keys() - fields - extras:
        raise ValueError("POLICY_EMISSION_MISMATCH")
    return cast(dict[str, object], value)


def _event_token(value: object, choices: tuple[str, ...]) -> bytes:
    if not isinstance(value, str) or value not in choices:
        raise ValueError("POLICY_EMISSION_MISMATCH")
    return _event_text(value)


def _event_optional_text(value: object) -> bytes:
    return b"\x00" if value is None else b"\x01" + _event_text(value)


def _event_order_bytes(value: object, operation: str) -> bytes:
    if operation == "cancel":
        row = _event_object(value, "ordId instId instIdCode")
        return _event_id(row["ordId"]) + _event_id(row["instId"]) + _event_integer(row["instIdCode"])
    row = _event_object(value, "instId instIdCode tdMode clOrdId tag side ordType px sz")
    data = _event_id(row["instId"]) + _event_integer(row["instIdCode"])
    data += _event_text(row["tdMode"]) + _event_id(row["clOrdId"]) + _event_text(row["tag"])
    data += _event_token(row["side"], ("buy", "sell"))
    data += _event_token(row["ordType"], ("limit", "market"))
    if row["ordType"] == "market":
        if row["px"] != "":
            raise ValueError("POLICY_EMISSION_MISMATCH")
        data += b"\x00"
    else:
        data += b"\x01" + _event_decimal(row["px"])
    return data + _event_decimal(row["sz"])


def _policy_event_bytes(event: object) -> bytes:
    """Closed 4C-compatible typed encoding; never mutate or hash JSON."""
    try:
        if not isinstance(event, dict) or not isinstance(event.get("kind"), str):
            raise ValueError("POLICY_EMISSION_MISMATCH")
        kind = event["kind"]
        data = _event_text("SPIDER_POLICY_EVENT_V1") + _event_integer(event.get("at_ms")) + _event_text(kind)
        if kind in ("request_market", "request_earn", "check_websocket"):
            _event_object(event, "at_ms kind")
        elif kind == "alert":
            reason = event.get("reason")
            if reason == "offline":
                _event_object(event, "at_ms kind reason operation")
                data += _event_text(reason) + _event_token(event["operation"], ("send", "cancel"))
            elif reason in ("reset_step", "high_low_limit", "individual_limit"):
                _event_object(event, "at_ms kind reason name")
                data += _event_text(reason) + _event_text(event["name"])
            elif reason in ("total_limit", "daily_stop"):
                _event_object(event, "at_ms kind reason")
                data += _event_text(reason)
            else:
                raise ValueError("POLICY_EMISSION_MISMATCH")
        elif kind in ("send", "cancel", "console_only"):
            row = _event_object(event, "at_ms kind operation route orders")
            operation = row["operation"]
            data += _event_token(operation, ("send", "cancel")) + _event_token(row["route"], ("REST", "WS"))
            if kind != "console_only" and operation != kind:
                raise ValueError("POLICY_EMISSION_MISMATCH")
            orders = row["orders"]
            if not isinstance(orders, list):
                raise ValueError("POLICY_EMISSION_MISMATCH")
            data += _event_integer(len(orders))
            data += b"".join(_event_order_bytes(order, cast(str, operation)) for order in orders)
        elif kind == "unsupported_source_path":
            row = _event_object(event, "at_ms kind reason order")
            data += _event_token(row["reason"], ("non_grid_fill_enters_orderFilled",))
            order = _event_object(row["order"], "instId side px clOrdId state sz accFillSz", "cTime sMsg")
            data += _event_id(order["instId"]) + _event_token(order["side"], ("buy", "sell"))
            data += b"\x00" if order["px"] == "" else b"\x01" + _event_decimal(order["px"])
            data += _event_id(order["clOrdId"]) + _event_text(order["state"])
            data += _event_decimal(order["sz"]) + _event_decimal(order["accFillSz"])
            data += _event_optional_text(order.get("cTime")) + _event_optional_text(order.get("sMsg"))
        else:
            raise ValueError("POLICY_EMISSION_MISMATCH")
        return data
    except (UnicodeError, OverflowError) as exc:
        raise ValueError("POLICY_EMISSION_MISMATCH") from exc


def _policy_event_digest(event: object) -> str:
    return hashlib.sha256(_policy_event_bytes(event)).hexdigest()


class _ExpectedPolicyEvent(TypedDict):
    event_digest: str
    kind: str


class _FinancialItem(TypedDict):
    event_digest: str
    action_kind: Literal["ORDER_INTENT", "CANCEL_REQUEST"]
    schedule_sequence: int
    expected_group: dict[str, object]


class _MarketRequest(TypedDict):
    event_digest: str
    capture_sequence: int
    snapshot_request: dict[str, object]
    delivery_projection: dict[str, object]


class _EmissionPlan(TypedDict):
    delivery_id: str
    expected_policy_events: list[_ExpectedPolicyEvent]
    financial_items: list[_FinancialItem]
    market_requests: list[_MarketRequest]


def _plan_hash(value: object) -> bytes:
    if not isinstance(value, str) or re.fullmatch("[0-9a-f]{64}", value) is None:
        raise ValueError("POLICY_EMISSION_MISMATCH")
    return bytes.fromhex(value)


def _plan_decimal(value: object) -> bytes:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("POLICY_EMISSION_MISMATCH")
    raw = format(value, "f") if value else "0"
    if "." in raw:
        raw = raw.rstrip("0").rstrip(".")
    return _event_text(raw)


def _plan_bool(value: object) -> bytes:
    if type(value) is not bool:
        raise ValueError("POLICY_EMISSION_MISMATCH")
    return bytes([value])


def _plan_sequence(value: object) -> bytes:
    encoded = _event_integer(value)
    if cast(int, value) < 0:
        raise ValueError("POLICY_EMISSION_MISMATCH")
    return encoded


def _plan_optional(value: object, encode: Callable[[object], bytes]) -> bytes:
    return b"\x00" if value is None else b"\x01" + encode(value)


def _plan_list(value: object, encode: Callable[[object], bytes]) -> bytes:
    if not isinstance(value, list):
        raise ValueError("POLICY_EMISSION_MISMATCH")
    return _event_integer(len(value)) + b"".join(encode(item) for item in value)


def _plan_account(value: object) -> bytes:
    row = _event_object(value, "venue environment account", "subaccount")
    return b"".join(_event_id(row[k]) for k in ("venue", "environment", "account")) + _plan_optional(
        row.get("subaccount"), _event_id
    )


def _plan_stamp(value: object) -> bytes:
    row = _event_object(
        value, "event_id effective_at causal_parent_ids ordering_contract_id scenario_ordinal", "source_sequence"
    )
    return (
        _event_id(row["event_id"])
        + _event_integer(row["effective_at"])
        + _plan_optional(row.get("source_sequence"), _event_integer)
        + _plan_list(row["causal_parent_ids"], _event_id)
        + _event_id(row["ordering_contract_id"])
        + _event_integer(row["scenario_ordinal"])
    )


def _plan_mark(value: object) -> bytes:
    row = _event_object(value, "product_id valid_from valid_to mark")
    return (
        _event_token(row["product_id"], ("BTC-USDT-SWAP", "ETH-USDT-SWAP"))
        + _event_integer(row["valid_from"])
        + _event_integer(row["valid_to"])
        + _plan_decimal(row["mark"])
    )


def _plan_target(value: object, effect: bool = False) -> bytes:
    row = _event_object(value, "target_order_id reason" + (" detecting_event_id" if effect else ""))
    return (
        (_event_id(row["detecting_event_id"]) if effect else b"")
        + _event_id(row["target_order_id"])
        + _event_token(
            row["reason"], ("EXPLICIT_SCENARIO", "RISK_SHORTFALL", "MMR_BREACH", "SPEC_MIGRATION", "UNSUPPORTED")
        )
    )


def _plan_member(value: object) -> bytes:
    row = _event_object(value, "stamp kind payload")
    kind, payload = row["kind"], row["payload"]
    data = _plan_stamp(row["stamp"]) + _event_text(kind)
    if kind == "CONTEXT_MARKS":
        p = _event_object(payload, "expected_before expected_after rows")
        return (
            data
            + _plan_hash(p["expected_before"])
            + _plan_hash(p["expected_after"])
            + _plan_list(p["rows"], _plan_mark)
        )
    if kind == "EXECUTION":
        p = _event_object(
            payload,
            "namespace product_id external_execution_id order_id side price quantity_contracts liquidity matching_effective_at candidate_id source_id visible_at expected_account_version expected_order_version spec_version rule_data_version",
            "fee_asset reported_fee",
        )
        data += _event_id(p["namespace"]) + _event_token(p["product_id"], ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "P_A"))
        data += (
            _event_id(p["external_execution_id"])
            + _event_id(p["order_id"])
            + _event_token(p["side"], ("LONG", "SHORT"))
        )
        data += (
            _plan_decimal(p["price"])
            + _plan_decimal(p["quantity_contracts"])
            + _event_token(p["liquidity"], ("SYNTHETIC_TAKER",))
        )
        data += _plan_optional(p.get("fee_asset"), _event_id) + _plan_optional(p.get("reported_fee"), _plan_decimal)
        data += _event_integer(p["matching_effective_at"]) + _event_id(p["candidate_id"]) + _event_id(p["source_id"])
        return (
            data
            + b"".join(
                _event_integer(p[k]) for k in ("visible_at", "expected_account_version", "expected_order_version")
            )
            + _event_id(p["spec_version"])
            + _event_id(p["rule_data_version"])
        )
    if kind == "INTENT":
        p = _event_object(
            payload,
            "intent_id client_order_id config_id product_id strategy_id side order_type quantity_contracts reduce_only requested_at",
            "limit_price",
        )
        data += b"".join(_event_id(p[k]) for k in ("intent_id", "client_order_id", "config_id"))
        data += _event_token(p["product_id"], ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "P_A")) + _event_id(p["strategy_id"])
        data += (
            _event_token(p["side"], ("LONG", "SHORT"))
            + _event_token(p["order_type"], ("LIMIT", "MARKET"))
            + _plan_decimal(p["quantity_contracts"])
        )
        return (
            data
            + _plan_optional(p.get("limit_price"), _plan_decimal)
            + _plan_bool(p["reduce_only"])
            + _event_integer(p["requested_at"])
        )
    if kind in ("CANCEL_REQUEST", "CANCEL_EFFECT"):
        key = "targets" if kind == "CANCEL_REQUEST" else "effects"
        p = _event_object(payload, key)
        return data + _plan_list(p[key], lambda item: _plan_target(item, kind == "CANCEL_EFFECT"))
    raise ValueError("POLICY_EMISSION_MISMATCH")


def _plan_group(value: object) -> bytes:
    row = _event_object(
        value,
        "schema_version group_id account_key ordering_contract_id group_effective_at declared_member_count members",
    )
    return (
        _event_token(row["schema_version"], ("scenario_group_v1",))
        + _event_id(row["group_id"])
        + _plan_account(row["account_key"])
        + _event_token(row["ordering_contract_id"], ("S_order_v1", "S_order_v1_reverse_execution_cancel_effective"))
        + _event_integer(row["group_effective_at"])
        + _event_integer(row["declared_member_count"])
        + _plan_list(row["members"], _plan_member)
    )


def _plan_snapshot(value: object) -> bytes:
    row = _event_object(
        value,
        "schema_version account_key snapshot_id snapshot_kind capture_mode captured_at",
        "fixture_key continuation_id",
    )
    return (
        _event_token(row["schema_version"], ("snapshot_request_v1",))
        + _plan_account(row["account_key"])
        + _event_id(row["snapshot_id"])
        + _event_token(row["snapshot_kind"], ("MARKET", "EARN", "TRADING", "POSITIONS", "OPEN_ORDERS"))
        + _event_token(row["capture_mode"], ("OWNER_CURRENT", "FROZEN_POLL_FIXTURE"))
        + _plan_optional(row.get("fixture_key"), _event_id)
        + _event_integer(row["captured_at"])
        + _plan_optional(row.get("continuation_id"), _event_id)
    )


def _plan_transport(value: object) -> bytes:
    row = _event_object(
        value, "route operation client_order_id code", "order_id message product_id side limit_price size_contracts"
    )
    return (
        _event_token(row["route"], ("REST", "WS"))
        + _event_token(row["operation"], ("ORDER", "CANCEL"))
        + _event_id(row["client_order_id"])
        + _plan_optional(row.get("order_id"), _event_id)
        + _event_text(row["code"])
        + _plan_optional(row.get("message"), _event_text)
        + _plan_optional(row.get("product_id"), _event_id)
        + _plan_optional(row.get("side"), lambda x: _event_token(x, ("buy", "sell")))
        + _plan_optional(row.get("limit_price"), _plan_decimal)
        + _plan_optional(row.get("size_contracts"), _plan_decimal)
    )


def _plan_projection(value: object) -> bytes:
    row = _event_object(
        value,
        "schema_version reference payload_kind occurrence_index schedule_sequence visible_at",
        "continuation_id transport",
    )
    ref = _event_object(row["reference"], "namespace fact_id")
    return (
        _event_token(row["schema_version"], ("delivery_projection_v1",))
        + _event_token(ref["namespace"], ("SOURCE", "LIQUIDATION", "SNAPSHOT"))
        + _event_id(ref["fact_id"])
        + _event_token(
            row["payload_kind"],
            (
                "EXECUTION_FACT",
                "TRANSPORT_ACK",
                "MARKET_SNAPSHOT",
                "EARN_SNAPSHOT",
                "TRADING_SNAPSHOT",
                "POSITION_SNAPSHOT",
                "OPEN_ORDER_SNAPSHOT",
            ),
        )
        + _plan_sequence(row["occurrence_index"])
        + _plan_sequence(row["schedule_sequence"])
        + _event_integer(row["visible_at"])
        + _plan_optional(row.get("continuation_id"), _event_id)
        + _plan_optional(row.get("transport"), _plan_transport)
    )


def _emission_plan_bytes(plan: object) -> bytes:
    row = _event_object(plan, "delivery_id expected_policy_events financial_items market_requests")

    def expected(value: object) -> bytes:
        p = _event_object(value, "event_digest kind")
        return _plan_hash(p["event_digest"]) + _event_token(
            p["kind"],
            (
                "request_market",
                "request_earn",
                "check_websocket",
                "alert",
                "send",
                "cancel",
                "console_only",
                "unsupported_source_path",
            ),
        )

    def financial(value: object) -> bytes:
        p = _event_object(value, "event_digest action_kind schedule_sequence expected_group")
        return (
            _plan_hash(p["event_digest"])
            + _event_token(p["action_kind"], ("ORDER_INTENT", "CANCEL_REQUEST"))
            + _plan_sequence(p["schedule_sequence"])
            + _plan_group(p["expected_group"])
        )

    def market(value: object) -> bytes:
        p = _event_object(value, "event_digest capture_sequence snapshot_request delivery_projection")
        return (
            _plan_hash(p["event_digest"])
            + _plan_sequence(p["capture_sequence"])
            + _plan_snapshot(p["snapshot_request"])
            + _plan_projection(p["delivery_projection"])
        )

    try:
        return (
            _event_text("SPIDER_CALLBACK_EMISSION_PLAN_V1")
            + _event_id(row["delivery_id"])
            + _plan_list(row["expected_policy_events"], expected)
            + _plan_list(row["financial_items"], financial)
            + _plan_list(row["market_requests"], market)
        )
    except (UnicodeError, OverflowError) as exc:
        raise ValueError("POLICY_EMISSION_MISMATCH") from exc


def _emission_plan_digest(plan: object) -> str:
    return hashlib.sha256(_emission_plan_bytes(plan)).hexdigest()


class _PollStagePlan(TypedDict):
    capture_sequence: int
    snapshot_request: dict[str, object]
    delivery_projection: dict[str, object]


class _PollOccurrencePlan(TypedDict):
    account_key: dict[str, object]
    poll_id: str
    issued_at: int
    continuation_id: str
    earn: NotRequired[_PollStagePlan | None]
    trading: _PollStagePlan
    positions: _PollStagePlan
    open_orders: _PollStagePlan


def _poll_stage_bytes(value: object) -> bytes:
    row = _event_object(value, "capture_sequence snapshot_request delivery_projection")
    return (
        _plan_sequence(row["capture_sequence"])
        + _plan_snapshot(row["snapshot_request"])
        + _plan_projection(row["delivery_projection"])
    )


def _poll_occurrence_plan_bytes(plan: object) -> bytes:
    """Encode representation only; poll timing and stage matching belong to composition."""
    try:
        row = _event_object(plan, "account_key poll_id issued_at continuation_id trading positions open_orders", "earn")
        return (
            _event_text("SPIDER_POLL_OCCURRENCE_PLAN_V1")
            + _plan_account(row["account_key"])
            + _event_id(row["poll_id"])
            + _event_integer(row["issued_at"])
            + _event_id(row["continuation_id"])
            + _plan_optional(row.get("earn"), _poll_stage_bytes)
            + b"".join(_poll_stage_bytes(row[k]) for k in ("trading", "positions", "open_orders"))
        )
    except (UnicodeError, OverflowError) as exc:
        raise ValueError("POLICY_EMISSION_MISMATCH") from exc


def _poll_occurrence_plan_digest(plan: object) -> str:
    return hashlib.sha256(_poll_occurrence_plan_bytes(plan)).hexdigest()
