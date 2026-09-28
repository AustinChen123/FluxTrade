"""Closed source-policy typed protocol encoding; no policy or owner dependency."""

import hashlib
import re
from typing import cast


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
