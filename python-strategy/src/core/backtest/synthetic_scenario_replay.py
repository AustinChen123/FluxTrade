"""Internal closed-policy composition; no scheduling or financial submission."""

from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal as D, localcontext
import hashlib
import re
from typing import cast

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_policy import Policy, fmt


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


def _closed_policy(profile: wire.Profile) -> Policy:
    golden = profile == "SYNTHETIC_GOLDEN_CANCEL_V1"
    rows = (
        [
            dict(
                name="P_A",
                active="true",
                leverage="4",
                歩差="0.02",
                單數="2",
                hold上限="0.8",
                hold下限="-0.8",
                hold="0",
            )
        ]
        if golden
        else []
    )
    if golden:
        products = [("P_A", "10", "1", "1", "1", 1)]
    else:
        minimum = profile == "SYNTHETIC_MIN_CASH_V1"
        products = [
            ("BTC-USDT-SWAP", "50000" if minimum else "50001", "0.01", "0.01", "0.1", 1),
            ("ETH-USDT-SWAP", "100" if minimum else "1900", "0.1", "0.01", "0.01", 2),
        ]
    markets = {
        name: dict(
            price=price, ctVal=ct, lotSz=lot, minSz=lot, increment=tick, ratioHL="0.1", state="live", instIdCode=code
        )
        for name, price, ct, lot, tick, code in products
    }
    policy = Policy(rows, markets, now_ms=100000)
    cash = D("1000" if golden else "100")
    policy.capital = dict(earn=D(0), usdt=cash, avail=cash, total=cash, position=D(0))
    policy.last_earn_ms = 100000
    policy.running = golden
    return policy


@dataclass(frozen=True)
class _CallbackPrefix:
    events: tuple[dict[str, object], ...]
    exception: Exception | None


class _ReplayComposition:
    """One native owner and one source cache, deliberately without a run API."""

    def __init__(self, profile: wire.Profile, account: wire.Account) -> None:
        self._codec = wire.ScenarioCodec(profile, account)
        self._policy = _closed_policy(profile)

    def _capture_callback(self, delivery: wire.Delivery) -> _CallbackPrefix:
        start = len(self._policy.events)
        error = None
        try:
            self._apply_payload(delivery)
        except Exception as exc:
            error = exc
        return _CallbackPrefix(tuple(deepcopy(self._policy.events[start:])), error)

    def _apply_payload(self, delivery: wire.Delivery) -> None:
        policy, kind = self._policy, delivery["payload_kind"]
        payload = delivery["immutable_payload"]
        if kind == "EXECUTION_FACT":
            fact = cast(wire.ExecutionFact, payload)
            policy.orders(
                dict(
                    ordId=fact["order_id"],
                    clOrdId=fact["policy_client_order_id"],
                    instId=fact["product_id"],
                    px=fmt(fact["limit_price"]),
                    fillPx=fmt(fact["fill_price"]),
                    sz=fmt(fact["original_size_contracts"]),
                    accFillSz=fmt(fact["cumulative_filled_size_contracts"]),
                    cTime=str(fact["execution_effective_at"]),
                    state=fact["state"],
                    side=fact["side"],
                )
            )
        elif kind == "TRANSPORT_ACK":
            ack = cast(wire.Transport, payload)
            if ack["route"] == "WS":
                return
            entry = {"clOrdId": ack["client_order_id"], "sCode": ack["code"]}
            for source, target in [
                ("order_id", "ordId"),
                ("message", "sMsg"),
                ("product_id", "instId"),
                ("side", "side"),
                ("limit_price", "px"),
                ("size_contracts", "sz"),
            ]:
                value = ack.get(source)
                if value is not None:
                    entry[target] = fmt(value) if isinstance(value, D) else str(value)
            policy.rest_ack("send" if ack["operation"] == "ORDER" else "cancel", [entry])
        elif kind == "MARKET_SNAPSHOT":
            market = cast(wire.MarketSnapshot, payload)
            policy.apply_market(
                {
                    row["product_id"]: dict(
                        price=row["price"],
                        ctVal=row["contract_value"],
                        lotSz=row["lot_size"],
                        minSz=row["minimum_size"],
                        increment=row["price_increment"],
                        ratioHL=row["high_low_ratio"],
                        state=row["state"],
                        instIdCode=row["instrument_code"],
                    )
                    for row in market["markets"]
                }
            )
        elif cast(wire.Failure, payload).get("outcome") == "FAILURE":
            return
        elif kind == "EARN_SNAPSHOT":
            policy.capital["earn"] = cast(wire.Earn, payload)["earn"]
        elif kind == "TRADING_SNAPSHOT":
            trading = cast(wire.Trading, payload)
            policy.capital.update(usdt=trading["equity"], avail=trading["available_equity"])
            if "earn" not in policy.capital:
                raise ValueError("Source total is NaN before an earn snapshot exists")
            with localcontext() as context:
                context.prec = 50
                policy.capital["total"] = policy.a("usdt") + policy.a("earn") - policy.p("fixed")
        elif kind == "POSITION_SNAPSHOT":
            positions = cast(wire.Positions, payload)
            policy.positions_response(
                [
                    dict(
                        instId=row["product_id"],
                        mgnMode=row["margin_mode"],
                        pos=row["position_contracts"],
                        **({"last": row["last_price"]} if "last_price" in row else {}),
                        **({"notionalUsd": row["notional_usd"]} if "notional_usd" in row else {}),
                    )
                    for row in positions["rows"]
                ]
            )
        elif kind == "OPEN_ORDER_SNAPSHOT":
            orders = cast(wire.OpenOrders, payload)
            policy.open_orders(
                [
                    dict(
                        ordId=row["order_id"],
                        clOrdId=row["client_order_id"],
                        instId=row["product_id"],
                        state=row["state"],
                        side=row["side"],
                        px=fmt(row["limit_price"]),
                        sz=fmt(row["original_size_contracts"]),
                        accFillSz=fmt(row["cumulative_filled_size_contracts"]),
                        cTime=str(row["created_at"]),
                    )
                    for row in orders["rows"]
                ]
            )
