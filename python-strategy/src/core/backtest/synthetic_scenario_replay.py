"""Internal closed-policy composition; no scheduling or financial submission."""

from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal as D, localcontext
from typing import cast

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest import spider_policy_protocol as policy_protocol
from src.core.backtest.spider_policy import Policy, fmt


def _pairing_check(condition: bool) -> None:
    if not condition:
        raise ValueError("POLICY_EMISSION_MISMATCH")


def _pair_financial(item, digest, operation, order, account, previous):
    row = policy_protocol._event_object(item, "event_digest action_kind schedule_sequence expected_group")
    policy_protocol._plan_hash(row["event_digest"])
    policy_protocol._plan_sequence(row["schedule_sequence"])
    policy_protocol._plan_group(row["expected_group"])
    group = cast(wire.Group, row["expected_group"])
    _pairing_check(row["event_digest"] == digest)
    _pairing_check(row["action_kind"] == ("ORDER_INTENT" if operation == "send" else "CANCEL_REQUEST"))
    _pairing_check(policy_protocol._plan_account(group["account_key"]) == policy_protocol._plan_account(account))
    _pairing_check(group["declared_member_count"] == 1 and len(group["members"]) == 1)
    member = group["members"][0]
    stamp = member["stamp"]
    _pairing_check(group["ordering_contract_id"] == stamp["ordering_contract_id"] == "S_order_v1")
    _pairing_check(group["group_effective_at"] == stamp["effective_at"] > previous)
    _pairing_check(stamp["scenario_ordinal"] == (60 if operation == "send" else 40))
    if operation == "send":
        _pairing_check(member["kind"] == "INTENT")
        payload = cast(wire.Intent, member["payload"])
        _pairing_check(payload["client_order_id"] == order["clOrdId"] and payload["product_id"] == order["instId"])
        _pairing_check(payload["side"] == ("LONG" if order["side"] == "buy" else "SHORT"))
        _pairing_check(payload["quantity_contracts"] == D(order["sz"]))
        _pairing_check(payload["order_type"] == ("LIMIT" if order["ordType"] == "limit" else "MARKET"))
        _pairing_check(payload.get("limit_price") == (D(order["px"]) if order["ordType"] == "limit" else None))
    else:
        _pairing_check(member["kind"] == "CANCEL_REQUEST")
        payload = cast(wire.CancelRequest, member["payload"])
        _pairing_check(payload["targets"] == [dict(target_order_id=order["ordId"], reason="EXPLICIT_SCENARIO")])
    return stamp["effective_at"]


def _pair_market(item, digest, account, visible_at):
    row = policy_protocol._event_object(item, "event_digest capture_sequence snapshot_request delivery_projection")
    policy_protocol._plan_hash(row["event_digest"])
    policy_protocol._plan_sequence(row["capture_sequence"])
    policy_protocol._plan_snapshot(row["snapshot_request"])
    policy_protocol._plan_projection(row["delivery_projection"])
    request = cast(wire.SnapshotRequest, row["snapshot_request"])
    projection = cast(wire.Projection, row["delivery_projection"])
    _pairing_check(row["event_digest"] == digest)
    _pairing_check(policy_protocol._plan_account(request["account_key"]) == policy_protocol._plan_account(account))
    _pairing_check(request["snapshot_kind"] == "MARKET" and request["capture_mode"] == "FROZEN_POLL_FIXTURE")
    _pairing_check(request.get("fixture_key") == "MARKET_GOLDEN_V1")
    _pairing_check(projection["reference"] == dict(namespace="SNAPSHOT", fact_id=request["snapshot_id"]))
    _pairing_check(projection["payload_kind"] == "MARKET_SNAPSHOT" and projection.get("transport") is None)
    _pairing_check(request.get("continuation_id") is None and projection.get("continuation_id") is None)
    _pairing_check(request["captured_at"] > visible_at and projection["visible_at"] >= request["captured_at"])


def _validate_emission_plan(account, delivery, events, plan):
    """Validate atomically as evidence; return detached items, never dispatch."""
    try:
        row = policy_protocol._event_object(plan, "delivery_id expected_policy_events financial_items market_requests")
        _pairing_check(row["delivery_id"] == delivery["delivery_id"])
        expected, financial, markets = (
            row[k] for k in ("expected_policy_events", "financial_items", "market_requests")
        )
        _pairing_check(isinstance(expected, list) and isinstance(financial, list) and isinstance(markets, list))
        expected, financial, markets = cast(list, expected), cast(list, financial), cast(list, markets)
        _pairing_check(len(events) == len(expected))
        digests = []
        for event, expectation in zip(events, expected, strict=True):
            entry = policy_protocol._event_object(expectation, "event_digest kind")
            digest = policy_protocol._policy_event_digest(event)
            _pairing_check(entry["kind"] == event["kind"] and entry["event_digest"] == digest)
            digests.append(digest)
        fi, mi, previous = 0, 0, delivery["visible_at"]
        validated = []
        for event, digest in zip(events, digests, strict=True):
            if event["kind"] in ("send", "cancel"):
                for order in event["orders"]:
                    if fi == len(financial):
                        raise ValueError("MISSING_NEXT_EVENT_STAMP")
                    previous = _pair_financial(financial[fi], digest, event["kind"], order, account, previous)
                    validated.append(financial[fi])
                    fi += 1
            elif event["kind"] == "request_market":
                if mi == len(markets):
                    raise ValueError("MISSING_NEXT_EVENT_STAMP")
                _pair_market(markets[mi], digest, account, delivery["visible_at"])
                validated.append(markets[mi])
                mi += 1
        _pairing_check(fi == len(financial))
        _pairing_check(mi == len(markets))
        policy_protocol._emission_plan_bytes(plan)
        return tuple(deepcopy(validated))
    except (UnicodeError, OverflowError) as exc:
        raise ValueError("POLICY_EMISSION_MISMATCH") from exc


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
