"""Internal closed-policy composition; no scheduling or financial submission."""

from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal as D, localcontext
from typing import cast

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_policy import Policy, fmt


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
