"""Private payload-to-policy callback translation for synthetic replay."""

from decimal import Decimal as D, localcontext
from typing import cast

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_policy import Policy, fmt


def apply_policy_payload(policy: Policy, delivery: wire.Delivery) -> None:
    kind = delivery["payload_kind"]
    payload = delivery["immutable_payload"]
    if kind == "EXECUTION_FACT":
        fact = cast(wire.ExecutionFact, payload)
        policy.orders(
            dict(
                ordId=fact["order_id"],
                clOrdId=fact["policy_client_order_id"],
                instId=fact["product_id"],
                px="" if fact["limit_price"] is None else fmt(fact["limit_price"]),
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
                    px="" if row["limit_price"] is None else fmt(row["limit_price"]),
                    sz=fmt(row["original_size_contracts"]),
                    accFillSz=fmt(row["cumulative_filled_size_contracts"]),
                    cTime=str(row["created_at"]),
                )
                for row in orders["rows"]
            ]
        )
