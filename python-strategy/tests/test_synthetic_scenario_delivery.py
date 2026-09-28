from copy import deepcopy
from decimal import Decimal as D, localcontext
from typing import cast

import pytest

from src.core.backtest import synthetic_scenario_codec as w
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition

ACCOUNT: w.Account = {"venue": "okx-scenario", "environment": "test", "account": "A"}
PROFILES = ["SYNTHETIC_BTC_ETH_V1", "SYNTHETIC_GOLDEN_CANCEL_V1", "SYNTHETIC_MIN_CASH_V1"]


def composition(profile="SYNTHETIC_BTC_ETH_V1"):
    return _ReplayComposition(cast(w.Profile, profile), ACCOUNT)


def deliver(replay, kind, payload):
    delivery = cast(w.Delivery, {"payload_kind": kind, "immutable_payload": payload})
    before, original = replay._codec.inspect_state(), deepcopy(delivery)
    result = replay._capture_callback(delivery)
    assert replay._codec.inspect_state() == before
    assert delivery == original
    assert replay._policy.now_ms == 100000
    return result


@pytest.mark.parametrize("profile", PROFILES)
def test_closed_profile_observation_fixtures(profile):
    replay = composition(profile)
    policy = replay._policy
    golden, minimum = "GOLDEN" in profile, "MIN_CASH" in profile
    cash = D(1000) if golden else D(100)
    assert policy.capital == dict(earn=D(0), usdt=cash, avail=cash, total=cash, position=D(0))
    assert policy.parameters["fixed"] == "0"
    assert (policy.now_ms, policy.last_earn_ms, policy.running) == (100000, 100000, golden)
    assert policy.rows == ([dict(name="P_A", active="true", leverage="4", 歩差="0.02",
        單數="2", hold上限="0.8", hold下限="-0.8", hold="0")] if golden else [])
    expected = [("P_A", "10", "1", "1", "1", 1)] if golden else [
        ("BTC-USDT-SWAP", "50000" if minimum else "50001", "0.01", "0.01", "0.1", 1),
        ("ETH-USDT-SWAP", "100" if minimum else "1900", "0.1", "0.01", "0.01", 2)]
    assert policy.markets == {name: dict(price=price, ctVal=ct, lotSz=lot, minSz=lot,
        increment=tick, ratioHL="0.1", state="live", instIdCode=code)
        for name, price, ct, lot, tick, code in expected}
    assert not hasattr(replay, "run")


def execution(product="BTC-USDT-SWAP", state="partially_filled"):
    return dict(order_id="O", owner_client_order_id="C", policy_client_order_id="0000015000",
        product_id=product, state=state, side="buy", limit_price=D(10), fill_price=D(9),
        original_size_contracts=D(10), cumulative_filled_size_contracts=D(4),
        contract_value=D("0.01"), execution_effective_at=501, commit_account_version=1,
        spec_version="v1", rule_data_version="r1")


def test_execution_contract_conversion_and_golden_refill_detached_prefix():
    replay = composition()
    assert deliver(replay, "EXECUTION_FACT", execution()).exception is None
    assert replay._policy.replies["O"] == dict(instId="BTC-USDT-SWAP", side="buy", px="10",
        clOrdId="0000015000", state="partially_filled", sz="0.1", accFillSz="0.04", cTime="501")
    assert replay._policy.markets["BTC-USDT-SWAP"]["price"] == "50001"
    golden = composition("SYNTHETIC_GOLDEN_CANCEL_V1")
    fact = execution("P_A", "filled") | {"cumulative_filled_size_contracts": D(10), "contract_value": D(1)}
    result = deliver(golden, "EXECUTION_FACT", fact)
    assert result.exception is None and len(result.events) == 1
    event = result.events[0]
    assert (event["kind"], event["operation"], event["route"], event["at_ms"]) == ("send", "send", "REST", 100000)
    orders = cast(list[dict[str, str]], event["orders"])
    assert [(o["clOrdId"], o["instId"], o["side"], o["sz"], o["px"]) for o in orders] == [
        ("0000014998", "P_A", "buy", "1", "9"), ("0000025001", "P_A", "sell", "1", "11")]
    assert golden._policy.markets["P_A"]["price"] == D(9)
    assert "O" not in golden._policy.replies
    orders[0]["px"] = "999"
    assert golden._policy.events[0]["orders"][0]["px"] == "9"


@pytest.mark.parametrize("route", ["REST", "WS"])
@pytest.mark.parametrize("operation", ["ORDER", "CANCEL"])
@pytest.mark.parametrize("code", ["0", "1"])
def test_transport_routes_operations_and_failures(route, operation, code):
    replay = composition()
    replay._policy.replies = {"C": dict(state="new", clOrdId="C")}
    before = deepcopy(replay._policy.__dict__)
    ack = dict(route=route, operation=operation, client_order_id="C", order_id="O", code=code,
        message="failure", product_id="BTC-USDT-SWAP", side="sell", limit_price=D(12), size_contracts=D(2))
    assert deliver(replay, "TRANSPORT_ACK", ack).exception is None
    if route == "WS":
        assert replay._policy.__dict__ == before
    elif operation == "ORDER":
        assert replay._policy.replies["O" if code == "0" else "C"]["state"] == ("sent" if code == "0" else "failed")
    else:
        assert replay._policy.replies["O"]["state"] == ("canceling" if code == "0" else "failed")
    if route == "REST" and operation == "ORDER" and code == "0":
        replay._policy.replies.clear()
        assert deliver(replay, "TRANSPORT_ACK", ack).exception is None
        assert replay._policy.replies["O"] == dict(instId="BTC-USDT-SWAP", side="sell", px="12",
            sz="2", state="sent", clOrdId="C")
    if route == "REST" and code == "1":
        assert replay._policy.replies["C" if operation == "ORDER" else "O"]["sMsg"] == "failure"


def test_market_positions_open_orders_and_capital_payloads():
    replay = composition()
    market = dict(product_id="BTC-USDT-SWAP", price=D(20), contract_value=D("0.1"), lot_size=D("0.2"),
        minimum_size=D("0.4"), price_increment=D("0.5"), high_low_ratio=D("0.3"), state="live", instrument_code=7)
    assert deliver(replay, "MARKET_SNAPSHOT", {"markets": [market]}).exception is None
    assert replay._policy.markets == {"BTC-USDT-SWAP": dict(price=D(20), ctVal=D("0.1"), lotSz=D("0.2"),
        minSz=D("0.4"), increment=D("0.5"), ratioHL=D("0.3"), state="live", instIdCode=7)}
    positions = [dict(product_id="BTC-USDT-SWAP", margin_mode="cross", position_contracts=D(-2)),
        dict(product_id="OUTSIDE", margin_mode="cross", position_contracts=D(-1), last_price=D(0), notional_usd=D(50))]
    assert deliver(replay, "POSITION_SNAPSHOT", dict(outcome="SUCCESS", rows=positions)).exception is None
    assert replay._policy.capital["position"] == D(-54)
    positions = [dict(product_id="OUTSIDE", margin_mode="cross", position_contracts=D(-1), last_price=D(7), notional_usd=D(50))]
    assert deliver(replay, "POSITION_SNAPSHOT", dict(outcome="SUCCESS", rows=positions)).exception is None
    assert replay._policy.capital["position"] == D(-7)
    row = dict(order_id="O", client_order_id="C", product_id="BTC-USDT-SWAP", state="partially_filled",
        side="sell", limit_price=D(21), original_size_contracts=D(3), cumulative_filled_size_contracts=D(1), created_at=507)
    assert deliver(replay, "OPEN_ORDER_SNAPSHOT", dict(outcome="SUCCESS", rows=[row])).exception is None
    assert replay._policy.replies == {"O": dict(clOrdId="C", instId="BTC-USDT-SWAP", state="partially_filled",
        side="sell", px="21", sz="0.3", accFillSz="0.1", cTime="507")}
    assert deliver(replay, "EARN_SNAPSHOT", dict(outcome="SUCCESS", earn=D("1.23"))).exception is None
    with localcontext() as context:
        context.prec = 2
        assert deliver(replay, "TRADING_SNAPSHOT", dict(outcome="SUCCESS", equity=D("12345.67"), available_equity=D("123"))).exception is None
    assert replay._policy.capital == dict(earn=D("1.23"), usdt=D("12345.67"), avail=D(123), total=D("12346.90"), position=D(-7))


@pytest.mark.parametrize("kind", ["EARN_SNAPSHOT", "TRADING_SNAPSHOT", "POSITION_SNAPSHOT", "OPEN_ORDER_SNAPSHOT"])
def test_failed_snapshot_preserves_all_policy_cache(kind):
    replay = composition()
    before = deepcopy(replay._policy.__dict__)
    result = deliver(replay, kind, dict(outcome="FAILURE", reason="SYNTHETIC_FAILURE"))
    assert result.events == () and result.exception is None
    assert replay._policy.__dict__ == before


def test_callback_exception_retains_mutation_and_only_new_event_prefix():
    replay = composition("SYNTHETIC_GOLDEN_CANCEL_V1")
    del replay._policy.capital["earn"]
    result = deliver(replay, "TRADING_SNAPSHOT", dict(outcome="SUCCESS", equity=D(123), available_equity=D(12)))
    assert isinstance(result.exception, ValueError)
    assert str(result.exception) == "Source total is NaN before an earn snapshot exists"
    assert replay._policy.capital["usdt"] == D(123) and replay._policy.capital["avail"] == D(12)
    assert replay._policy.capital["total"] == D(1000) and result.events == ()
    replay._policy.emit("check_websocket")
    del replay._policy.capital["total"]
    result = deliver(replay, "OPEN_ORDER_SNAPSHOT", dict(outcome="SUCCESS", rows=[]))
    assert isinstance(result.exception, KeyError)
    assert result.events == ({"at_ms": 100000, "kind": "request_market"},)
    assert replay._policy.last_market_ms == 100000
    assert [e["kind"] for e in replay._policy.events] == ["check_websocket", "request_market"]
