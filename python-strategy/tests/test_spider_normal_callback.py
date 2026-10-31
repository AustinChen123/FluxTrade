"""Pure source transition matrix for normal Spider callbacks."""

from decimal import Decimal

import pytest

from src.core.spider_management.normal_callback import (
    classify_source_candidate,
    SourceClockUnavailable,
    build_orders_source_input,
    classify_source_order_transition,
    full_callback_identity,
    source_alias,
    invoke_saved_source_callback,
)
from src.core.backtest.spider_policy import Policy


@pytest.mark.parametrize(
    ("status", "quantity", "proved", "expected", "full"),
    [
        ("NEW", "0", False, "new", False),
        ("SUBMITTED_UNCONFIRMED", "0", False, "sent", False),
        ("SUBMITTED", "0", False, "live", False),
        ("PARTIALLY_FILLED", "0", False, "live", False),
        ("SUBMITTED", "2", False, "partially_filled", False),
        ("PARTIALLY_FILLED", "2", False, "partially_filled", False),
        ("SUBMITTED", "10", True, "filled", True),
        ("PARTIALLY_FILLED", "10", True, "filled", True),
        ("FILLED", "10", True, "filled", True),
        ("closed", "10", True, "filled", True),
        ("CANCELLED", "2", False, "canceled", False),
        ("CANCELLED", "10", False, "canceled", False),
        ("FAILED", "2", False, "failed", False),
        ("failed", "0", False, "failed", False),
        ("LIQUIDATED", "10", False, "economic_terminal", False),
        ("SUBMITTED", "10", False, None, False),
        ("FILLED", "2", True, None, False),
        ("CANCELLED", "11", False, None, False),
        ("submitted", "0", False, None, False),
        ("CLOSED", "10", True, None, False),
        ("UNKNOWN", "0", False, None, False),
    ],
)
def test_source_transition_matrix(status, quantity, proved, expected, full):
    result = classify_source_order_transition(
        status,
        original_quantity=Decimal("10"),
        cumulative_quantity=Decimal(quantity),
        trade_chain_proved=proved,
    )
    assert result.state == expected
    assert result.full_callback is full


@pytest.mark.parametrize("quantity", ["-1", "11"])
def test_source_transition_rejects_out_of_range_quantity(quantity):
    result = classify_source_order_transition(
        "CANCELLED",
        original_quantity=Decimal("10"),
        cumulative_quantity=Decimal(quantity),
        trade_chain_proved=False,
    )
    assert result.state is None
    assert result.reason_code == "SOURCE_STATE_UNAVAILABLE"


def test_source_alias_is_exact_and_callback_key_is_scoped():
    scope = {"environment": "paper", "execution_scope_id": "scope", "strategy_id": "grid"}
    assert source_alias("source-client") == "source-client"
    first = full_callback_identity(scope, "local-order", 1)
    assert first == full_callback_identity(dict(scope), "local-order", 1)
    assert first != full_callback_identity(scope, "another-order", 1)
    assert first != full_callback_identity(scope, "local-order", 2)
    with pytest.raises(ValueError):
        source_alias("")


@pytest.mark.parametrize(
    ("operation", "member", "at_ms", "targets", "expected", "reason"),
    [
        ("PLACE", {"instId": "missing"}, 1000, (), "FILTERED_SOURCE", "SOURCE_PRODUCT_UNAVAILABLE"),
        (
            "PLACE",
            {"instId": "BTC", "side": "hold", "px": "1", "sz": "1", "clOrdId": "c", "instIdCode": 1},
            1000,
            (),
            "FILTERED_SOURCE",
            "SOURCE_TERMS_UNAVAILABLE",
        ),
        (
            "PLACE",
            {"instId": "BTC", "side": "buy", "px": "1", "sz": "1", "clOrdId": "c", "instIdCode": 0},
            1000,
            (),
            "FILTERED_SOURCE",
            "SOURCE_MARKET_UNAVAILABLE",
        ),
        (
            "PLACE",
            {"instId": "BTC", "side": "buy", "px": "1", "sz": "1", "clOrdId": "c", "instIdCode": 1},
            True,
            (),
            "FILTERED_SOURCE",
            "SOURCE_TERMS_UNAVAILABLE",
        ),
        (
            "PLACE",
            {"instId": "BTC", "side": "buy", "px": "1.25", "sz": "2", "clOrdId": "c", "instIdCode": 1},
            1000,
            (),
            "PLANNED",
            None,
        ),
        (
            "CANCEL",
            {"instId": "BTC", "ordId": "source-c"},
            None,
            (),
            "UNRESOLVED_TARGET",
            "SOURCE_CANCEL_TARGET_UNRESOLVED",
        ),
    ],
)
def test_source_candidate_classifier_preserves_member_disposition(
    operation, member, at_ms, targets, expected, reason
):
    result = classify_source_candidate(
        operation,
        member,
        event_at_ms=at_ms,
        admitted_products={"BTC": "BTC-USDT-SWAP"},
        cancel_targets=targets,
    )
    assert result.disposition == expected
    assert result.reason_code == reason
    if operation == "PLACE" and expected == "PLANNED":
        assert result.canonical_product_id == "BTC-USDT-SWAP"
        assert (result.side, result.price, result.quantity, result.source_market_code) == (
            "buy", Decimal("1.25"), Decimal("2"), 1
        )
        assert result.event_at_ms == 1000


@pytest.mark.parametrize(
    ("targets", "expected_disposition", "expected_action", "expected_order"),
    [
        ((), "UNRESOLVED_TARGET", None, None),
        ((("a1", "o1"),), "PLANNED", "a1", "o1"),
        ((("a1", "o1"), ("a2", "o2")), "UNRESOLVED_TARGET", None, None),
    ],
)
def test_cancel_candidate_classifier_requires_one_exact_target(
    targets, expected_disposition, expected_action, expected_order
):
    result = classify_source_candidate(
        "CANCEL",
        {"instId": "BTC", "ordId": "source-c"},
        event_at_ms=None,
        admitted_products={"BTC": "BTC-USDT-SWAP"},
        cancel_targets=targets,
    )
    assert result.disposition == expected_disposition
    assert result.target_action_id == expected_action
    assert result.target_local_order_id == expected_order


def test_orders_input_uses_local_source_alias_and_last_fill_price_slot():
    value = build_orders_source_input(
        source_alias_id="original-source-client",
        source_product_id="BTC-USDT-SWAP",
        side="buy",
        limit_price=Decimal("50"),
        original_quantity=Decimal("100"),
        cumulative_quantity=Decimal("100"),
        state="filled",
        fill_price=Decimal("49"),
        created_at_ms=1000,
    )
    assert value == {
        "type": "ORDERS",
        "order": {
            "ordId": "original-source-client",
            "clOrdId": "original-source-client",
            "instId": "BTC-USDT-SWAP",
            "side": "buy",
            "px": "50",
            "sz": "100",
            "accFillSz": "100",
            "state": "filled",
            "fillPx": "49",
            "cTime": "1000",
        },
    }


def _source_policy(*, minimum: str = "0.001") -> Policy:
    policy = Policy(
        rows=[{
            "name": "BTC-USDT-SWAP", "active": "true", "leverage": "6",
            "歩差": "1", "單數": "3", "hold上限": "1",
            "hold下限": "-1", "hold": "0",
        }],
        markets={"BTC-USDT-SWAP": {
            "price": Decimal("50"), "ctVal": "1", "lotSz": minimum,
            "minSz": minimum, "increment": "0.01", "ratioHL": Decimal("0.75"),
            "state": "live", "instIdCode": 1,
        }},
        now_ms=100000,
    )
    policy.capital = {"total": Decimal("10000")}
    policy.running = True
    policy.replies["0000000001"] = {
        "instId": "BTC-USDT-SWAP", "side": "buy", "px": "50",
        "sz": "100", "clOrdId": "0000000001", "state": "new",
    }
    return policy


def test_saved_policy_callback_uses_trade_price_for_market_and_limit_for_grid():
    policy = _source_policy()
    source_input = build_orders_source_input(
        source_alias_id="0000000001",
        source_product_id="BTC-USDT-SWAP",
        side="buy",
        limit_price=Decimal("50"),
        original_quantity=Decimal("100"),
        cumulative_quantity=Decimal("100"),
        state="filled",
        fill_price=Decimal("49"),
        created_at_ms=123456,
    )
    invoke_saved_source_callback(policy, source_input)
    assert policy.markets["BTC-USDT-SWAP"]["price"] == Decimal("49")
    assert [event["kind"] for event in policy.events] == ["send"]
    sent = policy.events[0]["orders"]
    assert [order["side"] for order in sent] == ["buy", "sell"]
    assert [Decimal(order["sz"]) for order in sent] == [Decimal("355.555"), Decimal("150")]
    assert [Decimal(order["px"]) for order in sent] == [Decimal("6.25"), Decimal("100")]


def test_saved_policy_reset_is_guarded_before_clock_dependent_reset():
    policy = _source_policy(minimum="1000000")
    source_input = build_orders_source_input(
        source_alias_id="0000000001",
        source_product_id="BTC-USDT-SWAP",
        side="buy",
        limit_price=Decimal("50"),
        original_quantity=Decimal("100"),
        cumulative_quantity=Decimal("100"),
        state="filled",
        fill_price=Decimal("49"),
        created_at_ms=123456,
    )
    with pytest.raises(SourceClockUnavailable):
        invoke_saved_source_callback(policy, source_input)
    assert policy.reset_ms == {}
