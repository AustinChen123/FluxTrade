from decimal import Decimal
from fractions import Fraction
from hashlib import sha256
from math import ceil, floor
from types import MappingProxyType
from typing import cast

import pytest

from src.core.adapters.okx_resting_order_plan import (
    IssuedOrder,
    OkxOwnRestingOrder,
    OkxPlanScope,
    OkxSwapInstrument,
    plan_okx_resting_orders,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.resting_order_strategy import DesiredOrder, Side
from src.core.adapters.okx_spider_transport import _decode_batch_request


INSTRUMENT = OkxSwapInstrument("TOY:SWAP", Decimal("1"), Decimal("1"), Decimal("1"), Decimal("0.5"))
S = OkxPlanScope("demo", "toy-account", "toy-strategy", "toy-ref")


def desired(side: Side = "buy", price: float = 10.3, quantity: float = 2.9) -> DesiredOrder:
    return DesiredOrder(side, price, quantity)


def plan(desired_rows: tuple[DesiredOrder, ...], resting: tuple[OkxOwnRestingOrder, ...] = (), issued: dict[str, IssuedOrder] | None = None, seq: int = 0):
    return plan_okx_resting_orders(desired=desired_rows, resting=resting, instrument=INSTRUMENT, scope=S, run_id="run_1", next_seq=seq, issued=issued or {})


def envelope(payload: bytes) -> dict[str, object]:
    assert _decode_batch_request(payload) is not None
    return cast(dict[str, object], decode_canonical(payload))


def test_rounding_table_minimum_and_float_repr() -> None:
    got = plan((desired("buy", 10.3, 2.9), desired("sell", 10.3, 2.9), desired("buy", 10.3, 0.99), desired("buy", 10.3, 1.0)))
    assert got.skipped == (desired("buy", 10.3, 0.99),)
    assert len(got.place_payloads) == 1
    rows = [cast(dict[str, object], m["request_payload"]) for p in got.place_payloads for m in cast(list[dict[str, object]], envelope(p)["members"])]
    assert rows == [
        {"instId": "TOY:SWAP", "tdMode": "cross", "side": "buy", "posSide": "net", "ordType": "post_only", "sz": "2", "px": "10", "clOrdId": rows[0]["clOrdId"], "reduceOnly": False},
        {"instId": "TOY:SWAP", "tdMode": "cross", "side": "sell", "posSide": "net", "ordType": "post_only", "sz": "2", "px": "10.5", "clOrdId": rows[1]["clOrdId"], "reduceOnly": False},
        {"instId": "TOY:SWAP", "tdMode": "cross", "side": "buy", "posSide": "net", "ordType": "post_only", "sz": "1", "px": "10", "clOrdId": rows[2]["clOrdId"], "reduceOnly": False},
    ]


def test_closed_diff_matrix_duplicate_foreign_and_empty_desired() -> None:
    known = IssuedOrder("a" * 64, "b" * 64, "old:0")
    exact = OkxOwnRestingOrder("ord-1", "wire1", "buy", Decimal("10"), Decimal("2"))
    side = OkxOwnRestingOrder("ord-2", "wire2", "sell", Decimal("10"), Decimal("2"))
    px = OkxOwnRestingOrder("ord-3", "wire3", "buy", Decimal("9.5"), Decimal("2"))
    size = OkxOwnRestingOrder("ord-4", "wire4", "buy", Decimal("10"), Decimal("3"))
    foreign = OkxOwnRestingOrder("ord-5", "outsider", "buy", Decimal("1"), Decimal("1"))
    state = {key: IssuedOrder(f"{n:064x}", known.binding_sha256, f"old:{n}") for n, key in enumerate(("wire1", "wire2", "wire3", "wire4"))}
    got = plan((desired("buy", 10, 2), desired("buy", 10, 2), desired("sell", 11, 1)), (exact, side, px, size, foreign), state)
    assert got.foreign == (foreign,)
    cancel_rows = [m for p in got.cancel_payloads for m in cast(list[dict[str, object]], envelope(p)["members"])]
    assert [m["target_ordId"] for m in cancel_rows] == ["ord-2", "ord-3", "ord-4"]
    assert len(got.place_payloads) == 1
    assert len(cast(list[object], envelope(got.place_payloads[0])["members"])) == 2
    empty = plan((), (exact,), {"wire1": known})
    assert [m["target_ordId"] for p in empty.cancel_payloads for m in cast(list[dict[str, object]], envelope(p)["members"])] == ["ord-1"]


def test_batches_identity_digest_order_and_determinism() -> None:
    rows = tuple(desired("buy", 10, 1) for _ in range(21))
    got = plan(rows, seq=5)
    again = plan(rows, seq=5)
    assert got.cancel_payloads == again.cancel_payloads
    assert got.place_payloads == again.place_payloads
    assert [len(cast(list[object], envelope(p)["members"])) for p in got.place_payloads] == [20, 1]
    assert got.next_seq == 26
    for p in got.place_payloads:
        body = envelope(p)
        digest = body.pop("payload_sha256")
        assert digest == sha256(canonical_bytes(body)).hexdigest()
    known = IssuedOrder("a" * 64, "b" * 64, "old:0")
    old = OkxOwnRestingOrder("real-ord", "wireold", "sell", Decimal("2"), Decimal("1"))
    mixed = plan((desired(),), (old,), {"wireold": known}, 0)
    assert mixed.cancel_payloads and mixed.place_payloads
    assert mixed.next_seq == 2
    cancel = cast(list[dict[str, object]], envelope(mixed.cancel_payloads[0])["members"])[0]
    assert cancel["target_ordId"] == "real-ord"
    assert cancel["request_payload"] == {"instId": "TOY:SWAP", "ordId": "real-ord"}


def test_float_repr_is_used_for_decimal_conversion() -> None:
    instrument = OkxSwapInstrument("TOY:SWAP", Decimal("1"), Decimal("0.00000000000000000001"), Decimal("0.00000000000000000001"), Decimal("1"))
    got = plan_okx_resting_orders(desired=(desired("buy", 10, 0.1 + 0.2),), resting=(), instrument=instrument, scope=S, run_id="repr", next_seq=0, issued={})
    member = cast(list[dict[str, object]], envelope(got.place_payloads[0])["members"])[0]
    assert cast(dict[str, object], member["request_payload"])["sz"] == "0.30000000000000004"


def test_identity_oracle_cancel_then_two_places_and_read_only_issued() -> None:
    prior = IssuedOrder("a" * 64, "c" * 64, "prior-local")
    old = OkxOwnRestingOrder("old-ord", "wireold", "sell", Decimal("8"), Decimal("1"))
    got = plan((desired("buy", 10, 1), desired("sell", 11, 2)), (old,), {"wireold": prior}, 7)
    cancels = [m for p in got.cancel_payloads for m in cast(list[dict[str, object]], envelope(p)["members"])]
    places = [m for p in got.place_payloads for m in cast(list[dict[str, object]], envelope(p)["members"])]
    assert len(cancels) == 1
    assert len(places) == 2
    cancel = cancels[0]
    expected_cancel = sha256(b"FLUXTRADE_RESTING_OKX_CANCEL_V1\0" + canonical_bytes({"run_id": "run_1", "seq": 7, "target": "wireold"})).hexdigest()
    assert cancel == {
        "cancel_action_id": expected_cancel,
        "product_id": "TOY:SWAP",
        "target_place_action_id": "a" * 64,
        "target_local_order_id": "prior-local",
        "target_binding_sha256": "c" * 64,
        "target_wire_clOrdId": "wireold",
        "target_ordId": "old-ord",
        "request_payload": {"instId": "TOY:SWAP", "ordId": "old-ord"},
    }
    expected_issued: dict[str, IssuedOrder] = {}
    for seq, member, side, px, sz in zip((8, 9), places, ("buy", "sell"), ("10", "11"), ("1", "2"), strict=True):
        action = sha256(b"FLUXTRADE_RESTING_OKX_ACTION_V1\0" + canonical_bytes({"run_id": "run_1", "seq": seq, "environment": "demo", "execution_scope_id": "toy-account", "strategy_id": "toy-strategy"})).hexdigest()
        binding = {"schema_version": "FLUXTRADE_RESTING_OKX_BINDING_V1", "environment": "demo", "execution_scope_id": "toy-account", "strategy_id": "toy-strategy", "action_id": action, "product_id": "TOY:SWAP"}
        binding_hash = sha256(canonical_bytes(binding)).hexdigest()
        wire = "ra" + binding_hash[:30]
        local = f"run_1:{seq}"
        expected_issued[wire] = IssuedOrder(action, binding_hash, local)
        assert member == {
            "action_id": action,
            "product_id": "TOY:SWAP",
            "local_order_id": local,
            "binding_sha256": binding_hash,
            "wire_clOrdId": wire,
            "ordId": None,
            "request_payload": {"instId": "TOY:SWAP", "tdMode": "cross", "side": side, "posSide": "net", "ordType": "post_only", "sz": sz, "px": px, "clOrdId": wire, "reduceOnly": False},
        }
        assert cast(dict[str, object], member["request_payload"])["clOrdId"] == wire
    assert got.new_issued == expected_issued
    assert set(got.new_issued) == set(expected_issued)
    assert type(got.new_issued) is MappingProxyType
    with pytest.raises(TypeError):
        cast(dict[str, IssuedOrder], got.new_issued)["other"] = next(iter(expected_issued.values()))
    cancel_group = cast(dict[str, object], envelope(got.cancel_payloads[0]))["transmission_group_id"]
    place_group = cast(dict[str, object], envelope(got.place_payloads[0]))["transmission_group_id"]
    assert cancel_group == sha256(canonical_bytes(cancels)).hexdigest()
    assert place_group == sha256(canonical_bytes(places)).hexdigest()


def test_diff_places_expected_side_price_size_and_duplicate_consumption() -> None:
    known = IssuedOrder("a" * 64, "b" * 64, "old")
    resting = OkxOwnRestingOrder("keep", "known", "buy", Decimal("10"), Decimal("1"))
    got = plan((desired("buy", 10, 1), desired("buy", 10, 1), desired("sell", 11.2, 2.8)), (resting,), {"known": known})
    members = [m for p in got.place_payloads for m in cast(list[dict[str, object]], envelope(p)["members"])]
    payloads = [cast(dict[str, object], m["request_payload"]) for m in members]
    assert [(row["side"], row["px"], row["sz"]) for row in payloads] == [("buy", "10", "1"), ("sell", "11.5", "2")]


def test_rounded_price_at_or_below_zero_is_skipped() -> None:
    instrument = OkxSwapInstrument("TOY:SWAP", Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"))
    below_tick = DesiredOrder("buy", 0.9, 1)
    at_tick = DesiredOrder("buy", 1.0, 1)
    got = plan_okx_resting_orders(desired=(below_tick, at_tick), resting=(), instrument=instrument, scope=S, run_id="edge", next_seq=0, issued={})
    assert got.skipped == (below_tick,)
    members = [m for p in got.place_payloads for m in cast(list[dict[str, object]], envelope(p)["members"])]
    assert [cast(dict[str, object], m["request_payload"])["px"] for m in members] == ["1"]


def test_decimal_division_uses_precision_fifty() -> None:
    instrument = OkxSwapInstrument("TOY:SWAP", Decimal("7.888609052210118054117285652827862296732064351090230047702789306640625e-29"), Decimal("1"), Decimal("1"), Decimal("1"))
    got = plan_okx_resting_orders(desired=(desired("buy", 1, 1.0),), resting=(), instrument=instrument, scope=S, run_id="precision", next_seq=0, issued={})
    member = cast(list[dict[str, object]], envelope(got.place_payloads[0])["members"])[0]
    assert cast(dict[str, object], member["request_payload"])["sz"] == "12676506002282294014967032053"


@pytest.mark.parametrize("bad_order", [
    lambda: OkxOwnRestingOrder("id", "wire", "hold", Decimal("1"), Decimal("1")),
    lambda: OkxOwnRestingOrder("id", "wire", "buy", Decimal("0"), Decimal("1")),
])
def test_resting_order_rejections_have_legal_twins(bad_order) -> None:
    with pytest.raises(ValueError):
        bad_order()
    assert OkxOwnRestingOrder("id", "wire", "buy", Decimal("1"), Decimal("1")).px == Decimal("1")


def test_run_id_maximum_and_issued_value_type_rejections_have_twins() -> None:
    assert plan_okx_resting_orders(desired=(), resting=(), instrument=INSTRUMENT, scope=S, run_id="a" * 40, next_seq=0, issued={}).next_seq == 0
    with pytest.raises(ValueError):
        plan_okx_resting_orders(desired=(), resting=(), instrument=INSTRUMENT, scope=S, run_id="a" * 41, next_seq=0, issued={})
    with pytest.raises(ValueError):
        plan_okx_resting_orders(desired=(), resting=(), instrument=INSTRUMENT, scope=S, run_id="valid", next_seq=0, issued={"wire": cast(IssuedOrder, "wrong")})
    assert plan_okx_resting_orders(desired=(), resting=(), instrument=INSTRUMENT, scope=S, run_id="valid", next_seq=0, issued={"wire": IssuedOrder("a" * 64, "b" * 64, "c")}).next_seq == 0


@pytest.mark.parametrize("factory", [
    lambda: OkxSwapInstrument("x", cast(Decimal, 1.0), Decimal("1"), Decimal("1"), Decimal("1")),
    lambda: OkxSwapInstrument("x", Decimal("1"), Decimal("1"), Decimal("1"), Decimal("0")),
    lambda: DesiredOrder(cast(Side, "BUY"), 1, 1),
    lambda: plan((desired(),), seq=-1),
    lambda: plan((desired(),)) if False else plan_okx_resting_orders(desired=(desired(),), resting=(), instrument=INSTRUMENT, scope=S, run_id="", next_seq=0, issued={}),
])
def test_invalid_inputs_reject_with_legal_twin(factory) -> None:
    with pytest.raises(ValueError):
        factory()
    assert len(plan((desired(),)).place_payloads) == 1


@pytest.mark.parametrize("action_id,binding", [("a", "b" * 64), ("g" * 64, "b" * 64), ("a" * 64, "B" * 64)])
def test_issued_identity_formats_match_transport_and_have_legal_twins(action_id: str, binding: str) -> None:
    with pytest.raises(ValueError, match="invalid issued identity"):
        IssuedOrder(action_id, binding, "local")
    assert IssuedOrder("a" * 64, "b" * 64, "local").action_id == "a" * 64


@pytest.mark.parametrize("kind", ["ord_id", "cl_ord_id", "local_order_id", "action_id"])
def test_cancel_identity_duplicates_reject_and_unique_twin_passes(kind: str) -> None:
    first = OkxOwnRestingOrder("ord-1", "wire1", "sell", Decimal("1"), Decimal("1"))
    second = OkxOwnRestingOrder("ord-2", "wire2", "sell", Decimal("2"), Decimal("1"))
    ids = {
        "wire1": IssuedOrder("a" * 64, "b" * 64, "local-1"),
        "wire2": IssuedOrder("c" * 64, "d" * 64, "local-2"),
    }
    if kind == "ord_id":
        with pytest.raises(ValueError, match="duplicate resting ord_id"):
            plan((), (first, OkxOwnRestingOrder("ord-1", "wire2", "sell", Decimal("2"), Decimal("1"))), ids)
    elif kind == "cl_ord_id":
        with pytest.raises(ValueError, match="duplicate resting cl_ord_id"):
            plan((), (first, OkxOwnRestingOrder("ord-2", "wire1", "sell", Decimal("2"), Decimal("1"))), ids)
    else:
        if kind == "local_order_id":
            ids["wire2"] = IssuedOrder("c" * 64, "d" * 64, "local-1")
        else:
            ids["wire2"] = IssuedOrder("a" * 64, "d" * 64, "local-2")
        with pytest.raises(ValueError, match="duplicate issued cancel identity"):
            plan((), (first, second), ids)
    unique = plan((), (first, second), {"wire1": ids["wire1"], "wire2": IssuedOrder("e" * 64, "f" * 64, "local-3")})
    assert all(_decode_batch_request(payload) is not None for payload in unique.cancel_payloads)


def test_grid_precision_bound_rejects_and_29_digit_on_grid_twin_passes() -> None:
    instrument = OkxSwapInstrument("TOY:SWAP", Decimal("1"), Decimal("11"), Decimal("1"), Decimal("11"))
    with pytest.raises(ValueError, match="value too large for instrument grid"):
        plan_okx_resting_orders(desired=(desired("buy", 1e60, 1e60),), resting=(), instrument=instrument, scope=S, run_id="grid", next_seq=0, issued={})
    legal_value = 1.1e29
    legal = plan_okx_resting_orders(desired=(desired("buy", legal_value, legal_value),), resting=(), instrument=instrument, scope=S, run_id="grid", next_seq=0, issued={})
    request = cast(dict[str, object], cast(list[dict[str, object]], envelope(legal.place_payloads[0])["members"])[0]["request_payload"])
    assert Fraction(Decimal(cast(str, request["px"]))) % 11 == 0
    assert Fraction(Decimal(cast(str, request["sz"]))) % 11 == 0


def test_grid_product_inexact_at_precision_fifty_is_rejected() -> None:
    instrument = OkxSwapInstrument("TOY:SWAP", Decimal("1"), Decimal("1"), Decimal("1"), Decimal("12345678901234567890123456789"))
    with pytest.raises(ValueError, match="value cannot be represented on instrument grid"):
        plan_okx_resting_orders(desired=(desired("buy", 1e58, 1),), resting=(), instrument=instrument, scope=S, run_id="inexact", next_seq=0, issued={})


def test_batch_decoder_guard_enforces_exact_one_mib_limit() -> None:
    initial = plan((desired(),))
    payload_size = len(initial.place_payloads[0])
    exact_scope_length = 1_048_576 - (payload_size - len(S.execution_scope_id))
    exact_scope = OkxPlanScope("demo", "x" * exact_scope_length, "toy-strategy", "toy-ref")
    exact = plan_okx_resting_orders(desired=(desired(),), resting=(), instrument=INSTRUMENT, scope=exact_scope, run_id="run_1", next_seq=0, issued={})
    assert len(exact.place_payloads[0]) == 1_048_576
    assert _decode_batch_request(exact.place_payloads[0]) is not None
    too_large = OkxPlanScope("demo", "x" * (exact_scope_length + 1), "toy-strategy", "toy-ref")
    with pytest.raises(ValueError, match="planned batch rejected by transport contract"):
        plan_okx_resting_orders(desired=(desired(),), resting=(), instrument=INSTRUMENT, scope=too_large, run_id="run_1", next_seq=0, issued={})


def _one(side: Side, price: float, quantity: float, tick: Decimal, ct_val: Decimal, lot: Decimal):
    instrument = OkxSwapInstrument("TOY:SWAP", ct_val, lot, lot, tick)
    return plan_okx_resting_orders(desired=(desired(side, price, quantity),), resting=(), instrument=instrument, scope=S, run_id="exact", next_seq=0, issued={})


def test_rounding_direction_is_exact_beyond_context_precision() -> None:
    tick = Decimal("0." + "9" * 50)
    with pytest.raises(ValueError):
        _one("sell", 1, 1, tick, Decimal("1"), Decimal("1"))
    short_tick = Decimal("0.99")
    member = cast(list[dict[str, object]], envelope(_one("sell", 1, 1, short_tick, Decimal("1"), Decimal("1")).place_payloads[0])["members"])[0]
    expected_px = ceil(Fraction(1) / Fraction(short_tick)) * Fraction(short_tick)
    assert Fraction(Decimal(cast(str, cast(dict[str, object], member["request_payload"])["px"]))) == expected_px
    assert expected_px >= 1
    ct_val = Decimal("1." + "0" * 50 + "1")
    over = _one("buy", 10, 1, Decimal("1"), ct_val, Decimal("1"))
    assert floor(Fraction(1) / Fraction(ct_val)) == 0
    assert over.place_payloads == () and over.skipped == (desired("buy", 10, 1),)
    exact = _one("buy", 10, 1, Decimal("1"), Decimal("1"), Decimal("1"))
    assert len(exact.place_payloads) == 1 and exact.skipped == ()
