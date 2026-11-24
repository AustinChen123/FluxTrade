from decimal import Decimal
from collections.abc import Sequence
from typing import cast

import pytest

from src.core.adapters.okx_resting_order_plan import IssuedOrder, OkxPlanScope, plan_okx_resting_orders
from src.core.adapters.okx_resting_order_reads import read_okx_own_resting_orders, read_okx_swap_instrument
from src.core.resting_order_strategy import DesiredOrder


INST = "TOY:SWAP"


def instrument_row(**changes: object) -> dict[str, object]:
    return {"instId": INST, "instType": "SWAP", "ctType": "linear", "ctMult": "1", "state": "live", "ctVal": "0.1", "lotSz": "0.01", "minSz": "0.02", "tickSz": "0.5", **changes}


def order_row(**changes: object) -> dict[str, object]:
    return {"instId": INST, "state": "live", "side": "buy", "ordId": "ord-1", "clOrdId": "wire1", "px": "10.5", "sz": "2", "accFillSz": "0", **changes}


class FakeClient:
    def __init__(self, instruments: object = None, pages: Sequence[object] | None = None) -> None:
        self.instruments = instruments if instruments is not None else {"code": "0", "data": [instrument_row()]}
        self.pages = pages or [{"code": "0", "data": []}]
        self.public_calls: list[dict[str, str]] = []
        self.private_calls: list[dict[str, str]] = []

    def publicGetPublicInstruments(self, params: dict[str, str]) -> object:
        self.public_calls.append(params)
        return self.instruments

    def privateGetTradeOrdersPending(self, params: dict[str, str]) -> object:
        self.private_calls.append(params)
        return self.pages[len(self.private_calls) - 1]


def envelope(rows: list[dict[str, object]], code: str = "0") -> dict[str, object]:
    return {"code": code, "data": rows}


def test_instrument_accepts_exact_spec_and_requests_one_instrument() -> None:
    client = FakeClient()
    got = read_okx_swap_instrument(client, INST)
    assert (got.inst_id, got.ct_val, got.lot_sz, got.min_sz, got.tick_sz) == (INST, Decimal("0.1"), Decimal("0.01"), Decimal("0.02"), Decimal("0.5"))
    assert client.public_calls == [{"instType": "SWAP", "instId": INST}]


@pytest.mark.parametrize("changes", [
    {"instId": "OTHER:SWAP"}, {"instType": "FUTURES"}, {"ctType": "inverse"}, {"ctMult": "10"}, {"state": "suspend"},
    {"ctVal": "0"}, {"lotSz": "abc"}, {"tickSz": None},
])
def test_instrument_rejects_each_contract_field(changes: dict[str, object]) -> None:
    bad = instrument_row(**changes)
    if changes == {"tickSz": None}:
        bad.pop("tickSz")
    client = FakeClient(envelope([bad]))
    with pytest.raises(ValueError):
        read_okx_swap_instrument(client, INST)
    assert client.public_calls == [{"instType": "SWAP", "instId": INST}]
    assert read_okx_swap_instrument(FakeClient(), INST).inst_id == INST


@pytest.mark.parametrize("response", [envelope([]), envelope([instrument_row(), instrument_row()]), envelope([instrument_row()], "1")])
def test_instrument_rejects_count_and_envelope(response: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        read_okx_swap_instrument(FakeClient(response), INST)


def test_orders_parse_partial_fill_and_exact_page_params() -> None:
    first = [order_row(ordId=f"ord-{i}") for i in range(100)]
    second = [order_row(ordId="ord-100", state="partially_filled", accFillSz="0.75")]
    client = FakeClient(pages=[envelope(first), envelope(second)])
    got = read_okx_own_resting_orders(client, INST)
    assert len(got.orders) == 101
    assert got.orders[-1].remaining_sz == Decimal("1.25")
    assert client.private_calls == [
        {"instType": "SWAP", "instId": INST, "limit": "100"},
        {"instType": "SWAP", "instId": INST, "limit": "100", "after": "ord-99"},
    ]


def test_orders_ten_full_pages_stop_before_an_eleventh() -> None:
    pages = [envelope([order_row(ordId=f"{page}-{index}") for index in range(100)]) for page in range(11)]
    client = FakeClient(pages=pages)
    with pytest.raises(ValueError):
        read_okx_own_resting_orders(client, INST)
    assert len(client.private_calls) == 10


@pytest.mark.parametrize("changes", [
    {"sz": "1", "accFillSz": "1"}, {"accFillSz": "-1"}, {"instId": "OTHER:SWAP"}, {"state": "filled"}, {"clOrdId": "x" * 33},
])
def test_orders_reject_invalid_or_fully_filled_rows(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        read_okx_own_resting_orders(FakeClient(pages=[envelope([order_row(**changes)])]), INST)
    assert read_okx_own_resting_orders(FakeClient(pages=[envelope([order_row()])]), INST).orders[0].ord_id == "ord-1"


def test_orders_reject_duplicate_ids_and_count_unlabelled() -> None:
    duplicate_pages = [envelope([order_row(ordId=f"ord-{i}") for i in range(100)]), envelope([order_row(ordId="ord-0")])]
    with pytest.raises(ValueError):
        read_okx_own_resting_orders(FakeClient(pages=duplicate_pages), INST)
    got = read_okx_own_resting_orders(FakeClient(pages=[envelope([order_row(clOrdId="")])]), INST)
    assert got.orders == () and got.unlabelled == 1
    with pytest.raises(ValueError):
        read_okx_own_resting_orders(FakeClient(pages=[envelope([order_row(clOrdId="", side="BUY")])]), INST)
    for bad_px in (None, "garbage", "NaN", "-1", "0"):
        with pytest.raises(ValueError):
            read_okx_own_resting_orders(FakeClient(pages=[envelope([order_row(clOrdId="", px=bad_px)])]), INST)


def test_client_errors_propagate_unchanged() -> None:
    class Broken(FakeClient):
        def privateGetTradeOrdersPending(self, params: dict[str, str]) -> object:
            error = RuntimeError("toy")
            raise error
    with pytest.raises(RuntimeError, match="toy"):
        read_okx_own_resting_orders(Broken(), INST)


def test_snapshot_types_feed_planner_and_keep_equal_own_order() -> None:
    instrument = read_okx_swap_instrument(FakeClient(envelope([instrument_row(ctVal="1")])), INST)
    order = order_row(px="10", sz="2")
    order["clOrdId"] = "ra" + "a" * 30
    snapshot = read_okx_own_resting_orders(FakeClient(pages=[envelope([order])]), INST)
    issued_id = cast(str, order["clOrdId"])
    issued = {issued_id: IssuedOrder("a" * 64, "b" * 64, "local:1")}
    result = plan_okx_resting_orders(
        desired=(DesiredOrder("buy", 10.0, 2.0),), resting=snapshot.orders, instrument=instrument,
        scope=OkxPlanScope("demo", "toy-account", "toy-strategy", "toy-ref"), run_id="run_1", next_seq=0, issued=issued,
    )
    assert not result.cancel_payloads and not result.place_payloads
