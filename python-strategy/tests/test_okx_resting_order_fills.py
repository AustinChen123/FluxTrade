from dataclasses import FrozenInstanceError
from decimal import Decimal
from typing import cast

import pytest

from src.core.adapters.okx_resting_order_fills import (
    OkxFillRead,
    OkxOwnFill,
    init_okx_fill_cursor,
    read_okx_new_fills,
)


INST = "TOY:SWAP"
OWN = frozenset({"wire-own"})


def fill_row(**changes: object) -> dict[str, object]:
    return {
        "instId": INST,
        "instType": "SWAP",
        "billId": "10",
        "tradeId": "20",
        "ordId": "30",
        "fillPx": "12.5",
        "fillSz": "2",
        "ts": "1700000000000",
        "side": "buy",
        "execType": "M",
        "clOrdId": "wire-own",
        **changes,
    }


def envelope(rows: list[dict[str, object]], code: str = "0") -> dict[str, object]:
    return {"code": code, "data": rows}


class FakeClient:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[dict[str, str]] = []

    def privateGetTradeFills(self, params: dict[str, str]) -> object:
        self.calls.append(params)
        return self.response


def read(rows: list[dict[str, object]], cursor: str | None = None) -> tuple[OkxFillRead, FakeClient]:
    client = FakeClient(envelope(rows))
    return read_okx_new_fills(client, INST, cursor, OWN), client


@pytest.mark.parametrize(
    ("cursor", "expected"),
    [
        (None, {"instType": "SWAP", "instId": INST, "limit": "100"}),
        ("8", {"instType": "SWAP", "instId": INST, "limit": "100", "before": "8"}),
    ],
)
def test_read_uses_exact_params(cursor: str | None, expected: dict[str, str]) -> None:
    _, client = read([fill_row(billId="9")], cursor)
    assert client.calls == [expected]


@pytest.mark.parametrize("bill_ids", [("11", "9"), ("9", "11")])
def test_response_order_does_not_change_ascending_bill_order(bill_ids: tuple[str, str]) -> None:
    result, _ = read([fill_row(billId=bill_ids[0]), fill_row(billId=bill_ids[1], tradeId="21")])
    assert [fill.bill_id for fill in result.fills] == ["9", "11"]


def test_own_foreign_and_cursor_cover_all_valid_rows() -> None:
    result, _ = read([fill_row(billId="9", clOrdId="wire-own"), fill_row(billId="10", clOrdId=""), fill_row(billId="11", clOrdId="other")])
    assert [fill.bill_id for fill in result.fills] == ["9"]
    assert result.foreign == 2
    assert result.cursor == "11"


def test_empty_page_preserves_cursor() -> None:
    result, _ = read([], "8")
    assert result.fills == () and result.foreign == 0 and result.cursor == "8"


@pytest.mark.parametrize(
    "changes",
    [
        {"instId": "OTHER:SWAP"},
        {"instType": "FUTURES"},
        {"billId": "12a"},
        {"tradeId": ""},
        {"ordId": "-1"},
        {"fillSz": "0"},
        {"fillPx": "NaN"},
        {"fillPx": "0"},
        {"fillPx": "-1"},
        {"fillSz": "-1"},
        {"side": "BUY"},
        {"execType": "X"},
        {"ts": ""},
        {"clOrdId": None},
    ],
)
def test_invalid_row_field_rejected_with_legal_twin(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        read([fill_row(**changes)])
    accepted, _ = read([fill_row(billId="12", tradeId="22")])
    assert accepted.fills[0].bill_id == "12"


def test_wrong_envelope_code_rejected_with_legal_twin() -> None:
    client = FakeClient(envelope([fill_row()], "1"))
    with pytest.raises(ValueError):
        read_okx_new_fills(client, INST, None, OWN)
    assert read([fill_row()])[0].fills[0].bill_id == "10"


def test_duplicate_bill_id_rejected_with_distinct_legal_twin() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        read([fill_row(), fill_row(tradeId="21")])
    assert len(read([fill_row(), fill_row(billId="11", tradeId="21")])[0].fills) == 2


@pytest.mark.parametrize("bad_id", ["10", "9"])
def test_bill_id_must_be_greater_than_cursor(bad_id: str) -> None:
    with pytest.raises(ValueError, match="non-new"):
        read([fill_row(billId=bad_id)], "10")
    assert read([fill_row(billId="11")], "10")[0].cursor == "11"


@pytest.mark.parametrize("bad_cursor", ["", "-1"])
def test_cursor_requires_nonempty_decimal_digits(bad_cursor: str) -> None:
    client = FakeClient(envelope([fill_row(billId="11")]))
    with pytest.raises(ValueError):
        read_okx_new_fills(client, INST, bad_cursor, OWN)
    assert client.calls == []
    assert read([fill_row(billId="11")], "10")[0].cursor == "11"


@pytest.mark.parametrize("count", [99, 100, 101])
def test_full_page_fails_closed(count: int) -> None:
    rows = [fill_row(billId=str(index + 1), tradeId=str(index + 1)) for index in range(count)]
    if count >= 100:
        with pytest.raises(ValueError, match="fill page full"):
            read(rows)
    else:
        assert len(read(rows)[0].fills) == 99


def test_bill_order_is_numeric_not_lexicographic() -> None:
    result, _ = read([fill_row(billId="10"), fill_row(billId="9", tradeId="21")])
    assert [fill.bill_id for fill in result.fills] == ["9", "10"]


@pytest.mark.parametrize("rows, expected", [([], None), ([fill_row(billId="9")], "9")])
def test_init_cursor_zero_or_one_row(rows: list[dict[str, object]], expected: str | None) -> None:
    client = FakeClient(envelope(rows))
    assert init_okx_fill_cursor(client, INST) == expected
    assert client.calls == [{"instType": "SWAP", "instId": INST, "limit": "1"}]


def test_init_cursor_rejects_more_than_one_row() -> None:
    client = FakeClient(envelope([fill_row(billId="9"), fill_row(billId="10", tradeId="21")]))
    with pytest.raises(ValueError):
        init_okx_fill_cursor(client, INST)
    assert init_okx_fill_cursor(FakeClient(envelope([])), INST) is None


def test_fill_is_frozen_slots_and_validates_constructor_values() -> None:
    fill = OkxOwnFill("9", "10", "11", "", "sell", Decimal("1"), Decimal("2"), 3, "T")
    with pytest.raises(FrozenInstanceError):
        setattr(fill, "bill_id", "10")
    with pytest.raises(ValueError):
        OkxOwnFill("9", "10", "11", "", "buy", Decimal("NaN"), Decimal("2"), 3, "M")
    assert not hasattr(fill, "__dict__")


def test_client_exception_propagates_unchanged() -> None:
    class Broken(FakeClient):
        def privateGetTradeFills(self, params: dict[str, str]) -> object:
            raise RuntimeError("toy")

    with pytest.raises(RuntimeError, match="toy"):
        read_okx_new_fills(Broken(None), INST, None, OWN)


def test_numeric_bill_ids_keep_exact_strings() -> None:
    result, _ = read([fill_row(billId="0009")])
    assert cast(OkxOwnFill, result.fills[0]).bill_id == "0009"


def test_duplicate_bill_id_is_numeric() -> None:
    with pytest.raises(ValueError):
        read([fill_row(billId="9"), fill_row(billId="0009", tradeId="21")])
    result, _ = read([fill_row(billId="9"), fill_row(billId="0010", tradeId="21")])
    assert [fill.bill_id for fill in result.fills] == ["9", "0010"]


def test_every_output_field_maps_from_its_own_row_field() -> None:
    row = fill_row(billId="41", tradeId="52", ordId="63", clOrdId="wire-own", side="sell", fillPx="7.25", fillSz="3", ts="1700000001234", execType="T")
    result, _ = read([row])
    assert result.fills == (
        OkxOwnFill("41", "52", "63", "wire-own", "sell", Decimal("7.25"), Decimal("3"), 1700000001234, "T"),
    )
    fill = result.fills[0]
    assert type(fill.px) is Decimal and type(fill.sz) is Decimal and type(fill.ts_ms) is int
    assert str(fill.px) == "7.25" and str(fill.sz) == "3"


def test_blank_client_id_is_never_own() -> None:
    client = FakeClient(envelope([fill_row(billId="9", clOrdId="")]))
    result = read_okx_new_fills(client, INST, None, frozenset({""}))
    assert result.fills == () and result.foreign == 1 and result.cursor == "9"
    twin = read_okx_new_fills(FakeClient(envelope([fill_row(billId="9")])), INST, None, OWN)
    assert len(twin.fills) == 1 and twin.foreign == 0
