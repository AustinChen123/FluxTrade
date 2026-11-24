from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, cast

from src.core.adapters.okx_spider_financial_profile import _checked_envelope, _decimal, _rows, _text


class _OkxFillReadClient(Protocol):
    def privateGetTradeFills(self, params: dict[str, str], /) -> object: ...


@dataclass(frozen=True, slots=True)
class OkxOwnFill:
    bill_id: str
    trade_id: str
    ord_id: str
    cl_ord_id: str
    side: str
    px: Decimal
    sz: Decimal
    ts_ms: int
    exec_type: str

    def __post_init__(self) -> None:
        for value in (self.bill_id, self.trade_id, self.ord_id):
            if type(value) is not str or not value.isascii() or not value.isdigit():
                raise ValueError("invalid OKX fill")
        if (
            type(self.cl_ord_id) is not str
            or type(self.side) is not str
            or self.side not in ("buy", "sell")
            or type(self.exec_type) is not str
            or self.exec_type not in ("M", "T")
        ):
            raise ValueError("invalid OKX fill")
        if type(self.px) is not Decimal or not self.px.is_finite() or self.px <= 0:
            raise ValueError("invalid OKX fill")
        if type(self.sz) is not Decimal or not self.sz.is_finite() or self.sz <= 0:
            raise ValueError("invalid OKX fill")
        if type(self.ts_ms) is not int or self.ts_ms <= 0:
            raise ValueError("invalid OKX fill")


@dataclass(frozen=True, slots=True)
class OkxFillRead:
    fills: tuple[OkxOwnFill, ...]
    foreign: int
    cursor: str | None

    def __post_init__(self) -> None:
        if type(self.fills) is not tuple or any(type(fill) is not OkxOwnFill for fill in self.fills):
            raise ValueError("invalid OKX fill read")
        if type(self.foreign) is not int or self.foreign < 0:
            raise ValueError("invalid OKX fill read")
        if self.cursor is not None and (type(self.cursor) is not str or not self.cursor.isascii() or not self.cursor.isdigit()):
            raise ValueError("invalid OKX fill read")


def _digits(value: object) -> str:
    if type(value) is not str or not value.isascii() or not value.isdigit():
        raise ValueError("invalid OKX fill read")
    return value


def _parse_fill(row: dict[str, object], inst_id: str) -> OkxOwnFill:
    if _text(row.get("instId")) != inst_id or ("instType" in row and row.get("instType") != "SWAP"):
        raise ValueError("invalid OKX fill read")
    bill_id = _digits(row.get("billId"))
    trade_id = _digits(row.get("tradeId"))
    ord_id = _digits(row.get("ordId"))
    px = _decimal(row.get("fillPx"))
    sz = _decimal(row.get("fillSz"))
    ts = _digits(row.get("ts"))
    return OkxOwnFill(
        bill_id, trade_id, ord_id, cast(str, row.get("clOrdId")), cast(str, row.get("side")), px, sz, int(ts),
        cast(str, row.get("execType")),
    )


def _read_rows(client: _OkxFillReadClient, params: dict[str, str]) -> list[OkxOwnFill]:
    envelope = _checked_envelope(client.privateGetTradeFills(params))
    if envelope.get("code") != "0":
        raise ValueError("invalid OKX fill read")
    rows = _rows(envelope)
    return [_parse_fill(row, params["instId"]) for row in rows]


def read_okx_new_fills(
    client: _OkxFillReadClient, inst_id: str, cursor: str | None, own_wire_ids: frozenset[str]
) -> OkxFillRead:
    if cursor is not None and (type(cursor) is not str or not cursor or not cursor.isascii() or not cursor.isdigit()):
        raise ValueError("invalid OKX fill cursor")
    params = {"instType": "SWAP", "instId": inst_id, "limit": "100"}
    if cursor is not None:
        params["before"] = cursor
    parsed = _read_rows(client, params)
    if len(parsed) >= 100:
        raise ValueError("fill page full")
    seen: set[int] = set()
    for fill in parsed:
        if int(fill.bill_id) in seen:
            raise ValueError("duplicate OKX fill billId")
        seen.add(int(fill.bill_id))
        if cursor is not None and int(fill.bill_id) <= int(cursor):
            raise ValueError("non-new OKX fill billId")
    parsed.sort(key=lambda fill: int(fill.bill_id))
    own = tuple(fill for fill in parsed if fill.cl_ord_id and fill.cl_ord_id in own_wire_ids)
    foreign = len(parsed) - len(own)
    new_cursor = max(parsed, key=lambda fill: int(fill.bill_id)).bill_id if parsed else cursor
    return OkxFillRead(own, foreign, new_cursor)


def init_okx_fill_cursor(client: _OkxFillReadClient, inst_id: str) -> str | None:
    parsed = _read_rows(client, {"instType": "SWAP", "instId": inst_id, "limit": "1"})
    if len(parsed) > 1:
        raise ValueError("invalid OKX fill cursor page")
    return parsed[0].bill_id if parsed else None
