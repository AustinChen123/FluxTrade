from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from src.core.adapters.okx_resting_order_plan import OkxOwnRestingOrder, OkxSwapInstrument
from src.core.adapters.okx_spider_financial_profile import _checked_envelope, _decimal, _rows, _text


class _OkxReadClient(Protocol):
    def publicGetPublicInstruments(self, params: dict[str, str], /) -> object: ...

    def privateGetTradeOrdersPending(self, params: dict[str, str], /) -> object: ...


class _OkxPositionClient(Protocol):
    def privateGetAccountPositions(self, params: dict[str, str], /) -> object: ...


@dataclass(frozen=True, slots=True)
class OkxRestingSnapshot:
    orders: tuple[OkxOwnRestingOrder, ...]
    unlabelled: int


_NUMBER = re.compile(r"-?[0-9]{1,20}(\.[0-9]{1,20})?")


def _number(value: object, *, nonnegative: bool = False) -> Decimal:
    if type(value) is not str or _NUMBER.fullmatch(value) is None:
        raise ValueError("invalid OKX resting order read")
    return _decimal(value, nonnegative=nonnegative)


def _positive_decimal(value: object) -> Decimal:
    parsed = _number(value)
    if parsed <= 0:
        raise ValueError("invalid OKX resting order read")
    return parsed


def read_okx_swap_instrument(client: _OkxReadClient, inst_id: str) -> OkxSwapInstrument:
    envelope = _checked_envelope(client.publicGetPublicInstruments({"instType": "SWAP", "instId": inst_id}))
    if envelope.get("code") != "0":
        raise ValueError("invalid OKX resting order read")
    rows = _rows(envelope)
    if len(rows) != 1:
        raise ValueError("invalid OKX resting order read")
    row = rows[0]
    if (
        _text(row.get("instId")) != inst_id
        or row.get("instType") != "SWAP"
        or row.get("ctType") != "linear"
        or row.get("ctMult") != "1"
        or row.get("state") != "live"
    ):
        raise ValueError("invalid OKX resting order read")
    return OkxSwapInstrument(
        inst_id,
        _positive_decimal(row.get("ctVal")),
        _positive_decimal(row.get("lotSz")),
        _positive_decimal(row.get("minSz")),
        _positive_decimal(row.get("tickSz")),
    )


def read_okx_own_resting_orders(client: _OkxReadClient, inst_id: str) -> OkxRestingSnapshot:
    orders: list[OkxOwnRestingOrder] = []
    seen: set[str] = set()
    unlabelled = 0
    cursor: str | None = None
    for page_number in range(10):
        params = {"instType": "SWAP", "instId": inst_id, "limit": "100"}
        if cursor is not None:
            params["after"] = cursor
        envelope = _checked_envelope(client.privateGetTradeOrdersPending(params))
        if envelope.get("code") != "0":
            raise ValueError("invalid OKX resting order read")
        rows = _rows(envelope)
        for row in rows:
            if _text(row.get("instId")) != inst_id:
                raise ValueError("invalid OKX resting order read")
            if row.get("state") not in ("live", "partially_filled"):
                raise ValueError("invalid OKX resting order read")
            side = row.get("side")
            if type(side) is not str or side not in ("buy", "sell"):
                raise ValueError("invalid OKX resting order read")
            order_id = _text(row.get("ordId"))
            if order_id in seen:
                raise ValueError("invalid OKX resting order read")
            seen.add(order_id)
            size = _positive_decimal(row.get("sz"))
            filled = _number(row.get("accFillSz"), nonnegative=True)
            remaining = size - filled
            if remaining <= 0:
                raise ValueError("invalid OKX resting order read")
            price = _positive_decimal(row.get("px"))
            client_id = row.get("clOrdId")
            if type(client_id) is not str:
                raise ValueError("invalid OKX resting order read")
            if client_id == "":
                unlabelled += 1
                continue
            if not 1 <= len(client_id) <= 32 or not client_id.isascii() or not client_id.isalnum():
                raise ValueError("invalid OKX resting order read")
            orders.append(OkxOwnRestingOrder(order_id, client_id, side, price, remaining))
        if len(rows) < 100:
            return OkxRestingSnapshot(tuple(orders), unlabelled)
        if page_number == 9:
            raise ValueError("invalid OKX resting order read")
        cursor = _text(rows[-1].get("ordId"))
    raise ValueError("invalid OKX resting order read")


def read_okx_net_position(client: _OkxPositionClient, inst_id: str) -> Decimal:
    envelope = _checked_envelope(client.privateGetAccountPositions({"instType": "SWAP", "instId": inst_id}))
    if envelope.get("code") != "0":
        raise ValueError("invalid OKX resting order read")
    rows = _rows(envelope)
    if not rows:
        return Decimal("0")
    if len(rows) != 1:
        raise ValueError("invalid OKX resting order read")
    row = rows[0]
    if (
        _text(row.get("instId")) != inst_id
        or row.get("instType") != "SWAP"
        or row.get("mgnMode") != "cross"
        or row.get("posSide") != "net"
    ):
        raise ValueError("invalid OKX resting order read")
    return _number(row.get("pos"))
