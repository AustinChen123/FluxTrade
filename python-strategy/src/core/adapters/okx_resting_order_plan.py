from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, Inexact, localcontext
from fractions import Fraction
from hashlib import sha256
from math import ceil, floor
from types import MappingProxyType
from typing import Mapping

from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.resting_order_strategy import DesiredOrder
from src.core.adapters.okx_spider_transport import _decode_batch_request


@dataclass(frozen=True, slots=True)
class OkxSwapInstrument:
    inst_id: str
    ct_val: Decimal
    lot_sz: Decimal
    min_sz: Decimal
    tick_sz: Decimal

    def __post_init__(self) -> None:
        if type(self.inst_id) is not str or not self.inst_id:
            raise ValueError("inst_id must be non-empty str")
        for name in ("ct_val", "lot_sz", "min_sz", "tick_sz"):
            _positive_decimal(name, getattr(self, name))


@dataclass(frozen=True, slots=True)
class OkxOwnRestingOrder:
    ord_id: str
    cl_ord_id: str
    side: str
    px: Decimal
    remaining_sz: Decimal

    def __post_init__(self) -> None:
        if type(self.ord_id) is not str or not self.ord_id:
            raise ValueError("ord_id must be non-empty str")
        if type(self.cl_ord_id) is not str or not 1 <= len(self.cl_ord_id) <= 32 or not self.cl_ord_id.isalnum():
            raise ValueError("cl_ord_id must be 1..32 alphanumeric characters")
        if type(self.side) is not str or self.side not in ("buy", "sell"):
            raise ValueError("invalid side")
        _positive_decimal("px", self.px)
        _positive_decimal("remaining_sz", self.remaining_sz)


@dataclass(frozen=True, slots=True)
class OkxPlanScope:
    environment: str
    execution_scope_id: str
    strategy_id: str
    executing_account_ref: str

    def __post_init__(self) -> None:
        for name in ("environment", "execution_scope_id", "strategy_id", "executing_account_ref"):
            value = getattr(self, name)
            if type(value) is not str or not value:
                raise ValueError(f"{name} must be non-empty str")


@dataclass(frozen=True, slots=True)
class IssuedOrder:
    action_id: str
    binding_sha256: str
    local_order_id: str

    def __post_init__(self) -> None:
        if any(type(value) is not str for value in (self.action_id, self.binding_sha256, self.local_order_id)):
            raise ValueError("issued identity fields must be str")
        if (
            len(self.action_id) != 64
            or any(c not in "0123456789abcdef" for c in self.action_id)
            or not self.local_order_id
            or len(self.binding_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.binding_sha256)
        ):
            raise ValueError("invalid issued identity")


@dataclass(frozen=True, slots=True)
class OkxRestingPlan:
    cancel_payloads: tuple[bytes, ...]
    place_payloads: tuple[bytes, ...]
    skipped: tuple[DesiredOrder, ...]
    foreign: tuple[OkxOwnRestingOrder, ...]
    new_issued: Mapping[str, IssuedOrder]
    next_seq: int


def _positive_decimal(name: str, value: Decimal) -> None:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be finite positive Decimal")


def _fmt(value: Decimal) -> str:
    result = format(value, "f")
    return result.rstrip("0").rstrip(".") if "." in result else result


def _batch(scope: OkxPlanScope, operation: str, members: list[dict[str, object]]) -> bytes:
    body: dict[str, object] = {
        "schema_version": "SPIDER_TRANSPORT_BATCH_V1",
        "scope": {"environment": scope.environment, "execution_scope_id": scope.execution_scope_id, "strategy_id": scope.strategy_id},
        "executing_account_ref": scope.executing_account_ref,
        "transmission_group_id": sha256(canonical_bytes(members)).hexdigest(),
        "operation": operation,
        "members": members,
    }
    body["payload_sha256"] = sha256(canonical_bytes(body)).hexdigest()
    return canonical_bytes(body)


def _grid_value(value: Fraction, step: Decimal, round_up: bool) -> Decimal:
    exact = value / Fraction(step)
    quotient = Decimal(ceil(exact) if round_up else floor(exact))
    if quotient and quotient.adjusted() >= 30:
        raise ValueError("value too large for instrument grid")
    with localcontext() as context:
        context.traps[Inexact] = True
        try:
            return quotient * step
        except Inexact as error:
            raise ValueError("value cannot be represented on instrument grid") from error


def plan_okx_resting_orders(
    *, desired: tuple[DesiredOrder, ...], resting: tuple[OkxOwnRestingOrder, ...],
    instrument: OkxSwapInstrument, scope: OkxPlanScope, run_id: str, next_seq: int,
    issued: Mapping[str, IssuedOrder],
) -> OkxRestingPlan:
    if type(desired) is not tuple or any(type(row) is not DesiredOrder for row in desired):
        raise ValueError("desired must be tuple of DesiredOrder")
    if type(resting) is not tuple or any(type(row) is not OkxOwnRestingOrder for row in resting):
        raise ValueError("resting must be tuple of OkxOwnRestingOrder")
    if type(instrument) is not OkxSwapInstrument or type(scope) is not OkxPlanScope:
        raise ValueError("invalid instrument or scope")
    if type(run_id) is not str or not 1 <= len(run_id) <= 40 or any(not (c.isascii() and (c.isalnum() or c in "_-")) for c in run_id):
        raise ValueError("invalid run_id")
    if type(next_seq) is not int or next_seq < 0:
        raise ValueError("next_seq must be non-negative int")
    if not isinstance(issued, Mapping) or any(type(k) is not str or type(v) is not IssuedOrder for k, v in issued.items()):
        raise ValueError("invalid issued mapping")
    if len({row.ord_id for row in resting}) != len(resting):
        raise ValueError("duplicate resting ord_id")
    if len({row.cl_ord_id for row in resting}) != len(resting):
        raise ValueError("duplicate resting cl_ord_id")

    rounded: list[tuple[DesiredOrder, str, Decimal, Decimal]] = []
    skipped: list[DesiredOrder] = []
    with localcontext() as context:
        context.prec = 50
        for row in desired:
            price = Fraction(Decimal(repr(row.price)))
            quantity = Fraction(Decimal(repr(row.quantity)))
            px = _grid_value(price, instrument.tick_sz, row.side == "sell")
            size = _grid_value(quantity / Fraction(instrument.ct_val), instrument.lot_sz, False)
            if px <= 0 or size < instrument.min_sz:
                skipped.append(row)
            else:
                rounded.append((row, row.side, px, size))

    foreign = tuple(row for row in resting if row.cl_ord_id not in issued)
    ours = [row for row in resting if row.cl_ord_id in issued]
    matched: set[int] = set()
    places: list[tuple[str, Decimal, Decimal]] = []
    for _, side, px, size in rounded:
        found = next((i for i, row in enumerate(ours) if i not in matched and row.side == side and row.px == px and row.remaining_sz == size), None)
        if found is None:
            found = next((i for i, row in enumerate(ours) if i not in matched and row.side == side and row.px == px and row.remaining_sz < size), None)
        if found is None:
            places.append((side, px, size))
        else:
            matched.add(found)

    seq = next_seq
    cancels: list[dict[str, object]] = []
    cancelled_local_ids: set[str] = set()
    cancelled_action_ids: set[str] = set()
    for i, row in enumerate(ours):
        if i in matched:
            continue
        prior = issued[row.cl_ord_id]
        cancel_identity = (prior.local_order_id, prior.action_id)
        if cancel_identity[0] in cancelled_local_ids or cancel_identity[1] in cancelled_action_ids:
            raise ValueError("duplicate issued cancel identity")
        cancelled_local_ids.add(cancel_identity[0])
        cancelled_action_ids.add(cancel_identity[1])
        cancel_id = sha256(b"FLUXTRADE_RESTING_OKX_CANCEL_V1\0" + canonical_bytes({"run_id": run_id, "seq": seq, "target": row.cl_ord_id})).hexdigest()
        cancels.append({"cancel_action_id": cancel_id, "product_id": instrument.inst_id, "target_place_action_id": prior.action_id, "target_local_order_id": prior.local_order_id, "target_binding_sha256": prior.binding_sha256, "target_wire_clOrdId": row.cl_ord_id, "target_ordId": row.ord_id, "request_payload": {"instId": instrument.inst_id, "ordId": row.ord_id}})
        seq += 1
    place_members: list[dict[str, object]] = []
    new_issued: dict[str, IssuedOrder] = {}
    scope_fields = {"environment": scope.environment, "execution_scope_id": scope.execution_scope_id, "strategy_id": scope.strategy_id}
    for side, px, size in places:
        action_id = sha256(b"FLUXTRADE_RESTING_OKX_ACTION_V1\0" + canonical_bytes({"run_id": run_id, "seq": seq, **scope_fields})).hexdigest()
        binding = {"schema_version": "FLUXTRADE_RESTING_OKX_BINDING_V1", **scope_fields, "action_id": action_id, "product_id": instrument.inst_id}
        binding_hash = sha256(canonical_bytes(binding)).hexdigest()
        wire = "ra" + binding_hash[:30]
        local = f"{run_id}:{seq}"
        issued_order = IssuedOrder(action_id, binding_hash, local)
        new_issued[wire] = issued_order
        place_members.append({"action_id": action_id, "product_id": instrument.inst_id, "local_order_id": local, "binding_sha256": binding_hash, "wire_clOrdId": wire, "ordId": None, "request_payload": {"instId": instrument.inst_id, "tdMode": "cross", "side": side, "posSide": "net", "ordType": "post_only", "sz": _fmt(size), "px": _fmt(px), "clOrdId": wire, "reduceOnly": False}})
        seq += 1
    cancel_payloads = tuple(_batch(scope, "CANCEL", cancels[i:i + 20]) for i in range(0, len(cancels), 20))
    place_payloads = tuple(_batch(scope, "PLACE", place_members[i:i + 20]) for i in range(0, len(place_members), 20))
    if any(_decode_batch_request(payload) is None for payload in cancel_payloads + place_payloads):
        raise ValueError("planned batch rejected by transport contract")
    return OkxRestingPlan(cancel_payloads, place_payloads, tuple(skipped), foreign, MappingProxyType(new_issued), seq)
